import asyncio
import json
import re
import os
import time
import random
import urllib.parse
from datetime import datetime
import duckdb
from loguru import logger
from curl_cffi.requests import AsyncSession
from scrapling import Fetcher

class MuscleEngine:
    CATEGORIES = {"地域": "m:90+t:1", "行业": "m:90+t:2", "概念": "m:90+t:3"}
    UT = "fa5fd1943c7b386f172d6893dbfba10b"
    FALLBACK_SECTORS = ["90.BK0896", "90.BK1036", "90.BK0475", "90.BK1027", "90.BK0800", "90.BK0427", "90.BK0473"]

    def __init__(self):
        # 1. 净化 Worker URL (防止 Secrets 里的 https://https:// 叠加)
        raw_env = os.getenv("CF_WORKER_URLS") or os.getenv("CF_WORKER_URL") or ""
        urls = [u.strip().replace("https://", "").replace("http://", "").rstrip("/") 
                for u in raw_env.split(",") if u.strip()]
        
        if not urls: raise RuntimeError("🚨 [Init] Secrets 里的 CF_WORKER_URLS 为空")
        
        # 核心修复：确保域名只有一份 https://
        self.worker_url = f"https://{urls[0]}"
        logger.warning(f"🌐 [System] 目标 Worker 节点锁定: {self.worker_url}")
            
        self.concurrency = int(os.getenv("CONCURRENCY", 30))
        self.db_path = "data/sector_quant.db"
        self.db_queue = asyncio.Queue() # 💡 异步 DB 写入队列
        self.trust_context = {"cookies": {}, "headers": {}}
        self.stats = {"total_tasks": 0, "failed_tasks": 0, "codes": {}}
        
        os.makedirs("data", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_klines (secid VARCHAR, date DATE, open DOUBLE, close DOUBLE, high DOUBLE, low DOUBLE, volume DOUBLE, amount DOUBLE, PRIMARY KEY(secid, date))")
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_master (secid VARCHAR PRIMARY KEY, last_update TIMESTAMP)")

    async def db_writer_task(self):
        """💡 专用 Writer 协程：保证 DuckDB 写入线程安全且串行"""
        while True:
            batch = await self.db_queue.get()
            if batch is None: break # 收到结束信号
            try:
                self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
            except Exception as e:
                logger.error(f"💾 DB 写入异常: {e}")
            finally:
                self.db_queue.task_done()

    async def build_trust_chain(self):
        logger.info("🔑 [Phase 0] 预热指纹并合成高权 Cookie...")
        try:
            response = await asyncio.to_thread(self._run_scrapling)
            cookies = response.cookies
            self.trust_context["cookies"] = {c['name']: c['value'] for c in cookies} if isinstance(cookies, list) else (cookies.get_dict() if hasattr(cookies, 'get_dict') else cookies)
            
            if not self.trust_context["cookies"]:
                self.trust_context["cookies"] = {"qgqp_b_id": "".join(random.choices("0123456789abcdef", k=32))}

            self.trust_context["headers"] = {
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
                "Connection": "keep-alive",
            }
        except Exception as e:
            logger.error(f"⚠️ 信任链构建异常: {e}")

    def _run_scrapling(self):
        fetcher = Fetcher()
        return fetcher.get("https://quote.eastmoney.com/bk/90.BK1063.html")

    async def _safe_request(self, session, url: str, label: str, secid: str) -> dict:
        ts = int(time.time() * 1000)
        full_url = f"{url}&cb=jQuery3510_{ts}&_={ts}"
        
        headers = self.trust_context["headers"].copy()
        headers["Referer"] = f"https://quote.eastmoney.com/bk/{secid}.html"
        routed_url = f"{self.worker_url}?url={urllib.parse.quote(full_url, safe='')}"

        # 🚀 调试：第一个请求打印生成的 URL
        if self.stats["total_tasks"] == 1:
            logger.warning(f"🌐 [Debug] 最终路由 URL 示例: {routed_url[:150]}...")

        for attempt in range(2):
            try:
                resp = await session.get(routed_url, headers=headers, cookies=self.trust_context["cookies"], timeout=20)
                if resp.status_code == 200:
                    text = resp.text.strip()
                    match = re.search(r'\((.*)\)', text, re.DOTALL)
                    if match:
                        data = json.loads(match.group(1))
                        if data and data.get("rc") == 0: return data
                
                # 记录失败状态码
                self.stats["codes"][str(resp.status_code)] = self.stats["codes"].get(str(resp.status_code), 0) + 1
            except Exception as e:
                if attempt == 1: logger.debug(f"🕒 {label} 物理失败: {str(e)[:50]}")
        
        self.stats["failed_tasks"] += 1 # 💡 纠偏统计
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        logger.info("📡 [Phase 1] 扫描名录...")
        all_codes = set()
        async with AsyncSession(impersonate="chrome124", http_version=2) as session:
            for cat_name, fs_raw in self.CATEGORIES.items():
                cat_count = 0
                for pn in range(1, 3):
                    self.stats["total_tasks"] += 1
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={fs_raw}&fields=f12&ut={self.UT}")
                    data = await self._safe_request(session, url, f"LIST_{cat_name}_P{pn}", "90.BK0447")
                    if data and data.get("data", {}).get("diff"):
                        for x in data["data"]["diff"]:
                            all_codes.add(f"90.{x['f12']}")
                            cat_count += 1
                    else: break
                logger.info(f"   ∟ [{cat_name}] 发现 {cat_count} 个")
        
        if not all_codes:
            logger.warning("⚠️ Phase 1 未通过，启动压测保底...")
            return self.FALLBACK_SECTORS
        return list(all_codes)

    async def sync_all_klines(self, sector_list: list):
        # 启动后台 Writer
        writer = asyncio.create_task(self.db_writer_task())
        
        logger.warning(f"🔥 [Nitro模式] 执行全量压测 | 板块数: {len(sector_list)}")
        self.conn.execute("DELETE FROM sector_klines")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        progress = {"done": 0, "total": len(sector_list)}
        start_time = time.time()
        
        async with AsyncSession(impersonate="chrome124", max_clients=self.concurrency, http_version=2) as session:
            tasks = [self._fetch_and_enqueue(session, sid, semaphore, progress, i*0.02) for i, sid in enumerate(sector_list)]
            await asyncio.gather(*tasks)

        # 优雅关闭 Writer
        await self.db_queue.put(None)
        await writer

        end_time = time.time()
        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        self.conn.execute(f"COPY sector_klines TO '{os.getenv('DATA_PATH')}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        
        cost = max(end_time - start_time, 0.001)
        logger.success(f"📊 同步完毕 | 总行数: {final_cnt} | 耗时: {cost:.1f}s | TPS: {final_cnt/cost:.0f} 行/秒")

    async def _fetch_and_enqueue(self, session, sid, sem, progress, delay):
        await asyncio.sleep(delay)
        url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={sid}"
               f"&ut={self.UT}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
               f"&klt=101&fqt=1&end=20500101&beg=19900101&lmt=100000")
        
        async with sem:
            self.stats["total_tasks"] += 1
            data = await self._safe_request(session, url, f"K_{sid}", sid)
            progress["done"] += 1
            if progress["done"] % 50 == 0 or progress["done"] == progress["total"]:
                logger.info(f"⏳ 进度: {progress['done']}/{progress['total']}...")
            
            if data and data.get("data", {}).get("klines"):
                batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                         float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                        for k in data["data"]["klines"]]
                await self.db_queue.put(batch) # 💡 塞入队列，不阻塞抓取
