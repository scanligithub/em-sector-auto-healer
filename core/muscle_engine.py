import asyncio
import json
import re
import os
import time
import random  # 💡 补齐缺失的导入
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
        raw_env = os.getenv("CF_WORKER_URLS") or os.getenv("CF_WORKER_URL") or ""
        urls = [u.strip() for u in raw_env.split(",") if u.strip()]
        if not urls: raise RuntimeError("🚨 未检测到 CF_WORKER_URLS")
        
        self.worker_url = f"https://{urls[0]}" if not urls[0].startswith("http") else urls[0]
        self.concurrency = int(os.getenv("CONCURRENCY", 30))
        self.db_path = "data/sector_quant.db"
        self.impersonate = "chrome124"
        self.trust_context = {"cookies": {}, "headers": {}}
        self.stats = {"total_tasks": 0, "failed_tasks": 0, "codes": {}}
        
        os.makedirs("data", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_klines (secid VARCHAR, date DATE, open DOUBLE, close DOUBLE, high DOUBLE, low DOUBLE, volume DOUBLE, amount DOUBLE, PRIMARY KEY(secid, date))")
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_master (secid VARCHAR PRIMARY KEY, last_update TIMESTAMP)")

    async def build_trust_chain(self):
        logger.info(f"🔑 [Phase 0] 预热指纹并合成高权 Cookie...")
        try:
            response = await asyncio.to_thread(self._run_scrapling)
            cookies = response.cookies
            self.trust_context["cookies"] = {c['name']: c['value'] for c in cookies} if isinstance(cookies, list) else (cookies.get_dict() if hasattr(cookies, 'get_dict') else cookies)
            
            # 💡 强制合成保底指纹 (修复 random 未定义问题)
            if not self.trust_context["cookies"] or "qgqp_b_id" not in self.trust_context["cookies"]:
                self.trust_context["cookies"].update({
                    "qgqp_b_id": "".join(random.choices("0123456789abcdef", k=32)),
                    "st_pvi": "".join(random.choices("0123456789", k=15)),
                    "st_psi": f"{datetime.now().strftime('%Y%m%d%H%M%S')}-0-0"
                })

            self.trust_context["headers"] = {
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Connection": "keep-alive",
            }
            logger.success(f"✅ 信任链就绪 | Cookie: {len(self.trust_context['cookies'])}")
        except Exception as e:
            logger.error(f"⚠️ 信任链异常: {e}")

    def _run_scrapling(self):
        fetcher = Fetcher()
        return fetcher.get("https://quote.eastmoney.com/bk/90.BK1063.html")

    def _generate_jquery_cb(self):
        ts = int(time.time() * 1000)
        return f"jQuery3510_{ts}", ts

    async def _safe_request(self, session, url: str, label: str, secid: str) -> dict:
        cb_name, ts = self._generate_jquery_cb()
        full_url = f"{url}&cb={cb_name}&_={ts}"
        
        headers = self.trust_context["headers"].copy()
        headers["Referer"] = f"https://quote.eastmoney.com/bk/{secid}.html"
        # 💡 修正编码：仅对东财 API URL 进行一次整体转义
        routed_url = f"{self.worker_url}?url={urllib.parse.quote(full_url, safe='')}"

        for attempt in range(2):
            try:
                resp = await session.get(routed_url, headers=headers, cookies=self.trust_context["cookies"], timeout=15)
                text = resp.text.strip()
                
                if resp.status_code == 200:
                    match = re.search(r'jQuery\d+_\d+\((.*)\)', text, re.DOTALL)
                    if match:
                        data = json.loads(match.group(1))
                        if data and data.get("rc") == 0: return data
                
                # 💡 核心观察：如果没拿到数据，打印出 Worker 到底吐出了什么
                logger.debug(f"🔍 {label} 异常(HTTP {resp.status_code}): {text[:150]}")
                self.stats["codes"][str(resp.status_code)] = self.stats["codes"].get(str(resp.status_code), 0) + 1
            except Exception as e:
                logger.debug(f"🕒 {label} 物理失败: {str(e)[:50]}")
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        logger.info("📡 [Phase 1] 扫描名录...")
        all_codes = set()
        async with AsyncSession(impersonate=self.impersonate, http_version=2) as session:
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
                        if len(data["data"]["diff"]) < 250: break
                    else: break
                logger.info(f"   ∟ [{cat_name}] 发现 {cat_count} 个")
        
        if not all_codes:
            logger.warning("⚠️ Phase 1 扫描未通过，启动保底名录...")
            return self.FALLBACK_SECTORS
        return list(all_codes)

    async def sync_all_klines(self, sector_list: list):
        logger.warning(f"🔥 [Nitro模式] 执行全量压测 | 板块数: {len(sector_list)}")
        self.conn.execute("DELETE FROM sector_klines")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        progress = {"done": 0, "total": len(sector_list)}
        start = time.time()
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency, http_version=2) as session:
            tasks = []
            for i, sid in enumerate(sector_list):
                # 💡 每一个请求增加极小的抖动（0.05秒），防止同一毫秒冲击 Worker
                tasks.append(self._fetch_and_save(session, sid, semaphore, progress, i*0.05))
            await asyncio.gather(*tasks)

        end = time.time()
        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        self.conn.execute(f"COPY sector_klines TO '{os.getenv('DATA_PATH')}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        
        cost = max(end - start, 0.001)
        logger.success(f"📊 压测完毕 | 总行数: {final_cnt} | 耗时: {cost:.1f}s | 速度: {final_cnt/cost:.0f} 行/秒")

    async def _fetch_and_save(self, session, sid, sem, progress, delay):
        await asyncio.sleep(delay) # 🚀 均匀发车
        url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={sid}"
               f"&ut={self.UT}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
               f"&klt=101&fqt=1&end=20500101&beg=19900101&lmt=100000")
        
        async with sem:
            data = await self._safe_request(session, url, f"K_{sid}", sid)
            progress["done"] += 1
            if progress["done"] % 50 == 0 or progress["done"] == progress["total"]:
                logger.info(f"⏳ 进度: {progress['done']}/{progress['total']}...")
            
            if data and data.get("data", {}).get("klines"):
                batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) for k in data["data"]["klines"]]
                if batch: self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
