import asyncio
import json
import re
import os
import time
import random
import urllib.parse
from datetime import datetime, timedelta
import duckdb
from loguru import logger
from curl_cffi.requests import AsyncSession
from scrapling import Fetcher

class MuscleEngine:
    CATEGORIES = {"地域": "m:90%2Bt:1", "行业": "m:90%2Bt:2", "概念": "m:90%2Bt:3"}
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        # 1. 兼容单 Worker 配置
        raw_env = os.getenv("CF_WORKER_URLS") or os.getenv("CF_WORKER_URL") or ""
        self.worker_pool = [u.strip() for u in raw_env.split(",") if u.strip()]
        if not self.worker_pool: raise RuntimeError("🚨 未配置 CF_WORKER_URL")
            
        self.concurrency = int(os.getenv("CONCURRENCY", 5))
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
        """Phase 0: 隔离线程建立信任态"""
        logger.info(f"🔑 [Phase 0] 启动信任链构建...")
        try:
            response = await asyncio.to_thread(self._run_scrapling)
            cookies = response.cookies
            self.trust_context["cookies"] = {c['name']: c['value'] for c in cookies} if isinstance(cookies, list) else cookies
            self.trust_context["headers"] = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/",
                "Accept": "*/*"
            }
            logger.success(f"✅ 信任链就绪 | 持有 Cookie: {len(self.trust_context['cookies'])}")
        except Exception as e:
            logger.error(f"⚠️ 信任链构建异常: {e}")

    def _run_scrapling(self):
        fetcher = Fetcher()
        return fetcher.get("https://quote.eastmoney.com/center/hsbk.html")

    def _route_url(self, target_url: str, use_cache: bool = False) -> str:
        # 单 Worker 模式下直接使用，不需要 random.choice
        worker_base = self.worker_pool[0]
        if not worker_base.startswith("http"): worker_base = f"https://{worker_base}"
        suffix = f"&_ts={int(time.time()/30)}" if use_cache else f"&_cb={time.time_ns()}"
        return f"{worker_base}?url={urllib.parse.quote(target_url + suffix, safe='')}"

    async def _safe_request(self, session, url: str, label: str, cache: bool = False) -> dict:
        """针对单 Worker 优化的退避重试请求器"""
        for attempt in range(3):
            routed = self._route_url(url, use_cache=cache)
            try:
                # 指数退避：第一次重试等 2s，第二次等 6s
                if attempt > 0: 
                    wait_time = 2 * (attempt ** 2) + random.uniform(0.5, 1.5)
                    await asyncio.sleep(wait_time)
                
                resp = await session.get(routed, headers=self.trust_context["headers"], 
                                         cookies=self.trust_context["cookies"], timeout=45)
                
                if resp.status_code == 200:
                    text = resp.text.strip()
                    if "(" in text and ")" in text and text.startswith("jQuery"):
                        text = re.search(r'\((.*)\)', text, re.DOTALL).group(1)
                    
                    data = json.loads(text)
                    if data and data.get("rc") == 0: return data
                
                # 记录状态码
                code = str(resp.status_code)
                self.stats["codes"][code] = self.stats["codes"].get(code, 0) + 1
                
            except Exception as e:
                logger.debug(f"🕒 {label} 尝试 {attempt+1} 异常: {str(e)[:50]}")
        
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        existing = self.conn.execute("SELECT secid FROM sector_master").fetchall()
        if existing and not force_reconcile:
            logger.success(f"✅ [Phase 1] 缓存命中使用 {len(existing)} 个板块")
            return [r[0] for r in existing]

        logger.info("📡 [Phase 1] 正在同步名录...")
        all_codes = set()
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs_code in self.CATEGORIES.items():
                cat_count = 0
                for pn in range(1, 10):
                    self.stats["total_tasks"] += 1
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={fs_code}&fields=f12&ut={self.UT}")
                    data = await self._safe_request(session, url, f"LIST_{cat_name}_P{pn}")
                    if data and data.get("data", {}).get("diff"):
                        items = data["data"]["diff"]
                        for x in items:
                            if x.get("f12"):
                                all_codes.add(f"90.{x['f12']}")
                                cat_count += 1
                        if len(items) < 250: break
                    else:
                        self.stats["failed_tasks"] += 1
                        break
                logger.info(f"   ∟ [{cat_name}] 发现 {cat_count} 个")
        
        if all_codes:
            self.conn.execute("DELETE FROM sector_master")
            self.conn.executemany("INSERT INTO sector_master VALUES (?, ?)", [(c, datetime.now()) for c in all_codes])
            logger.success(f"✅ [Phase 1] 捕获 {len(all_codes)} 个板块")
            return list(all_codes)
        return [r[0] for r in existing] if existing else []

    async def sync_all_klines(self, sector_list: list):
        """Phase 2: 极致补扫逻辑"""
        init_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        res = self.conn.execute("SELECT secid, MAX(date) FROM sector_klines GROUP BY secid").fetchall()
        anchors = {row[0]: (row[1] + timedelta(days=1)).strftime("%Y%m%d") for row in res if row[1]}
        
        logger.info(f"🚀 [Phase 2] 同步开始 | 目标: {len(sector_list)} | 并发: {self.concurrency}")
        
        failed_list = []
        semaphore = asyncio.Semaphore(self.concurrency)
        
        # 第一轮：并发同步 (带抖动启动)
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = []
            for i, sid in enumerate(sector_list):
                # 💡 抖动启动：每隔 0.5 秒启动一个并发任务，防止单 Worker 瞬间爆炸
                task = self._fetch_with_stagger(session, sid, anchors.get(sid, "19900101"), semaphore, delay=i*0.2)
                tasks.append(task)
            
            for coro in asyncio.as_completed(tasks):
                sid, batch = await coro
                self.stats["total_tasks"] += 1
                if batch:
                    self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                else:
                    failed_list.append(sid)
        
        # 第二轮：单线程串行补扫 (给 Worker 降温)
        if failed_list:
            logger.warning(f"🔄 补扫开启: 串行处理 {len(failed_list)} 个遗漏板块...")
            async with AsyncSession(impersonate=self.impersonate) as session:
                for sid in failed_list:
                    await asyncio.sleep(1.5) # 串行间隔
                    _, batch = await self._fetch_single(session, sid, anchors.get(sid, "19900101"), asyncio.Semaphore(1))
                    if batch:
                        self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                    else:
                        self.stats["failed_tasks"] += 1 # 补扫也失败才计入最终失败
        
        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        self.conn.execute(f"COPY sector_klines TO '{os.getenv('DATA_PATH')}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        logger.success(f"📊 [Final Report] 总行数: {final_cnt} | 本次新增 {final_cnt - init_cnt}")

    async def _fetch_with_stagger(self, session, secid, beg_date, sem, delay):
        """带初始抖动延迟的抓取"""
        await asyncio.sleep(min(delay, 30)) # 限制最大启动延迟
        return await self._fetch_single(session, secid, beg_date, sem)

    async def _fetch_single(self, session, secid, beg_date, sem):
        async with sem:
            clean_beg = beg_date.replace("-", "")
            url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                   f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                   f"&klt=101&fqt=0&end=20500101&beg={clean_beg}&lmt=50000&ut={self.UT}")
            
            data = await self._safe_request(session, url, f"K_{secid}", cache=True)
            if data and data.get("data", {}).get("klines"):
                rows = [(secid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                         float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                        for k in data["data"]["klines"]]
                return secid, rows
            return secid, None
