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
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        raw_env = os.getenv("CF_WORKER_URLS") or os.getenv("CF_WORKER_URL") or ""
        raw_urls = raw_env.split(",")
        self.worker_pool = [u.strip() for u in raw_urls if u.strip()]
        
        if not self.worker_pool:
            raise RuntimeError("🚨 [Init] 未检测到 CF_WORKER_URLS，请检查 Secrets 配置。")
            
        self.concurrency = int(os.getenv("CONCURRENCY", 8))
        self.db_path = "data/sector_quant.db"
        self.impersonate = "chrome124"
        self.trust_context = {"cookies": {}, "headers": {}}
        self.stats = {"total": 0, "errors": 0, "codes": {}}
        
        os.makedirs("data", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sector_klines (
                secid VARCHAR, date DATE, open DOUBLE, close DOUBLE,
                high DOUBLE, low DOUBLE, volume DOUBLE, amount DOUBLE,
                PRIMARY KEY(secid, date)
            )
        """)
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_master (secid VARCHAR PRIMARY KEY, last_update TIMESTAMP)")

    async def build_trust_chain(self):
        logger.info(f"🔑 [Phase 0] 构建信任态 | 负载池大小: {len(self.worker_pool)}")
        try:
            response = await asyncio.to_thread(self._run_scrapling)
            raw_cookies = response.cookies
            self.trust_context["cookies"] = {c['name']: c['value'] for c in raw_cookies} if isinstance(raw_cookies, list) else raw_cookies
            self.trust_context["headers"] = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/",
                "Accept": "*/*"
            }
            logger.success("✅ 信任链已建立，Cookie 缓存完毕")
        except Exception as e:
            logger.error(f"⚠️ 信任链构建异常: {e}")

    def _run_scrapling(self):
        fetcher = Fetcher()
        return fetcher.get("https://quote.eastmoney.com/center/hsbk.html")

    def _route_url(self, target_url: str, use_cache: bool = False) -> str:
        now = datetime.now()
        cache_window = 3600 if now.hour >= 16 or now.hour < 9 else 30
        if use_cache:
            target_url += f"&_ts={int(time.time() / cache_window)}"
        else:
            target_url += f"&_cb={time.time_ns()}"
        worker_base = random.choice(self.worker_pool)
        if not worker_base.startswith("http"): worker_base = f"https://{worker_base}"
        return f"{worker_base}?url={urllib.parse.quote(target_url, safe='')}"

    async def _safe_request(self, session, url: str, label: str, cache: bool = False) -> dict:
        routed = self._route_url(url, use_cache=cache)
        for attempt in range(3):
            try:
                await asyncio.sleep(random.uniform(0.1, 0.3) * attempt)
                resp = await session.get(routed, headers=self.trust_context["headers"], 
                                         cookies=self.trust_context["cookies"], timeout=20)
                self.stats["total"] += 1
                if resp.status_code == 200:
                    match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', resp.text, re.DOTALL)
                    return json.loads(match.group(1) if match else resp.text)
                self.stats["errors"] += 1
                code_key = str(resp.status_code)
                self.stats["codes"][code_key] = self.stats["codes"].get(code_key, 0) + 1
            except Exception: self.stats["errors"] += 1
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        """Phase 1: 智能目录管理"""
        existing = self.conn.execute("SELECT secid FROM sector_master").fetchall()
        if existing and not force_reconcile:
            logger.success(f"✅ [Phase 1] 缓存命中: 已从 DB 加载 {len(existing)} 个板块名录")
            return [r[0] for r in existing]

        logger.info("📡 [Phase 1] 启动全量板块目录扫描...")
        all_codes = set()
        categories = {"地域": "m:90+t:1", "行业": "m:90+t:2", "概念": "m:90+t:3"}
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs in categories.items():
                cat_count = 0
                for pn in range(1, 10):
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={urllib.parse.quote(fs)}&fields=f12&ut={self.UT}")
                    data = await self._safe_request(session, url, f"LIST_{cat_name}")
                    if data and data.get("data", {}).get("diff"):
                        items = data["data"]["diff"]
                        for x in items: 
                            all_codes.add(f"90.{x['f12']}")
                            cat_count += 1
                        if len(items) < 250: break
                    else: break
                logger.info(f"   ∟ 分类 [{cat_name}] 扫描完成: 发现 {cat_count} 个板块")
        
        if all_codes:
            self.conn.execute("DELETE FROM sector_master")
            self.conn.executemany("INSERT INTO sector_master VALUES (?, ?)", [(c, datetime.now()) for c in all_codes])
            logger.success(f"✅ [Phase 1] 全量扫描完成: 捕获 {len(all_codes)} 个板块编码并持久化")
        return list(all_codes)

    async def sync_all_klines(self, sector_list: list):
        """Phase 2: 增量并发同步"""
        # 获取初始行数用于计算新增量
        initial_count = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        
        res = self.conn.execute("SELECT secid, MAX(date) FROM sector_klines GROUP BY secid").fetchall()
        anchors = {row[0]: (row[1] + timedelta(days=1)).strftime("%Y%m%d") for row in res if row[1]}
        
        logger.info(f"🚀 [Phase 2] 增量同步启动 | 目标板块: {len(sector_list)} | 增量锚点: {len(anchors)}")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single(session, sid, anchors.get(sid, "19900101"), semaphore) for sid in sector_list]
            for coro in asyncio.as_completed(tasks):
                batch = await coro
                if batch:
                    self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
        
        # 任务结束审计
        final_count = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        added_count = final_count - initial_count
        
        # 导出 Parquet
        output_parquet = os.getenv("DATA_PATH", "data/sector_klines_full.parquet")
        self.conn.execute(f"COPY sector_klines TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        
        logger.success(f"📊 [Final Report] 任务圆满完成")
        logger.info(f"   ∟ 存量板块: {len(sector_list)} 个")
        logger.info(f"   ∟ K线总数: {final_count} 行")
        logger.info(f"   ∟ 本次新增: {added_count} 行")
        logger.info(f"   ∟ 数据落盘: {output_parquet}")

    async def _fetch_single(self, session, secid, beg_date, sem):
        async with sem:
            clean_beg = beg_date.replace("-", "")
            url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                   f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt=101&fqt=0&end=20500101&beg={clean_beg}&ut={self.UT}")
            data = await self._safe_request(session, url, f"K_{secid}", cache=True)
            if data and data.get("data", {}).get("klines"):
                return [(secid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                         float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                        for k in data["data"]["klines"]]
            return None
