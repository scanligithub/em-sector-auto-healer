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
        # 1. 配置加载与池化
        raw_urls = os.getenv("CF_WORKER_URLS", "").split(",")
        self.worker_pool = [u.strip() for u in raw_urls if u.strip()]
        self.concurrency = int(os.getenv("CONCURRENCY", 8))
        self.db_path = "data/sector_quant.db"
        self.impersonate = "chrome124"
        self.trust_context = {"cookies": {}, "headers": {}}
        self.stats = {"total": 0, "errors": 0, "codes": {}}
        
        # 初始化 DuckDB
        os.makedirs("data", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        """初始化工业级 DuckDB 表结构"""
        # K线主表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sector_klines (
                secid VARCHAR,
                date DATE,
                open DOUBLE,
                close DOUBLE,
                high DOUBLE,
                low DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                PRIMARY KEY(secid, date)
            )
        """)
        # 板块名录表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sector_master (
                secid VARCHAR PRIMARY KEY,
                last_update TIMESTAMP
            )
        """)

    async def build_trust_chain(self):
        """Phase 0: 线程隔离的信任链构建 (不再阻塞 Event Loop)"""
        logger.info("🔑 [Phase 0] 启动 Scrapling 隔离线程构建信任态...")
        try:
            # 封装阻塞调用到单独线程
            response = await asyncio.to_thread(self._run_scrapling)
            
            raw_cookies = response.cookies
            self.trust_context["cookies"] = {c['name']: c['value'] for c in raw_cookies} if isinstance(raw_cookies, list) else raw_cookies
            self.trust_context["headers"] = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/",
                "Accept": "*/*"
            }
            logger.success(f"✅ 信任链已建立，Cookie 缓存完毕")
        except Exception as e:
            logger.error(f"⚠️ 信任链构建异常: {e}")

    def _run_scrapling(self):
        """同步的 Scrapling 调用"""
        fetcher = Fetcher(auto_match=True)
        return fetcher.get("https://quote.eastmoney.com/center/hsbk.html")

    def _route_url(self, target_url: str, use_cache: bool = False) -> str:
        """多 Worker 随机路由 + 智能缓存键"""
        # 盘后缓存 1 小时，盘中缓存 30 秒
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
            except Exception:
                self.stats["errors"] += 1
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        """Phase 1: 智能目录管理 - 优先从 DB 加载，降低扫描频率"""
        # 检查 DB 中是否有板块
        existing = self.conn.execute("SELECT secid FROM sector_master").fetchall()
        if existing and not force_reconcile:
            logger.info(f"📁 从本地存储加载 {len(existing)} 个板块名录")
            return [r[0] for r in existing]

        logger.info("📡 [Phase 1] 启动全量板块目录扫描...")
        all_codes = set()
        categories = {"地域": "m:90+t:1", "行业": "m:90+t:2", "概念": "m:90+t:3"}
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs in categories.items():
                empty_count = 0
                for pn in range(1, 10):
                    if empty_count >= 2: break
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={urllib.parse.quote(fs)}&fields=f12&ut={self.UT}")
                    data = await self._safe_request(session, url, f"LIST_{cat_name}")
                    if data and data.get("data", {}).get("diff"):
                        for x in data["data"]["diff"]: all_codes.add(f"90.{x['f12']}")
                        empty_count = 0
                    else: empty_count += 1
        
        # 更新本地名录
        if all_codes:
            self.conn.execute("DELETE FROM sector_master")
            self.conn.executemany("INSERT INTO sector_master VALUES (?, ?)", 
                                  [(c, datetime.now()) for c in all_codes])
            logger.success(f"💾 名录已同步，捕获 {len(all_codes)} 个板块")
        return list(all_codes)

    def get_incremental_anchors(self) -> dict:
        """DuckDB 极速增量锚点计算 (last_date + 1)"""
        res = self.conn.execute("SELECT secid, MAX(date) FROM sector_klines GROUP BY secid").fetchall()
        anchors = {}
        for secid, m_date in res:
            if m_date:
                # 核心改进：Beg = Last_Date + 1 day，彻底避开重复下载
                next_day = m_date + timedelta(days=1)
                anchors[secid] = next_day.strftime("%Y%m%d")
        return anchors

    async def sync_all_klines(self, sector_list: list):
        """Phase 2: 并发增量同步"""
        anchors = self.get_incremental_anchors()
        semaphore = asyncio.Semaphore(self.concurrency)
        logger.info(f"🚀 [Phase 2] 增量同步模式 | 线程数: {self.concurrency}")
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single(session, sid, anchors.get(sid, "19900101"), semaphore) for sid in sector_list]
            for coro in asyncio.as_completed(tasks):
                batch = await coro
                if batch:
                    # DuckDB 流式写入：不需要 pl.concat，直接写
                    self.conn.executemany("""
                        INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch)
        
        # 导出 Parquet 作为外部 Artifact (Actions 使用)
        self.conn.execute(f"COPY sector_klines TO 'data/sector_klines_full.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)")
        logger.success("💾 任务圆满完成，DuckDB 与 Parquet 同步完毕")

    async def _fetch_single(self, session, secid, beg_date, sem):
        async with sem:
            url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                   f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt=101&fqt=0&end=20500101&beg={beg_date}&ut={self.UT}")
            data = await self._safe_request(session, url, f"K_{secid}", cache=True)
            if data and data.get("data", {}).get("klines"):
                return [(secid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                         float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                        for k in data["data"]["klines"]]
            return None
