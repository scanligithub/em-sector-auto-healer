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
        # 1. 强化版配置加载：兼容单数和复数环境变量
        raw_env = os.getenv("CF_WORKER_URLS") or os.getenv("CF_WORKER_URL") or ""
        raw_urls = raw_env.split(",")
        self.worker_pool = [u.strip() for u in raw_urls if u.strip()]
        
        # 防御性退出：如果没有有效 Worker，不启动程序
        if not self.worker_pool:
            msg = "🚨 [Init] 未检测到 CF_WORKER_URLS。请在 GitHub Secrets 中配置该变量！"
            logger.critical(msg)
            raise RuntimeError(msg)
            
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
        # K线主表：增加主键约束防止重复
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
        """Phase 0: 线程隔离的信任链构建 (保护 Event Loop)"""
        logger.info(f"🔑 [Phase 0] 构建信任态 | 负载池大小: {len(self.worker_pool)}")
        try:
            # 将同步的阻塞调用放到线程池
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
            logger.error(f"⚠️ 信任链构建异常: {e} (系统将尝试直连运行)")

    def _run_scrapling(self):
        """适配 Scrapling v0.3.x+ 最新 API"""
        fetcher = Fetcher()
        fetcher.configure(auto_match=True) # 解决 deprecated 警告
        return fetcher.get("https://quote.eastmoney.com/center/hsbk.html")

    def _route_url(self, target_url: str, use_cache: bool = False) -> str:
        """多 Worker 随机路由 + 智能缓存策略"""
        now = datetime.now()
        # 盘后(16点后)或盘前(9点前)使用长缓存 1 小时，盘中使用短缓存 30 秒
        cache_window = 3600 if now.hour >= 16 or now.hour < 9 else 30
        
        if use_cache:
            target_url += f"&_ts={int(time.time() / cache_window)}"
        else:
            target_url += f"&_cb={time.time_ns()}"

        worker_base = random.choice(self.worker_pool)
        if not worker_base.startswith("http"):
            worker_base = f"https://{worker_base}"
        
        return f"{worker_base}?url={urllib.parse.quote(target_url, safe='')}"

    async def _safe_request(self, session, url: str, label: str, cache: bool = False) -> dict:
        routed = self._route_url(url, use_cache=cache)
        for attempt in range(3):
            try:
                await asyncio.sleep(random.uniform(0.1, 0.3) * attempt)
                resp = await session.get(
                    routed, 
                    headers=self.trust_context["headers"], 
                    cookies=self.trust_context["cookies"], 
                    timeout=20
                )
                self.stats["total"] += 1
                if resp.status_code == 200:
                    match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', resp.text, re.DOTALL)
                    return json.loads(match.group(1) if match else resp.text)
                
                self.stats["errors"] += 1
                code_key = str(resp.status_code)
                self.stats["codes"][code_key] = self.stats["codes"].get(code_key, 0) + 1
            except Exception:
                self.stats["errors"] += 1
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        """Phase 1: 智能目录管理 - 缓存于 DB"""
        existing = self.conn.execute("SELECT secid FROM sector_master").fetchall()
        if existing and not force_reconcile:
            logger.info(f"📁 已从 DB 加载 {len(existing)} 个板块名录，跳过线上扫描")
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
                        items = data["data"]["diff"]
                        for x in items: all_codes.add(f"90.{x['f12']}")
                        empty_count = 0
                        if len(items) < 250: break
                    else:
                        empty_count += 1
        
        if all_codes:
            # 更新本地存储的名录
            self.conn.execute("DELETE FROM sector_master")
            self.conn.executemany(
                "INSERT INTO sector_master VALUES (?, ?)", 
                [(c, datetime.now()) for c in all_codes]
            )
            logger.success(f"💾 板块名录更新完毕，捕获 {len(all_codes)} 个编码")
        return list(all_codes)

    def get_incremental_anchors(self) -> dict:
        """DuckDB 极速增量锚点计算 (last_date + 1)"""
        res = self.conn.execute("SELECT secid, MAX(date) FROM sector_klines GROUP BY secid").fetchall()
        anchors = {}
        for secid, m_date in res:
            if m_date:
                # 核心改进：Beg = Last_Date + 1 day
                next_day = m_date + timedelta(days=1)
                anchors[secid] = next_day.strftime("%Y%m%d")
        return anchors

    async def sync_all_klines(self, sector_list: list):
        """Phase 2: 增量并发同步"""
        anchors = self.get_incremental_anchors()
        semaphore = asyncio.Semaphore(self.concurrency)
        logger.info(f"🚀 [Phase 2] 增量同步启动 | 并发度: {self.concurrency}")
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single(session, sid, anchors.get(sid, "19900101"), semaphore) for sid in sector_list]
            for coro in asyncio.as_completed(tasks):
                batch = await coro
                if batch:
                    # 批量插入数据库，IGNORE 自动跳过重复主键
                    self.conn.executemany("""
                        INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch)
        
        # 导出 Parquet 供外部分析 (Actions Artifact)
        output_parquet = os.getenv("DATA_PATH", "data/sector_klines_full.parquet")
        self.conn.execute(f"COPY sector_klines TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        logger.success(f"💾 增量同步圆满完成 | 数据源: {self.db_path} | 输出: {output_parquet}")

    async def _fetch_single(self, session, secid, beg_date, sem):
        async with sem:
            # 去除日期横杠适配 API
            clean_beg = beg_date.replace("-", "")
            url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                   f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt=101&fqt=0&end=20500101&beg={clean_beg}&ut={self.UT}")
            
            data = await self._safe_request(session, url, f"K_{secid}", cache=True)
            if data and data.get("data", {}).get("klines"):
                return [(
                    secid, 
                    k.split(',')[0], 
                    float(k.split(',')[1]), 
                    float(k.split(',')[2]), 
                    float(k.split(',')[3]), 
                    float(k.split(',')[4]), 
                    float(k.split(',')[5]), 
                    float(k.split(',')[6])
                ) for k in data["data"]["klines"]]
            return None
