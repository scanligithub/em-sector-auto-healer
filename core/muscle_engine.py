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
    # 💡 行业/概念/地域编码
    CATEGORIES = {"地域": "m:90%2Bt:1", "行业": "m:90%2Bt:2", "概念": "m:90%2Bt:3"}
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        raw_env = os.getenv("CF_WORKER_URLS") or os.getenv("CF_WORKER_URL") or ""
        self.worker_pool = [u.strip() for u in raw_env.split(",") if u.strip()]
        if not self.worker_pool: raise RuntimeError("🚨 未配置 CF_WORKER_URLS")
            
        self.concurrency = int(os.getenv("CONCURRENCY", 5)) # 💡 降低并发保护单 Worker
        self.db_path = "data/sector_quant.db"
        self.impersonate = "chrome124"
        self.trust_context = {"cookies": {}, "headers": {}}
        self.stats = {"total": 0, "errors": 0, "codes": {}}
        
        os.makedirs("data", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_klines (secid VARCHAR, date DATE, open DOUBLE, close DOUBLE, high DOUBLE, low DOUBLE, volume DOUBLE, amount DOUBLE, PRIMARY KEY(secid, date))")
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_master (secid VARCHAR PRIMARY KEY, last_update TIMESTAMP)")

    async def build_trust_chain(self):
        """Phase 0: 强力种下风控 Cookie"""
        logger.info(f"🔑 [Phase 0] 正在尝试建立东财信任态...")
        try:
            # 💡 换回更通用的行情中心页，这个页面种下的 Cookie 最全
            response = await asyncio.to_thread(self._run_scrapling)
            cookies = response.cookies
            self.trust_context["cookies"] = {c['name']: c['value'] for c in cookies} if isinstance(cookies, list) else cookies
            
            # 💡 注入浏览器真实的 Referer
            self.trust_context["headers"] = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/center/hsbk.html",
                "Accept": "*/*"
            }
            logger.success(f"✅ 信任链就绪 | 捕获 {len(self.trust_context['cookies'])} 枚有效 Cookie")
        except Exception as e:
            logger.error(f"⚠️ 信任链失败: {e}")

    def _run_scrapling(self):
        fetcher = Fetcher()
        # 💡 使用最基础的行情入口，确保兼容性
        return fetcher.get("https://quote.eastmoney.com/center/hsbk.html")

    def _route_url(self, target_url: str, use_cache: bool = False) -> str:
        worker_base = random.choice(self.worker_pool)
        if not worker_base.startswith("http"): worker_base = f"https://{worker_base}"
        suffix = f"&_ts={int(time.time()/30)}" if use_cache else f"&_cb={time.time_ns()}"
        return f"{worker_base}?url={urllib.parse.quote(target_url + suffix, safe='')}"

    async def _safe_request(self, session, url: str, label: str, cache: bool = False) -> dict:
        routed = self._route_url(url, use_cache=cache)
        for attempt in range(3):
            try:
                if attempt > 0: await asyncio.sleep(1 * attempt)
                resp = await session.get(routed, headers=self.trust_context["headers"], 
                                         cookies=self.trust_context["cookies"], timeout=20)
                self.stats["total"] += 1
                text = resp.text.strip()
                
                if resp.status_code == 200:
                    # 💡 保持预览，方便监控 rc: 102 是否消失
                    if "diff" not in text and "klines" not in text:
                        logger.debug(f"📦 {label} 内容预览: {text[:150]}")
                    
                    if text.startswith("jQuery") or ("(" in text and text.endswith(");")):
                        text = re.search(r'\((.*)\)', text, re.DOTALL).group(1)
                    
                    data = json.loads(text)
                    # 💡 rc: 0 代表真正的业务成功
                    if data and data.get("rc") == 0: return data
                    if data and data.get("rc") == 102:
                        logger.warning(f"⚠️ {label} 命中 rc: 102 参数错误，正在检查 URL 协议...")
                
                self.stats["errors"] += 1
            except Exception: self.stats["errors"] += 1
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        """Phase 1: 扫除 100 条限流天花板"""
        existing = self.conn.execute("SELECT secid FROM sector_master").fetchall()
        if existing and not force_reconcile:
            logger.success(f"✅ [Phase 1] 缓存命中使用 {len(existing)} 板块")
            return [r[0] for r in existing]

        logger.info("📡 [Phase 1] 开始全量扫描...")
        all_codes = set()
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs_code in self.CATEGORIES.items():
                cat_count = 0
                for pn in range(1, 10):
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
                    else: break
                logger.info(f"   ∟ [{cat_name}] 发现 {cat_count} 个板块")
        
        if all_codes:
            self.conn.execute("DELETE FROM sector_master")
            self.conn.executemany("INSERT INTO sector_master VALUES (?, ?)", [(c, datetime.now()) for c in all_codes])
            logger.success(f"✅ [Phase 1] 成功扫除限流，捕获 {len(all_codes)} 个板块")
            return list(all_codes)
        return [r[0] for r in existing] if existing else []

    async def sync_all_klines(self, sector_list: list):
        """Phase 2: 修复 rc: 102 并增量同步"""
        init_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        res = self.conn.execute("SELECT secid, MAX(date) FROM sector_klines GROUP BY secid").fetchall()
        anchors = {row[0]: (row[1] + timedelta(days=1)).strftime("%Y%m%d") for row in res if row[1]}
        
        logger.info(f"🚀 [Phase 2] 同步中 | 目标: {len(sector_list)} | 锚点: {len(anchors)}")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single(session, sid, anchors.get(sid, "19900101"), semaphore) for sid in sector_list]
            for coro in asyncio.as_completed(tasks):
                batch = await coro
                if batch:
                    self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
        
        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        self.conn.execute(f"COPY sector_klines TO '{os.getenv('DATA_PATH')}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        logger.success(f"📊 同步完成: 本次新增 {final_cnt - init_cnt} 行数据")

    async def _fetch_single(self, session, secid, beg_date, sem):
        async with sem:
            # 💡 核心修复：push2his 接口必须带上 fields1，否则返回 rc: 102
            clean_beg = beg_date.replace("-", "")
            url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                   f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58"
                   f"&klt=101&fqt=0&end=20500101&beg={clean_beg}&ut={self.UT}")
            
            data = await self._safe_request(session, url, f"K_{secid}", cache=True)
            if data and data.get("data", {}).get("klines"):
                return [(secid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                         float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                        for k in data["data"]["klines"]]
            return None
