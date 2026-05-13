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
            raise RuntimeError("🚨 [Init] 缺少 CF_WORKER_URLS 配置")
            
        self.concurrency = int(os.getenv("CONCURRENCY", 8))
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
        """Phase 0: 隔离线程建立信任链路"""
        logger.info(f"🔑 [Phase 0] 建立信任态 | 节点数: {len(self.worker_pool)}")
        try:
            response = await asyncio.to_thread(self._run_scrapling)
            raw_cookies = response.cookies
            self.trust_context["cookies"] = {c['name']: c['value'] for c in raw_cookies} if isinstance(raw_cookies, list) else raw_cookies
            self.trust_context["headers"] = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/",
                "Accept": "*/*"
            }
            logger.success(f"✅ 信任链就绪 | Cookie: {len(self.trust_context['cookies'])} 枚")
        except Exception as e:
            logger.error(f"⚠️ 信任链异常: {e}")

    def _run_scrapling(self):
        fetcher = Fetcher()
        return fetcher.get("https://quote.eastmoney.com/center/hsbk.html")

    def _route_url(self, target_url: str, use_cache: bool = False) -> str:
        """保持编码一致性的路由转发"""
        worker_base = random.choice(self.worker_pool)
        if not worker_base.startswith("http"): worker_base = f"https://{worker_base}"
        
        # 增加缓存/随机数
        full_target = target_url + (f"&_ts={int(time.time()/30)}" if use_cache else f"&_cb={time.time_ns()}")
        
        # 强制单次转义，确保 Worker 收到的是标准合法的 URL
        return f"{worker_base}?url={urllib.parse.quote(full_target, safe='')}"

    async def _safe_request(self, session, url: str, label: str, cache: bool = False) -> dict:
        """打开可观测性的核心请求器"""
        routed = self._route_url(url, use_cache=cache)
        for attempt in range(3):
            try:
                if attempt > 0: await asyncio.sleep(0.5 * attempt)
                resp = await session.get(routed, headers=self.trust_context["headers"], 
                                         cookies=self.trust_context["cookies"], timeout=20)
                self.stats["total"] += 1
                
                if resp.status_code == 200:
                    text = resp.text.strip()
                    
                    # 💡 核心观察点：打印响应预览，防止“盲飞”
                    logger.debug(f"📦 {label} 预览: {text[:180]}...")
                    
                    # 更安全的 JSONP/JSON 解析
                    if text.startswith("jQuery") or ("(" in text and text.endswith(");")):
                        match = re.search(r'\((.*)\)', text, re.DOTALL)
                        if match: text = match.group(1)
                    
                    if not text.startswith("{"):
                        logger.warning(f"⚠️ {label} 返回内容非 JSON 格式，疑似 Challenge 或 Error Page")
                        continue

                    data = json.loads(text)
                    if data and data.get("data"): 
                        return data
                
                self.stats["errors"] += 1
                self.stats["codes"][str(resp.status_code)] = self.stats["codes"].get(str(resp.status_code), 0) + 1
            except Exception as e:
                self.stats["errors"] += 1
                logger.debug(f"🕒 {label} 网络物理波动: {e}")
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        """Phase 1: 高韧性名录扫描"""
        existing = self.conn.execute("SELECT secid FROM sector_master").fetchall()
        if existing and not force_reconcile:
            logger.success(f"✅ [Phase 1] 缓存命中: 加载存量 {len(existing)} 个板块")
            return [r[0] for r in existing]

        logger.info("📡 [Phase 1] 启动名录同步...")
        all_codes = set()
        categories = {"地域": "m:90+t:1", "行业": "m:90+t:2", "概念": "m:90+t:3"}
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs_val in categories.items():
                cat_count = 0
                for pn in range(1, 10):
                    # 💡 恢复：urlencode(fs) + fields=f12
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={urllib.parse.quote(fs_val)}&fields=f12&ut={self.UT}")
                    
                    data = await self._safe_request(session, url, f"LIST_{cat_name}_P{pn}")
                    if data and data.get("data", {}).get("diff"):
                        items = data["data"]["diff"]
                        for x in items:
                            if x.get("f12"):
                                all_codes.add(f"90.{x['f12']}")
                                cat_count += 1
                        if len(items) < 250: break
                    else: break
                logger.info(f"   ∟ [{cat_name}] 扫描结束: 发现 {cat_count} 个")
        
        if all_codes:
            self.conn.execute("DELETE FROM sector_master")
            self.conn.executemany("INSERT INTO sector_master VALUES (?, ?)", 
                                  [(c, datetime.now()) for c in all_codes])
            logger.success(f"✅ [Phase 1] 名录已同步: 捕获 {len(all_codes)} 个编码")
            return list(all_codes)
        
        if existing:
            logger.warning("⚠️ [Phase 1] 网络扫描无结果，降级使用存量名录")
            return [r[0] for r in existing]
        return []

    async def sync_all_klines(self, sector_list: list):
        """Phase 2: 并发增量同步"""
        initial_count = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
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
        
        final_count = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        added = final_count - initial_count
        
        output_parquet = os.getenv("DATA_PATH", "data/sector_klines_full.parquet")
        self.conn.execute(f"COPY sector_klines TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        
        logger.success(f"📊 [Final] 数据库同步完成 | 总行数: {final_count} | 新增: {added}")

    async def _fetch_single(self, session, secid, beg_date, sem):
        async with sem:
            clean_beg = beg_date.replace("-", "")
            url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                   f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt=101&fqt=0&end=20500101&beg={clean_beg}&ut={self.UT}")
            data = await self._safe_request(session, url, f"K_{secid}", cache=True)
            if data and data.get("data", {}).get("klines"):
                return [(secid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), float(k.split(',')[3]), 
                         float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) for k in data["data"]["klines"]]
            return None
