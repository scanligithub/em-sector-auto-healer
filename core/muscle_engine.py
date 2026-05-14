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

    def __init__(self):
        raw_env = os.getenv("CF_WORKER_URLS") or os.getenv("CF_WORKER_URL") or ""
        urls = [u.strip() for u in raw_env.split(",") if u.strip()]
        if not urls:
            raise RuntimeError("🚨 未检测到 CF_WORKER_URLS")
            
        # 🚀 极速版：固定 Worker 提高 TCP 复用率
        self.worker_url = f"https://{urls[0]}" if not urls[0].startswith("http") else urls[0]
            
        # 🚀 极速版：并发 30，开启 HTTP/2 榨干带宽
        self.concurrency = int(os.getenv("CONCURRENCY", 30))
        self.db_path = "data/sector_quant.db"
        self.impersonate = "chrome124"
        self.trust_context = {"cookies": {}, "headers": {}}
        self.stats = {"total_tasks": 0, "failed_tasks": 0, "codes": {}}
        
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
        logger.info(f"🔑 [Phase 0] 极速预热指纹...")
        try:
            response = await asyncio.to_thread(self._run_scrapling)
            cookies = response.cookies
            if isinstance(cookies, list):
                self.trust_context["cookies"] = {c['name']: c['value'] for c in cookies}
            elif hasattr(cookies, 'get_dict'):
                self.trust_context["cookies"] = cookies.get_dict()
            else:
                self.trust_context["cookies"] = cookies if isinstance(cookies, dict) else {}

            if not self.trust_context["cookies"] or "qgqp_b_id" not in self.trust_context["cookies"]:
                self.trust_context["cookies"] = {
                    "qgqp_b_id": "99999999999999999999999999999999",
                    "st_pvi": str(int(time.time()))
                }

            self.trust_context["headers"] = {
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
                "Connection": "keep-alive",
                "Cache-Control": "no-cache",
            }
            logger.success(f"✅ 信任链就绪 | Cookie: {len(self.trust_context['cookies'])}")
        except Exception as e:
            logger.error(f"⚠️ 信任链异常: {e}")

    def _run_scrapling(self):
        fetcher = Fetcher()
        return fetcher.get("https://quote.eastmoney.com/bk/90.BK1063.html")

    def _generate_jquery_cb(self):
        ts = int(time.time() * 1000)
        return f"jQuery35108888_{ts}", ts

    async def _safe_request(self, session, url: str, label: str, secid: str) -> dict:
        cb_name, ts = self._generate_jquery_cb()
        full_url = f"{url}&cb={cb_name}&_={ts}"
        
        headers = self.trust_context["headers"].copy()
        headers["Referer"] = f"https://quote.eastmoney.com/bk/{secid}.html"
        routed_url = f"{self.worker_url}?url={urllib.parse.quote(full_url, safe='')}"

        for attempt in range(2):
            try:
                # 🚀 开启 HTTP/2 极速传输
                resp = await session.get(routed_url, headers=headers, cookies=self.trust_context["cookies"], timeout=20)
                if resp.status_code == 200:
                    text = resp.text.strip()
                    match = re.search(r'jQuery\d+_\d+\((.*)\)', text, re.DOTALL)
                    if match:
                        data = json.loads(match.group(1))
                        if data and data.get("rc") == 0: return data
                self.stats["codes"][str(resp.status_code)] = self.stats["codes"].get(str(resp.status_code), 0) + 1
            except Exception:
                pass
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        logger.info("📡 [Phase 1] 同步板块名录...")
        all_codes = set()
        # 🚀 核心：开启 http_version=2
        async with AsyncSession(impersonate=self.impersonate, http_version=2) as session:
            for cat_name, fs_code in self.CATEGORIES.items():
                cat_count = 0
                for pn in range(1, 10):
                    self.stats["total_tasks"] += 1
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={urllib.parse.quote(fs_code)}&fields=f12&ut={self.UT}")
                    data = await self._safe_request(session, url, f"LIST_{cat_name}_P{pn}", "90.BK0447")
                    if data and data.get("data", {}).get("diff"):
                        for x in data["data"]["diff"]:
                            all_codes.add(f"90.{x['f12']}")
                            cat_count += 1
                        if len(data["data"]["diff"]) < 250: break
                    else: break
                logger.info(f"   ∟ [{cat_name}] 发现 {cat_count} 个")
        
        if all_codes:
            self.conn.execute("DELETE FROM sector_master")
            self.conn.executemany("INSERT INTO sector_master VALUES (?, ?)", [(c, datetime.now()) for c in all_codes])
            return list(all_codes)
        return []

    async def sync_all_klines(self, sector_list: list):
        logger.warning("🔥 [压测模式] 执行全量高速拉取...")
        self.conn.execute("DELETE FROM sector_klines")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        progress = {"done": 0, "total": len(sector_list)}
        start = time.time()
        
        # 🚀 核心：开启 http_version=2 和高并发
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency, http_version=2) as session:
            tasks = []
            for sid in sector_list:
                url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={sid}"
                       f"&ut={self.UT}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                       f"&klt=101&fqt=1&end=20500101&beg=19900101&lmt=100000")
                tasks.append(self._fetch_and_save(session, sid, url, semaphore, progress))
            await asyncio.gather(*tasks)

        end = time.time()
        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        self.conn.execute(f"COPY sector_klines TO '{os.getenv('DATA_PATH')}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        logger.success(f"📊 同步完毕 | 总行数: {final_cnt} | 耗时: {end - start:.1f}s")

    async def _fetch_and_save(self, session, sid, url, sem, progress):
        async with sem:
            data = await self._safe_request(session, url, f"K_{sid}", sid)
            progress["done"] += 1
            if progress["done"] % 50 == 0:
                logger.info(f"⏳ 极速同步中: {progress['done']}/{progress['total']}...")
            if data and data.get("data", {}).get("klines"):
                batch = []
                for k in data["data"]["klines"]:
                    r = k.split(',')
                    batch.append((sid, r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), float(r[6])))
                if batch:
                    self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
