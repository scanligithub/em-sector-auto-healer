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
    # 板块分类代码 (保持原样)
    CATEGORIES = {"地域": "m:90%2Bt:1", "行业": "m:90%2Bt:2", "概念": "m:90%2Bt:3"}
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        # 1. 负载池加载
        raw_env = os.getenv("CF_WORKER_URLS") or os.getenv("CF_WORKER_URL") or ""
        self.worker_pool = [u.strip() for u in raw_env.split(",") if u.strip()]
        if not self.worker_pool:
            raise RuntimeError("🚨 [Init] 未检测到 CF_WORKER_URLS，请检查环境变量。")
            
        self.concurrency = int(os.getenv("CONCURRENCY", 5))
        self.db_path = "data/sector_quant.db"
        self.impersonate = "chrome124"
        self.trust_context = {"cookies": {}, "headers": {}}
        self.stats = {"total_tasks": 0, "failed_tasks": 0, "codes": {}}
        
        # 2. DuckDB 初始化
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
        """Phase 0: 像素级环境预热"""
        logger.info(f"🔑 [Phase 0] 启动 1:1 浏览器环境预热...")
        try:
            # 随机挑选一个板块作为“落地页”进行预热
            sample_secid = "90.BK1063"
            response = await asyncio.to_thread(self._run_scrapling, sample_secid)
            
            # 强化 Cookie 提取逻辑
            cookies = response.cookies
            if isinstance(cookies, list):
                self.trust_context["cookies"] = {c['name']: c['value'] for c in cookies}
            elif hasattr(cookies, 'get_dict'):
                self.trust_context["cookies"] = cookies.get_dict()
            else:
                self.trust_context["cookies"] = cookies if isinstance(cookies, dict) else {}

            # 💡 镜像复刻你提供的 cURL Headers
            self.trust_context["headers"] = {
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Connection": "keep-alive",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
                "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "Sec-Fetch-Dest": "script",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Site": "same-site",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
            }
            logger.success(f"✅ 信任链就绪 | 捕获 Cookie: {len(self.trust_context['cookies'])} 枚")
        except Exception as e:
            logger.error(f"⚠️ 信任链构建异常: {e}")

    def _run_scrapling(self, secid):
        # 强制使用 playwright 驱动以获取最全的动态 Cookie
        fetcher = Fetcher()
        return fetcher.get(f"https://quote.eastmoney.com/bk/{secid}.html")

    def _generate_jquery_cb(self):
        """像素级模拟 jQuery 回调标识"""
        rand_part = "3510" + "".join(random.choices("0123456789", k=16))
        ts = int(time.time() * 1000)
        return f"jQuery{rand_part}_{ts}", ts

    async def _safe_request(self, session, url: str, label: str, secid: str) -> dict:
        """带镜像参数补全的请求器"""
        cb_name, ts = self._generate_jquery_cb()
        # 补全 cURL 里的 _=时间戳参数
        full_url = f"{url}&cb={cb_name}&_={ts + random.randint(1, 10)}"
        
        # 镜像 Referer
        headers = self.trust_context["headers"].copy()
        headers["Referer"] = f"https://quote.eastmoney.com/bk/{secid}.html"
        
        worker_base = random.choice(self.worker_pool)
        if not worker_base.startswith("http"): worker_base = f"https://{worker_base}"
        routed_url = f"{worker_base}?url={urllib.parse.quote(full_url, safe='')}"

        for attempt in range(3):
            try:
                if attempt > 0: await asyncio.sleep(2 * attempt)
                resp = await session.get(routed_url, headers=headers, cookies=self.trust_context["cookies"], timeout=45)
                
                if resp.status_code == 200:
                    text = resp.text.strip()
                    # 镜像 JSONP 解包
                    match = re.search(r'jQuery\d+_\d+\((.*)\)', text, re.DOTALL)
                    if match:
                        data = json.loads(match.group(1))
                        if data and data.get("rc") == 0: return data
                
                self.stats["codes"][str(resp.status_code)] = self.stats["codes"].get(str(resp.status_code), 0) + 1
            except Exception as e:
                logger.debug(f"🕒 {label} 异常: {str(e)[:50]}")
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        """Phase 1: 扫描板块名录 (已修复 AttributeError)"""
        existing = self.conn.execute("SELECT secid FROM sector_master").fetchall()
        if existing and not force_reconcile:
            logger.success(f"✅ [Phase 1] 缓存命中使用 {len(existing)} 个板块")
            return [r[0] for r in existing]

        logger.info("📡 [Phase 1] 启动板块名录同步...")
        all_codes = set()
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs_code in self.CATEGORIES.items():
                cat_count = 0
                for pn in range(1, 12):
                    # 名录接口使用常规 fields=f12
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={fs_code}&fields=f12&ut={self.UT}")
                    
                    # 💡 名录扫描使用随机生成的 secid 作为 Referer 占位
                    data = await self._safe_request(session, url, f"LIST_{cat_name}_P{pn}", "90.BK0447")
                    if data and data.get("data", {}).get("diff"):
                        items = data["data"]["diff"]
                        for x in items:
                            if x.get("f12"):
                                all_codes.add(f"90.{x['f12']}")
                                cat_count += 1
                        if len(items) < 250: break
                    else: break
                logger.info(f"   ∟ [{cat_name}] 发现 {cat_count} 个")
        
        if all_codes:
            self.conn.execute("DELETE FROM sector_master")
            self.conn.executemany("INSERT INTO sector_master VALUES (?, ?)", [(c, datetime.now()) for c in all_codes])
            logger.success(f"✅ [Phase 1] 捕获 {len(all_codes)} 个编码")
            return list(all_codes)
        return [r[0] for r in existing] if existing else []

    async def sync_all_klines(self, sector_list: list):
        """Phase 2: 镜像回溯同步"""
        init_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        logger.info(f"🚀 [Phase 2] 镜像同步启动 | 目标: {len(sector_list)} 个板块")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = []
            for i, sid in enumerate(sector_list):
                # 💡 1:1 镜像你提供的 cURL 参数：fqt=1, lmt=1000000 (全量)
                url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={sid}"
                       f"&ut={self.UT}&fields1=f1,f2,f3,f4,f5,f6"
                       f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                       f"&klt=101&fqt=1&end=20500101&lmt=1000000")
                
                tasks.append(self._fetch_and_save(session, sid, url, semaphore, delay=i*0.3))

            await asyncio.gather(*tasks)

        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        output_parquet = os.getenv("DATA_PATH", "data/sector_klines_full.parquet")
        self.conn.execute(f"COPY sector_klines TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        logger.success(f"📊 [Final Report] 镜像同步完成 | 新增: {final_cnt - init_cnt} 行数据")

    async def _fetch_and_save(self, session, sid, url, sem, delay):
        await asyncio.sleep(min(delay, 20))
        async with sem:
            data = await self._safe_request(session, url, f"K_{sid}", sid)
            if data and data.get("data", {}).get("klines"):
                batch = []
                for k in data["data"]["klines"]:
                    r = k.split(',')
                    batch.append((sid, r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), float(r[6])))
                if batch:
                    self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                return True
            return False
