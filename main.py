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
    CATEGORIES = {"地域": "m:90%2Bt:1", "行业": "m:90%2Bt:2", "概念": "m:90%2Bt:3"}
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        raw_env = os.getenv("CF_WORKER_URLS") or os.getenv("CF_WORKER_URL") or ""
        self.worker_pool = [u.strip() for u in raw_env.split(",") if u.strip()]
        if not self.worker_pool:
            raise RuntimeError("🚨 [Init] 未检测到 CF_WORKER_URLS")
            
        # 💡 压测时可通过修改环境变量改变此值 (建议测试范围 3 到 10)
        self.concurrency = int(os.getenv("CONCURRENCY", 5))
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
        logger.info(f"🔑 [Phase 0] 启动环境预热与指纹合成...")
        try:
            sample_secid = "90.BK1063"
            response = await asyncio.to_thread(self._run_scrapling, sample_secid)
            
            cookies = response.cookies
            if isinstance(cookies, list):
                self.trust_context["cookies"] = {c['name']: c['value'] for c in cookies}
            elif hasattr(cookies, 'get_dict'):
                self.trust_context["cookies"] = cookies.get_dict()
            else:
                self.trust_context["cookies"] = cookies if isinstance(cookies, dict) else {}

            if not self.trust_context["cookies"] or "qgqp_b_id" not in self.trust_context["cookies"]:
                logger.warning("⚠️ 动态提取失败，正在注入强力合成指纹...")
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.trust_context["cookies"] = {
                    "qgqp_b_id": "".join(random.choices("0123456789abcdef", k=32)),
                    "st_pvi": "".join(random.choices("0123456789", k=14)),
                    "st_sp": urllib.parse.quote(now_str),
                    "st_inirUrl": "https%3A%2F%2Fquote.eastmoney.com%2Fcenter%2Fgridlist.html",
                    "st_sn": "2",
                    "st_psi": f"{datetime.now().strftime('%Y%m%d%H%M%S')}000-113200301353-{random.randint(1000000000, 9999999999)}"
                }

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
            logger.success(f"✅ 信任链就绪 | 已装载高权 Cookie: {len(self.trust_context['cookies'])} 枚")
        except Exception as e:
            logger.error(f"⚠️ 信任链构建异常: {e}")

    def _run_scrapling(self, secid):
        fetcher = Fetcher()
        return fetcher.get(f"https://quote.eastmoney.com/bk/{secid}.html")

    def _generate_jquery_cb(self):
        rand_part = "3510" + "".join(random.choices("0123456789", k=16))
        ts = int(time.time() * 1000)
        return f"jQuery{rand_part}_{ts}", ts

    async def _safe_request(self, session, url: str, label: str, secid: str) -> dict:
        cb_name, ts = self._generate_jquery_cb()
        full_url = f"{url}&cb={cb_name}&_={ts + random.randint(1, 10)}"
        
        headers = self.trust_context["headers"].copy()
        headers["Referer"] = f"https://quote.eastmoney.com/bk/{secid}.html"
        
        worker_base = random.choice(self.worker_pool)
        if not worker_base.startswith("http"): worker_base = f"https://{worker_base}"
        routed_url = f"{worker_base}?url={urllib.parse.quote(full_url, safe='')}"

        for attempt in range(3):
            try:
                if attempt > 0: await asyncio.sleep(3 * attempt)
                # 💡 压测模式下，全量大包极易超时，设定 60 秒容忍度
                resp = await session.get(routed_url, headers=headers, cookies=self.trust_context["cookies"], timeout=60)
                
                if resp.status_code == 200:
                    text = resp.text.strip()
                    match = re.search(r'jQuery\d+_\d+\((.*)\)', text, re.DOTALL)
                    if match:
                        data = json.loads(match.group(1))
                        if data and data.get("rc") == 0: return data
                    
                    if attempt == 2: logger.debug(f"🔍 {label} 异常内容: {text[:100]}")
                
                self.stats["codes"][str(resp.status_code)] = self.stats["codes"].get(str(resp.status_code), 0) + 1
            except Exception as e:
                logger.debug(f"🕒 {label} 尝试 {attempt+1} 失败: {str(e)[:50]}")
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
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
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={fs_code}&fields=f12&ut={self.UT}")
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
            return list(all_codes)
        return [r[0] for r in existing] if existing else []

    async def sync_all_klines(self, sector_list: list):
        """Phase 2: 全量极限压测引擎"""
        logger.warning("🔥 [压测模式警告] 正在清空数据库历史，即将执行全量回溯！")
        self.conn.execute("DELETE FROM sector_klines")
        
        logger.info(f"🚀 [Phase 2] 全量并发压测启动 | 目标: {len(sector_list)} 个板块 | 并发限制: {self.concurrency}")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        progress_counter = {"done": 0, "total": len(sector_list)}
        start_time = time.time()
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = []
            for i, sid in enumerate(sector_list):
                # 💡 强制写死 19900101，彻底剥离增量逻辑
                url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={sid}"
                       f"&ut={self.UT}&fields1=f1,f2,f3,f4,f5,f6"
                       f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                       f"&klt=101&fqt=1&end=20500101&beg=19900101&lmt=100000")
                
                tasks.append(self._fetch_and_save(session, sid, url, semaphore, i*0.5, progress_counter))

            await asyncio.gather(*tasks)

        end_time = time.time()
        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        output_parquet = os.getenv("DATA_PATH", "data/sector_klines_full.parquet")
        self.conn.execute(f"COPY sector_klines TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        
        logger.success(f"📊 [压测报告] 全量同步完毕 | 落盘: {final_cnt} 行 | 耗时: {end_time - start_time:.1f} 秒 | TPS: {final_cnt/(end_time - start_time):.1f} 行/秒")

    async def _fetch_and_save(self, session, sid, url, sem, delay, progress):
        await asyncio.sleep(min(delay, 15))
        async with sem:
            data = await self._safe_request(session, url, f"K_{sid}", sid)
            
            # 进度条播报
            progress["done"] += 1
            if progress["done"] % 20 == 0 or progress["done"] == progress["total"]:
                logger.info(f"⏳ 进度更新: 已压测 {progress['done']} / {progress['total']} 个板块...")
                
            if data and data.get("data", {}).get("klines"):
                batch = []
                for k in data["data"]["klines"]:
                    r = k.split(',')
                    batch.append((sid, r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), float(r[6])))
                if batch:
                    # 💡 DuckDB 全量插入
                    self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                return True
            return False
