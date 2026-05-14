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
        # 1. 节点加载
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
        """Phase 0: 模拟浏览器访问板块详情页，获取完整 Cookie 链"""
        logger.info(f"🔑 [Phase 0] 启动 1:1 浏览器环境预热...")
        try:
            # 💡 随机选一个板块作为预热页
            sample_secid = "90.BK1063"
            response = await asyncio.to_thread(self._run_scrapling, sample_secid)
            
            cookies = response.cookies
            self.trust_context["cookies"] = {c['name']: c['value'] for c in cookies} if isinstance(cookies, list) else cookies
            
            # 💡 像素级复刻 cURL 里的 Headers
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
            logger.success(f"✅ 信任链就绪 | 已捕获浏览器级 Cookie: {len(self.trust_context['cookies'])} 枚")
        except Exception as e:
            logger.error(f"⚠️ 信任链构建异常: {e}")

    def _run_scrapling(self, secid):
        fetcher = Fetcher()
        # 💡 模拟用户真实进入板块页的行为
        return fetcher.get(f"https://quote.eastmoney.com/bk/{secid}.html")

    def _generate_jquery_cb(self):
        """模拟 jQuery 生成的随机回调函数名"""
        rand_part = "3510" + "".join(random.choices("0123456789", k=16))
        timestamp = int(time.time() * 1000)
        return f"jQuery{rand_part}_{timestamp}", timestamp

    async def _safe_request(self, session, url: str, label: str, secid: str) -> dict:
        """带 JSONP 模拟和 Referer 动态对齐的请求器"""
        cb_name, ts = self._generate_jquery_cb()
        # 💡 动态注入 jQuery 参数和 Referer，与 cURL 保持 1:1
        full_url = f"{url}&cb={cb_name}&_={ts + 5}"
        
        # 修正 Headers，针对每个板块对齐 Referer
        current_headers = self.trust_context["headers"].copy()
        current_headers["Referer"] = f"https://quote.eastmoney.com/bk/{secid}.html"
        
        worker_base = random.choice(self.worker_pool)
        if not worker_base.startswith("http"): worker_base = f"https://{worker_base}"
        routed_url = f"{worker_base}?url={urllib.parse.quote(full_url, safe='')}"

        for attempt in range(3):
            try:
                if attempt > 0: await asyncio.sleep(2 * attempt)
                resp = await session.get(routed_url, headers=current_headers, 
                                         cookies=self.trust_context["cookies"], timeout=45)
                
                if resp.status_code == 200:
                    text = resp.text.strip()
                    # 💡 严格的 JSONP 解包逻辑
                    match = re.search(r'jQuery\d+_\d+\((.*)\)', text, re.DOTALL)
                    if match:
                        data = json.loads(match.group(1))
                        if data and data.get("rc") == 0: return data
                
                self.stats["codes"][str(resp.status_code)] = self.stats["codes"].get(str(resp.status_code), 0) + 1
            except Exception as e:
                logger.debug(f"🕒 {label} 异常: {str(e)[:50]}")
        return {}

    async def sync_all_klines(self, sector_list: list):
        """Phase 2: 像素级镜像全量拉取"""
        logger.info(f"🚀 [Phase 2] 镜像抓取启动 | 目标: {len(sector_list)} 个板块")
        init_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        
        semaphore = asyncio.Semaphore(self.concurrency)
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = []
            for i, sid in enumerate(sector_list):
                # 💡 按照 cURL 的参数进行 1:1 像素复刻
                url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={sid}"
                       f"&ut={self.UT}&fields1=f1,f2,f3,f4,f5,f6"
                       f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                       f"&klt=101&fqt=1&end=20500101&lmt=1000000") # 💡 100万条，全量回溯！
                
                tasks.append(self._fetch_and_save(session, sid, url, semaphore, delay=i*0.3))

            await asyncio.gather(*tasks)

        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        self.conn.execute(f"COPY sector_klines TO '{os.getenv('DATA_PATH')}' (FORMAT PARQUET, COMPRESSION ZSTD)")
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
                # 💡 拿到数据后立即执行 DuckDB INSERT
                if batch:
                    self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                return True
            return False
