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
        raw_env = os.getenv("CF_WORKER_URLS") or os.getenv("CF_WORKER_URL") or ""
        self.worker_pool = [u.strip() for u in raw_env.split(",") if u.strip()]
        if not self.worker_pool: raise RuntimeError("🚨 未配置 CF_WORKER_URLS")
            
        self.concurrency = int(os.getenv("CONCURRENCY", 5))
        self.db_path = "data/sector_quant.db"
        self.impersonate = "chrome124"
        self.trust_context = {"cookies": {}, "headers": {}}
        self.stats = {"total": 0, "errors": 0, "codes": {}} # 重置统计
        
        os.makedirs("data", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_klines (secid VARCHAR, date DATE, open DOUBLE, close DOUBLE, high DOUBLE, low DOUBLE, volume DOUBLE, amount DOUBLE, PRIMARY KEY(secid, date))")
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_master (secid VARCHAR PRIMARY KEY, last_update TIMESTAMP)")

    async def build_trust_chain(self):
        """Phase 0: 强制认证态生成"""
        logger.info(f"🔑 [Phase 0] 正在通过 Scrapling 提取高权指纹...")
        try:
            # 💡 访问这个地址会强制浏览器生成风控指纹
            response = await asyncio.to_thread(self._run_scrapling)
            cookies = response.cookies
            self.trust_context["cookies"] = {c['name']: c['value'] for c in cookies} if isinstance(cookies, list) else cookies
            
            # 💡 如果 Scrapling 还是拿不到，手动注入一个通用的标识
            if not self.trust_context["cookies"]:
                self.trust_context["cookies"] = {"st_pvi": str(int(time.time()*1000))}
            
            self.trust_context["headers"] = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/center/hsbk.html",
                "Accept": "*/*"
            }
            logger.success(f"✅ 信任链就绪 | Cookie: {len(self.trust_context['cookies'])} 枚")
        except Exception as e:
            logger.error(f"⚠️ 信任链构建失败: {e}")

    def _run_scrapling(self):
        fetcher = Fetcher()
        # 💡 改为访问列表 API 的前端承载页
        return fetcher.get("https://quote.eastmoney.com/center/gridlist.html#hs_bks")

    def _route_url(self, target_url: str, use_cache: bool = False) -> str:
        worker_base = random.choice(self.worker_pool)
        if not worker_base.startswith("http"): worker_base = f"https://{worker_base}"
        # 增加 Cache-Buster
        suffix = f"&_ts={int(time.time()/30)}" if use_cache else f"&_cb={time.time_ns()}"
        return f"{worker_base}?url={urllib.parse.quote(target_url + suffix, safe='')}"

    async def _safe_request(self, session, url: str, label: str, cache: bool = False) -> dict:
        routed = self._route_url(url, use_cache=cache)
        for attempt in range(3):
            try:
                # 💡 每次请求前增加微量抖动
                await asyncio.sleep(random.uniform(0.2, 0.5) * attempt)
                resp = await session.get(routed, headers=self.trust_context["headers"], 
                                         cookies=self.trust_context["cookies"], timeout=20)
                
                # 只有主请求计入总数，重试不重复计入 total 以免失败率破 100%
                if attempt == 0: self.stats["total"] += 1
                
                if resp.status_code == 200:
                    text = resp.text.strip()
                    if "(" in text and ")" in text and text.startswith("jQuery"):
                        text = re.search(r'\((.*)\)', text, re.DOTALL).group(1)
                    
                    data = json.loads(text)
                    if data and data.get("rc") == 0: return data
                    
                    # 如果业务失败 (rc != 0)，记录错误日志
                    logger.debug(f"❌ {label} 业务失败 rc={data.get('rc')} | Body: {text[:100]}")
                else:
                    self.stats["codes"][str(resp.status_code)] = self.stats["codes"].get(str(resp.status_code), 0) + 1
            except Exception as e:
                if attempt == 2: logger.error(f"🕒 {label} 最终重试失败: {str(e)[:100]}")
        
        self.stats["errors"] += 1
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        existing = self.conn.execute("SELECT secid FROM sector_master").fetchall()
        if existing and not force_reconcile:
            logger.success(f"✅ [Phase 1] 缓存命中使用 {len(existing)} 板块")
            return [r[0] for r in existing]

        logger.info("📡 [Phase 1] 扫描板块名录...")
        all_codes = set()
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs_code in self.CATEGORIES.items():
                cat_count = 0
                for pn in range(1, 10):
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={fs_code}&fields=f12&ut={self.UT}")
                    data = await self._safe_request(session, url, f"LIST_{cat_name}_P{pn}")
                    if data and data.get("data", {}).get("diff"):
                        for x in data["data"]["diff"]:
                            if x.get("f12"):
                                all_codes.add(f"90.{x['f12']}")
                                cat_count += 1
                        if len(data["data"]["diff"]) < 250: break
                    else: break
                logger.info(f"   ∟ [{cat_name}] 发现 {cat_count} 个")
        
        if all_codes:
            self.conn.execute("DELETE FROM sector_master")
            self.conn.executemany("INSERT INTO sector_master VALUES (?, ?)", [(c, datetime.now()) for c in all_codes])
            logger.success(f"✅ [Phase 1] 捕获 {len(all_codes)} 个板块")
            return list(all_codes)
        return [r[0] for r in existing] if existing else []

    async def sync_all_klines(self, sector_list: list):
        init_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        # 获取锚点逻辑
        res = self.conn.execute("SELECT secid, MAX(date) FROM sector_klines GROUP BY secid").fetchall()
        anchors = {row[0]: (row[1] + timedelta(days=1)).strftime("%Y%m%d") for row in res if row[1]}
        
        logger.info(f"🚀 [Phase 2] 同步中 | 目标: {len(sector_list)} | 存量锚点: {len(anchors)}")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single(session, sid, anchors.get(sid, "19900101"), semaphore) for sid in sector_list]
            for coro in asyncio.as_completed(tasks):
                batch = await coro
                if batch:
                    self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
        
        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        added = final_cnt - init_cnt
        
        # 导出 Parquet
        output_parquet = os.getenv("DATA_PATH", "data/sector_klines_full.parquet")
        self.conn.execute(f"COPY sector_klines TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        logger.success(f"📊 [Final Report] 总行数: {final_count if 'final_count' in locals() else final_cnt} | 新增: {added}")

    async def _fetch_single(self, session, secid, beg_date, sem):
        async with sem:
            # 💡 终极修复：字段补全到 f61，并增加 lmt 确保历史深度
            clean_beg = beg_date.replace("-", "")
            url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                   f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                   f"&klt=101&fqt=0&end=20500101&beg={clean_beg}&lmt=50000&ut={self.UT}")
            
            data = await self._safe_request(session, url, f"K_{secid}", cache=True)
            if data and data.get("data", {}).get("klines"):
                rows = []
                for k in data["data"]["klines"]:
                    r = k.split(',')
                    rows.append((secid, r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), float(r[6])))
                return rows
            return None
