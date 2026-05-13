import asyncio
import json
import re
import os
import time
import random
import urllib.parse
import polars as pl
import duckdb
from loguru import logger
from curl_cffi.requests import AsyncSession
from scrapling import Fetcher

class MuscleEngine:
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        # 1. Worker 池化加载
        raw_urls = os.getenv("CF_WORKER_URLS", "").split(",")
        self.worker_pool = [
            (url.strip() if url.startswith("http") else f"https://{url.strip()}")
            for url in raw_urls if url.strip()
        ]
        if not self.worker_pool:
            logger.critical("🚨 未检测到 CF_WORKER_URLS，请检查环境变量设置！")
            
        self.concurrency = int(os.getenv("CONCURRENCY", 10))
        self.data_path = os.getenv("DATA_PATH", "data/sector_klines_full.parquet")
        self.impersonate = "chrome124"
        self.trust_context = {"cookies": {}, "headers": {}}
        self.stats = {"total": 0, "errors": 0, "codes": {}}

    async def build_trust_chain(self):
        """Phase 0: 使用 Scrapling Fetcher 建立信任链"""
        logger.info("🔑 [Phase 0] 启动 Scrapling 建立信任态...")
        try:
            # 自动处理 Stealth 指纹和挑战
            fetcher = Fetcher(auto_match=True)
            response = fetcher.get("https://quote.eastmoney.com/center/hsbk.html")
            
            # 导出 Cookies (Scrapling 返回的是列表或字典，统一转为字典)
            raw_cookies = response.cookies
            if isinstance(raw_cookies, list):
                self.trust_context["cookies"] = {c['name']: c['value'] for c in raw_cookies}
            else:
                self.trust_context["cookies"] = raw_cookies

            self.trust_context["headers"] = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/",
                "Accept": "*/*"
            }
            logger.success(f"✅ 信任链构建成功，已同步 {len(self.trust_context['cookies'])} 项风控 Cookie")
        except Exception as e:
            logger.error(f"⚠️ 信任链构建失败: {e}，系统将尝试降级运行")

    def _route_url(self, target_url: str, use_smart_cache: bool = False) -> str:
        """Worker 随机路由 + 智能缓存键"""
        if use_smart_cache:
            # 30秒缓存窗口，平衡实时性与命中率
            target_url += f"&_ts={int(time.time() / 30)}"
        else:
            target_url += f"&_cb={time.time_ns()}"

        worker_base = random.choice(self.worker_pool)
        return f"{worker_base}?url={urllib.parse.quote(target_url, safe='')}"

    async def _safe_request(self, session, url: str, label: str, cache: bool = False) -> dict:
        routed = self._route_url(url, use_smart_cache=cache)
        for attempt in range(3):
            try:
                await asyncio.sleep(random.uniform(0.1, 0.4) * attempt)
                resp = await session.get(
                    routed, 
                    headers=self.trust_context["headers"], 
                    cookies=self.trust_context["cookies"],
                    timeout=25
                )
                self.stats["total"] += 1
                if resp.status_code == 200:
                    text = resp.text
                    match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
                    return json.loads(match.group(1) if match else text)
                
                self.stats["errors"] += 1
                self.stats["codes"][resp.status_code] = self.stats["codes"].get(resp.status_code, 0) + 1
            except Exception as e:
                self.stats["errors"] += 1
                logger.debug(f"🕒 {label} 网络抖动: {e}")
        return {}

    async def fetch_dynamic_sector_list(self) -> list:
        """Phase 1: 高韧性多维度扫描"""
        logger.info("📡 [Phase 1] 扫描活跃板块名录...")
        all_codes = set()
        categories = {"地域": "m:90+t:1", "行业": "m:90+t:2", "概念": "m:90+t:3"}
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs in categories.items():
                empty_count = 0
                for pn in range(1, 10): 
                    if empty_count >= 2: break 
                    
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={urllib.parse.quote(fs)}&fields=f12&ut={self.UT}")
                    
                    data = await self._safe_request(session, url, f"LIST_{cat_name}_P{pn}")
                    if data and data.get("data") and data["data"].get("diff"):
                        items = data["data"]["diff"]
                        for x in items: all_codes.add(f"90.{x['f12']}")
                        empty_count = 0
                        if len(items) < 250: break
                    else:
                        empty_count += 1
        
        logger.success(f"💪 扫描完成，捕获活跃板块: {len(all_codes)} 个")
        return list(all_codes)

    def get_last_dates(self) -> dict:
        """DuckDB 极速增量锚点提取"""
        if not os.path.exists(self.data_path):
            return {}
        try:
            # 仅扫描 Parquet 元数据，不加载全量数据入内存
            con = duckdb.connect(database=':memory:')
            res = con.execute(f"""
                SELECT secid, MAX(date) as max_date 
                FROM read_parquet('{self.data_path}') 
                GROUP BY secid
            """).fetchall()
            return {row[0]: row[1].replace("-", "") for row in res}
        except Exception as e:
            logger.error(f"❌ 增量锚点提取失败: {e}")
            return {}

    async def fetch_all_sectors(self, sector_list: list):
        """Phase 2: 增量并发同步"""
        last_dates = self.get_last_dates()
        logger.info(f"🚀 [Phase 2] 并发同步启动 | 增量模式: {len(last_dates)} 板块已存在")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = []
            for sid in sector_list:
                beg_d = last_dates.get(sid, "19900101")
                tasks.append(self._fetch_kline_incremental(session, sid, beg_d, semaphore))
            
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res: all_results.extend(res)
        
        if all_results:
            self._save_data(all_results)

    async def _fetch_kline_incremental(self, session, secid: str, beg_date: str, sem: asyncio.Semaphore):
        async with sem:
            # 去除日期中的横杠以匹配 API 格式
            clean_beg = beg_date.replace("-", "")
            url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                   f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                   f"&klt=101&fqt=0&end=20500101&beg={clean_beg}&lmt=100000&ut={self.UT}")
            
            data = await self._safe_request(session, url, f"KLINE_{secid}", cache=True)
            if data and data.get("data") and data["data"].get("klines"):
                res = []
                for r in data["data"]["klines"]:
                    row = r.split(",")
                    res.append({
                        "secid": secid, "date": row[0],
                        "open": float(row[1]), "close": float(row[2]),
                        "high": float(row[3]), "low": float(row[4]),
                        "volume": float(row[5]), "amount": float(row[6])
                    })
                return res
            return []

    def _save_data(self, new_data: list):
        os.makedirs("data", exist_ok=True)
        new_df = pl.DataFrame(new_data)
        
        if os.path.exists(self.data_path):
            hist_df = pl.read_parquet(self.data_path)
            # 合并、去重（保留最新的数据行）、排序
            final_df = pl.concat([hist_df, new_df]).unique(subset=["secid", "date"], keep="last").sort(["secid", "date"])
            logger.info(f"📊 增量合并: 历史 {len(hist_df)} + 新增 {len(new_data)} -> 总量 {len(final_df)}")
        else:
            final_df = new_df.sort(["secid", "date"])
            logger.info(f"📊 初始建库: {len(final_df)} 行")
            
        final_df.write_parquet(self.data_path, compression="zstd")
        logger.success(f"💾 数据已安全存入 {self.data_path}")
