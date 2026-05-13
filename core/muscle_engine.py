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
from scrapling import Stealer

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
            logger.critical("🚨 未配置 CF_WORKER_URLS，请检查环境变量！")
            
        self.concurrency = int(os.getenv("CONCURRENCY", 10))
        self.data_path = os.getenv("DATA_PATH", "data/sector_klines_full.parquet")
        self.impersonate = "chrome124"
        self.trust_context = {"cookies": {}, "headers": {}}
        
        # 统计指标
        self.stats = {"total": 0, "errors": 0, "codes": {}}

    async def build_trust_chain(self):
        """Phase 0: 使用 Scrapling 建立信任链，导出 Cookie 和指纹"""
        logger.info("🔑 [Phase 0] 启动 Scrapling 建立信任链...")
        try:
            # 仅在获取初始信任态时启动轻量级浏览器
            stealer = Stealer(headless=True)
            result = stealer.get("https://quote.eastmoney.com/center/hsbk.html")
            
            # 提取东财核心风控 Cookie
            self.trust_context["cookies"] = result.cookies
            self.trust_context["headers"] = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/",
                "Accept": "*/*"
            }
            logger.success(f"✅ 信任链构建成功，获取 Cookie: {len(result.cookies)} 项")
            stealer.quit() # 立即释放内存
        except Exception as e:
            logger.error(f"⚠️ 信任链构建失败 (将尝试空态运行): {e}")

    def _route_url(self, target_url: str, use_smart_cache: bool = False) -> str:
        """Worker 池化随机路由 + 分级缓存策略"""
        # 如果是 K 线请求，使用 30 秒窗口缓存以降低回源压力
        if use_smart_cache:
            cache_window = int(time.time() / 30)
            target_url += f"&_ts={cache_window}"
        else:
            target_url += f"&_cb={time.time_ns()}"

        worker_base = random.choice(self.worker_pool)
        return f"{worker_base}?url={urllib.parse.quote(target_url, safe='')}"

    async def _safe_request(self, session, url: str, label: str, cache: bool = False) -> dict:
        routed = self._route_url(url, use_smart_cache=cache)
        for attempt in range(3):
            try:
                await asyncio.sleep(random.uniform(0.1, 0.5) * attempt)
                resp = await session.get(
                    routed, 
                    headers=self.trust_context["headers"], 
                    cookies=self.trust_context["cookies"],
                    timeout=25
                )
                self.stats["total"] += 1
                if resp.status_code == 200:
                    # 兼容 JSONP
                    text = resp.text
                    match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
                    return json.loads(match.group(1) if match else text)
                
                self.stats["errors"] += 1
                self.stats["codes"][resp.status_code] = self.stats["codes"].get(resp.status_code, 0) + 1
            except Exception as e:
                self.stats["errors"] += 1
                logger.debug(f"🕒 {label} 网络波动: {e}")
        return {}

    async def fetch_dynamic_sector_list(self) -> list:
        """Phase 1: 高韧性分页扫描目录"""
        logger.info("📡 [Phase 1] 启动高韧性目录扫描...")
        all_codes = set()
        categories = {"地域": "m:90+t:1", "行业": "m:90+t:2", "概念": "m:90+t:3"}
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs in categories.items():
                empty_tolerance = 0
                for pn in range(1, 15): # 扩大扫描范围
                    if empty_tolerance >= 3: break # 连续 3 页失败/空才停止
                    
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={urllib.parse.quote(fs)}&fields=f12&ut={self.UT}")
                    
                    data = await self._safe_request(session, url, f"LIST_{cat_name}_P{pn}")
                    if data and data.get("data") and data["data"].get("diff"):
                        items = data["data"]["diff"]
                        for x in items: all_codes.add(f"90.{x['f12']}")
                        empty_tolerance = 0 # 重置容错计数
                        if len(items) < 250: break
                    else:
                        empty_tolerance += 1
                        logger.warning(f"⚠️ {cat_name} 第 {pn} 页异常，容错计数: {empty_tolerance}/3")
        
        logger.success(f"💪 扫描完成，共获取 {len(all_codes)} 个活跃板块")
        return list(all_codes)

    def get_last_dates(self) -> dict:
        """利用 DuckDB 极速提取存量数据的最后日期"""
        if not os.path.exists(self.data_path):
            return {}
        try:
            logger.info("🔎 正在提取存量数据最后截止日期...")
            # DuckDB 谓词下推扫描，内存占用极低
            df_dates = duckdb.query(f"""
                SELECT secid, MAX(CAST(date AS VARCHAR)) as max_date 
                FROM read_parquet('{self.data_path}') 
                GROUP BY secid
            """).pl()
            return dict(zip(df_dates["secid"], df_dates["max_date"]))
        except Exception as e:
            logger.error(f"❌ 提取存量日期失败: {e}")
            return {}

    async def fetch_all_sectors(self, sector_list: list):
        """Phase 2: 增量并发引擎"""
        last_dates = self.get_last_dates()
        logger.info(f"🚀 [Phase 2] 启动增量并发引擎 | 存量锚点: {len(last_dates)} 个")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = []
            for sid in sector_list:
                # 识别增量日期：如果是存量板块，则从最后一天开始拉取
                last_d = last_dates.get(sid, "19900101").replace("-", "")
                tasks.append(self._fetch_kline_incremental(session, sid, last_d, semaphore))
            
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res: all_results.extend(res)
        
        if all_results:
            self._save_data(all_results)

    async def _fetch_kline_incremental(self, session, secid: str, beg_date: str, sem: asyncio.Semaphore):
        async with sem:
            url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                   f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                   f"&klt=101&fqt=0&end=20500101&beg={beg_date}&lmt=100000&ut={self.UT}")
            
            # 使用智能缓存路由，降低回源频率
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
            # 增量合并与去重
            history_df = pl.read_parquet(self.data_path)
            final_df = pl.concat([history_df, new_df]).unique(subset=["secid", "date"], keep="last")
            logger.info(f"📊 增量合并完成: 存量 {len(history_df)} + 新增 {len(new_data)} -> 总计 {len(final_df)}")
        else:
            final_df = new_df
            logger.info(f"📊 初始落盘: {len(final_df)} 行")
            
        final_df.sort(["secid", "date"]).write_parquet(self.data_path, compression="zstd")
        logger.success(f"💾 数据已安全持久化至 {self.data_path}")
