import json, os, asyncio, polars as pl
from loguru import logger
from curl_cffi.requests import AsyncSession

class MuscleEngine:
    def __init__(self):
        self._load_all_config()
        self.concurrency = int(os.getenv("CONCURRENCY", 20))
        self.impersonate = "chrome120"
        self.worker_url = os.getenv("CF_WORKER_URL", "").strip()

    def _load_all_config(self):
        path = "config/active_rules.json"
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.params = data.get("params", {})
                self.headers = data.get("headers", {})
        else:
            self.params, self.headers = {}, {}

    def reload_rules(self):
        self._load_all_config()
        logger.info("💪 [Muscle Engine] 行为指纹已热加载。")

    async def _request(self, session, url, params, timeout=30):
        actual_url = url
        p = {**params}
        if self.worker_url:
            p["api_url"] = url 
            actual_url = self.worker_url

        try:
            resp = await session.get(
                actual_url, params=p, headers=self.headers,
                impersonate=self.impersonate, timeout=timeout, verify=False
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.debug(f"⚠️ 请求异常 (将重试): {str(e)}")
        return {}

    async def probe(self) -> bool:
        if not self.params: return False
        logger.info("💪 [Muscle Engine] 执行分布式行为探针...")
        async with AsyncSession() as s:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            p = {"secid": "90.BK0896", "fields1": "f1", "fields2": "f51", "beg": "20240101", "end": "20990101", "lmt": "1", **self.params}
            data = await self._request(s, url, p)
            if data.get("data"):
                logger.success("💪 [Muscle Engine] 分布式链路验证通过。")
                return True
            return False

    async def _fetch_category(self, session, fs_code):
        """内部方法：抓取单个板块分类"""
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        p = {
            "pn": 1, "pz": 1000, "po": 1, "np": 1, 
            "fltt": 2, "invt": 2, "fid": "f3", 
            "fs": fs_code, "fields": "f12", 
            **self.params
        }
        res = await self._request(session, url, p)
        if res.get("data") and res["data"].get("diff"):
            diff = res["data"]["diff"]
            items = list(diff.values()) if isinstance(diff, dict) else diff
            return [f"90.{x['f12']}" for x in items]
        return []

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle Engine] 启动并行分类扫描...")
        async with AsyncSession() as session:
            # 💡 将三个分类由串行改为并行
            tasks = [
                self._fetch_category(session, "m:90+t:2"), # 行业
                self._fetch_category(session, "m:90+t:3"), # 概念
                self._fetch_category(session, "m:90+t:1")  # 地域
            ]
            results = await asyncio.gather(*tasks)
            
        # 扁平化并去重
        all_sectors = list(set([item for sublist in results for item in sublist]))
        logger.success(f"💪 [Muscle Engine] 目录扫描完成，共捕获 {len(all_sectors)} 个板块。")
        return all_sectors

    async def _fetch_single(self, session, secid, semaphore):
        async with semaphore:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            p = {
                "secid": secid, 
                "fields1": "f1,f2", 
                "fields2": "f51,f52,f53,f54,f55,f56", 
                "beg": "19900101", 
                "end": "20990101", 
                "lmt": "100000", 
                **self.params
            }
            data = await self._request(session, url, p)
            if data.get("data") and data["data"].get("klines"):
                return [{"secid": secid, "date": r.split(",")[0], "close": float(r.split(",")[2])} for r in data["data"]["klines"]]
            return []

    async def fetch_all_sectors(self, sector_list: list):
        if not sector_list: return
        logger.info(f"💪 [Muscle Engine] 开启全量同步，并发控制: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_data = []
        
        async with AsyncSession(max_clients=self.concurrency) as session:
            tasks = [self._fetch_single(session, s, semaphore) for s in sector_list]
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res: all_data.extend(res)
        
        if all_data:
            os.makedirs("data", exist_ok=True)
            pl.DataFrame(all_data).write_parquet("data/sector_klines_full.parquet")
            logger.success(f"💾 任务圆满完成，最终落盘 {len(all_data)} 行数据！")
