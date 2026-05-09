import json, os, asyncio, polars as pl
from loguru import logger
from curl_cffi.requests import AsyncSession

class MuscleEngine:
    def __init__(self):
        data = self._load_rules()
        self.params = data.get("params", {})
        self.headers = data.get("headers", {})
        self.concurrency = int(os.getenv("CONCURRENCY", 20))
        self.worker_url = os.getenv("CF_WORKER_URL", "").strip() # ⬅️ 从环境变量读取 Worker 地址

    def _load_rules(self):
        path = "config/active_rules.json"
        if not os.path.exists(path): return {}
        with open(path, "r", encoding="utf-8") as f: return json.load(f)

    async def _request(self, session, url, params):
        """支持分布式散射的通用请求器"""
        actual_url = url
        # 组装参数
        p = {**params}
        
        # 💡 如果配置了 Worker，则通过代理中转
        if self.worker_url:
            # 假设你的 Worker 逻辑是解析 query 里的 target_url
            p["api_url"] = url 
            actual_url = self.worker_url

        try:
            resp = await session.get(
                actual_url, params=p, headers=self.headers,
                impersonate="chrome120", timeout=15
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.debug(f"请求失败: {e}")
        return {}

    async def probe(self) -> bool:
        if not self.params: return False
        logger.info("💪 [Muscle Engine] 正在通过中转执行环境探针...")
        async with AsyncSession() as s:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            p = {"secid": "90.BK0896", "fields1": "f1,f2", "fields2": "f51,f52", "beg": "20240101", "end": "20990101", "lmt": "1", **self.params}
            data = await self._request(s, url, p)
            if data.get("data") and data["data"].get("klines"):
                logger.success("💪 [Muscle Engine] 环境校验成功，Worker 链路畅通。")
                return True
            return False

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle Engine] 动态拉取板块目录...")
        sectors = []
        async with AsyncSession() as s:
            for fs in ["m:90+t:2", "m:90+t:3", "m:90+t:1"]:
                url = "https://push2.eastmoney.com/api/qt/clist/get"
                p = {"pn": 1, "pz": 2000, "po": 1, "np": 1, "fltt": 2, "invt": 2, "fid": "f3", "fs": fs, "fields": "f12,f14", **self.params}
                res = await self._request(s, url, p)
                if res.get("data") and res["data"].get("diff"):
                    diff = res["data"]["diff"]
                    items = list(diff.values()) if isinstance(diff, dict) else diff
                    sectors.extend([f"90.{x['f12']}" for x in items])
        return list(set(sectors))

    async def _fetch_single(self, session, secid, semaphore):
        async with semaphore:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            p = {"secid": secid, "fields1": "f1,f2,f3,f4,f5,f6", "fields2": "f51,f52,f53,f54,f55,f56", "beg": "19900101", "end": "20990101", "lmt": "100000", **self.params}
            data = await self._request(session, url, p)
            if data.get("data") and data["data"].get("klines"):
                return [x.split(",") for x in data["data"]["klines"]]
            return []

    # ... fetch_all_sectors 逻辑保持不变 ...
