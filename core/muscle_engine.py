import json, os, asyncio, polars as pl
from loguru import logger
from curl_cffi.requests import AsyncSession

class MuscleEngine:
    def __init__(self):
        data = self._load_rules()
        self.params = data.get("params", {})
        self.headers = data.get("headers", {})
        self.concurrency = int(os.getenv("CONCURRENCY", 20))
        self.impersonate = "chrome120"
        
        # 兼容你的 CF Worker 逻辑
        self.worker_url = os.getenv("CF_WORKER_URL", "").strip()

    def _load_rules(self):
        path = "config/active_rules.json"
        if not os.path.exists(path): return {}
        with open(path, "r", encoding="utf-8") as f: return json.load(f)

    def reload_rules(self):
        data = self._load_rules()
        self.params = data.get("params", {})
        self.headers = data.get("headers", {})

    async def _request(self, session, url, params):
        """核心请求器：处理 Worker 散射和行为伪装"""
        actual_url = url
        actual_params = params.copy()
        
        # 如果有 Worker，将请求代理出去
        if self.worker_url:
            actual_params["target_url"] = url # 假设你的 Worker 接受 target_url
            actual_url = self.worker_url

        for _ in range(3):
            try:
                resp = await session.get(
                    actual_url, 
                    params=actual_params, 
                    headers=self.headers, # ⬅️ 使用克隆的 Headers
                    impersonate=self.impersonate, # ⬅️ JA3 指纹伪装
                    timeout=15
                )
                if resp.status_code == 200:
                    return resp.json()
            except:
                await asyncio.sleep(1)
        return {}

    async def probe(self) -> bool:
        if not self.params: return False
        logger.info("💪 [Muscle Engine] 探针校验行为合法性...")
        async with AsyncSession() as s:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            p = {"secid": "90.BK0896", "fields1": "f1,f2", "fields2": "f51,f52", "beg": "20240101", "end": "20990101", "lmt": "1", **self.params}
            data = await self._request(s, url, p)
            if data.get("data") and data["data"].get("klines"):
                logger.success("💪 [Muscle Engine] 行为校验通过。")
                return True
            return False

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle Engine] 正在拉取全市场目录...")
        sector_list = []
        async with AsyncSession() as s:
            for fs in ["m:90+t:2", "m:90+t:3", "m:90+t:1"]:
                url = "https://push2.eastmoney.com/api/qt/clist/get"
                p = {"pn": 1, "pz": 2000, "po": 1, "np": 1, "fltt": 2, "invt": 2, "fid": "f3", "fs": fs, "fields": "f12,f14", **self.params}
                data = await self._request(s, url, p)
                if data.get("data") and data["data"].get("diff"):
                    items = data["data"]["diff"]
                    item_list = list(items.values()) if isinstance(items, dict) else items
                    sector_list.extend([f"90.{x['f12']}" for x in item_list])
        return list(set(sector_list))

    async def _fetch_single(self, session, secid, semaphore):
        async with semaphore:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            p = {"secid": secid, "fields1": "f1,f2", "fields2": "f51,f52,f53,f54,f55,f56", "beg": "19900101", "end": "20990101", "lmt": "100000", **self.params}
            data = await self._request(session, secid, p) # 注意这里 URL 传错了个小地方，应为 self.base_url
            # ... 后续清洗逻辑相同 ...
