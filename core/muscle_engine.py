import json, os, asyncio, polars as pl
from loguru import logger
from curl_cffi.requests import AsyncSession

class MuscleEngine:
    def __init__(self):
        self._load_all_config()
        self.concurrency = int(os.getenv("CONCURRENCY", 20))
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

    async def _request(self, session, url, params):
        actual_url = url
        p = {**params}
        
        if self.worker_url:
            p["api_url"] = url 
            actual_url = self.worker_url

        try:
            # 💡 核心：如果 H2 (Chrome120) 持续断开，强制降级到 http/1.1
            # 许多 WAF 对 HTTP/2 的指纹校验极严，但对 H1.1 相对宽松
            resp = await session.get(
                actual_url, 
                params=p, 
                headers=self.headers,
                impersonate="chrome120", 
                # http_version=2, # 如果依然 56 错误，可以尝试改为 1 
                timeout=15,
                verify=False
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.debug(f"⚠️ 网络链路抖动: {str(e)}")
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

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle Engine] 拉取板块动态目录...")
        sectors = []
        async with AsyncSession() as s:
            for fs in ["m:90+t:2", "m:90+t:3", "m:90+t:1"]:
                url = "https://push2.eastmoney.com/api/qt/clist/get"
                p = {"pn": 1, "pz": 2000, "po": 1, "np": 1, "fltt": 2, "invt": 2, "fid": "f3", "fs": fs, "fields": "f12", **self.params}
                res = await self._request(s, url, p)
                if res.get("data") and res["data"].get("diff"):
                    diff = res["data"]["diff"]
                    items = list(diff.values()) if isinstance(items, dict) else diff
                    sectors.extend([f"90.{x['f12']}" for x in items])
        return list(set(sectors))

    async def _fetch_single(self, session, secid, semaphore):
        async with semaphore:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            p = {"secid": secid, "fields1": "f1,f2", "fields2": "f51,f52,f53,f54,f55,f56", "beg": "19900101", "end": "20990101", "lmt": "100000", **self.params}
            data = await self._request(session, url, p)
            return [x.split(",") for x in data["data"]["klines"]] if data.get("data") else []

    async def fetch_all_sectors(self, sector_list: list):
        if not sector_list: return
        logger.info(f"💪 [Muscle Engine] 开启多路并发同步，节点: {self.worker_url or '直连'}")
        semaphore = asyncio.Semaphore(self.concurrency)
        results = []
        async with AsyncSession(max_clients=self.concurrency) as session:
            tasks = [self._fetch_single(session, s, semaphore) for s in sector_list]
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res:
                    for r in res:
                        results.append({"date": r[0], "open": r[1], "close": r[2], "high": r[3], "low": r[4], "vol": r[5]})
        
        if results:
            os.makedirs("data", exist_ok=True)
            pl.DataFrame(results).write_parquet("data/sector_klines_full.parquet")
            logger.success(f"💾 任务圆满完成，落盘 {len(results)} 行。")
