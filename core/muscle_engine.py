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
        """加载环境快照和参数"""
        path = "config/active_rules.json"
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.params = data.get("params", {})
                self.headers = data.get("headers", {})
        else:
            self.params = {}
            self.headers = {}

    def reload_rules(self):
        """主控程序自愈后调用，重新加载内存配置"""
        self._load_all_config()
        logger.info("💪 [Muscle Engine] 配置已动态重载。")

    async def _request(self, session, url, params):
        """高度仿生的散射请求器"""
        actual_url = url
        p = {**params}
        
        # 💡 散射逻辑：如果设置了 Worker，则通过 Worker 转发
        if self.worker_url:
            # 根据你 Worker 的脚本逻辑，这里通常需要将目标 URL 传给 Worker
            # 假设你的 Worker 接口字段是 api_url
            p["api_url"] = url 
            actual_url = self.worker_url

        try:
            # 💡 行为伪装核心：同时使用 TLS 指纹伪装 + 浏览器真实 Header 快照
            resp = await session.get(
                actual_url, 
                params=p, 
                headers=self.headers,
                impersonate=self.impersonate, 
                timeout=15,
                verify=False # 在 GitHub Actions 环境下，有时需要忽略 SSL 校验以防证书链报错
            )
            if resp.status_code == 200:
                return resp.json()
            else:
                logger.debug(f"HTTP 异常: {resp.status_code}")
        except Exception as e:
            logger.debug(f"请求异常: {str(e)}")
        return {}

    async def probe(self) -> bool:
        """行为合法性探针"""
        if not self.params: return False
        logger.info("💪 [Muscle Engine] 正在通过中转执行环境探针...")
        async with AsyncSession() as s:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            p = {
                "secid": "90.BK0896", 
                "fields1": "f1,f2", 
                "fields2": "f51,f52", 
                "beg": "20240101", 
                "end": "20990101", 
                "lmt": "1", 
                **self.params
            }
            data = await self._request(s, url, p)
            if data.get("data") and data["data"].get("klines"):
                logger.success("💪 [Muscle Engine] 环境校验成功，分布式链路已就绪。")
                return True
            return False

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle Engine] 正在通过环境仿真拉取板块目录...")
        sectors = []
        async with AsyncSession() as s:
            for fs in ["m:90+t:2", "m:90+t:3", "m:90+t:1"]:
                url = "https://push2.eastmoney.com/api/qt/clist/get"
                p = {
                    "pn": 1, "pz": 2000, "po": 1, "np": 1, 
                    "fltt": 2, "invt": 2, "fid": "f3", 
                    "fs": fs, "fields": "f12,f14", 
                    **self.params
                }
                res = await self._request(s, url, p)
                if res.get("data") and res["data"].get("diff"):
                    diff = res["data"]["diff"]
                    items = list(diff.values()) if isinstance(diff, dict) else diff
                    sectors.extend([f"90.{x['f12']}" for x in items])
        
        sectors = list(set(sectors))
        logger.success(f"💪 [Muscle Engine] 目录拉取完成，发现 {len(sectors)} 个板块。")
        return sectors

    async def _fetch_single(self, session, secid, semaphore):
        """单板块拉取逻辑"""
        async with semaphore:
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            p = {
                "secid": secid, 
                "fields1": "f1,f2,f3,f4,f5,f6", 
                "fields2": "f51,f52,f53,f54,f55,f56", 
                "beg": "19900101", 
                "end": "20990101", 
                "lmt": "100000", 
                **self.params
            }
            data = await self._request(session, url, p)
            if data.get("data") and data["data"].get("klines"):
                return [x.split(",") for x in data["data"]["klines"]]
            return []

    async def fetch_all_sectors(self, sector_list: list):
        if not sector_list: return
        logger.info(f"💪 [Muscle Engine] 历史数据同步启动，并发数: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        results = []
        
        async with AsyncSession(max_clients=self.concurrency) as session:
            tasks = [self._fetch_single(session, s, semaphore) for s in sector_list]
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res:
                    # 注意：secid 在这里需要处理，为了简化，结果集里建议带上 code
                    # 这里假设 res[0] 这种结构，根据需要可以进一步结构化
                    for r in res:
                        results.append({
                            "date": r[0], "open": r[1], "close": r[2], 
                            "high": r[3], "low": r[4], "vol": r[5], "amount": r[6]
                        })
        
        if results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(results)
            # 简单转换
            df = df.with_columns([
                pl.col("open").cast(pl.Float32, strict=False),
                pl.col("close").cast(pl.Float32, strict=False),
                pl.col("vol").cast(pl.Float64, strict=False)
            ])
            df.write_parquet("data/sector_klines_full.parquet")
            logger.success(f"💾 数据同步完成，落盘 {len(results)} 行。")
