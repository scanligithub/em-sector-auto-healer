import json
import os
import asyncio
import polars as pl
from loguru import logger
from curl_cffi.requests import AsyncSession

class MuscleEngine:
    def __init__(self):
        self.rules = self._load_rules()
        self.impersonate = "chrome120" 
        self.concurrency = int(os.getenv("CONCURRENCY", 20))
        # 💡 核心修正：历史数据必须请求 push2his 域名
        self.base_url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        self.list_url = "https://push2.eastmoney.com/api/qt/clist/get"
        
    def _load_rules(self):
        rule_path = "config/active_rules.json"
        if not os.path.exists(rule_path): return {}
        try:
            with open(rule_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except: return {}
            
    def reload_rules(self):
        self.rules = self._load_rules()

    async def probe(self) -> bool:
        """探针：验证板块历史数据提取权限"""
        if not self.rules or "ut" not in self.rules: return False
        logger.info("💪 [Muscle Engine] 正在发射探针，验证历史数据接口...")
        try:
            async with AsyncSession(impersonate=self.impersonate) as session:
                params = {
                    "secid": "90.BK0896", 
                    "fields1": "f1,f2,f3,f4,f5,f6",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
                    "beg": "20230101",
                    "end": "20500101",
                    "lmt": "10",
                    **self.rules
                }
                resp = await session.get(self.base_url, params=params, timeout=10)
                data = resp.json()
                if data.get("rc") == 0 and data.get("data") and data["data"].get("klines"):
                    logger.success("💪 [Muscle Engine] 探针通过，历史数据通道开启！")
                    return True
                logger.warning(f"💪 [Muscle Engine] 探针未通过，报文: {str(data)[:100]}")
                return False
        except Exception as e:
            logger.error(f"💪 [Muscle Engine] 网络异常: {e}")
            return False

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle Engine] 正在动态扫描全市场板块目录...")
        sector_list = []
        targets = ["m:90+t:2", "m:90+t:3", "m:90+t:1"] 
        try:
            async with AsyncSession(impersonate=self.impersonate) as session:
                for fs in targets:
                    params = {
                        "pn": 1, "pz": 2000, "po": 1, "np": 1,
                        "fltt": 2, "invt": 2, "fid": "f3",
                        "fs": fs, "fields": "f12,f13,f14",
                        **self.rules
                    }
                    resp = await session.get(self.list_url, params=params, timeout=15)
                    data = resp.json()
                    if data.get("data") and data["data"].get("diff"):
                        items = data["data"]["diff"]
                        item_list = list(items.values()) if isinstance(items, dict) else items
                        for item in item_list:
                            code = item.get("f12")
                            if code: sector_list.append(f"90.{code}")
        except Exception as e:
            logger.error(f"💪 [Muscle Engine] 目录获取异常: {e}")
        sector_list = list(set(sector_list))
        logger.success(f"💪 [Muscle Engine] 共发现 {len(sector_list)} 个板块。")
        return sector_list

    async def _fetch_single_sector(self, session, secid, semaphore):
        async with semaphore:
            params = {
                "secid": secid,
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
                "beg": "19900101",
                "end": "20500101",
                "lmt": "100000",
                **self.rules
            }
            try:
                resp = await session.get(self.base_url, params=params, timeout=15)
                data = resp.json()
                if data.get("data") and data["data"].get("klines"):
                    rows = [k.split(",") for k in data["data"]["klines"]]
                    return {"secid": secid, "klines": rows}
            except: pass
            return {"secid": secid, "klines": []}

    async def fetch_all_sectors(self, sector_list: list):
        if not sector_list: return
        logger.info(f"💪 [Muscle Engine] 历史数据同步启动，并发: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        results = []
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single_sector(session, secid, semaphore) for secid in sector_list]
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res["klines"]:
                    results.extend([{
                        "secid": res["secid"], "date": r[0], "open": r[1], "close": r[2], 
                        "high": r[3], "low": r[4], "vol": r[5], "amount": r[6]
                    } for r in res["klines"]])
        if results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(results).with_columns([
                pl.col("open").cast(pl.Float32), pl.col("close").cast(pl.Float32),
                pl.col("high").cast(pl.Float32), pl.col("low").cast(pl.Float32),
                pl.col("vol").cast(pl.Float64), pl.col("amount").cast(pl.Float64)
            ])
            df.write_parquet("data/sector_klines_full.parquet")
            logger.success(f"💾 任务完成，成功落盘 {len(results)} 行历史数据！")
