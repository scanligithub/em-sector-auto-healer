import json
import os
import asyncio
import polars as pl
from loguru import logger
from curl_cffi.requests import AsyncSession

class MuscleEngine:
    def __init__(self):
        self.rules = self._load_rules()
        # 完美伪装成最新版 Chrome 浏览器
        self.impersonate = "chrome120" 
        self.concurrency = int(os.getenv("CONCURRENCY", 20))
        
    def _load_rules(self):
        rule_path = "config/active_rules.json"
        if not os.path.exists(rule_path):
            return {}
        with open(rule_path, "r", encoding="utf-8") as f:
            return json.load(f)
            
    def reload_rules(self):
        self.rules = self._load_rules()

    async def probe(self) -> bool:
        """探针：测试当前规则是否依然有效"""
        logger.info("💪 [Muscle Engine] 正在发射探针，验证当前规则有效性...")
        try:
            # 试探性拉取一条数据 (例如 上证指数 1.000001)
            async with AsyncSession(impersonate=self.impersonate) as session:
                params = {
                    "secid": "1.000001",
                    "fields1": "f1,f2,f3,f4,f5,f6",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
                    "beg": "20230101",
                    "end": "20500101",
                    "lmt": "10",
                    **self.rules
                }
                resp = await session.get("https://push2.eastmoney.com/api/qt/stock/kline/get", params=params, timeout=10)
                data = resp.json()
                
                # 东方财富特色：封杀时返回 200，但 rc != 0 且 data 为空
                if data.get("rc") == 0 and data.get("data") is not None:
                    logger.success("💪 [Muscle Engine] 探针返回正常，当前规则有效！")
                    return True
                else:
                    logger.warning(f"💪 [Muscle Engine] 探针检测到规则已失效！响应报文: {data}")
                    return False
        except Exception as e:
            logger.error(f"💪 [Muscle Engine] 探针网络异常: {e}")
            return False

    async def _fetch_single_sector(self, session, secid, semaphore):
        """单只板块的异步拉取任务"""
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
            # 极简重试机制
            for _ in range(3):
                try:
                    resp = await session.get("https://push2.eastmoney.com/api/qt/stock/kline/get", params=params, timeout=15)
                    data = resp.json()
                    if data.get("data") and data["data"].get("klines"):
                        # 结构化清洗
                        rows = [k.split(",") for k in data["data"]["klines"]]
                        return {"secid": secid, "klines": rows}
                except:
                    await asyncio.sleep(1)
            return {"secid": secid, "klines": []}

    async def fetch_all_sectors(self, sector_list: list):
        """满血火力：全异步高并发抓取"""
        logger.info(f"💪 [Muscle Engine] 肌肉引擎全速启动！目标数量: {len(sector_list)}，并发数: {self.concurrency}")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        results = []
        
        # 维持同一连接池提升极速
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single_sector(session, secid, semaphore) for secid in sector_list]
            
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res["klines"]:
                    results.extend([{
                        "secid": res["secid"], "date": r[0], "open": r[1], "close": r[2], 
                        "high": r[3], "low": r[4], "vol": r[5], "amount": r[6]
                    } for r in res["klines"]])

        logger.success(f"💪 [Muscle Engine] 抓取完成！共获取 K 线数据 {len(results)} 条。正在交由 Polars 落盘...")
        
        # 极速落盘
        if results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(results)
            # 瘦身：将字符串转为浮点数
            df = df.with_columns([
                pl.col("open").cast(pl.Float32), pl.col("close").cast(pl.Float32),
                pl.col("high").cast(pl.Float32), pl.col("low").cast(pl.Float32),
                pl.col("vol").cast(pl.Float64), pl.col("amount").cast(pl.Float64)
            ])
            df.write_parquet("data/sector_klines_full.parquet")
            logger.info("💾 [System] 数据已成功保存至 data/sector_klines_full.parquet")
