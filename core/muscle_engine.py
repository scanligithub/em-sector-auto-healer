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
        
    def _load_rules(self):
        rule_path = "config/active_rules.json"
        if not os.path.exists(rule_path):
            return {}
        with open(rule_path, "r", encoding="utf-8") as f:
            return json.load(f)
            
    def reload_rules(self):
        self.rules = self._load_rules()

    async def probe(self) -> bool:
        """探针升级：必须验证板块数据，而不是大盘指数"""
        logger.info("💪 [Muscle Engine] 正在发射探针，验证板块数据提取权限...")
        try:
            async with AsyncSession(impersonate=self.impersonate) as session:
                params = {
                    "secid": "90.BK0896", # 必须拿白酒板块试刀
                    "fields1": "f1,f2,f3,f4,f5,f6",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
                    "beg": "20230101",
                    "end": "20500101",
                    "lmt": "10",
                    **self.rules
                }
                resp = await session.get("https://push2.eastmoney.com/api/qt/stock/kline/get", params=params, timeout=10)
                data = resp.json()
                
                # 精准判断：不仅仅是 rc=0，而且必须有真实的 klines 数组
                if data.get("rc") == 0 and data.get("data") and data["data"].get("klines"):
                    logger.success("💪 [Muscle Engine] 探针返回正常，当前鉴权规则依然有效！")
                    return True
                else:
                    logger.warning(f"💪 [Muscle Engine] 探针检测到规则已失效 (被影子封杀)！东财返回报文: {str(data)[:150]}")
                    return False
        except Exception as e:
            logger.error(f"💪 [Muscle Engine] 探针网络异常: {e}")
            return False

    async def fetch_dynamic_sector_list(self) -> list:
        """动态获取东方财富最新全市场板块目录 (行业 + 概念 + 地域)"""
        logger.info("💪 [Muscle Engine] 正在动态扫描全市场板块目录...")
        sector_list = []
        # m:90 t:2 (行业), m:90 t:3 (概念), m:90 t:1 (地域)
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
                    resp = await session.get("https://push2.eastmoney.com/api/qt/clist/get", params=params, timeout=15)
                    data = resp.json()
                    if data.get("data") and data["data"].get("diff"):
                        items = data["data"]["diff"]
                        # 处理东财返回的可能为 list 或 dict 的情况
                        item_list = list(items.values()) if isinstance(items, dict) else items
                        for item in item_list:
                            code = item.get("f12")
                            if code:
                                sector_list.append(f"90.{code}")
                                
        except Exception as e:
            logger.error(f"💪 [Muscle Engine] 板块目录获取异常: {e}")

        # 去重
        sector_list = list(set(sector_list))
        logger.success(f"💪 [Muscle Engine] 目录扫描完成！共发现 {len(sector_list)} 个有效板块。")
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
            for attempt in range(3):
                try:
                    resp = await session.get("https://push2.eastmoney.com/api/qt/stock/kline/get", params=params, timeout=15)
                    data = resp.json()
                    if data.get("data") and data["data"].get("klines"):
                        rows = [k.split(",") for k in data["data"]["klines"]]
                        return {"secid": secid, "klines": rows}
                    else:
                        # 重点：记录被拒绝的真实原因
                        if attempt == 2:
                            logger.debug(f"⚠️ [{secid}] 返回空或异常报文: {str(data)[:100]}")
                except Exception as e:
                    await asyncio.sleep(1)
            return {"secid": secid, "klines": []}

    async def fetch_all_sectors(self, sector_list: list):
        if not sector_list:
            logger.error("💪 [Muscle Engine] 目标板块列表为空，取消抓取任务。")
            return

        logger.info(f"💪 [Muscle Engine] 肌肉引擎全速启动！目标数量: {len(sector_list)}，并发数: {self.concurrency}")
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

        logger.success(f"💪 [Muscle Engine] 抓取完成！共获取 K 线数据 {len(results)} 条。正在交由 Polars 落盘...")
        
        if results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(results)
            df = df.with_columns([
                pl.col("open").cast(pl.Float32), pl.col("close").cast(pl.Float32),
                pl.col("high").cast(pl.Float32), pl.col("low").cast(pl.Float32),
                pl.col("vol").cast(pl.Float64), pl.col("amount").cast(pl.Float64)
            ])
            df.write_parquet("data/sector_klines_full.parquet")
            logger.info("💾 [System] 数据已成功保存至 data/sector_klines_full.parquet")
