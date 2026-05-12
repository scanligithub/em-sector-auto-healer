import asyncio, json, os, polars as pl
from loguru import logger

class MuscleEngine:
    def __init__(self, brain_page):
        self.page = brain_page # 借用 Brain 的‘活着的’页面
        self.ut = None
        self.concurrency = int(os.getenv("CONCURRENCY", 10)) # 浏览器内并发不宜过高

    def set_ut(self, ut):
        self.ut = ut

    async def probe(self) -> bool:
        """原生探针：在浏览器上下文内执行 fetch"""
        if not self.ut: return False
        logger.info("💪 [Muscle] 发射浏览器原生探针...")
        
        # 💡 核心：在东财页面内部执行 fetch，自动继承所有指纹
        script = f"""
        fetch("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=90.BK0896&fields2=f51&lmt=1&ut={self.ut}")
        .then(res => res.json())
        """
        try:
            data = await self.page.evaluate(script)
            if data.get("data"):
                logger.success("💪 [Muscle] 原生身份验证通过！")
                return True
        except Exception as e:
            logger.error(f"💪 [Muscle] 原生探针异常: {e}")
        return False

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle] 正在原生扫描板块目录...")
        # 同样使用 evaluate fetch
        script = f"""
        fetch("https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1500&fs=m:90+t:2,m:90+t:3&fields=f12&ut={self.ut}")
        .then(res => res.json())
        """
        res = await self.page.evaluate(script)
        codes = [f"90.{x['f12']}" for x in res['data']['diff']]
        logger.success(f"💪 [Muscle] 扫描完成，捕获 {len(codes)} 个板块。")
        return codes

    async def fetch_all_sectors(self, sector_list: list):
        """
        在浏览器内进行分批次、高信誉度的数据提取
        """
        logger.info(f"💪 [Muscle] 正在进行 Browser-Native 并发抓取...")
        all_results = []
        
        # 分批处理，防止浏览器卡死
        batch_size = 5 
        for i in range(0, len(sector_list), batch_size):
            batch = sector_list[i:i+batch_size]
            tasks = []
            for secid in batch:
                script = f"""
                fetch("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields2=f51,f52,f53&lmt=100000&ut={self.ut}")
                .then(res => res.json())
                """
                tasks.append(self.page.evaluate(script))
            
            # 并发执行浏览器内的 fetch
            batch_data = await asyncio.gather(*tasks)
            
            for idx, data in enumerate(batch_data):
                if data.get("data") and data["data"].get("klines"):
                    secid = batch[idx]
                    for r in data["data"]["klines"]:
                        row = r.split(",")
                        all_results.append({"secid": secid, "date": row[0], "close": float(row[2])})
            
            logger.debug(f"📊 已完成 {i + len(batch)} / {len(sector_list)}")
            await asyncio.sleep(random.uniform(0.5, 1.5)) # 注入人类节律噪声

        if all_results:
            os.makedirs("data", exist_ok=True)
            pl.DataFrame(all_results).write_parquet("data/sector_klines_full.parquet")
            logger.success(f"💾 任务圆满完成，最终落盘 {len(all_results)} 行数据！")
