import asyncio
import json
import re
import os
import time
import random
import urllib.parse
import polars as pl
from loguru import logger
from curl_cffi.requests import AsyncSession

class MuscleEngine:
    def __init__(self, trust_context: dict):
        self.kline_template = trust_context.get("kline_url", "")
        raw_worker = os.getenv("CF_WORKER_URL", "").strip()
        self.worker_url = raw_worker if raw_worker.startswith("http") else f"https://{raw_worker}"
        
        self.headers = {
            "User-Agent": trust_context.get("ua", ""),
            "Cookie": trust_context.get("cookies", ""),
            "Referer": "https://quote.eastmoney.com/"
        }
        self.concurrency = int(os.getenv("CONCURRENCY", 15))
        self.impersonate = "chrome120"

    def _classify_sector(self, code: str) -> str:
        """【注入 stockA 逻辑】基于 BK 编码执行物理分类"""
        if re.search(r"BK014[5-9]|BK015|BK016|BK017|BK018|BK019", code):
            return "地域板块"
        elif re.search(r"BK042[7-9]|BK04[3-9]|BK0[5-8]|BK091[0-7]", code):
            return "行业板块"
        return "概念板块"

    def _route_through_worker(self, target_url: str) -> str:
        connector = "&" if "?" in target_url else "?"
        bust_url = f"{target_url}{connector}_cb={time.time_ns()}"
        if self.worker_url:
            return f"{self.worker_url}?url={urllib.parse.quote(bust_url, safe='')}"
        return bust_url

    async def _safe_request(self, session, url: str, label: str) -> dict:
        routed_url = self._route_through_worker(url)
        for attempt in range(3):
            try:
                await asyncio.sleep(random.uniform(0.1, 0.2))
                resp = await session.get(routed_url, headers=self.headers, timeout=25)
                if resp.status_code == 200:
                    match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', resp.text, re.DOTALL)
                    return json.loads(match.group(1)) if match else json.loads(resp.text)
                raise Exception(f"HTTP_{resp.status_code}")
            except Exception as e:
                wait = (attempt + 1) * 3
                logger.debug(f"🕒 {label} 抖动: {e} | {wait}s 后重试")
                await asyncio.sleep(wait)
        return {}

    async def discover_sectors_via_seeds(self, baostock_seeds: list) -> pl.DataFrame:
        """【Discovery】利用 BaoStock 给出的全量个股种子，反向扫射东财板块"""
        logger.info(f"🔍 [Discovery] 正在通过 {len(baostock_seeds)} 只 BaoStock 种子股反向探测全市场板块...")
        
        # 东财个股所属板块接口
        mapping_api = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&fields=f12,f14&secid={secid}"
        
        semaphore = asyncio.Semaphore(self.concurrency * 2) # 探测阶段并发可稍高
        sector_dict = {}

        async with AsyncSession(impersonate=self.impersonate) as session:
            async def _scan_one(bs_code):
                # 将 BaoStock 格式 (sh.600000) 转为东财 secid (1.600000)
                pure_code = bs_code.split(".")[1]
                prefix = "1." if bs_code.startswith("sh") else "0."
                secid = f"{prefix}{pure_code}"
                
                async with semaphore:
                    url = mapping_api.format(secid=secid)
                    res = await self._safe_request(session, url, f"SCAN_{secid}")
                    if res.get("data") and res["data"].get("diff"):
                        for x in res["data"]["diff"]:
                            code = x["f12"]
                            if code.startswith("BK") and code not in sector_dict:
                                sector_dict[code] = x["f14"]

            # 执行全量扫射
            await asyncio.gather(*[_scan_one(s) for s in baostock_seeds])

        if not sector_dict:
            return pl.DataFrame()

        # 转换为 Polars 并应用 stockA 分类逻辑
        df = pl.DataFrame([{"sector_code": k, "sector_name": v} for k, v in sector_dict.items()])
        df = df.with_columns(
            pl.col("sector_code").map_elements(self._classify_sector, return_dtype=pl.String).alias("sector_type")
        )
        logger.success(f"✅ [Discovery] 探测完成，共归纳出 {len(df)} 个板块。")
        return df

    async def _fetch_single_sector(self, session, row: dict, semaphore: asyncio.Semaphore):
        """【Action】并发拉取板块 K 线数据"""
        code = row['sector_code']
        secid = f"90.{code}"
        async with semaphore:
            # 使用 BrainEngine 窃取到的最新签名 URL 模板
            url = re.sub(r'secid=[^&]+', f'secid={secid}', self.kline_template)
            url = re.sub(r'lmt=\d+', 'lmt=5000', url) # 抓取更长历史

            data = await self._safe_request(session, url, secid)
                
            results = []
            if data.get("data") and data["data"].get("klines"):
                for k in data["data"]["klines"]:
                    r = k.split(",")
                    results.append({
                        "date": r[0],
                        "sector_code": code,
                        "sector_name": row['sector_name'],
                        "sector_type": row['sector_type'],
                        "open": float(r[1]), "close": float(r[2]),
                        "high": float(r[3]), "low": float(r[4]),
                        "vol": float(r[5]), "amt": float(r[6])
                    })
            return results

    async def fetch_all_sectors(self, sector_df: pl.DataFrame):
        """执行全量数据采集与落盘"""
        logger.info(f"🚀 [Action] 启动工业下载引擎，并发量: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_data = []
        
        target_list = sector_df.to_dicts()
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single_sector(session, row, semaphore) for row in target_list]
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res: all_data.extend(res)
        
        if all_data:
            final_df = pl.DataFrame(all_data).unique(subset=["date", "sector_code"])
            os.makedirs("data", exist_ok=True)
            # 最终产物：parquet 格式，由 polars 强力驱动
            final_df.sort(["date", "sector_type"]).write_parquet("data/sector_klines_final.parquet")
            logger.success(f"💾 [System] 任务圆满完成，落盘数据量: {len(final_df)} 行。")
