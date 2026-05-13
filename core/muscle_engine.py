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
from playwright.async_api import async_playwright

class MuscleEngine:
    FALLBACK_SECTORS = ["90.BK0896", "90.BK1036"]
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        raw_worker = os.getenv("CF_WORKER_URL", "").strip()
        self.worker_url = raw_worker if raw_worker.startswith("http") else f"https://{raw_worker}" if raw_worker else ""
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }
        self.concurrency = int(os.getenv("CONCURRENCY", 10))
        self.impersonate = "chrome124"

    def _extract_json(self, text: str) -> dict:
        if not text: return {}
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        try: return json.loads(match.group(1) if match else text)
        except: return {}

    # --- 阶段一：Playwright APIRequestContext 幽灵模式 ---

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("🧠 [Phase 1] 启动 Playwright APIRequestContext 幽灵扫描...")
        all_codes = set()
        categories = {"地域": "m:90+t:1", "行业": "m:90+t:2", "概念": "m:90+t:3"}
        
        async with async_playwright() as p:
            # 💡 建立纯净上下文，不创建 Page，直接操作 request 引擎
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            
            try:
                for cat_name, fs_param in categories.items():
                    logger.info(f"➡️ 正在扫描: {cat_name}")
                    pn = 1
                    while True:
                        target_url = (
                            f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=50&po=1&np=1"
                            f"&fltt=2&invt=2&fid=f3&fs={urllib.parse.quote(fs_param)}&fields=f12&ut={self.UT}"
                        )
                        
                        # 💡 核心逻辑：利用浏览器底层网络栈发起 API 请求
                        response = await context.request.get(target_url, timeout=30000)
                        if response.status == 200:
                            data = self._extract_json(await response.text())
                            if data and data.get("data") and data["data"].get("diff"):
                                diff = data["data"]["diff"]
                                for x in diff: all_codes.add(f"90.{x['f12']}")
                                if len(diff) < 50: break # 触底
                                pn += 1
                                await asyncio.sleep(random.uniform(0.3, 0.6))
                            else: break
                        else:
                            logger.warning(f"⚠️ {cat_name} P{pn} 状态码异常: {response.status}")
                            break
            finally:
                await browser.close()

        if not all_codes:
            return self.FALLBACK_SECTORS
            
        logger.success(f"🧠 [Phase 1] 幽灵扫描完成！共捕获 {len(all_codes)} 个唯一板块。")
        return list(all_codes)

    # --- 阶段二：curl_cffi + Worker 工业并发 ---

    def _route_url(self, target_url: str) -> str:
        bust_url = f"{target_url}&_cb={time.time_ns()}"
        if self.worker_url:
            return f"{self.worker_url}?url={urllib.parse.quote(bust_url, safe='')}"
        return bust_url

    async def _curl_worker(self, session, secid: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            await asyncio.sleep(random.uniform(0.1, 1.5))
            target_url = (
                f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                f"&klt=101&fqt=0&end=20500101&lmt=100000&ut={self.UT}"
            )
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    resp = await session.get(self._route_url(target_url), headers=self.headers, timeout=30)
                    if resp.status_code == 200:
                        data = self._extract_json(resp.text)
                        if data and data.get("data") and data["data"].get("klines"):
                            res = []
                            for r in data["data"]["klines"]:
                                row = r.split(",")
                                try:
                                    res.append({"secid": secid, "date": row[0], "open": float(row[1]), "close": float(row[2]), 
                                                "high": float(row[3]), "low": float(row[4]), "volume": float(row[5]), "amount": float(row[6])})
                                except: continue
                            return res
                    await asyncio.sleep(2 ** attempt)
                except: await asyncio.sleep(1)
            return []

    async def fetch_all_sectors(self, sector_list: list):
        logger.info(f"💪 [Phase 2] 启动并发引擎 | Concurrency: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [asyncio.create_task(self._curl_worker(session, sid, semaphore)) for sid in sector_list]
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res: all_results.extend(res)
                if len(all_results) > 0 and len(all_results) % 200000 == 0:
                    logger.info(f"📊 进度监控: 已安全缓存 {len(all_results)} 条 K 线数据")
        
        if all_results:
            os.makedirs("data", exist_ok=True)
            pl.DataFrame(all_results).write_parquet("data/sector_klines_full.parquet", compression="zstd")
            logger.success(f"💾 任务圆满完成！最终落盘 {len(all_results)} 行数据。")
