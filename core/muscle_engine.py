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
    FALLBACK_SECTORS = [
        "90.BK0896", "90.BK1036", "90.BK0475", "90.BK0733", "90.BK0427",
        "90.BK1027", "90.BK0477", "90.BK0474", "90.BK0456", "90.BK0480"
    ]
    
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
        
        # 💡 并发策略：curl_cffi 环境下，10-15 是稳态平衡点
        self.concurrency = int(os.getenv("CONCURRENCY", 10))
        self.impersonate = "chrome124"

    # --- 阶段一：Playwright 幽灵扫描 (建立信任并获取全量目录) ---
    
    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("🧠 [Phase 1] 启动 Playwright 幽灵扫描器 (建立浏览器级信任链)...")
        all_codes = set()
        categories = {"地域板块": "m:90+t:1", "行业板块": "m:90+t:2", "概念板块": "m:90+t:3"}
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                for cat_name, fs_param in categories.items():
                    logger.info(f"➡️ 正在渗透分类: {cat_name}")
                    for pn in range(1, 15): # 降低翻页深度，防止被 WAF 锁定
                        target_url = (
                            f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=50&po=1&np=1"
                            f"&fltt=2&invt=2&fid=f3&fs={urllib.parse.quote(fs_param)}&fields=f12&ut={self.UT}"
                        )
                        
                        # 💡 利用浏览器真实的导航行为获取目录
                        await page.goto(target_url, wait_until="networkidle", timeout=30000)
                        content = await page.locator("body").inner_text()
                        
                        data = self._extract_json(content)
                        if data and data.get("data") and data["data"].get("diff"):
                            diff = data["data"]["diff"]
                            for x in diff: all_codes.add(f"90.{x['f12']}")
                            if len(diff) < 50: break
                        else: break
                        await asyncio.sleep(1.0) # 慢速翻页，保护 IP
            finally:
                await browser.close()

        if not all_codes:
            logger.warning("⚠️ 扫描全线失败，启用静态库。")
            return self.FALLBACK_SECTORS
            
        logger.success(f"🧠 [Phase 1] 渗透成功！捕获 {len(all_codes)} 个原始板块 ID。")
        return list(all_codes)

    # --- 阶段二：curl_cffi 暴力美学 (高并发 K 线拉取) ---

    def _extract_json(self, text: str) -> dict:
        if not text: return {}
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        try: return json.loads(match.group(1) if match else text)
        except: return {}

    def _route_url(self, target_url: str) -> str:
        bust_url = f"{target_url}&_cb={time.time_ns()}"
        if self.worker_url:
            return f"{self.worker_url}?url={urllib.parse.quote(bust_url, safe='')}"
        return bust_url

    async def _curl_worker(self, session, secid: str, semaphore: asyncio.Semaphore):
        """工业级 API 抓取：利用 libcurl 的稳健连接池"""
        async with semaphore:
            # 💡 流量平滑：给每一个请求加一个微小的随机启动延迟，消除惊群效应
            await asyncio.sleep(random.uniform(0.1, 1.2))
            
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
                                res.append({"secid": secid, "date": row[0], "open": float(row[1]), "close": float(row[2]), 
                                            "high": float(row[3]), "low": float(row[4]), "volume": float(row[5]), "amount": float(row[6])})
                            return res
                    # 520 / 502 处理
                    await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    await asyncio.sleep(1)
            return []

    async def fetch_all_sectors(self, sector_list: list):
        logger.info(f"💪 [Phase 2] 启动 curl_cffi 并发引擎 | Concurrency: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        # 💡 建立统一的 libcurl session，优化连接复用
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._curl_worker(session, sid, semaphore) for sid in sector_list]
            
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res: 
                    all_results.extend(res)
                if len(all_results) > 0 and len(all_results) % 100000 == 0:
                    logger.info(f"📊 已拉取 {len(all_results)} 行底层数据")
        
        if all_results:
            os.makedirs("data", exist_ok=True)
            pl.DataFrame(all_results).write_parquet("data/sector_klines_full.parquet", compression="zstandard")
            logger.success(f"💾 任务圆满完成！最终落盘 {len(all_results)} 行数据。")
