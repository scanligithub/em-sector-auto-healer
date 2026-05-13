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
    # 💡 静态核心兜底库：防断流最后防线
    FALLBACK_SECTORS = [
        "90.BK0896", "90.BK1036", "90.BK0475", "90.BK0733", "90.BK0427",
        "90.BK1027", "90.BK0477", "90.BK0474", "90.BK0456", "90.BK0480"
    ]
    
    # 东财官方通用鉴权 Token
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

    # --- 阶段一：Playwright 幽灵扫描 (高容错版) ---
    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("🧠 [Phase 1] 启动 Playwright 目录扫描 (强化容错版)...")
        all_codes = set()
        categories = {"地域板块": "m:90+t:1", "行业板块": "m:90+t:2", "概念板块": "m:90+t:3"}
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                for cat_name, fs_param in categories.items():
                    logger.info(f"➡️ 开始渗透分类: {cat_name}")
                    fail_count = 0  # 连续失败计数
                    
                    for pn in range(1, 25):
                        target_url = (
                            f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=50&po=1&np=1"
                            f"&fltt=2&invt=2&fid=f3&fs={urllib.parse.quote(fs_param)}&fields=f12&ut={self.UT}"
                        )
                        
                        try:
                            # 增加随机等待，降低被封概率
                            await asyncio.sleep(random.uniform(0.5, 1.2))
                            await page.goto(target_url, wait_until="networkidle", timeout=20000)
                            content = await page.locator("body").inner_text()
                            
                            data = self._extract_json(content)
                            if data and data.get("data") and data["data"].get("diff"):
                                diff = data["data"]["diff"]
                                for x in diff:
                                    all_codes.add(f"90.{x['f12']}")
                                fail_count = 0  # 重置失败计数
                                if len(diff) < 50:
                                    break
                            else:
                                fail_count += 1
                                logger.warning(f"⚠️ {cat_name} 第 {pn} 页数据为空或格式不对 [{fail_count}/3]")
                                if fail_count >= 3:
                                    break  # 连续 3 页没数据才判定结束
                        except Exception as e:
                            logger.error(f"🕒 Playwright 网络异常 {cat_name} P{pn}: {str(e).splitlines()[0]}")
                            fail_count += 1
                            if fail_count >= 3:
                                break
            finally:
                await browser.close()

        if not all_codes:
            logger.warning("⚠️ 扫描全线失败，启用静态库。")
            return self.FALLBACK_SECTORS

        logger.success(f"🧠 [Phase 1] 渗透成功！实际共捕获 {len(all_codes)} 个原始板块 ID。")
        return list(all_codes)

    # --- 阶段二：curl_cffi 并发抓取 ---
    def _extract_json(self, text: str) -> dict:
        if not text:
            return {}
        # 兼容 JSONP 剥壳
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        try:
            return json.loads(match.group(1) if match else text)
        except:
            return {}

    def _route_url(self, target_url: str) -> str:
        # 注入 Cache-Buster 确保命中 Worker 统计
        bust_url = f"{target_url}&_cb={time.time_ns()}"
        if self.worker_url:
            return f"{self.worker_url}?url={urllib.parse.quote(bust_url, safe='')}"
        return bust_url

    async def _curl_worker(self, session, secid: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            # 流量平滑抖动
            await asyncio.sleep(random.uniform(0.1, 1.5))
            target_url = (
                f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                f"&klt=101&fqt=0&end=20500101&lmt=100000&ut={self.UT}"
            )
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # 使用工业级 curl_cffi 进行底层 TLS 伪装
                    resp = await session.get(self._route_url(target_url), headers=self.headers, timeout=30)
                    if resp.status_code == 200:
                        data = self._extract_json(resp.text)
                        if data and data.get("data") and data["data"].get("klines"):
                            res = []
                            for r in data["data"]["klines"]:
                                row = r.split(",")
                                try:
                                    res.append({
                                        "secid": secid,
                                        "date": row[0],
                                        "open": float(row[1]),
                                        "close": float(row[2]),
                                        "high": float(row[3]),
                                        "low": float(row[4]),
                                        "volume": float(row[5]),
                                        "amount": float(row[6])
                                    })
                                except:
                                    continue
                            return res
                    await asyncio.sleep(2 ** attempt)
                except Exception:
                    await asyncio.sleep(1)
            return []

    async def fetch_all_sectors(self, sector_list: list):
        logger.info(f"💪 [Phase 2] 启动并发引擎 | Concurrency: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            # 发牌器：打散瞬间突发连接
            tasks = []
            for sid in sector_list:
                await asyncio.sleep(0.05)
                tasks.append(asyncio.create_task(self._curl_worker(session, sid, semaphore)))
            
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res:
                    all_results.extend(res)
                if len(all_results) > 0 and len(all_results) % 200000 == 0:
                    logger.info(f"📊 已拉取 {len(all_results)} 条 K 线数据切片...")

        if all_results:
            os.makedirs("data", exist_ok=True)
            # 💡 修正点：将 zstandard 修改为 polars 识别的 zstd
            pl.DataFrame(all_results).write_parquet(
                "data/sector_klines_full.parquet",
                compression="zstd"
            )
            logger.success(f"💾 工业级作业完成！最终落盘 {len(all_results)} 行数据。")
