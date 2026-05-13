import asyncio
import json
import re
import os
import random
import urllib.parse
import polars as pl
from loguru import logger
from playwright.async_api import async_playwright, BrowserContext

class MuscleEngine:
    FALLBACK_SECTORS = [
        "90.BK0896", "90.BK1036", "90.BK0475", "90.BK0733", "90.BK0427",
        "90.BK1027", "90.BK0477", "90.BK0474", "90.BK0456", "90.BK0480"
    ]
    
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        # 💡 浏览器非常吃内存，GitHub Actions 的极限并发建议在 5-8 之间
        self.concurrency = int(os.getenv("CONCURRENCY", 5))
        logger.info(f"🤖 [Engine] 启动无头浏览器 API 盲打模式 | 并发限制: {self.concurrency}")

    def _extract_json_with_diag(self, text: str, secid: str) -> dict:
        if not text: return {"_err": "EMPTY"}
        if "安全验证" in text or "访问受限" in text: return {"_err": "WAF_BLOCK"}
            
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        try:
            json_str = match.group(1) if match else text
            return json.loads(json_str)
        except:
            return {"_err": "PARSE_FAIL"}

    async def _safe_browser_request(self, context: BrowserContext, url: str, secid: str, timeout: int = 30000) -> dict:
        """核心：通过开启新的浏览器 Tab 直接访问 API 接口"""
        max_retries = 3
        for attempt in range(max_retries):
            # 开启新标签页
            page = await context.new_page()
            try:
                # 拟人化抖动
                await asyncio.sleep(random.uniform(0.1, 0.5))
                
                # 💡 wait_until="domcontentloaded" 对于纯文本 JSON 接口极快
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                
                # 浏览器访问 API 时，Chrome 会将纯文本包裹在 <body> 或 <pre> 中
                content = await page.locator("body").inner_text()
                
                data = self._extract_json_with_diag(content, secid)
                if "_err" not in data:
                    return data
                    
                logger.debug(f"⚠️ {secid} 数据解析异常 | 重试 {attempt+1}/{max_retries}")
                
            except Exception as e:
                # 捕获 Timeout 等网络异常
                logger.debug(f"🕒 {secid} 浏览器网络波动 ({str(e).splitlines()[0]}) | 重试 {attempt+1}")
            finally:
                # 💡 极其重要：抓完必须关闭 Tab，否则内存瞬间撑爆
                await page.close()
                
        return {}

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle] 正在启动 Chromium 引擎扫描目录...")
        all_codes = set()
        categories = {
            "地域板块": "m:90+t:1",
            "行业板块": "m:90+t:2",
            "概念板块": "m:90+t:3"
        }
        
        async with async_playwright() as p:
            # 开启无头浏览器
            browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            
            try:
                for cat_name, fs_param in categories.items():
                    logger.info(f"➡️ 开始扫描分类: {cat_name}")
                    encoded_fs = urllib.parse.quote(fs_param)
                    cat_count = 0
                    
                    for pn in range(1, 21): 
                        target_url = (
                            f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=50&po=1&np=1"
                            f"&fltt=2&invt=2&fid=f3&fs={encoded_fs}&fields=f12&ut={self.UT}"
                        )
                        
                        data = await self._safe_browser_request(context, target_url, f"{cat_name}_P{pn}")
                        
                        if data and data.get("data") and data["data"].get("diff"):
                            diff = data["data"]["diff"]
                            for x in diff:
                                all_codes.add(f"90.{x['f12']}")
                                cat_count += 1
                                
                            if len(diff) < 50:
                                logger.debug(f"✅ {cat_name} 扫描触底，共 {pn} 页，捕获 {cat_count} 个。")
                                break
                        else:
                            logger.debug(f"⚠️ {cat_name} 第 {pn} 页无数据，提前结束本分类。")
                            break
                            
                        await asyncio.sleep(0.5)
            finally:
                await browser.close()

        if not all_codes:
            logger.warning("❌ 浏览器目录扫描失败，启用静态核心库兜底！")
            return self.FALLBACK_SECTORS
            
        logger.success(f"💪 [Muscle] 目录扫描完美收官！共捕获 {len(all_codes)} 个唯一板块。")
        return list(all_codes)

    async def _fetch_single_sector(self, context: BrowserContext, secid: str, semaphore: asyncio.Semaphore):
        """控制同时打开的浏览器标签页数量"""
        async with semaphore:
            target_url = (
                f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                f"&klt=101&fqt=0&end=20500101&lmt=100000&ut={self.UT}"
            )
            
            data = await self._safe_browser_request(context, target_url, secid)
            
            if data and data.get("data") and data["data"].get("klines"):
                res = []
                for r in data["data"]["klines"]:
                    row = r.split(",")
                    try:
                        res.append({
                            "secid": secid, "date": row[0],
                            "open": float(row[1]), "close": float(row[2]),
                            "high": float(row[3]), "low": float(row[4]),
                            "volume": float(row[5]), "amount": float(row[6])
                        })
                    except (IndexError, ValueError): continue
                return res
            return []

    async def fetch_all_sectors(self, sector_list: list):
        logger.info(f"💪 [Muscle] 启动浏览器并发抓取引擎，Tab 限制: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            
            try:
                tasks = []
                for secid in sector_list:
                    # 强力发牌器，防止瞬间打开几十个 Tab 导致内存溢出
                    await asyncio.sleep(0.1)
                    tasks.append(asyncio.create_task(self._fetch_single_sector(context, secid, semaphore)))
                
                for coro in asyncio.as_completed(tasks):
                    res = await coro
                    if res: 
                        all_results.extend(res)
                    if len(all_results) > 0 and len(all_results) % 50000 == 0:
                        logger.info(f"📊 内存池堆叠中: 已安全缓存 {len(all_results)} 条 K 线切片")
            finally:
                await browser.close()
        
        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            df.write_parquet("data/sector_klines_full.parquet", compression="zstd")
            logger.success(f"💾 工业级作业完成！成功抗击风控，落盘 {len(all_results)} 行底层数据。")
