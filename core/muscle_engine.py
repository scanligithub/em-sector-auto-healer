import asyncio
import json
import re
import os
import random
import urllib.parse
import polars as pl
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    FALLBACK_SECTORS = [
        "90.BK0896", "90.BK1036", "90.BK0475", "90.BK0733", "90.BK0427",
        "90.BK1027", "90.BK0477", "90.BK0474", "90.BK0456", "90.BK0480"
    ]
    
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        # 💡 使用浏览器网络栈，并发可以稍微调高到 15-20
        self.concurrency = int(os.getenv("CONCURRENCY", 15))
        logger.info(f"🤖 [Engine] 启动 Chromium 网络栈直连模式 | 并发: {self.concurrency}")

    def _extract_json_with_diag(self, text: str, secid: str) -> dict:
        if not text: return {"_err": "EMPTY"}
        # 如果还是被拦截，这里能捕获到
        if "安全验证" in text or "访问受限" in text: 
            return {"_err": "WAF_BLOCK"}
            
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        try:
            json_str = match.group(1) if match else text
            return json.loads(json_str)
        except Exception:
            # 💡 增加样本打印，防止再次出现“解析异常”却不知道为什么
            sample = text[:100].replace('\n', '')
            logger.error(f"❌ {secid} 解析失败，原始内容样本: {sample}")
            return {"_err": "PARSE_FAIL"}

    async def _safe_browser_fetch(self, context, url: str, secid: str) -> dict:
        """💡 核心黑科技：使用 context.request 绕过 DOM 渲染，直取 API 原始数据"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 拟人化抖动
                await asyncio.sleep(random.uniform(0.1, 0.4))
                
                # 💡 使用浏览器内建的 API 请求引擎
                response = await context.request.get(url, timeout=30000)
                
                if response.status == 200:
                    content = await response.text()
                    data = self._extract_json_with_diag(content, secid)
                    if "_err" not in data:
                        return data
                    if data["_err"] == "WAF_BLOCK":
                        logger.warning(f"🚨 {secid} 触发了东财 IP 校验，重试中...")
                else:
                    logger.debug(f"⚠️ {secid} 状态码异常: {response.status}")
                
            except Exception as e:
                logger.debug(f"🕒 {secid} 网络抖动: {str(e).splitlines()[0]}")
            
            await asyncio.sleep(2 ** attempt) # 指数退避
                
        return {}

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle] 正在建立 Chromium 信任链...")
        all_codes = set()
        categories = {
            "地域板块": "m:90+t:1",
            "行业板块": "m:90+t:2",
            "概念板块": "m:90+t:3"
        }
        
        async with async_playwright() as p:
            # 💡 模拟最真实的环境，不加各种过时的 disable-blink 参数
            browser = await p.chromium.launch(headless=True)
            # 建立一个干净的 context，自动继承高质量 UA
            context = await browser.new_context()
            
            try:
                for cat_name, fs_param in categories.items():
                    logger.info(f"➡️ 开始扫描分类: {cat_name}")
                    encoded_fs = urllib.parse.quote(fs_param)
                    
                    for pn in range(1, 21): 
                        target_url = (
                            f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=50&po=1&np=1"
                            f"&fltt=2&invt=2&fid=f3&fs={encoded_fs}&fields=f12&ut={self.UT}"
                        )
                        
                        data = await self._safe_browser_fetch(context, target_url, f"{cat_name}_P{pn}")
                        
                        if data and data.get("data") and data["data"].get("diff"):
                            diff = data["data"]["diff"]
                            for x in diff:
                                all_codes.add(f"90.{x['f12']}")
                            if len(diff) < 50: break
                        else:
                            break
                        await asyncio.sleep(0.3)
            finally:
                await browser.close()

        if not all_codes:
            return self.FALLBACK_SECTORS
            
        logger.success(f"💪 [Muscle] 扫描完成！共捕获 {len(all_codes)} 个唯一板块。")
        return list(all_codes)

    async def _fetch_single_sector(self, context, secid: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            target_url = (
                f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                f"&klt=101&fqt=0&end=20500101&lmt=100000&ut={self.UT}"
            )
            
            data = await self._safe_browser_fetch(context, target_url, secid)
            
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
                    except: continue
                return res
            return []

    async def fetch_all_sectors(self, sector_list: list):
        logger.info(f"💪 [Muscle] 启动浏览器网络栈并发抓取...")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            
            try:
                tasks = []
                for secid in sector_list:
                    await asyncio.sleep(0.05)
                    tasks.append(asyncio.create_task(self._fetch_single_sector(context, secid, semaphore)))
                
                for coro in asyncio.as_completed(tasks):
                    res = await coro
                    if res: all_results.extend(res)
                    if len(all_results) > 0 and len(all_results) % 100000 == 0:
                        logger.info(f"📊 已拉取 {len(all_results)} 条 K 线数据")
            finally:
                await browser.close()
        
        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            df.write_parquet("data/sector_klines_full.parquet", compression="zstd")
            logger.success(f"💾 工业级作业完成！落盘 {len(all_results)} 行数据。")
