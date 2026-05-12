import asyncio
import random
from loguru import logger
from playwright.async_api import async_playwright

class BrainEngine:
    def __init__(self):
        self.ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    async def steal_trust_context(self) -> dict:
        logger.info("🧠 [Brain] 启动幽灵盗贼：准备双重窃取 (K线 + 目录) 原生凭证...")
        stolen_kline_url = ""
        stolen_clist_url = ""

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(user_agent=self.ua)
            page = await context.new_page()

            async def on_request(request):
                nonlocal stolen_kline_url, stolen_clist_url
                if "api/qt/stock/kline/get" in request.url and "secid=90.BK0896" in request.url:
                    stolen_kline_url = request.url
                # 💡 修复：去掉 "fs=m:90" 的硬性要求，无视 URL 编码 (m%3A90)
                if "api/qt/clist/get" in request.url:
                    if not stolen_clist_url: # 只要抓到一个就行
                        stolen_clist_url = request.url

            page.on("request", on_request)

            try:
                logger.debug("🌐 洗白：访问东财首页...")
                await page.goto("https://www.eastmoney.com/", wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(random.uniform(1.0, 2.0))

                logger.debug("🌐 触发一：进入行情中心，诱捕合法 Clist URL...")
                await page.goto("https://quote.eastmoney.com/center/gridlist.html#boards", wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(random.uniform(2.0, 3.0))

                logger.debug("🌐 触发二：进入白酒板块，诱捕合法 Kline URL...")
                await page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", wait_until="domcontentloaded", timeout=15000)
                
                # 等待双重 URL 落网
                for _ in range(10):
                    if stolen_kline_url and stolen_clist_url: break
                    await asyncio.sleep(1)

                if not stolen_kline_url or not stolen_clist_url:
                    logger.warning(f"⚠️ 捕获不完整: kline={bool(stolen_kline_url)}, clist={bool(stolen_clist_url)}")

                cookies = await context.cookies()
                cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

                # 💡 核心修复 1：Cookie 沉淀时间，等待异步 Storage 落盘
                logger.debug("⏳ 凭证已就绪，正在进行 Cookie 持久化沉淀 (5s)...")
                await asyncio.sleep(5)

                logger.success("🧠 [Brain] 双重凭证窃取成功！")
                return {
                    "kline_url": stolen_kline_url,
                    "clist_url": stolen_clist_url,
                    "cookies": cookie_str,
                    "ua": self.ua
                }
            finally:
                await browser.close()
