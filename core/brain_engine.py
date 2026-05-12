import asyncio
import random
from loguru import logger
from playwright.async_api import async_playwright

class BrainEngine:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    async def build_trust_context(self):
        """完全放弃主动请求，专注于构建一个无懈可击的浏览器上下文"""
        logger.info("🧠 [Brain] 正在初始化浏览器母体 (Traffic Hijacker Mode)...")
        self.playwright = await async_playwright().start()
        
        # 注入基础反混淆参数
        self.browser = await self.playwright.chromium.launch(
            headless=True, 
            args=['--disable-blink-features=AutomationControlled']
        )
        
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        self.page = await self.context.new_page()

        # 模拟真人导航，洗白 Context 的信誉度
        logger.debug("🌐 正在构建信任链：首页...")
        await self.page.goto("https://www.eastmoney.com/", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(1.0, 2.0))
        
        logger.debug("🌐 正在构建信任链：行情中心...")
        await self.page.goto("https://quote.eastmoney.com/center/gridlist.html", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(1.0, 2.0))
        
        logger.success("🧠 [Brain] 信任上下文建立完毕。准备移交劫持引擎。")
        return self.context, self.page

    async def close(self):
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()
