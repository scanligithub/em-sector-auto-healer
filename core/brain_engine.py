import asyncio
import random
from loguru import logger
from playwright.async_api import async_playwright

class BrainEngine:
    def __init__(self):
        # 预设高信誉度 UA
        self.ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    async def steal_trust_context(self) -> dict:
        """
        启动无头浏览器，模拟真实人类行为建立信任。
        在访问白酒板块时，拦截并窃取官方 JS 算好的合法 K 线 URL 和 Cookie。
        完成后立刻销毁浏览器，避免指纹长期暴露。
        """
        logger.info("🧠 [Brain] 启动幽灵盗贼模式 (Playwright) 窃取原生凭证...")
        stolen_url = ""
        cookie_str = ""

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(user_agent=self.ua)
            page = await context.new_page()

            # 挂载雷达，一旦发现 K 线请求，立刻记录
            async def on_request(request):
                nonlocal stolen_url
                if "api/qt/stock/kline/get" in request.url and "secid=90.BK0896" in request.url:
                    stolen_url = request.url

            page.on("request", on_request)

            try:
                # 第一步：访问首页，拿到入口 Cookie
                logger.debug("🌐 洗白：访问东财首页...")
                await page.goto("https://www.eastmoney.com/", wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(random.uniform(1.0, 2.0))

                # 第二步：进入行情中心，完善 Referer 链
                logger.debug("🌐 洗白：访问行情中心...")
                await page.goto("https://quote.eastmoney.com/center/gridlist.html", wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(random.uniform(1.0, 2.0))

                # 第三步：进入白酒板块，触发数据加载
                logger.debug("🌐 触发：进入白酒板块诱捕合法 API...")
                await page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", wait_until="domcontentloaded", timeout=15000)
                
                # 等待官方 JS 发出网络请求 (最多等 10 秒)
                for _ in range(10):
                    if stolen_url: break
                    await asyncio.sleep(1)

                if not stolen_url:
                    raise Exception("未能截获官方 K 线 URL，可能是网络超时")

                # 第四步：提取带有 Trust 标记的完整 Cookie
                cookies = await context.cookies()
                cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

                logger.success("🧠 [Brain] 凭证窃取成功！浏览器即刻销毁，切断指纹暴露。")
                return {
                    "url": stolen_url,
                    "cookies": cookie_str,
                    "ua": self.ua
                }
            finally:
                await browser.close()
