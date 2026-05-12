import os, json, re, asyncio, random
from loguru import logger
from playwright.async_api import async_playwright
from openai import AsyncOpenAI

class BrainEngine:
    def __init__(self):
        self.llm_client = AsyncOpenAI(
            api_key=os.getenv("LLM_API_KEY", "").strip(),
            base_url=os.getenv("LLM_BASE_URL", "").strip() or "https://integrate.api.nvidia.com/v1"
        )
        self.model_name = os.getenv("LLM_MODEL_NAME", "").strip() or "openai/gpt-oss-120b"
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    async def init_session(self):
        """建立一个持久化的‘活着的’浏览器会话"""
        logger.info("🧠 [Brain] 正在初始化浏览器母体 (Browser-Native Engine)...")
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        self.page = await self.context.new_page()
        
        # 模拟人类导航路径：建立 Trust Context
        await self.page.goto("https://www.eastmoney.com/", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(2, 4))
        await self.page.goto("https://quote.eastmoney.com/center/gridlist.html", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(2, 4))
        await self.page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", wait_until="domcontentloaded")
        logger.success("🧠 [Brain] 浏览器母体已就绪，已成功打通东财 Trust Domain。")

    async def discover_api_template(self):
        """通过嗅探发现当前的 API 结构和最新的 ut"""
        logger.info("🧠 [Brain] 正在嗅探当前环境的 API 模版...")
        kline_url = ""
        
        async def handle_request(request):
            nonlocal kline_url
            if "api/qt/stock/kline/get" in request.url:
                kline_url = request.url

        self.page.on("request", handle_request)
        await self.page.mouse.wheel(0, 500) # 触发加载
        await asyncio.sleep(5)
        
        if not kline_url:
            raise Exception("无法在当前会话中嗅探到 K 线接口")

        # 让 AI 提取最新 ut 并生成 fetch 模版
        prompt = f"从URL提取ut参数并返回JSON: {kline_url}"
        response = await self.llm_client.chat.completions.create(
            model=self.model_name, messages=[{"role": "user", "content": prompt}], temperature=0
        )
        ut = json.loads(re.search(r'\{.*\}', response.choices[0].message.content, re.DOTALL).group()).get("ut")
        
        logger.success(f"🧠 [Brain] 发现最新鉴权 Token: {ut[:8]}***")
        return ut

    async def close(self):
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()
