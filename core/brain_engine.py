import os
import json
import re
import asyncio
import random
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
        
        # 💡 新增：全局记录器，存放捕获到的 API
        self.captured_kline_url = ""

    async def init_session(self):
        """建立持久化会话，并沿途记录所有 API 流量"""
        logger.info("🧠 [Brain] 正在初始化浏览器母体 (Browser-Native Engine)...")
        self.playwright = await async_playwright().start()
        # 注入基础反混淆参数
        self.browser = await self.playwright.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        self.page = await self.context.new_page()
        
        # 💡 核心修复：雷达前置。在任何导航开始前，先挂载监听器
        async def handle_request(request):
            # 只要发现目标，立刻保存
            if "api/qt/stock/kline/get" in request.url:
                self.captured_kline_url = request.url

        self.page.on("request", handle_request)
        
        # 模拟人类导航路径
        logger.debug("🌐 正在构建信任链：首页...")
        await self.page.goto("https://www.eastmoney.com/", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(1.5, 3.0))
        
        logger.debug("🌐 正在构建信任链：行情中心...")
        await self.page.goto("https://quote.eastmoney.com/center/gridlist.html", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(1.5, 3.0))
        
        logger.debug("🌐 正在触发核心数据链：进入白酒板块...")
        # 💡 在这次跳转中，东财会发出 kline 接口请求，我们的雷达将完美捕获
        await self.page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", wait_until="domcontentloaded")
        
        # 多等一会，确保 Ajax 异步请求已经发出
        await asyncio.sleep(4)
        logger.success("🧠 [Brain] 浏览器母体已就绪，信任上下文建立完毕。")

    async def discover_api_template(self):
        """让 AI 对捕获到的流量进行提纯提取"""
        logger.info("🧠 [Brain] 正在从历史捕获记录中分析 API 模版...")
        
        # 容错：如果一次没抓到，刷新页面再试一次
        if not self.captured_kline_url:
            logger.warning("⚠️ 第一次未捕捉到流量，正在执行页面重载嗅探...")
            await self.page.reload(wait_until="domcontentloaded")
            await asyncio.sleep(5)
            
        if not self.captured_kline_url:
            raise Exception("❌ 严重错误：无法在当前会话中嗅探到 K 线接口！请检查网站结构。")

        # 让 AI 提取最新 ut 并生成规范 JSON
        prompt = f"""
        请从以下 URL 中提取 `ut` 参数，并严格以 JSON 格式返回。
        URL: {self.captured_kline_url}
        输出示例: {{"ut": "fa5fd19..."}}
        """
        
        response = await self.llm_client.chat.completions.create(
            model=self.model_name, 
            messages=[{"role": "user", "content": prompt}], 
            temperature=0
        )
        
        # 鲁棒的 JSON 提取
        match = re.search(r'\{.*\}', response.choices[0].message.content, re.DOTALL)
        if not match:
            raise Exception("AI 返回的数据不符合 JSON 格式")
            
        ut = json.loads(match.group(0)).get("ut")
        
        if not ut:
            raise Exception("AI 无法找到有效的 ut 参数")
            
        logger.success(f"🧠 [Brain] 成功提取当前会话原生 Token: {ut[:8]}***")
        return ut

    async def close(self):
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()
