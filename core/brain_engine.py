import os
import json
import asyncio
from loguru import logger
from playwright.async_api import async_playwright
from openai import AsyncOpenAI

class BrainEngine:
    def __init__(self):
        self.llm_client = AsyncOpenAI(
            api_key=os.getenv("LLM_API_KEY"),
            base_url=os.getenv("LLM_BASE_URL")
        )
        self.model_name = os.getenv("LLM_MODEL_NAME", "glm-4")

    async def _capture_real_traffic(self) -> str:
        """步骤1：启动隐身浏览器，嗅探真实的 K 线请求 URL"""
        logger.info("🧠 [Brain Engine] 正在启动隐身浏览器进行流量嗅探...")
        target_url = None
        
        async with async_playwright() as p:
            # 启动无头浏览器 (绕过常规检测)
            browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            # 拦截器：寻找目标 API
            async def handle_request(route, request):
                nonlocal target_url
                if "api/qt/stock/kline/get" in request.url:
                    target_url = request.url
                    logger.success(f"🧠 [Brain Engine] 成功截获底层 K 线请求: {target_url[:80]}...")
                await route.continue_()

            await page.route("**/*", handle_request)

            try:
                # 访问东财任意板块详情页（例如：白酒板块 BK0896）
                await page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", timeout=15000)
                await page.wait_for_timeout(3000) # 等待页面 JS 执行并发出请求
            except Exception as e:
                logger.warning(f"🧠 [Brain Engine] 页面加载可能超时，但嗅探仍在继续: {e}")
            finally:
                await browser.close()
                
        return target_url

    async def _extract_rules_via_llm(self, raw_url: str) -> dict:
        """步骤2：利用 LLM 从混乱的 URL 中提取核心鉴权参数"""
        logger.info(f"🧠 [Brain Engine] 正在唤醒大模型 [{self.model_name}] 进行参数逆向...")
        
        prompt = f"""
        你是一个网络协议逆向专家。以下是我通过浏览器抓包获取到的最新有效的东方财富K线 API 请求URL：
        {raw_url}
        
        请分析这个URL，提取出用于鉴权和控制的核心参数。重点提取 `ut`、`fltt`、`invt`、`klt` 等参数。
        请忽略随机的时间戳参数（如 cb, _ 结尾的随机数）。
        
        请严格输出一个 JSON 格式，不要包含任何 markdown 标记或多余的解释。例如：
        {{
            "ut": "提取到的ut值",
            "fltt": "2",
            "invt": "2",
            "klt": "101",
            "fqt": "0"
        }}
        """
        
        response = await self.llm_client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}, # 强制输出 JSON
            temperature=0.1
        )
        
        raw_json = response.choices[0].message.content
        rules = json.loads(raw_json)
        logger.success(f"🧠 [Brain Engine] AI 破译完成！新规则: {rules}")
        return rules

    async def heal(self):
        """执行完整的自愈闭环"""
        raw_url = await self._capture_real_traffic()
        if not raw_url:
            raise Exception("无法截获到真实请求，可能网络异常或页面结构大改。")
            
        new_rules = await self._extract_rules_via_llm(raw_url)
        
        # 写入最新配置
        os.makedirs("config", exist_ok=True)
        with open("config/active_rules.json", "w", encoding="utf-8") as f:
            json.dump(new_rules, f, indent=4)
        logger.info("🧠 [Brain Engine] 自愈完成，新规则已注入 config/active_rules.json")
