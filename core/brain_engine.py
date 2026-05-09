import os, json, re, asyncio
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

    async def heal(self):
        logger.info("🧠 [Brain Engine] 启动环境快照捕获...")
        target_url = ""
        captured_headers = {}

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            async def handle_request(request):
                nonlocal target_url, captured_headers
                if "api/qt/stock/kline/get" in request.url:
                    target_url = request.url
                    # 💡 核心：只保留合法的 Header，剔除以 ':' 开头的 H2 伪头部
                    captured_headers = {
                        k: v for k, v in request.headers.items() 
                        if not k.startswith(':') and k.lower() not in ['content-length', 'host']
                    }

            page.on("request", handle_request)
            try:
                # 访问东财详情页
                await page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", timeout=30000)
                await page.wait_for_timeout(3000)
            finally:
                await browser.close()

        if not target_url: raise Exception("嗅探失败")

        # 提取动态参数
        prompt = f"从该URL提取ut参数并返回JSON: {target_url}"
        response = await self.llm_client.chat.completions.create(
            model=self.model_name, messages=[{"role": "user", "content": prompt}], temperature=0
        )
        match = re.search(r'\{.*\}', response.choices[0].message.content, re.DOTALL)
        ut_val = json.loads(match.group(0)).get("ut")

        rules = {
            "params": {"ut": ut_val, "fltt": "2", "invt": "2", "klt": "101", "fqt": "0"},
            "headers": captured_headers
        }
        
        os.makedirs("config", exist_ok=True)
        with open("config/active_rules.json", "w", encoding="utf-8") as f:
            json.dump(rules, f, indent=4)
        logger.success("🧠 [Brain Engine] 环境快照克隆并清洗完毕。")
