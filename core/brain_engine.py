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
        logger.info("🧠 [Brain Engine] 正在克隆高信誉度行为指纹...")
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
                    # 💡 核心：必须剔除所有可能引起连接重置的 Header
                    forbidden_keys = [':', 'host', 'connection', 'content-length', 'accept-encoding']
                    captured_headers = {
                        k: v for k, v in request.headers.items() 
                        if not any(k.lower().startswith(f) for f in forbidden_keys)
                    }
                    # 强行指定一个稳定的编码，防止 H2 帧解析错误
                    captured_headers["Accept-Encoding"] = "gzip, deflate"

            page.on("request", handle_request)
            try:
                await page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", timeout=30000)
                await page.wait_for_timeout(3000)
            finally:
                await browser.close()

        if not target_url: raise Exception("嗅探失败，请检查网络环境")

        prompt = f"从URL提取ut参数并返回JSON: {target_url}"
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
        logger.success("🧠 [Brain Engine] 行为指纹克隆完毕。")
