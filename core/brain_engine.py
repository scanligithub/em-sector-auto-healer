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
        target_url = None
        target_headers = {}

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
            page = await context.new_page()

            async def handle_request(request):
                nonlocal target_url, target_headers
                if "api/qt/stock/kline/get" in request.url:
                    target_url = request.url
                    target_headers = request.headers # ⬅️ 捕获整套真实 Headers

            page.on("request", handle_request)
            try:
                await page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", timeout=30000, wait_until="domcontentloaded")
                await page.wait_for_timeout(3000)
            finally:
                await browser.close()

        if not target_url: raise Exception("流量嗅探失败")

        # 让 AI 只负责提取动态参数，Header 我们直接存
        prompt = f"从该URL提取ut,fltt,invt,klt参数并返回纯JSON: {target_url}"
        response = await self.llm_client.chat.completions.create(
            model=self.model_name, messages=[{"role": "user", "content": prompt}], temperature=0
        )
        
        # 解析 AI 结果
        match = re.search(r'\{.*\}', response.choices[0].message.content, re.DOTALL)
        ai_params = json.loads(match.group(0)) if match else {}
        
        # 组装最终规则包：参数 + 环境快照
        rules = {
            "params": {
                "ut": ai_params.get("ut"), "fltt": "2", "invt": "2", "klt": "101", "fqt": "0"
            },
            "headers": {
                "Referer": "https://quote.eastmoney.com/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Cookie": target_headers.get("cookie", "") # ⬅️ 携带真实 Cookie
            }
        }
        
        os.makedirs("config", exist_ok=True)
        with open("config/active_rules.json", "w", encoding="utf-8") as f:
            json.dump(rules, f, indent=4)
        logger.success("🧠 [Brain Engine] 环境快照克隆成功。")
