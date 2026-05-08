import os
import json
import re
import asyncio
from loguru import logger
from playwright.async_api import async_playwright
from openai import AsyncOpenAI

class BrainEngine:
    def __init__(self):
        api_key = os.getenv("LLM_API_KEY", "").strip()
        if not api_key: raise ValueError("Missing LLM_API_KEY")
        
        base_url = os.getenv("LLM_BASE_URL", "").strip() or "https://integrate.api.nvidia.com/v1"
        if not base_url.startswith("http"): base_url = f"https://{base_url}"

        model_name = os.getenv("LLM_MODEL_NAME", "").strip() or "openai/gpt-oss-120b"

        self.model_name = model_name
        self.llm_client = AsyncOpenAI(api_key=api_key, base_url=base_url)

    async def _capture_real_traffic(self) -> str:
        logger.info("🧠 [Brain Engine] 正在启动浏览器嗅探流量...")
        target_url = None
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
            page = await context.new_page()

            async def handle_request(route, request):
                nonlocal target_url
                if "api/qt/stock/kline/get" in request.url:
                    target_url = request.url
                await route.continue_()

            await page.route("**/*", handle_request)
            try:
                await page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", timeout=25000)
                await page.wait_for_timeout(5000)
            finally:
                await browser.close()
        
        if target_url:
            logger.success("🧠 [Brain Engine] 成功截获请求 URL")
        return target_url

    def _clean_json_response(self, raw_str: str) -> dict:
        match = re.search(r'\{.*\}', raw_str, re.DOTALL)
        return json.loads(match.group(0)) if match else {}

    async def _extract_rules_via_llm(self, raw_url: str) -> dict:
        logger.info(f"🧠 [Brain Engine] 正在通过模型 [{self.model_name}] 逆向参数...")
        prompt = "从该URL提取ut,fltt,invt,klt,fqt参数并返回纯JSON: " + raw_url
        
        response = await self.llm_client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0
        )
        ai_rules = self._clean_json_response(response.choices[0].message.content)
        
        # --- 工业级硬核修正 ---
        # 1. 强制补全必须参数
        defaults = {"fltt": "2", "invt": "2", "klt": "101"}
        final_rules = {k: str(ai_rules.get(k) or v) for k, v in defaults.items()}
        final_rules["ut"] = ai_rules.get("ut")

        # 2. 强制覆盖掉错误的 fqt 参数
        final_rules["fqt"] = "0" # 板块指数必须使用不复权数据！
        logger.info("🔩 [Brain Engine] 强制覆盖 fqt=0 以确保板块指数数据兼容性。")
        
        logger.success(f"🧠 [Brain Engine] 最终规则生成: {final_rules}")
        return final_rules

    async def heal(self):
        raw_url = await self._capture_real_traffic()
        if not raw_url: raise Exception("无法截获真实请求")
        new_rules = await self._extract_rules_via_llm(raw_url)
        os.makedirs("config", exist_ok=True)
        with open("config/active_rules.json", "w", encoding="utf-8") as f:
            json.dump(new_rules, f, indent=4)
        logger.info("🧠 [Brain Engine] 自愈完成。")
