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
        logger.info("🧠 [Brain Engine] 正在启动弹性嗅探器 (Resilient Capture)...")
        target_url = None
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
            page = await context.new_page()

            async def handle_request(route, request):
                nonlocal target_url
                if "api/qt/stock/kline/get" in request.url and "BK0896" in request.url:
                    target_url = request.url
                    # 💡 只要抓到目标 URL，立刻停止页面加载，节省时间
                    logger.success("🧠 [Brain Engine] 成功截获请求 URL！")
                await route.continue_()

            await page.route("**/*", handle_request)
            
            try:
                # 1. 降低等待权重，只等 domcontentloaded 而不是全页面 load
                # 2. 增加总超时到 60s
                await page.goto(
                    "https://quote.eastmoney.com/bk/90.BK0896.html", 
                    timeout=60000, 
                    wait_until="domcontentloaded"
                )
                await page.wait_for_timeout(3000)
            except Exception as e:
                # 🚀 核心改进：即使 goto 超时，只要 target_url 拿到了，就不报错
                if target_url:
                    logger.warning("🧠 [Brain Engine] 页面加载虽超时，但已成功截获关键流量。")
                else:
                    logger.error(f"🧠 [Brain Engine] 嗅探彻底失败: {e}")
            finally:
                await browser.close()
        
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
        
        # 强制修正板块指数不支持复权的限制
        final_rules = {
            "ut": ai_rules.get("ut"),
            "fltt": "2",
            "invt": "2",
            "klt": "101",
            "fqt": "0" 
        }
        logger.success(f"🧠 [Brain Engine] 最终规则生成 (fqt强制设为0): {final_rules}")
        return final_rules

    async def heal(self):
        raw_url = await self._capture_real_traffic()
        if not raw_url: raise Exception("嗅探器未能在规定时间内截获流量。")
        new_rules = await self._extract_rules_via_llm(raw_url)
        os.makedirs("config", exist_ok=True)
        with open("config/active_rules.json", "w", encoding="utf-8") as f:
            json.dump(new_rules, f, indent=4)
        logger.info("🧠 [Brain Engine] 自愈完成。")
