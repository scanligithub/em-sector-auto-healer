import os
import json
import re
import asyncio
from loguru import logger
from playwright.async_api import async_playwright
from openai import AsyncOpenAI

class BrainEngine:
    def __init__(self):
        # 1. 获取并清洗 API KEY
        api_key = os.getenv("LLM_API_KEY", "").strip()
        if not api_key:
            logger.critical("❌ 致命错误：未找到 LLM_API_KEY！")
            raise ValueError("Missing LLM_API_KEY")
            
        # 2. 获取并清洗 BASE URL
        base_url = os.getenv("LLM_BASE_URL", "").strip() or "https://integrate.api.nvidia.com/v1"
        if not base_url.startswith("http"):
            base_url = f"https://{base_url}"

        # 3. 获取并清洗模型名称
        model_name = os.getenv("LLM_MODEL_NAME", "").strip() or "openai/gpt-oss-120b"

        self.model_name = model_name
        self.llm_client = AsyncOpenAI(api_key=api_key, base_url=base_url)

    async def _capture_real_traffic(self) -> str:
        logger.info("🧠 [Brain Engine] 正在启动隐身浏览器进行流量嗅探...")
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
                # 访问东财白酒板块触发请求
                await page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", timeout=20000)
                await page.wait_for_timeout(5000)
            except Exception as e:
                logger.warning(f"🧠 [Brain Engine] 嗅探提示: {e}")
            finally:
                await browser.close()
        
        if target_url:
            logger.success(f"🧠 [Brain Engine] 成功截获请求 URL")
        return target_url

    def _clean_json_response(self, raw_str: str) -> dict:
        # 鲁棒性提取 JSON
        match = re.search(r'\{.*\}', raw_str, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except:
                pass
        return {}

    async def _extract_rules_via_llm(self, raw_url: str) -> dict:
        logger.info(f"🧠 [Brain Engine] 正在通过模型 [{self.model_name}] 逆向参数...")
        prompt = f"""
        你是一个API分析专家。请从下面的 URL 中提取 API 参数。
        URL: {raw_url}
        请提取：ut, fltt, invt, klt, fqt。
        如果 URL 缺失某个参数，请设为：fltt=2, invt=2, klt=101, fqt=0。
        请直接返回纯 JSON 对象，严禁返回 null。
        """
        try:
            response = await self.llm_client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            raw_output = response.choices[0].message.content
            ai_rules = self._clean_json_response(raw_output)
            
            # 补全逻辑
            defaults = {"fltt": "2", "invt": "2", "klt": "101", "fqt": "0"}
            final_rules = {k: str(ai_rules.get(k) or v) for k, v in defaults.items()}
            # ut 是核心，必须有 AI 提取的，如果没有则用一个已知的兜底
            final_rules["ut"] = ai_rules.get("ut") or "fa5fd1943c7b386f172d6893dbfba10b"
            
            logger.success(f"🧠 [Brain Engine] 破译完成: {final_rules}")
            return final_rules
        except Exception as e:
            logger.error(f"❌ AI 逆向失败: {e}")
            raise e

    async def heal(self):
        raw_url = await self._capture_real_traffic()
        if not raw_url:
            raise Exception("无法截获真实请求")
        new_rules = await self._extract_rules_via_llm(raw_url)
        os.makedirs("config", exist_ok=True)
        with open("config/active_rules.json", "w", encoding="utf-8") as f:
            json.dump(new_rules, f, indent=4)
        logger.info("🧠 [Brain Engine] 自愈完成。")
