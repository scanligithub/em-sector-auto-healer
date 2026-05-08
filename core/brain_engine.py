import os
import json
import re
import asyncio
from loguru import logger
from playwright.async_api import async_playwright
from openai import AsyncOpenAI
from urllib.parse import urlparse

class BrainEngine:
    def __init__(self):
        # 1. 获取并清洗 API KEY
        api_key = os.getenv("LLM_API_KEY", "").strip()
        if not api_key:
            logger.critical("❌ 致命错误：未找到 LLM_API_KEY！")
            raise ValueError("Missing LLM_API_KEY")
            
        # 2. 获取并清洗 BASE URL
        raw_base_url = os.getenv("LLM_BASE_URL", "").strip()
        if not raw_base_url:
            raw_base_url = "https://integrate.api.nvidia.com/v1"
        
        # 自动补全协议头并确保没有双斜杠
        if not raw_base_url.startswith("http"):
            base_url = f"https://{raw_base_url}"
        else:
            base_url = raw_base_url

        # 打印解析出的域名（用于排查 DNS 问题）
        parsed = urlparse(base_url)
        logger.info(f"🌐 [Brain Engine] AI 接口域名解析测试: {parsed.netloc}")

        # 3. 获取并清洗模型名称
        model_name = os.getenv("LLM_MODEL_NAME", "").strip()
        if not model_name:
            model_name = "openai/gpt-oss-120b"

        self.model_name = model_name
        self.llm_client = AsyncOpenAI(
            api_key=api_key,
            base_url=base_url
        )

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
                    logger.success(f"🧠 [Brain Engine] 成功截获底层 K 线请求: {target_url[:80]}...")
                await route.continue_()

            await page.route("**/*", handle_request)
            try:
                await page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", timeout=20000)
                await page.wait_for_timeout(5000)
            except Exception as e:
                logger.warning(f"🧠 [Brain Engine] 流量嗅探阶段提示: {e}")
            finally:
                await browser.close()
        return target_url

    def _clean_json_response(self, raw_str: str) -> dict:
        match = re.search(r'\{.*\}', raw_str, re.DOTALL)
        if match:
            clean_str = match.group(0)
        else:
            clean_str = raw_str
        try:
            return json.loads(clean_str)
        except Exception as e:
            logger.error(f"❌ JSON 解析失败: {raw_str}")
            raise e

    async def _extract_rules_via_llm(self, raw_url: str) -> dict:
        logger.info(f"🧠 [Brain Engine] 正在通过模型 [{self.model_name}] 逆向参数...")
        prompt = f"你是一个API分析专家。从该URL提取ut, fltt, invt, klt参数并返回纯JSON: {raw_url}"
        
        try:
            # 💡 注意：移除了 response_format 兼容 NVIDIA 接口
            response = await self.llm_client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            raw_output = response.choices[0].message.content
            rules = self._clean_json_response(raw_output)
            logger.success(f"🧠 [Brain Engine] AI 破译完成: {rules}")
            return rules
        except Exception as e:
            logger.error(f"❌ 调用 AI 失败: {str(e)}")
            raise e

    async def heal(self):
        raw_url = await self._capture_real_traffic()
        if not raw_url:
            raise Exception("无法截获真实请求")
        new_rules = await self._extract_rules_via_llm(raw_url)
        os.makedirs("config", exist_ok=True)
        with open("config/active_rules.json", "w", encoding="utf-8") as f:
            json.dump(new_rules, f, indent=4)
        logger.info("🧠 [Brain Engine] 自愈完成，规则已保存。")
