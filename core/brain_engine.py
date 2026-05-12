import os
import json
import re
import asyncio
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

    async def _capture_by_navigation(self) -> dict:
        """核心进化：通过真实导航路径发现 API 和 凭证"""
        logger.info("🧠 [Brain Engine] 启动导航链模拟: 首页 -> 行情中心 -> 板块")
        discovery = {"kline_url": "", "clist_url": "", "headers": {}, "cookies": ""}

        async with async_playwright() as p:
            # 注入避开自动化检测的参数
            browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            # 监听请求，发现 API 模式
            async def handle_request(request):
                if "api/qt/stock/kline/get" in request.url:
                    discovery["kline_url"] = request.url
                    # 抓取基础 Headers
                    forbidden = [':', 'host', 'connection', 'content-length']
                    discovery["headers"] = {k: v for k, v in request.headers.items() if not any(f in k.lower() for f in forbidden)}
                if "api/qt/clist/get" in request.url:
                    discovery["clist_url"] = request.url

            page.on("request", handle_request)

            try:
                # 1. 进入首页
                await page.goto("https://www.eastmoney.com/", wait_until="networkidle", timeout=20000)
                await page.wait_for_timeout(1000)

                # 2. 点击行情中心 (模拟用户行为)
                await page.goto("https://quote.eastmoney.com/center/gridlist.html", wait_until="networkidle")
                
                # 3. 点击导航中的“板块行情”
                logger.debug("🖱️ 正在模拟点击导航中的 '板块' 节点...")
                await page.get_by_role("link", name=re.compile("板块")).first.click()
                await page.wait_for_load_state("networkidle")

                # 4. 进入列表页后，点击第一个板块触发 K 线 API
                await page.locator("td.next.column_node a").first.click()
                await page.wait_for_timeout(5000)

                # 提取产生的最新 Cookie
                cookies = await context.cookies()
                discovery["cookies"] = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
            except Exception as e:
                logger.warning(f"⚠️ 导航链部分中断，但可能已捕获到关键信息: {e}")
            finally:
                await browser.close()
        return discovery

    async def heal(self):
        logger.info("🧠 [Brain Engine] 正在通过 AI 提取导航产生的最新鉴权参数...")
        raw_data = await self._capture_by_navigation()
        
        if not raw_data["kline_url"]:
            raise Exception("无法自动发现 K 线接口，请检查东财 UI 是否大规模重构")

        # 使用 AI 提取 ut 等动态参数
        prompt = f"分析此URL并提取ut参数，以JSON格式返回: {raw_data['kline_url']}"
        response = await self.llm_client.chat.completions.create(
            model=self.model_name, messages=[{"role": "user", "content": prompt}], temperature=0
        )
        
        # 鲁棒解析 JSON
        match = re.search(r'\{.*\}', response.choices[0].message.content, re.DOTALL)
        ut_val = json.loads(match.group(0)).get("ut")

        # 构造最终攻略
        final_rules = {
            "params": {
                "ut": ut_val,
                "fltt": "2",
                "invt": "2",
                "klt": "101",
                "fqt": "0"
            },
            "headers": {
                **raw_data["headers"],
                "Cookie": raw_data["cookies"],
                "Referer": "https://quote.eastmoney.com/"
            }
        }
        
        os.makedirs("config", exist_ok=True)
        with open("config/active_rules.json", "w", encoding="utf-8") as f:
            json.dump(final_rules, f, indent=4)
        logger.success("🧠 [Brain Engine] AI 自愈完成：已同步最新的行为指纹和鉴权参数。")
