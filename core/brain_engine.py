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

    async def _capture_with_semantic_filter(self) -> dict:
        """
        传感器层：Playwright 负责捕获原始信号，并进行初步‘语义提纯’
        """
        logger.info("🧠 [Brain Engine] 启动‘语义提纯’探测模式...")
        
        # 语义中间层：仅记录有意义的 API 候选者
        candidate_apis = []
        final_discovery = {"target_req": None, "cookies": "", "ua": ""}

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            # 监听器：将杂乱的网络流压缩为‘语义候选列表’
            async def handle_request(request):
                url = request.url
                # 语义过滤：只关注包含 kline 或 clist 的数据接口
                if "api/qt/" in url and ("kline" in url or "clist" in url):
                    candidate_apis.append({
                        "url": url,
                        "method": request.method,
                        "type": "data_api"
                    })

            page.on("request", handle_request)

            try:
                # 策略升级：不再死等 networkidle，只要 domcontentloaded 
                logger.debug("🌐 探测首页 (语义初审)...")
                await page.goto("https://www.eastmoney.com/", wait_until="domcontentloaded", timeout=15000)
                
                # 路径容错：如果首页响应慢，直接切入核心行情中心
                logger.debug("🌐 探测行情中心 (行为连续性注入)...")
                await page.goto("https://quote.eastmoney.com/center/gridlist.html", wait_until="domcontentloaded")
                
                # 模拟点击：寻找语义节点 '板块'
                # 这就是你说的：AI 应该通过语义去寻找，而不是硬编码
                board_link = page.get_by_role("link", name=re.compile("板块")).first
                if await board_link.is_visible():
                    await board_link.click()
                    await page.wait_for_timeout(3000)
                
                # 再次嗅探具体数据点
                await page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", wait_until="domcontentloaded")
                await page.wait_for_timeout(4000)

                cookies = await context.cookies()
                final_discovery["cookies"] = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
                final_discovery["ua"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                
            except Exception as e:
                logger.warning(f"⚠️ 传感器部分链路超时，进入残留信号分析模式: {e}")
            finally:
                await browser.close()
        
        # 语义压缩：只把过滤后的候选 API 交给 AI 决策
        final_discovery["api_candidates"] = candidate_apis[:10] # 只取前10个最重要的
        return final_discovery

    async def _decide_with_llm(self, semantic_data: dict) -> dict:
        """
        决策层：AI 不再读网页，而是‘理解浏览器的理解’
        """
        logger.info(f"🧠 [Brain Engine] AI 正在对语义中间层进行决策分析...")
        
        prompt = f"""
        你现在是量化系统决策器。浏览器传感器捕获到了以下‘候选API列表’：
        {json.dumps(semantic_data['api_candidates'], indent=2)}
        
        任务：
        1. 从列表中找出最像‘全量K线历史数据’的 URL。
        2. 提取该 URL 中的 `ut` 参数。
        3. 给出该请求的标准 Referer。
        
        请严格按 JSON 格式返回：
        {{
          "detected_ut": "...",
          "logic_referer": "https://quote.eastmoney.com/",
          "is_confidence_high": true
        }}
        """

        response = await self.llm_client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"}
        )
        
        decision = json.loads(response.choices[0].message.content)
        return decision

    async def heal(self):
        # 1. 获取提纯后的语义数据
        semantic_data = await self._capture_with_semantic_filter()
        
        if not semantic_data["api_candidates"]:
            # 如果彻底没抓到，尝试最后一次强行跳过首页的路径
            raise Exception("传感器未能捕获任何有效语义信号，请检查网络出口。")

        # 2. AI 决策
        decision = await self._decide_with_llm(semantic_data)
        
        # 3. 生成攻略
        final_profile = {
            "params": {
                "ut": decision["detected_ut"],
                "fltt": "2", "invt": "2", "klt": "101", "fqt": "0"
            },
            "headers": {
                "User-Agent": semantic_data["ua"],
                "Cookie": semantic_data["cookies"],
                "Referer": decision["logic_referer"],
                "Accept-Encoding": "gzip, deflate"
            }
        }
        
        os.makedirs("config", exist_ok=True)
        with open("config/active_rules.json", "w", encoding="utf-8") as f:
            json.dump(final_profile, f, indent=4)
        logger.success(f"🧠 [Brain Engine] 决策完成。信任度: {decision.get('is_confidence_high')}")
