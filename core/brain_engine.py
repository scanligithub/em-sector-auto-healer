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
            logger.critical("❌ 致命错误：未找到 LLM_API_KEY！请检查 GitHub Secrets 配置。")
            raise ValueError("Missing LLM_API_KEY")
            
        # 2. 获取并清洗 BASE URL (自带智谱兜底)
        base_url = os.getenv("LLM_BASE_URL", "").strip()
        if not base_url:
            base_url = "https://open.bigmodel.cn/api/paas/v4/"
            logger.warning(f"⚠️ 未检测到 LLM_BASE_URL，自动使用默认智谱地址: {base_url}")
        elif not base_url.startswith("http"):
            base_url = f"https://{base_url}"
            logger.warning(f"⚠️ 自动为 LLM_BASE_URL 补充 https 协议头: {base_url}")
            
        # 3. 获取并清洗模型名称 (自带智谱兜底)
        model_name = os.getenv("LLM_MODEL_NAME", "").strip()
        if not model_name:
            model_name = "glm-4"
            logger.warning(f"⚠️ 未检测到 LLM_MODEL_NAME，自动使用默认模型: {model_name}")

        self.model_name = model_name
        self.llm_client = AsyncOpenAI(
            api_key=api_key,
            base_url=base_url
        )

    async def _capture_real_traffic(self) -> str:
        """步骤1：启动隐身浏览器，嗅探真实的 K 线请求 URL"""
        logger.info("🧠 [Brain Engine] 正在启动隐身浏览器进行流量嗅探...")
        target_url = None
        
        async with async_playwright() as p:
            # 启动无头浏览器 (关闭自动化特征)
            browser = await p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            # 网络请求拦截器：寻找目标 API
            async def handle_request(route, request):
                nonlocal target_url
                # 东财的K线接口特征
                if "api/qt/stock/kline/get" in request.url:
                    target_url = request.url
                    logger.success(f"🧠 [Brain Engine] 成功截获底层 K 线请求: {target_url[:80]}...")
                await route.continue_()

            await page.route("**/*", handle_request)

            try:
                # 访问东财白酒板块详情页，刺激其触发K线数据请求
                logger.info("🧠 [Brain Engine] 正在静默访问东方财富行情页...")
                await page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", timeout=15000)
                await page.wait_for_timeout(4000) # 等待页面 JS 引擎计算签名并发出请求
            except Exception as e:
                logger.warning(f"🧠 [Brain Engine] 页面加载超时或被强制打断，但不影响已截获的流量: {e}")
            finally:
                await browser.close()
                
        return target_url

    def _clean_json_response(self, raw_str: str) -> dict:
        """清洗大模型输出，防止其输出带有 markdown 格式的无效 json"""
        # 如果模型输出了 ```json ... ```，用正则提取里面的内容
        match = re.search(r'```(?:json)?(.*?)```', raw_str, re.DOTALL)
        if match:
            clean_str = match.group(1).strip()
        else:
            clean_str = raw_str.strip()
            
        try:
            return json.loads(clean_str)
        except json.JSONDecodeError as e:
            logger.error(f"❌ AI 返回的 JSON 格式损坏: {clean_str}")
            raise e

    async def _extract_rules_via_llm(self, raw_url: str) -> dict:
        """步骤2：利用 LLM 从混乱的 URL 中提取核心鉴权参数"""
        logger.info(f"🧠 [Brain Engine] 正在唤醒大模型 [{self.model_name}] 进行参数逆向...")
        
        prompt = f"""
        你是一个网络协议逆向专家。以下是我通过浏览器抓包获取到的最新有效的东方财富K线 API 请求URL：
        {raw_url}
        
        请分析这个URL，提取出用于鉴权和控制的核心参数。重点提取 `ut`、`fltt`、`invt`、`klt` 等固定参数。
        请忽略随机的时间戳参数（如 cb 结尾的随机数、_ 结尾的随机数）。
        
        请严格只输出一个 JSON 格式，例如：
        {{
            "ut": "提取到的ut值",
            "fltt": "2",
            "invt": "2",
            "klt": "101",
            "fqt": "0"
        }}
        绝对不要输出任何多余的解释、寒暄或 markdown 代码块标记。
        """
        
        try:
            response = await self.llm_client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                # 部分第三方 API 可能不支持 response_format，如果报错可将此行注释掉
                # response_format={"type": "json_object"}, 
                temperature=0.1
            )
            
            raw_output = response.choices[0].message.content
            rules = self._clean_json_response(raw_output)
            
            logger.success(f"🧠 [Brain Engine] AI 破译完成！新规则: {rules}")
            return rules
            
        except Exception as e:
            logger.error(f"❌ 调用大模型 API 失败: {e}")
            raise e

    async def heal(self):
        """执行完整的自愈闭环"""
        raw_url = await self._capture_real_traffic()
        if not raw_url:
            raise Exception("无法截获到真实请求，可能网络异常或东方财富彻底重构了页面！")
            
        new_rules = await self._extract_rules_via_llm(raw_url)
        
        # 写入最新配置到 JSON 文件
        os.makedirs("config", exist_ok=True)
        with open("config/active_rules.json", "w", encoding="utf-8") as f:
            json.dump(new_rules, f, indent=4)
        logger.info("🧠 [Brain Engine] 核心密钥自愈完成，新规则已成功注入 config/active_rules.json")
