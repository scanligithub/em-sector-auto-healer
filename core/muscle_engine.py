import asyncio
import json
import os
import random
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    def __init__(self):
        os.makedirs("data", exist_ok=True)

    async def fetch_sector_kline(self, context, sid: str) -> bool:
        url = (
            f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
            f"?secid={sid}"
            f"&ut=fa5fd1943c7b386f172d6893dbfba10b"
            f"&fields1=f1,f2,f3,f4,f5,f6"
            f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
            f"&klt=101"
            f"&fqt=1"
            f"&end=20500101"
            f"&lmt=1000000"
        )
        
        try:
            logger.info(f"🚀 [API 请求] 正在拉取 {sid}...")
            
            # 发起请求。由于 context 已预热，此处的 get 请求将自动携带合法的浏览器 Cookies
            response = await context.request.get(url, headers={
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Referer": "https://quote.eastmoney.com/",
                "Connection": "keep-alive"
            })
            
            if response.status != 200:
                logger.error(f"❌ {sid} 接口请求失败，HTTP 状态码: {response.status}")
                return False
                
            data_json = await response.json()
            
            if not data_json or "data" not in data_json or data_json["data"] is None:
                logger.warning(f"⚠️ 板块 {sid} 未返回有效内容")
                return False
                
            payload = data_json["data"]
            name = payload.get("name", "未知")
            code = payload.get("code", "未知")
            dktotal = payload.get("dktotal", 0)
            klines = payload.get("klines", [])
            
            logger.success(
                f"🎯 [数据就绪] 板块: {name} ({code}) | "
                f"历史天数: {dktotal} | "
                f"实际拉取记录数: {len(klines)}"
            )
            
            output_path = f"data/{sid}_history.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=4)
            
            if klines:
                logger.info(f"📊 样本数据检验 -> [首条] {klines[0]} | [末条] {klines[-1]}")
            return True
            
        except Exception as e:
            logger.error(f"💥 数据拉取或解析异常 ({sid}): {e}")
            return False

    async def run_factory(self, sector_list):
        logger.info(f"🔬 启动安全脱敏与会话预热的浏览器网络栈代理引擎...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            
            # 伪装成标准的 Windows 桌面端 Chrome 浏览器
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                locale="zh-CN",
                timezone_id="Asia/Shanghai"
            )
            
            # =================【核心优化：会话 Cookie 预热】=================
            logger.info("⏳ 正在进行全局会话 Cookie 预热（模拟首次真人访问）...")
            warmup_page = await context.new_page()
            try:
                # 访问东财板块主页，让 WAF 写入合法的全局会话 Cookie
                await warmup_page.goto(
                    "https://quote.eastmoney.com/bk/90.BK1063.html", 
                    wait_until="domcontentloaded", 
                    timeout=30000
                )
                # 给浏览器充足的时间完成 Cookie 的写入与稳定
                await asyncio.sleep(2.0)
                logger.success("🔑 Cookie 会话预热成功，已获取合法浏览器凭证。")
            except Exception as e:
                logger.warning(f"⚠️ 会话预热超时或失败 (将尝试裸请求): {e}")
            finally:
                await warmup_page.close()
            # ===============================================================
            
            results = []
            for i, sid in enumerate(sector_list):
                if i > 0:
                    # 引入 1.5s 到 3s 之间的随机人类抖动延迟，打破固定频率特征
                    delay = random.uniform(1.5, 3.0)
                    logger.info(f"💤 随机静默 {delay:.2f} 秒以模拟人类行为节奏...")
                    await asyncio.sleep(delay)
                
                res = await self.fetch_sector_kline(context, sid)
                results.append(res)
                
            await browser.close()
            
        success_count = sum(1 for r in results if r)
        logger.info(f"🏁 本轮测试执行完毕。成功率: {success_count}/{len(sector_list)}")
