import asyncio
import json
import os
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    def __init__(self):
        os.makedirs("data", exist_ok=True)

    async def fetch_sector_kline(self, context, sid: str) -> bool:
        # 构建黄金 K 线 API
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
            logger.info(f"🚀 [API 请求] 正在通过浏览器网络栈拉取 {sid}...")
            
            # 使用 context.request.get 发起请求，底层使用 Chromium 真实的 TLS 握手特征，避开 WAF 拦截
            response = await context.request.get(url, headers={
                "Referer": "https://quote.eastmoney.com/"
            })
            
            if response.status != 200:
                logger.error(f"❌ 接口请求失败，HTTP 状态码: {response.status}")
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
        logger.info(f"🔬 启动 Playwright 浏览器网络栈代理引擎...")
        async with async_playwright() as p:
            # 开启 headless=True 即可，利用浏览器底层网络特征，无需虚拟显示服务 Xvfb 支持
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            context = await browser.new_context()
            
            tasks = []
            for i, sid in enumerate(sector_list):
                if i > 0:
                    # 错开 200ms 发射请求，避免瞬时并发过高
                    await asyncio.sleep(0.2)
                tasks.append(self.fetch_sector_kline(context, sid))
                
            results = await asyncio.gather(*tasks)
            await browser.close()
            
        success_count = sum(1 for r in results if r)
        logger.info(f"🏁 本轮测试执行完毕。成功率: {success_count}/{len(sector_list)}")
