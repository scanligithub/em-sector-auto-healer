import asyncio
import json
import os
import random
import time
import sys
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    def __init__(self, chunk_id: int):
        self.chunk_id = chunk_id
        self.output_dir = f"success_data_{chunk_id}"
        os.makedirs(self.output_dir, exist_ok=True)
        self.data_limit = 1000000

    async def fetch_sector(self, context, sid: str, name: str) -> bool:
        page = await context.new_page()
        # fqt 设为 0（不复权）确保指数行情 100% 成功返回
        url = (
            f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
            f"?secid={sid}&ut=fa5fd1943c7b386f172d6893dbfba10b"
            f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
            f"&klt=101&fqt=0&end=20500101&lmt={self.data_limit}"
        )
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            raw_text = await page.evaluate("() => document.body.innerText")
            data = json.loads(raw_text)
            if not data or "data" not in data or data["data"] is None: return False
            
            payload = data["data"]
            with open(os.path.join(self.output_dir, f"{sid}.json"), "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            logger.success(f"🎯 [Job {self.chunk_id}] 成功: {name} ({sid})")
            return True
        except Exception:
            return False
        finally:
            await page.close()

    async def run(self):
        # 1. 载入本节点的代办分块
        with open(f"chunks/chunk_{self.chunk_id}.json", "r", encoding="utf-8") as f:
            sectors = json.load(f)
            
        pending_list = [x.copy() for x in sectors]  # 独立深拷贝，跟踪生存状态
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36")
            
            # 会话预热
            warmup = await context.new_page()
            try:
                await warmup.goto(f"https://quote.eastmoney.com/bk/{sectors[0]['sid']}.html", timeout=20000)
                await asyncio.sleep(2)
            except: pass
            finally: await warmup.close()
            
            consecutive_failures = 0
            
            for item in sectors:
                sid, name = item["sid"], item["name"]
                await asyncio.sleep(random.uniform(2.0, 3.5))  # 大吞吐强制控流
                
                # 寻找当前 pending 列表里的对象引用
                ref_item = next(x for x in pending_list if x["sid"] == sid)
                
                if await self.fetch_sector(context, sid, name):
                    pending_list.remove(ref_item)  # 成功直接剔除出待办名单
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    # ⚠️ 失败了：给这个板块在这个节点上的失败计数加 1
                    ref_item["fail_count"] += 1
                    logger.error(f"❌ [Job {self.chunk_id}] 失败: {name} ({sid}) | 单体累计失败: {ref_item['fail_count']}/3")
                    
                    if consecutive_failures >= 2:
                        logger.critical(f"🚨 [Job {self.chunk_id}] 触发熔断！未运行板块保持原样退回队列。")
                        break
                        
            await browser.close()
            
        # 2. 将当前节点未完成或失败（包含已更新 fail_count）的板块退回文件，供裁判汇总
        with open(f"failed_list_{self.chunk_id}.json", "w", encoding="utf-8") as f:
            json.dump(pending_list, f, ensure_ascii=False)

if __name__ == "__main__":
    chunk_id = int(sys.argv[1])
    engine = MuscleEngine(chunk_id)
    asyncio.run(engine.run())
