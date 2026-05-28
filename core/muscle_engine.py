import asyncio
import json
import os
import sys
import random
import time
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
            logger.success(f"🎯 [Job {self.chunk_id}] 完成: {name} ({sid})")
            return True
        except Exception:
            return False
        finally:
            await page.close()

    async def run(self):
        # 1. 载入分配给本节点的待办清单
        with open(f"chunks/chunk_{self.chunk_id}.json", "r", encoding="utf-8") as f:
            sectors = json.load(f)
            
        pending_list = list(sectors)  # 剩余未完成名单
        
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
                await asyncio.sleep(random.uniform(2.0, 3.5))  # 全量数据严格控流
                
                if await self.fetch_sector(context, sid, name):
                    pending_list.remove(item)  # 成功后移出待办清单
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    logger.error(f"❌ [Job {self.chunk_id}] 挂起: {name} ({sid}) - 连败 {consecutive_failures}/2")
                    if consecutive_failures >= 2:
                        logger.critical(f"🚨 [Job {self.chunk_id}] 触发熔断！主动停止，保留未完成名单交由下一轮处理。")
                        break
                        
            await browser.close()
            
        # 2. 无论是否熔断，都将“剩余未完成名单”写入文件，供裁判 Job 汇总
        with open(f"failed_list_{self.chunk_id}.json", "w", encoding="utf-8") as f:
            json.dump(pending_list, f, ensure_ascii=False)

if __name__ == "__main__":
    chunk_id = int(sys.argv[1])
    engine = MuscleEngine(chunk_id)
    asyncio.run(engine.run())
