import asyncio
import json
import os
import re
import duckdb
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    def __init__(self):
        # 🧪 诊断阶段：并发强制设为 1-2，看清行为真相
        self.concurrency = 1 
        self.db_path = "data/sector_quant.db"
        self.db_queue = asyncio.Queue()
        self.stats = {"total": 0, "success": 0, "failed": 0}
        
        os.makedirs("data", exist_ok=True)
        os.makedirs("screenshots", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_klines (secid VARCHAR, date DATE, open DOUBLE, close DOUBLE, high DOUBLE, low DOUBLE, volume DOUBLE, amount DOUBLE, PRIMARY KEY(secid, date))")

    async def db_writer_task(self):
        while True:
            item = await self.db_queue.get()
            if item is None: break
            sid, batch = item
            try:
                self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                self.stats["success"] += 1
            finally:
                self.db_queue.task_done()

    async def probe_behavior(self, context, sid, semaphore):
        """V8 探测器：模拟真实拖拽并截获流量"""
        async with semaphore:
            self.stats["total"] += 1
            page = await context.new_page()
            
            # 这里的逻辑是捕获所有东财行情服务器的响应
            captured_data = []

            async def handle_response(response):
                if "push2his.eastmoney.com" in response.url:
                    logger.debug(f"📡 探测到流量: {response.url[:100]}...")
                    try:
                        text = await response.text()
                        captured_data.append(text)
                    except:
                        pass

            page.on("response", handle_response)

            try:
                # 1. 进入页面
                url = f"https://quote.eastmoney.com/bk/{sid}.html"
                await page.goto(url, wait_until="networkidle", timeout=60000)
                
                # 2. 行为模拟：猛烈拖拽 K 线图中心区域
                # 假设 Canvas 在页面中心 (600, 400)
                await page.mouse.move(800, 400)
                await page.mouse.down()
                await page.mouse.move(200, 400, steps=20) # 缓慢向左拖动，模拟查看历史
                await page.mouse.up()
                
                # 3. 等待数据回传
                await asyncio.sleep(5) 

                if captured_data:
                    # 取最后一个请求（通常是拖拽触发的全量请求）
                    raw_text = captured_data[-1]
                    match = re.search(r'\((.*)\)', raw_text, re.DOTALL)
                    clean_json = match.group(1) if match else raw_text
                    data = json.loads(clean_json)

                    if data.get("data", {}).get("klines"):
                        klines = data["data"]["klines"]
                        batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                                 float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                                for k in klines]
                        await self.db_queue.put((sid, batch))
                        logger.success(f"🎯 [V8 Capture] {sid} | 成功获取 {len(klines)} 行")
                        return

                # 如果走到这里说明没捕获到，存个档
                await page.screenshot(path=f"screenshots/{sid}_failed.png")
                logger.warning(f"❌ {sid} 未截获到关键历史流量")

            except Exception as e:
                logger.error(f"⚠️ {sid} 探测崩溃: {str(e)[:100]}")
            finally:
                await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"🔍 [V8 Probe Mode] 启动单线程真相探测器 | 目标: {len(sector_list)}")
        writer = asyncio.create_task(self.db_writer_task())
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(viewport={'width': 1280, 'height': 800})
            
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.probe_behavior(context, sid, semaphore) for sid in sector_list]
            await asyncio.gather(*tasks)
            
            await browser.close()

        await self.db_queue.put(None)
        await writer
        logger.success(f"🏁 探测结束")
