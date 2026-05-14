import asyncio
import json
import os
import re
import duckdb
import random
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    def __init__(self):
        self.concurrency = 1
        self.db_path = "data/sector_quant.db"
        self.db_queue = asyncio.Queue()
        self.stats = {"total": 0, "success": 0, "failed": 0, "rows": 0}
        os.makedirs("data", exist_ok=True)
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
                self.stats["rows"] += len(batch)
            finally: self.db_queue.task_done()

    async def run_v13_mission(self, context, sid, semaphore):
        async with semaphore:
            self.stats["total"] += 1
            page = await context.new_page()
            
            captured_url = {"url": None}
            captured_data = {"raw": None}

            async def handle_response(resp):
                # 记录所有 push2his 请求，方便诊断
                if "push2his.eastmoney.com" in resp.url:
                    captured_url["url"] = resp.url
                    if "lmt=1000000" in resp.url or "lmt=144" not in resp.url:
                        try:
                            text = await resp.text()
                            match = re.search(r'\((.*)\)', text, re.DOTALL)
                            data = json.loads(match.group(1) if match else text)
                            if data.get("data", {}).get("klines"):
                                captured_data["raw"] = data["data"]["klines"]
                        except: pass

            page.on("response", handle_response)

            try:
                url = f"https://quote.eastmoney.com/bk/{sid}.html"
                await page.goto(url, wait_until="load", timeout=60000)
                
                # 1. 策略 A：点击“拉长线”按钮（最高优先级）
                # 根据截图，该按钮通常包含“拉长线”文本
                logger.info(f"🖱️ [V13] {sid} 尝试点击‘拉长线’按钮...")
                try:
                    # 使用文本定位，更稳定
                    long_btn = page.get_by_text("拉长线")
                    await long_btn.wait_for(state="visible", timeout=10000)
                    # 连点 3 次，强制触发历史回溯
                    for _ in range(3):
                        await long_btn.click()
                        await asyncio.sleep(0.5)
                except:
                    logger.debug(f"⚠️ {sid} 未发现‘拉长线’按钮，转向滑块操作")

                # 2. 策略 B：物理滑块精准操作
                if not captured_data["raw"]:
                    left_handle = page.locator(".__sb_left")
                    if await left_handle.is_visible():
                        box = await left_handle.bounding_box()
                        # 起点：手柄中心
                        sx, sy = box['x'] + box['width']/2, box['y'] + box['height']/2
                        # 终点：由于 x=350，我们挪到 x=50 (保证在视口内)
                        tx = 50
                        
                        await page.mouse.move(sx, sy)
                        await page.mouse.down()
                        await page.mouse.move(tx, sy, steps=100) # 极慢速，模拟拖动过程
                        await page.mouse.up()
                        logger.info(f"🖱️ [V13] {sid} 滑块拖拽已执行 (从 {sx:.0f} 到 {tx})")

                # 3. 等待截流
                for _ in range(10):
                    if captured_data["raw"]:
                        logger.success(f"🎯 [V13] {sid} 劫持成功！行数: {len(captured_data['raw'])}")
                        break
                    await asyncio.sleep(1)

                if captured_data["raw"]:
                    klines = captured_data["raw"]
                    batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                             float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                            for k in klines]
                    await self.db_queue.put((sid, batch))
                else:
                    logger.warning(f"🚫 {sid} 失败。最近截获 URL: {captured_url['url']}")

            except Exception as e: logger.error(f"💥 {sid} 异常: {str(e)[:100]}")
            finally: await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"🚀 [V13 Final Edition] 启动按钮+拖拽组合探测...")
        writer = asyncio.create_task(self.db_writer_task())
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
            context = await browser.new_context(viewport={'width': 1280, 'height': 800})
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.run_v13_mission(context, sid, semaphore) for sid in sector_list]
            await asyncio.gather(*tasks)
            await browser.close()
        await self.db_queue.put(None)
        await writer
        logger.success(f"🏁 压测总结 | 成功: {self.stats['success']}")
