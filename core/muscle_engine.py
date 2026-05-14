import asyncio
import json
import os
import re
import duckdb
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

    async def run_v12_mission(self, context, sid, semaphore):
        async with semaphore:
            self.stats["total"] += 1
            page = await context.new_page()
            
            captured_data = {"raw": None}
            async def handle_response(resp):
                if "push2his.eastmoney.com" in resp.url and "lmt=1000000" in resp.url:
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
                
                # 1. 寻找核心 DOM 元素：左侧拖拽手柄
                # 根据截图，Class 是 .__sb_left
                left_handle = page.locator(".__sb_left")
                
                # 2. 确保手柄已经加载并可见
                try:
                    await left_handle.wait_for(state="visible", timeout=15000)
                    logger.info(f"✅ [V12] {sid} 发现滑块手柄，准备拖拽...")
                except:
                    logger.warning(f"⚠️ {sid} 未发现 .__sb_left 手柄，尝试备选按钮...")
                    # 备选：点击“拉长线”按钮
                    long_btn = page.get_by_text("拉长线")
                    if await long_btn.is_visible():
                        await long_btn.click()
                    else:
                        return

                # 3. 执行 DOM 级别的拖拽
                # 获取手柄位置
                box = await left_handle.bounding_box()
                if box:
                    # 从当前手柄位置按住，往左拖 400 像素
                    await page.mouse.move(box['x'] + box['width']/2, box['y'] + box['height']/2)
                    await page.mouse.down()
                    await page.mouse.move(box['x'] - 400, box['y'] + box['height']/2, steps=50)
                    await page.mouse.up()
                
                # 4. 轮询截获
                for _ in range(10):
                    if captured_data["raw"]:
                        logger.success(f"🎯 [V12] {sid} 劫持成功！获取到数据")
                        break
                    await asyncio.sleep(1)

                if captured_data["raw"]:
                    klines = captured_data["raw"]
                    batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                             float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                            for k in klines]
                    await self.db_queue.put((sid, batch))
                else:
                    logger.warning(f"🚫 {sid} 拖拽已执行，但未嗅探到全量包")

            except Exception as e: logger.error(f"💥 {sid} 任务失败: {str(e)[:50]}")
            finally: await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"🚀 [V12 DOM Edition] 启动...")
        writer = asyncio.create_task(self.db_writer_task())
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
            context = await browser.new_context(viewport={'width': 1280, 'height': 800})
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.run_v12_mission(context, sid, semaphore) for sid in sector_list]
            await asyncio.gather(*tasks)
            await browser.close()
        await self.db_queue.put(None)
        await writer
