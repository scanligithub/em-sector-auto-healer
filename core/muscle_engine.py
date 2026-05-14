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

    async def run_v11_mission(self, context, sid, semaphore):
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
                # wait_until 使用 load，确保 JS 引擎完全启动
                await page.goto(url, wait_until="load", timeout=60000)
                
                # 1. 寻找主 K 线 Canvas（面积最大的那个）
                canvases = await page.locator("canvas").all()
                main_canvas = None
                max_area = 0
                for c in canvases:
                    box = await c.bounding_box()
                    if box and (box['width'] * box['height'] > max_area):
                        max_area = box['width'] * box['height']
                        main_canvas = c
                
                if not main_canvas:
                    logger.warning(f"⚠️ {sid} 未发现有效图表")
                    return

                # 2. 核心步骤：将图表滚动到视口中央，确保坐标在 0-800 之间
                await main_canvas.scroll_into_view_if_needed()
                await asyncio.sleep(1) # 等待滚动平稳
                
                # 3. 重新获取滚动后的物理坐标
                box = await main_canvas.bounding_box()
                logger.info(f"📍 [V11 Layout] {sid} 视口内坐标: x={box['x']}, y={box['y']}, w={box['width']}, h={box['height']}")

                # 4. 精准计算滑块位置 (在 Canvas 底部边缘向上 20 像素处)
                # 这个位置通常是 Navigator 的操作手柄
                drag_y = box['y'] + box['height'] - 25 
                start_x = box['x'] + box['width'] - 30 # 右侧手柄
                end_x = box['x'] + 30                 # 左侧终点

                # 5. 执行物理模拟
                await page.mouse.move(start_x, drag_y)
                await page.mouse.down()
                # 匀速拖拽，模拟人类“拉历史”的行为
                await page.mouse.move(end_x, drag_y, steps=60)
                await page.mouse.up()

                # 6. 等待截流
                for _ in range(12):
                    if captured_data["raw"]:
                        logger.success(f"🎯 [V11] {sid} 物理劫持成功！")
                        break
                    await asyncio.sleep(1)

                if captured_data["raw"]:
                    klines = captured_data["raw"]
                    batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                             float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                            for k in klines]
                    await self.db_queue.put((sid, batch))
                else:
                    logger.warning(f"🚫 {sid} 拖拽点 y={drag_y:.0f} (视口内)，但未捕获请求")

            except Exception as e: logger.error(f"💥 {sid} 异常: {str(e)[:50]}")
            finally: await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"🚀 [V11] 启动视口对齐探测器...")
        writer = asyncio.create_task(self.db_writer_task())
        async with async_playwright() as p:
            # 开启软件 GPU 模拟，确保 Headless 下 Canvas 渲染正常
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage', '--use-gl=desktop'])
            context = await browser.new_context(viewport={'width': 1280, 'height': 800})
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.run_v11_mission(context, sid, semaphore) for sid in sector_list]
            await asyncio.gather(*tasks)
            await browser.close()
        await self.db_queue.put(None)
        await writer
