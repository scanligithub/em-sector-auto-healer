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

    async def run_v10_mission(self, context, sid, semaphore):
        async with semaphore:
            self.stats["total"] += 1
            page = await context.new_page()
            # 抹除特征
            await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            captured_data = {"raw": None}
            async def handle_response(resp):
                if "push2his.eastmoney.com" in resp.url and "lmt=1000000" in resp.url:
                    try:
                        text = await resp.text()
                        match = re.search(r'\((.*)\)', text, re.DOTALL)
                        data = json.loads(match.group(1) if match else text)
                        if data.get("data", {}).get("klines"):
                            captured_data["raw"] = data["data"]["klines"]
                            logger.success(f"🎯 [V10] {sid} 截获全量包！")
                    except: pass

            page.on("response", handle_response)

            try:
                url = f"https://quote.eastmoney.com/bk/{sid}.html"
                await page.goto(url, wait_until="domcontentloaded")
                
                # 策略：东财页面通常有多个 Canvas，Navigator 滑块通常是最后 1-2 个
                # 我们获取所有 canvas 并对看起来像滑块的那个进行操作
                canvases = await page.locator("canvas").all()
                if len(canvases) < 2:
                    logger.warning(f"⚠️ {sid} 未能发现足够的 Canvas 元素")
                    return

                # 选取最后一个 Canvas（通常是 Navigator）
                target_canvas = canvases[-1]
                box = await target_canvas.bounding_box()
                logger.info(f"📍 [V10 Layout] {sid} 滑块定位: x={box['x']}, y={box['y']}, w={box['width']}, h={box['height']}")

                # 关键：在 Canvas 的 Y 轴正中心操作，x 轴从右往左猛拉
                mid_y = box['y'] + box['height'] / 2
                start_x = box['x'] + box['width'] - 10 # 右侧边缘
                end_x = box['x'] + 10                 # 左侧边缘

                # 模拟动作：先点一下激活，再拖拽
                await page.mouse.click(start_x, mid_y)
                await asyncio.sleep(0.5)
                await page.mouse.move(start_x, mid_y)
                await page.mouse.down()
                # steps=30 保持足够的速度触发位移监听，但又不过快
                await page.mouse.move(end_x, mid_y, steps=40)
                await page.mouse.up()

                # 如果没触发，再试一次“反向拉取”
                if not captured_data["raw"]:
                    await page.mouse.move(box['x'] + 50, mid_y)
                    await page.mouse.down()
                    await page.mouse.move(box['x'] + 200, mid_y, steps=30)
                    await page.mouse.up()

                # 轮询等待
                for _ in range(10):
                    if captured_data["raw"]: break
                    await asyncio.sleep(1)

                if captured_data["raw"]:
                    klines = captured_data["raw"]
                    batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                             float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                            for k in klines]
                    await self.db_queue.put((sid, batch))
                else:
                    # 诊断截图
                    await page.screenshot(path=f"data/{sid}_v10_fail.png")
                    logger.warning(f"🚫 {sid} 拖拽尝试结束，仍无全量包")

            except Exception as e: logger.error(f"💥 {sid} 异常: {str(e)[:50]}")
            finally: await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"🚀 [V10] 启动高精度物理探测器...")
        writer = asyncio.create_task(self.db_writer_task())
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
            context = await browser.new_context(viewport={'width': 1280, 'height': 800})
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.run_v10_mission(context, sid, semaphore) for sid in sector_list]
            await asyncio.gather(*tasks)
            await browser.close()
        await self.db_queue.put(None)
        await writer
        logger.success(f"🏁 压测总结 | 成功: {self.stats['success']}")
