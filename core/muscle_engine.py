import asyncio
import json
import os
import re
import duckdb
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    def __init__(self):
        # 既然是点击，速度极快，并发可以稍微提高到 2-3
        self.concurrency = int(os.getenv("CONCURRENCY", 2))
        self.db_path = "data/sector_quant.db"
        self.db_queue = asyncio.Queue()
        self.stats = {"total": 0, "success": 0, "failed": 0, "rows": 0}
        os.makedirs("data", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sector_klines (
                secid VARCHAR, date DATE, open DOUBLE, close DOUBLE, 
                high DOUBLE, low DOUBLE, volume DOUBLE, amount DOUBLE, 
                PRIMARY KEY(secid, date)
            )
        """)

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

    async def run_god_mode_mission(self, context, sid, semaphore):
        async with semaphore:
            self.stats["total"] += 1
            page = await context.new_page()
            
            captured_data = {"raw": None}
            async def handle_response(resp):
                # 嗅探所有 push2his 且包含 lmt=1000000 的包
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
                # 加载页面，只需要 DOM 就绪即可
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                
                # 1. 寻找“上帝开关”：.kzoom 容器下的“拉长K线”按钮
                god_btn = page.locator(".kzoom a:has-text('拉长K线')")
                
                # 2. 等待按钮出现（给图表 JS 一点加载时间）
                await god_btn.wait_for(state="visible", timeout=15000)
                
                # 3. 模拟点击：这是触发全量的物理钥匙
                # 点击 2 次，确保范围撑到最大
                await god_btn.click()
                await asyncio.sleep(0.5)
                await god_btn.click()
                logger.info(f"⚡ [GodMode] {sid} 上帝开关已激活 (Clicked)")

                # 4. 轮询截流结果
                for _ in range(10):
                    if captured_data["raw"]:
                        logger.success(f"🎯 [Success] {sid} 劫持成功 | 行数: {len(captured_data['raw'])}")
                        break
                    await asyncio.sleep(1)

                if captured_data["raw"]:
                    klines = captured_data["raw"]
                    batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                             float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                            for k in klines]
                    await self.db_queue.put((sid, batch))
                else:
                    logger.warning(f"🚫 {sid} 点击了按钮但未嗅探到数据包")

            except Exception as e:
                logger.error(f"💥 {sid} 异常: {str(e)[:50]}")
            finally:
                await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"🏗️ [V13-GodMode] 启动上帝开关数据工厂...")
        writer = asyncio.create_task(self.db_writer_task())
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
            context = await browser.new_context(viewport={'width': 1280, 'height': 800})
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.run_god_mode_mission(context, sid, semaphore) for sid in sector_list]
            await asyncio.gather(*tasks)
            await browser.close()
        await self.db_queue.put(None)
        await writer
        logger.success(f"🏁 压测总结 | 成功: {self.stats['success']} | 总行数: {self.stats['rows']}")
