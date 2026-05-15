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
        # 行为模拟模式，建议并发 1，确保持久化上下文不冲突
        self.concurrency = 1
        self.db_path = "data/sector_quant.db"
        self.user_data_dir = "data/browser_profile"
        self.db_queue = asyncio.Queue()
        self.stats = {"total": 0, "success": 0, "failed": 0, "rows": 0}
        
        os.makedirs("data", exist_ok=True)
        os.makedirs(self.user_data_dir, exist_ok=True)
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

    async def run_v14_mission(self, context, sid, semaphore):
        async with semaphore:
            self.stats["total"] += 1
            # 在持久化上下文中新建页面
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
                # 1. 进入页面
                await page.goto(url, wait_until="load", timeout=60000)
                
                # 2. [关键] 检查 Chart Engine 是否加载
                engine_check = await page.evaluate("() => typeof window.EMChart !== 'undefined' || typeof window.KKE !== 'undefined'")
                logger.info(f"🔍 [Check] {sid} 引擎状态: {'已激活' if engine_check else '未激活(降级)'}")

                # 3. [行为熟化] 模拟真人看盘，激活 JS 监听器
                await page.mouse.move(random.randint(200, 800), random.randint(200, 600))
                await page.mouse.wheel(0, 300)
                await asyncio.sleep(random.uniform(1, 2))
                await page.mouse.wheel(0, -300)

                # 4. 寻找并点击“上帝开关”
                god_btn = page.locator("a:has-text('拉长K线')").first
                if await god_btn.is_visible():
                    # 模拟真人点击：先 Hover，停顿，再点击
                    await god_btn.hover()
                    await asyncio.sleep(0.5)
                    await god_btn.click()
                    logger.info(f"⚡ [Action] {sid} 上帝开关已点击")
                else:
                    logger.warning(f"⚠️ {sid} 未发现‘拉长K线’按钮")

                # 5. 等待截流
                for _ in range(12):
                    if captured_data["raw"]:
                        logger.success(f"🎯 [V14 Success] {sid} 获取数据成功！")
                        break
                    await asyncio.sleep(1)

                if captured_data["raw"]:
                    klines = captured_data["raw"]
                    batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                             float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                            for k in klines]
                    await self.db_queue.put((sid, batch))
                else:
                    logger.warning(f"🚫 {sid} 任务结束，未截获全量包")

            except Exception as e: logger.error(f"💥 {sid} 异常: {str(e)[:100]}")
            finally: await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"🚀 [V14 Persistent Mode] 启动中...")
        writer = asyncio.create_task(self.db_writer_task())
        
        async with async_playwright() as p:
            # 关键：使用持久化上下文
            context = await p.chromium.launch_persistent_context(
                user_data_dir=self.user_data_dir,
                headless=True, # 在 GitHub Actions 里只能设为 True，但配合持久化目录效果更好
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-dev-shm-usage'
                ],
                viewport={'width': 1280, 'height': 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            
            # 注入脚本，彻底抹除自动化指纹
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            semaphore = asyncio.Semaphore(self.concurrency)
            # 为了维持 Session 连续性，建议串行或极低并发
            for sid in sector_list:
                await self.run_v14_mission(context, sid, semaphore)
            
            await context.close()
            
        await self.db_queue.put(None)
        await writer
        logger.success(f"🏁 压测总结 | 成功: {self.stats['success']}")
