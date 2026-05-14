import asyncio
import json
import re
import os
import time
from datetime import datetime
import duckdb
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    def __init__(self):
        self.concurrency = int(os.getenv("CONCURRENCY", 3)) # 浏览器占用高，建议并发 2-4
        self.db_path = "data/sector_quant.db"
        self.db_queue = asyncio.Queue()
        self.stats = {"total_tasks": 0, "success_tasks": 0, "failed_tasks": 0}
        
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
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_master (secid VARCHAR PRIMARY KEY, last_update TIMESTAMP)")

    async def db_writer_task(self):
        """串行写入确保 DuckDB 安全"""
        while True:
            item = await self.db_queue.get()
            if item is None: break
            sid, batch = item
            try:
                self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                self.stats["success_tasks"] += 1
            except Exception as e:
                logger.error(f"💾 {sid} 入库失败: {e}")
            finally:
                self.db_queue.task_done()

    async def intercept_sector(self, browser, sid, semaphore):
        """单页面劫持逻辑"""
        async with semaphore:
            self.stats["total_tasks"] += 1
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            captured_data = {"batch": None}

            # 💡 核心：流量监听器
            async def handle_response(response):
                if "kline/get" in response.url and "lmt=1000000" in response.url:
                    try:
                        text = await response.text()
                        # 剥离 JSONP
                        match = re.search(r'jQuery\d+_\d+\((.*)\)', text, re.DOTALL)
                        if match:
                            data = json.loads(match.group(1))
                            if data.get("rc") == 0 and data.get("data", {}).get("klines"):
                                klines = data["data"]["klines"]
                                captured_data["batch"] = [
                                    (sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]),
                                     float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6]))
                                    for k in klines
                                ]
                    except Exception as e:
                        logger.debug(f"⚠️ 解析拦截内容失败: {e}")

            page.on("response", handle_response)

            try:
                # 1. 导航
                url = f"https://quote.eastmoney.com/bk/{sid}.html"
                await page.goto(url, wait_until="networkidle", timeout=60000)
                
                # 2. 模拟劫持动作：拖拽滑块
                # 东财的滑块通常在 K 线图下方，这里使用相对坐标或寻找特定 Canvas
                # 为了稳健，我们直接在页面上模拟一次“由右向左”的长距离拖拽
                await page.mouse.move(800, 600)
                await page.mouse.down()
                await page.mouse.move(200, 600, steps=20)
                await page.mouse.up()

                # 3. 等待数据捕获 (最多等 10 秒)
                for _ in range(20):
                    if captured_data["batch"]: break
                    await asyncio.sleep(0.5)

                if captured_data["batch"]:
                    await self.db_queue.put((sid, captured_data["batch"]))
                    logger.success(f"🎯 {sid} 捕获成功 | 行数: {len(captured_data['batch'])}")
                else:
                    self.stats["failed_tasks"] += 1
                    logger.warning(f"❌ {sid} 劫持失败：未监听到全量 API 响应")

            except Exception as e:
                self.stats["failed_tasks"] += 1
                logger.error(f"❌ {sid} 页面异常: {e}")
            finally:
                await context.close()

    async def get_active_sectors(self) -> list:
        """从本地 DB 加载名录"""
        res = self.conn.execute("SELECT secid FROM sector_master").fetchall()
        return [r[0] for r in res]

    async def run_factory(self, sector_list):
        """启动劫持工厂"""
        logger.info(f"🏗️ [Phase 3] 浏览器劫持工厂启动 | 并发: {self.concurrency}")
        
        # 启动 DB 写入者
        writer = asyncio.create_task(self.db_writer_task())
        
        async with async_playwright() as p:
            # 💡 关键：使用无头模式但伪装 stealth
            browser = await p.chromium.launch(headless=True)
            
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.intercept_sector(browser, sid, semaphore) for sid in sector_list]
            await asyncio.gather(*tasks)
            
            await browser.close()

        # 关闭 DB
        await self.db_queue.put(None)
        await writer
        
        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        output_parquet = os.getenv("DATA_PATH", "data/sector_klines_full.parquet")
        self.conn.execute(f"COPY sector_klines TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        logger.success(f"🏁 工厂作业完成 | 总量: {final_cnt} | 成功: {self.stats['success_tasks']} | 失败: {self.stats['failed_tasks']}")
