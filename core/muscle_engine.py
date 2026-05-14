import asyncio
import json
import os
import re
import duckdb
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        # 行为模拟较慢，但成功率 100%，建议并发 2-3
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
            finally:
                self.db_queue.task_done()

    async def behavioral_sniffing(self, browser_context, sid, semaphore):
        """核心：通过模拟用户行为触发原生 Chart 请求并截获"""
        async with semaphore:
            self.stats["total"] += 1
            page = await browser_context.new_page()
            
            # 这里的逻辑是：在页面打开的同时，等待一个符合 URL 模式的响应
            try:
                # 1. 准备拦截器
                target_url_part = "push2his.eastmoney.com/api/qt/stock/kline/get"
                
                # 2. 导航到板块页面
                url = f"https://quote.eastmoney.com/bk/{sid}.html"
                
                # 监听响应的异步上下文
                async with page.expect_response(lambda r: target_url_part in r.url, timeout=30000) as response_info:
                    await page.goto(url, wait_until="domcontentloaded")
                    
                    # 3. 行为触发：模拟鼠标滚轮或轻微拖动 K 线图区域
                    # 很多时候 goto 完 chart 就自动发请求了，如果没有，我们补一刀：
                    await page.mouse.move(400, 300)
                    await page.mouse.wheel(0, -5000) # 向上滚轮触发历史加载
                    
                    response = await response_info.value
                    raw_text = await response.text()

                    # 4. JSONP 解析 (截取括号内的内容)
                    match = re.search(r'\((.*)\)', raw_text, re.DOTALL)
                    clean_json = match.group(1) if match else raw_text
                    data = json.loads(clean_json)

                    if data.get("data") and data["data"].get("klines"):
                        klines = data["data"]["klines"]
                        batch = []
                        for k in klines:
                            p = k.split(',')
                            batch.append((sid, p[0], float(p[1]), float(p[2]), 
                                         float(p[3]), float(p[4]), float(p[5]), float(p[6])))
                        
                        await self.db_queue.put((sid, batch))
                        logger.success(f"💎 [V7 Sniff] {sid} | 捕获原生流量: {len(klines)} 行")
                    else:
                        logger.warning(f"⚠️ {sid} 截获流量但数据为空")

            except Exception as e:
                self.stats["failed"] += 1
                logger.error(f"❌ {sid} 行为模拟失败: {str(e)[:50]}")
            finally:
                await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"🎭 [V7 Behavioral Mode] 启动监听器工厂 | 并发: {self.concurrency}")
        writer = asyncio.create_task(self.db_writer_task())
        
        async with async_playwright() as p:
            # 使用统一的 BrowserContext，保持 Cookie 连续性
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
            # 模拟一个真实的现代 Chrome 环境
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={'width': 1280, 'height': 800}
            )
            
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.behavioral_sniffing(context, sid, semaphore) for sid in sector_list]
            
            await asyncio.gather(*tasks)
            
            await context.close()
            await browser.close()

        await self.db_queue.put(None)
        await writer
        
        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        logger.success(f"🏁 压测结束 | 成功: {self.stats['success']} | 库内总数: {final_cnt}")
