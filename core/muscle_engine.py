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
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        # 🚀 降维：在 GitHub Actions 里，2 个并发浏览器才是最快的
        self.concurrency = 2 
        self.db_path = "data/sector_quant.db"
        self.db_queue = asyncio.Queue()
        self.stats = {"total": 0, "success": 0, "failed": 0}
        
        os.makedirs("data", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_klines (secid VARCHAR, date DATE, open DOUBLE, close DOUBLE, high DOUBLE, low DOUBLE, volume DOUBLE, amount DOUBLE, PRIMARY KEY(secid, date))")
        self.conn.execute("CREATE TABLE IF NOT EXISTS sector_master (secid VARCHAR PRIMARY KEY, last_update TIMESTAMP)")

    async def db_writer_task(self):
        while True:
            item = await self.db_queue.get()
            if item is None: break
            sid, batch = item
            try:
                self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                self.stats["success"] += 1
            except Exception as e:
                logger.error(f"💾 {sid} 入库失败: {e}")
            finally:
                self.db_queue.task_done()

    async def hijack_fetch(self, browser, sid, semaphore):
        """核心：在浏览器上下文内执行伪造 Fetch"""
        async with semaphore:
            self.stats["total"] += 1
            context = await browser.new_context(viewport={'width': 800, 'height': 600})
            page = await context.new_page()
            
            try:
                # 1. 快速导航（只需加载基础框架）
                url = f"https://quote.eastmoney.com/bk/{sid}.html"
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                
                # 💡 留出 1.5 秒给浏览器种下 psi 等动态 Cookie
                await asyncio.sleep(1.5)

                # 2. 注入 JS：直接在浏览器内部发起 Fetch 请求
                # 这是最强劫持：它继承了当前页面的所有 Cookie、TLS 和身份
                api_url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={sid}"
                           f"&ut={self.UT}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                           f"&klt=101&fqt=1&end=20500101&lmt=1000000")
                
                js_code = f"""
                async () => {{
                    const resp = await fetch('{api_url}');
                    return await resp.text();
                }}
                """
                raw_text = await page.evaluate(js_code)

                # 3. 解析结果
                if raw_text:
                    # 剥离可能存在的 JSONP 外壳
                    match = re.search(r'\((.*)\)', raw_text, re.DOTALL)
                    clean_json = match.group(1) if match else raw_text
                    data = json.loads(clean_json)
                    
                    if data.get("rc") == 0 and data.get("data", {}).get("klines"):
                        klines = data["data"]["klines"]
                        batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                                 float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                                for k in klines]
                        await self.db_queue.put((sid, batch))
                        logger.success(f"🎯 {sid} 劫持成功 | 获得 {len(klines)} 行全量数据")
                    else:
                        logger.warning(f"❌ {sid} 接口回馈异常: {raw_text[:100]}")
                        self.stats["failed"] += 1
                else:
                    self.stats["failed"] += 1

            except Exception as e:
                self.stats["failed"] += 1
                logger.debug(f"⚠️ {sid} 渗透失败: {str(e)[:50]}")
            finally:
                await context.close()

    async def get_active_sectors(self) -> list:
        res = self.conn.execute("SELECT secid FROM sector_master").fetchall()
        return [r[0] for r in res]

    async def run_factory(self, sector_list):
        logger.info(f"🏗️ [Phase 3] 浏览器渗透工厂启动 | 并发: {self.concurrency}")
        writer = asyncio.create_task(self.db_writer_task())
        
        async with async_playwright() as p:
            # 💡 增加启动参数，降低内存占用
            browser = await p.chromium.launch(headless=True, args=['--disable-dev-shm-usage', '--no-sandbox'])
            
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.hijack_fetch(browser, sid, semaphore) for sid in sector_list]
            await asyncio.gather(*tasks)
            await browser.close()

        await self.db_queue.put(None)
        await writer
        
        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        self.conn.execute(f"COPY sector_klines TO '{os.getenv('DATA_PATH')}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        logger.success(f"🏁 同步完成 | 库内记录: {final_cnt} | 成功: {self.stats['success']} | 失败: {self.stats['failed']}")
