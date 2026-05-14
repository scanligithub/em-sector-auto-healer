import asyncio
import json
import os
import re
import duckdb
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    def __init__(self):
        # 行为驱动模式建议并发 1-2，GitHub Actions 环境下稳定第一
        self.concurrency = int(os.getenv("CONCURRENCY", 1))
        self.db_path = "data/sector_quant.db"
        self.db_queue = asyncio.Queue()
        self.stats = {"total": 0, "success": 0, "failed": 0, "rows": 0}
        
        os.makedirs("data", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        """主键使用 (secid, date)，防止重复拉取导致数据污染"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sector_klines (
                secid VARCHAR, date DATE, open DOUBLE, close DOUBLE, 
                high DOUBLE, low DOUBLE, volume DOUBLE, amount DOUBLE, 
                PRIMARY KEY(secid, date)
            )
        """)

    async def db_writer_task(self):
        """高效入库消费者"""
        while True:
            item = await self.db_queue.get()
            if item is None: break
            sid, batch = item
            try:
                if batch:
                    self.conn.executemany(
                        "INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                        batch
                    )
                    self.stats["success"] += 1
                    self.stats["rows"] += len(batch)
            except Exception as e:
                logger.error(f"💾 {sid} 入库异常: {e}")
            finally:
                self.db_queue.task_done()

    async def run_sniffing_mission(self, context, sid, semaphore):
        """V8 物理打击核心逻辑"""
        async with semaphore:
            self.stats["total"] += 1
            page = await context.new_page()
            
            # 用于存储本次交互捕获到的数据
            captured_payload = {"data": None}

            async def handle_response(response):
                """流量嗅探回调"""
                url = response.url
                # 目标：带有 lmt=1000000 的 K 线全量包
                if "push2his.eastmoney.com/api/qt/stock/kline/get" in url and "lmt=1000000" in url:
                    try:
                        text = await response.text()
                        # 解析 JSONP
                        match = re.search(r'\((.*)\)', text, re.DOTALL)
                        clean_json = match.group(1) if match else text
                        data = json.loads(clean_json)
                        if data.get("data", {}).get("klines"):
                            captured_payload["data"] = data["data"]["klines"]
                    except Exception as e:
                        logger.debug(f"⚠️ 解析拦截包失败: {e}")

            # 注册监听器
            page.on("response", handle_response)

            try:
                # 1. 导航并等待基础 UI 渲染
                url = f"https://quote.eastmoney.com/bk/{sid}.html"
                await page.goto(url, wait_until="networkidle", timeout=60000)
                
                # 2. 给 Chart 渲染留一点时间
                await asyncio.sleep(2)

                # 3. 执行物理拖拽 (1280x800 下的 Navigator 滑块坐标)
                # 起点：滑块右侧区域
                start_x, start_y = 1150, 560
                # 终点：最左侧（触发全量加载）
                end_x, end_y = 200, 560

                await page.mouse.move(start_x, start_y)
                await page.mouse.down()
                # steps=50 模拟真人平滑拖动的加速度
                await page.mouse.move(end_x, end_y, steps=50)
                await page.mouse.up()

                # 4. 等待网络回传 (东财全量数据包通常较大，需要 2-5 秒)
                for _ in range(10):
                    if captured_payload["data"]:
                        break
                    await asyncio.sleep(1)

                # 5. 处理捕获到的数据
                if captured_payload["data"]:
                    klines = captured_payload["data"]
                    batch = []
                    for k in klines:
                        p = k.split(',')
                        batch.append((sid, p[0], float(p[1]), float(p[2]), 
                                     float(p[3]), float(p[4]), float(p[5]), float(p[6])))
                    
                    await self.db_queue.put((sid, batch))
                    logger.success(f"💎 [V8 PRO] {sid} | 物理劫持成功 | 获得 {len(klines)} 行全量数据")
                else:
                    logger.warning(f"❌ {sid} 物理触发失败（未嗅探到全量包）")
                    self.stats["failed"] += 1

            except Exception as e:
                self.stats["failed"] += 1
                logger.error(f"💥 {sid} 任务崩溃: {str(e)[:100]}")
            finally:
                await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"🎭 [V8 Pro Behavior Mode] 启动物理截流工厂 | 目标: {len(sector_list)}")
        writer = asyncio.create_task(self.db_writer_task())
        
        async with async_playwright() as p:
            # 使用高拟人化参数启动浏览器
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox', 
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            
            # 建立一个统一的上下文环境，维持 Cookie 活性
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.run_sniffing_mission(context, sid, semaphore) for sid in sector_list]
            
            await asyncio.gather(*tasks)
            
            await context.close()
            await browser.close()

        # 等待队列清理
        await self.db_queue.put(None)
        await writer
        
        # 导出 Parquet
        parquet_path = os.getenv('DATA_PATH', 'data/sector_klines_full.parquet')
        self.conn.execute(f"COPY sector_klines TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        
        logger.success(f"🏁 任务总结 | 成功板块: {self.stats['success']} | 写入行数: {self.stats['rows']}")
