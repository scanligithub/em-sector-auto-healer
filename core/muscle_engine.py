import asyncio
import json
import os
import re
import duckdb
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    def __init__(self):
        self.concurrency = int(os.getenv("CONCURRENCY", 1))
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

    async def run_sniffing_mission(self, context, sid, semaphore):
        async with semaphore:
            self.stats["total"] += 1
            page = await context.new_page()
            
            # 策略 1: 拦截并丢弃所有无关资源（图片、广告、字体），极大提升加载速度
            await page.route("**/*.{png,jpg,jpeg,gif,woff,woff2,css}", lambda route: route.abort())
            await page.route(re.compile(r"analytics|pos\.baidu\.com|stats"), lambda route: route.abort())

            captured_payload = {"data": None}

            # 诊断日志：记录每一个请求
            async def log_request(request):
                if "push2his" in request.url:
                    logger.debug(f"🔍 [Intercept] 发现候选包: {request.url[:80]}...")

            async def handle_response(response):
                url = response.url
                if "push2his.eastmoney.com/api/qt/stock/kline/get" in url and "lmt=1000000" in url:
                    try:
                        text = await response.text()
                        match = re.search(r'\((.*)\)', text, re.DOTALL)
                        clean_json = match.group(1) if match else text
                        data = json.loads(clean_json)
                        if data.get("data", {}).get("klines"):
                            captured_payload["data"] = data["data"]["klines"]
                            logger.success(f"✅ [Sniffer] 成功捕获全量 K 线包: {sid}")
                    except Exception as e:
                        logger.error(f"❌ [Sniffer] 包解析失败: {e}")

            page.on("request", log_request)
            page.on("response", handle_response)
            
            # 诊断：记录加载失败的请求
            page.on("requestfailed", lambda req: logger.debug(f"⚠️ 请求失败: {req.url[:60]} | {req.failure}"))

            try:
                url = f"https://quote.eastmoney.com/bk/{sid}.html"
                logger.info(f"🚀 [Target] 正在导航至: {sid}")
                
                # 策略 2: 放弃 networkidle，改用 commit 后立即开始轮询关键元素
                await page.goto(url, wait_until="commit", timeout=45000)
                
                # 等待 DOM 树中的 Chart 容器出现即可，不理会其他广告
                try:
                    await page.wait_for_selector("canvas", timeout=15000)
                    logger.debug(f"📈 [UI] {sid} K 线图容器已就绪")
                except:
                    logger.warning(f"⚠️ [UI] {sid} K 线图容器未在 15s 内加载，尝试盲拖")

                # 策略 3: 模拟物理拖拽（增加偏移量容错）
                # 我们在图表大概位置进行一次“暴力拉取”
                await asyncio.sleep(3)
                
                logger.info(f"🖱️ [Action] {sid} 执行物理拖拽触发历史补全...")
                # 尝试多个可能的滑块高度，增加成功率
                for y_offset in [560, 580]:
                    await page.mouse.move(1100, y_offset)
                    await page.mouse.down()
                    await page.mouse.move(200, y_offset, steps=40)
                    await page.mouse.up()
                    await asyncio.sleep(1)
                    if captured_payload["data"]: break

                # 等待数据包到达
                for _ in range(5):
                    if captured_payload["data"]: break
                    await asyncio.sleep(1)

                if captured_payload["data"]:
                    klines = captured_payload["data"]
                    batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                             float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                            for k in klines]
                    await self.db_queue.put((sid, batch))
                else:
                    # 失败截图，帮助我们在 Github Artifacts 中定位原因
                    await page.screenshot(path=f"data/{sid}_fail.png")
                    logger.warning(f"🚫 {sid} 最终未能捕获全量包")

            except Exception as e:
                logger.error(f"💥 {sid} 运行异常: {str(e)}")
            finally:
                await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"🎭 [V8-Nitro] 并发: {self.concurrency}")
        writer = asyncio.create_task(self.db_writer_task())
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
            context = await browser.new_context(viewport={'width': 1280, 'height': 800})
            
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.run_sniffing_mission(context, sid, semaphore) for sid in sector_list]
            await asyncio.gather(*tasks)
            await browser.close()

        await self.db_queue.put(None)
        await writer
        logger.success(f"🏁 压测总结 | 成功: {self.stats['success']} | 总行数: {self.stats['rows']}")
