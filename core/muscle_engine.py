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

    async def apply_stealth(self, page):
        """注入隐匿脚本，抹除 Playwright 特征"""
        stealth_js = """
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh']});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        """
        await page.add_init_script(stealth_js)

    async def run_stealth_sniffing(self, context, sid, semaphore):
        async with semaphore:
            self.stats["total"] += 1
            page = await context.new_page()
            await self.apply_stealth(page)

            # 精准路由：只拦截广告和统计，保留基础 CSS 保证布局
            await page.route(re.compile(r"pos\.baidu\.com|stats|analytics|adshow"), lambda r: r.abort())

            captured_payload = {"data": None}

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
                            logger.success(f"🎯 [V9] {sid} 全量包截获成功！")
                    except: pass

            page.on("response", handle_response)

            try:
                # 1. 拟人化打开页面
                url = f"https://quote.eastmoney.com/bk/{sid}.html"
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                
                # 2. 检查是否触发验证码
                if "smartvcode" in page.url or await page.locator("#smartvcode").is_visible():
                    logger.error(f"🛑 {sid} 触发验证码拦截，跳过")
                    return

                # 3. 动态定位 K 线图容器（根据截图，找到 chart 区域）
                # 东财通常使用 canvas 绘图，我们找最显眼的那个
                chart_locator = page.locator("canvas").first
                await chart_locator.wait_for(state="visible", timeout=20000)
                box = await chart_locator.bounding_box()
                
                if not box:
                    logger.warning(f"⚠️ {sid} 无法获取图表布局")
                    return

                logger.info(f"📍 [Layout] {sid} 容器定位: x={box['x']}, y={box['y']}, w={box['width']}")

                # 4. 拟人化滑块操作
                # 根据截图，Navigator 在 Chart 底部，我们取底部向上 15% 的位置
                drag_y = box['y'] + box['height'] * 0.88 
                start_x = box['x'] + box['width'] * 0.9  # 右侧手柄
                end_x = box['x'] + box['width'] * 0.1    # 向左拉到底

                # 模拟真人：先 hover，停顿，再拖拽
                await page.mouse.move(start_x, drag_y)
                await asyncio.sleep(random.uniform(0.5, 1.2))
                await page.mouse.down()
                await page.mouse.move(end_x, drag_y, steps=random.randint(60, 100)) # 极慢速平滑拖拽
                await page.mouse.up()

                # 5. 等待数据回传
                for _ in range(8):
                    if captured_payload["data"]: break
                    await asyncio.sleep(1)

                if captured_payload["data"]:
                    klines = captured_payload["data"]
                    batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                             float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                            for k in klines]
                    await self.db_queue.put((sid, batch))
                else:
                    logger.warning(f"🚫 {sid} 拖拽完成但无流量回传")

            except Exception as e:
                logger.error(f"💥 {sid} 任务异常: {str(e)[:100]}")
            finally:
                await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"👻 [V9 Stealth Mode] 启动中...")
        writer = asyncio.create_task(self.db_writer_task())
        
        async with async_playwright() as p:
            # 模拟真实的浏览器启动参数
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--use-gl=desktop', # 强制开启 GPU 渲染支持，防止 Canvas 不加载
                    '--no-sandbox'
                ]
            )
            
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.run_stealth_sniffing(context, sid, semaphore) for sid in sector_list]
            await asyncio.gather(*tasks)
            await browser.close()

        await self.db_queue.put(None)
        await writer
        logger.success(f"🏁 同步结束")
