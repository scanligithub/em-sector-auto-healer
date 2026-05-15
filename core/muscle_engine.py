import asyncio
import json
import os
import re
import duckdb
import random
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

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

    async def apply_ultra_stealth(self, page):
        """顶级隐匿：伪造 WebGL 渲染器和硬件特征，欺骗东财环境检测"""
        stealth_script = """
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        window.chrome = { runtime: {} };
        const getParameter = HTMLCanvasElement.prototype.getContext('2d').getParameter;
        // 伪造显卡信息
        const debugInfo = { unmaskedVendorWebgl: 'Google Inc. (Intel)', unmaskedRendererWebgl: 'ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, Generic)' };
        """
        await page.add_init_script(stealth_script)

    async def run_v16_mission(self, context, sid, semaphore):
        async with semaphore:
            self.stats["total"] += 1
            page = await context.new_page()
            await self.apply_ultra_stealth(page)

            try:
                # 1. 导航至板块页（为了获得合法的 Referer 和 Cookie 上下文）
                url = f"https://quote.eastmoney.com/bk/{sid}.html"
                await page.goto(url, wait_until="domcontentloaded")
                
                # 💡 留出 2 秒给浏览器生成 st_psi 等关键动态 Cookie
                await asyncio.sleep(2)

                # 2. 注入“原生脚本桥”：手动创建 script 标签并监听回调
                # 这是最强劫持：它强制生成 Sec-Fetch-Dest: script 头部
                api_url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={sid}"
                           f"&ut={self.UT}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                           f"&klt=101&fqt=1&end=20500101&lmt=1000000")
                
                bridge_js = f"""
                async () => {{
                    return new Promise((resolve, reject) => {{
                        const cbName = "callback_" + Math.random().toString(36).slice(2);
                        window[cbName] = (data) => {{
                            resolve(data);
                            delete window[cbName];
                        }};
                        const script = document.createElement('script');
                        script.src = "{api_url}&cb=" + cbName;
                        script.onerror = () => reject("WAF_BLOCK_OR_NETWORK_ERROR");
                        document.head.appendChild(script);
                        // 20秒超时保护
                        setTimeout(() => reject("TIMEOUT"), 20000);
                    }});
                }}
                """
                
                logger.info(f"🧬 [V16 Bridge] {sid} 正在通过原生脚本桥渗透...")
                data = await page.evaluate(bridge_js)

                if data and data.get("rc") == 0 and data.get("data", {}).get("klines"):
                    klines = data["data"]["klines"]
                    batch = [(sid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                             float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                            for k in klines]
                    await self.db_queue.put((sid, batch))
                    logger.success(f"🎯 [V16 Success] {sid} 获取数据成功！共 {len(klines)} 行")
                else:
                    logger.warning(f"🚫 {sid} 接口回馈异常或格式不符")

            except Exception as e:
                logger.error(f"💥 {sid} 任务失败: {str(e)[:100]}")
            finally:
                await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"🚀 [V16 Ultimate Edition] 启动原生桥接同步...")
        writer = asyncio.create_task(self.db_writer_task())
        async with async_playwright() as p:
            # 💡 增加启动参数：伪造渲染栈
            browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage', '--use-gl=desktop'])
            context = await browser.new_context(viewport={'width': 1280, 'height': 800}, user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
            
            semaphore = asyncio.Semaphore(self.concurrency)
            for sid in sector_list:
                await self.run_v16_mission(context, sid, semaphore)
            await browser.close()

        await self.db_queue.put(None)
        await writer
