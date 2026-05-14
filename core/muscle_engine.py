import asyncio
import json
import os
import random
import string
import duckdb
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        # 压力测试建议：GitHub Actions 环境设为 2-3，本地高性能机器可设为 10+
        self.concurrency = int(os.getenv("CONCURRENCY", 2))
        self.db_path = "data/sector_quant.db"
        self.db_queue = asyncio.Queue()
        self.stats = {"total": 0, "success": 0, "failed": 0, "rows": 0}
        
        os.makedirs("data", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        """全量压测表结构：Primary Key 保证重复写入不污染"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sector_klines (
                secid VARCHAR, 
                date DATE, 
                open DOUBLE, 
                close DOUBLE, 
                high DOUBLE, 
                low DOUBLE, 
                volume DOUBLE, 
                amount DOUBLE, 
                PRIMARY KEY(secid, date)
            )
        """)

    async def db_writer_task(self):
        """高频写入缓冲区"""
        while True:
            item = await self.db_queue.get()
            if item is None: break
            sid, batch = item
            try:
                if batch:
                    # 使用 INSERT OR IGNORE 应对压测中的重复拉取
                    self.conn.executemany(
                        "INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                        batch
                    )
                    self.stats["success"] += 1
                    self.stats["rows"] += len(batch)
            except Exception as e:
                logger.error(f"💾 {sid} 入库失败: {e}")
            finally:
                self.db_queue.task_done()

    async def hijack_jsonp_stress(self, browser, sid, semaphore):
        """核心：JSONP 脚本注入劫持（全量压测版）"""
        async with semaphore:
            self.stats["total"] += 1
            # 开启无痕上下文，模拟纯净访问环境
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                # 1. 宿主环境导航（快速模式）
                url = f"https://quote.eastmoney.com/bk/{sid}.html"
                await page.goto(url, wait_until="commit", timeout=30000)
                
                # 2. 全量 URL 构造 (lmt=1000000)
                # 强制 fqt=1 (前复权)
                api_url = (
                    f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={sid}"
                    f"&ut={self.UT}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                    f"&klt=101&fqt=1&beg=19900101&end=20500101&lmt=1000000"
                )

                cb_name = "cb_" + ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

                js_code = f"""
                async () => {{
                    return new Promise((resolve, reject) => {{
                        const timeout = setTimeout(() => reject('STRESS_TIMEOUT'), 20000);
                        
                        window['{cb_name}'] = (data) => {{
                            clearTimeout(timeout);
                            resolve(data);
                            delete window['{cb_name}'];
                            const el = document.getElementById('{cb_name}_script');
                            if(el) el.remove();
                        }};

                        const script = document.createElement('script');
                        script.id = '{cb_name}_script';
                        script.src = '{api_url}&cb={cb_name}';
                        script.onerror = () => reject('SCRIPT_LOAD_ERROR');
                        document.body.appendChild(script);
                    }});
                }}
                """
                
                # 3. 注入执行
                data = await page.evaluate(js_code)

                if data and data.get("rc") == 0 and data.get("data", {}).get("klines"):
                    klines = data["data"]["klines"]
                    batch = []
                    for k in klines:
                        p = k.split(',')
                        batch.append((sid, p[0], float(p[1]), float(p[2]), 
                                     float(p[3]), float(p[4]), float(p[5]), float(p[6])))
                    
                    await self.db_queue.put((sid, batch))
                    logger.success(f"🔥 [STRESS] {sid} | 全量拉取 {len(klines)} 行 | 成功")
                else:
                    self.stats["failed"] += 1
                    logger.warning(f"⚠️ {sid} 未返回有效数据")

            except Exception as e:
                self.stats["failed"] += 1
                logger.error(f"❌ {sid} 压测穿透失败: {str(e)[:50]}")
            finally:
                # 关键：压测期间必须及时关闭上下文释放内存
                await context.close()

    async def run_factory(self, sector_list):
        logger.info(f"🚀 [V6 Stress Mode] 启动全量压力测试 | 目标: {len(sector_list)} 个板块")
        
        writer = asyncio.create_task(self.db_writer_task())
        
        async with async_playwright() as p:
            # 压测启动参数优化
            browser = await p.chromium.launch(
                headless=True, 
                args=[
                    '--disable-dev-shm-usage', 
                    '--no-sandbox', 
                    '--disable-gpu',
                    '--disable-extensions',
                    '--proxy-server="direct://"',
                    '--proxy-bypass-list=*'
                ]
            )
            
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self.hijack_jsonp_stress(browser, sid, semaphore) for sid in sector_list]
            
            await asyncio.gather(*tasks)
            await browser.close()

        await self.db_queue.put(None)
        await writer
        
        # 导出结果
        parquet_path = os.getenv('DATA_PATH', 'data/sector_klines_full.parquet')
        self.conn.execute(f"COPY sector_klines TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        
        logger.success(f"🏁 压测总结 | 成功率: {self.stats['success']}/{len(sector_list)} | 总行数: {self.stats['rows']}")
