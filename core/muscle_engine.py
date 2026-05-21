import asyncio
import os
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    def __init__(self):
        # 视觉验证，只测 1-2 个板块即可
        self.concurrency = 1
        os.makedirs("data", exist_ok=True)

    async def verify_rendering(self, context, sid):
        page = await context.new_page()
        screenshot_path = f"data/{sid}_headed_proof.png"
        
        try:
            url = f"https://quote.eastmoney.com/bk/{sid}.html"
            logger.info(f"🚀 [Headed Test] 正在打开 {sid}...")
            
            # 【关键修改】：放弃 networkidle，只要 DOM 出来就强行往下走
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            
            logger.info(f"⏳ DOM已就绪，死等 30 秒让东财的 K线JS 慢慢画图...")
            await asyncio.sleep(30)
            
            # 验证 1：全屏截图取证
            await page.screenshot(path=screenshot_path, full_page=True)
            logger.success(f"📸 正常截图已保存: {screenshot_path}")

            # 验证 2：探测那个决定命运的按钮是否存在
            god_btn = page.locator("a:has-text('拉长K线')").first
            is_visible = await god_btn.is_visible()
            
            if is_visible:
                logger.success(f"🎯 [Bingo!] {sid} 的 '拉长K线' 按钮真实可见！引擎渲染成功！")
            else:
                logger.warning(f"🚫 [Failed] {sid} 按钮不可见，请下载截图查看真实现场。")

            # 验证 3：看看底层的 JS 变量活了没有
            chart_state = await page.evaluate("""
            () => {
                return {
                    jquery: typeof window.jQuery,
                    echarts: typeof window.echarts,
                    chart: typeof window.KKE,
                    emchart: typeof window.EMChart
                }
            }
            """)
            logger.info(f"🧬 [JS State] 引擎状态: {chart_state}")

        except Exception as e:
            logger.error(f"💥 {sid} 崩溃: {e}")
            # 【关键防御】：如果发生异常，也要强行拍一张“死亡遗照”
            try:
                await page.screenshot(path=f"data/{sid}_crash_proof.png")
                logger.info(f"📸 崩溃现场截图已保存")
            except:
                pass
        finally:
            await page.close()

    async def run_factory(self, sector_list):
        logger.info(f"🔬 启动 headed+xvfb 视觉取证环境...")
        async with async_playwright() as p:
            # 【核心改动】：headless=False
            browser = await p.chromium.launch(
                headless=False, 
                args=[
                    '--no-sandbox', 
                    '--disable-dev-shm-usage',
                    '--window-size=1280,800'
                ]
            )
            context = await browser.new_context(viewport={'width': 1280, 'height': 800})
            
            for sid in sector_list:
                await self.verify_rendering(context, sid)
                
            await browser.close()
