import asyncio
import json
import os
import random
import time
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    def __init__(self, data_limit: int = 100):
        self.output_dir = "data"
        os.makedirs(self.output_dir, exist_ok=True)
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        self.data_limit = data_limit

    async def get_active_sectors(self, context) -> list:
        """
        从东财行情中心实时获取当前交易排名前 100 的行业板块
        """
        logger.info("📡 [CI 准备] 正在从数据源在线获取前 100 个活跃行业板块列表...")
        page = await context.new_page()
        list_url = (
            "https://push2.eastmoney.com/api/qt/clist/get"
            "?pn=1&pz=100&po=1&np=1"
            "&ut=bd1d9ddb04089700cf9c27f6f7426281"
            "&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!12&fields=f12,f14"
        )
        try:
            await page.goto(list_url, wait_until="domcontentloaded", timeout=20000)
            raw_text = await page.evaluate("() => document.body.innerText")
            list_data = json.loads(raw_text)
            sector_items = list_data.get("data", {}).get("diff", [])
            
            sectors = []
            for item in sector_items:
                code = item.get("f12")
                name = item.get("f14")
                if code and name:
                    sectors.append({"sid": f"90.{code}", "name": name})
            
            logger.success(f"✅ 在线板块列表加载成功，共获取到 {len(sectors)} 个板块。")
            return sectors
        except Exception as e:
            logger.error(f"💥 动态获取板块列表失败 (转为降级备用列表): {e}")
            return [{"sid": "90.BK1063", "name": "重组蛋白"}]
        finally:
            await page.close()

    async def fetch_sector_api(self, context, sid: str, name: str) -> bool:
        page = await context.new_page()
        url = (
            f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
            f"?secid={sid}"
            f"&ut=fa5fd1943c7b386f172d6893dbfba10b"
            f"&fields1=f1,f2,f3,f4,f5,f6"
            f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
            f"&klt=101"
            f"&fqt=1"
            f"&end=20500101"
            f"&lmt={self.data_limit}"
        )
        
        try:
            # 浏览器静默导航至目标 API 网页
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            raw_text = await page.evaluate("() => document.body.innerText")
            data_json = json.loads(raw_text)
            
            if not data_json or "data" not in data_json or data_json["data"] is None:
                return False
                
            payload = data_json["data"]
            klines = payload.get("klines", [])
            
            # 以紧凑 JSON 格式落地
            output_path = os.path.join(self.output_dir, f"{sid}_direct.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
                
            logger.success(f"🎯 [完成] 板块: {name} ({sid}) | 实拉记录数: {len(klines)}")
            return True
            
        except Exception:
            return False
        finally:
            await page.close()

    async def run_factory(self, max_sectors: int = 100):
        logger.info("🧪 启动云端 headless 无头浏览器数据吞吐管线...")
        
        async with async_playwright() as p:
            # 启动无头浏览器，注入防止内存崩溃的优化参数
            browser = await p.chromium.launch(
                headless=True, 
                args=[
                    '--no-sandbox', 
                    '--disable-dev-shm-usage',
                    '--disable-gpu'
                ]
            )
            
            context = await browser.new_context(
                user_agent=self.user_agent,
                viewport={"width": 800, "height": 600},
                locale="zh-CN",
                timezone_id="Asia/Shanghai"
            )
            
            # 获取交易板块
            sectors_all = await self.get_active_sectors(context)
            sectors = sectors_all[:max_sectors]
            
            if not sectors:
                await browser.close()
                return
            
            # 会话凭证预热
            logger.info("⏳ 正在进行全局会话 Cookie 预热（穿透测试的关键起点）...")
            warmup_page = await context.new_page()
            try:
                # 访问列表第一个板块的 HTML 页面，强制生成基本 Cookie 凭证
                await warmup_page.goto(
                    f"https://quote.eastmoney.com/bk/{sectors[0]['sid']}.html", 
                    wait_until="domcontentloaded", 
                    timeout=25000
                )
                await asyncio.sleep(2.0)
                logger.success("🔑 Cookie 预热执行完毕，会话凭证已在上下文中同步。")
            except Exception as e:
                logger.warning(f"⚠️ 会话预热遇到阻碍: {e}")
            finally:
                await warmup_page.close()
            
            # 启动计时
            start_time = time.time()
            success_count = 0
            
            # 顺序采集
            for i, item in enumerate(sectors):
                sid = item["sid"]
                name = item["name"]
                
                if i > 0:
                    # 引入随机人类抖动，防止频次检测
                    delay = random.uniform(1.2, 2.5)
                    await asyncio.sleep(delay)
                
                res = await self.fetch_sector_api(context, sid, name)
                if res:
                    success_count += 1
                else:
                    logger.error(f"❌ [挂起] 板块: {name} ({sid}) 网络通道被 WAF 强行重置。")
            
            # 停止计时
            end_time = time.time()
            await browser.close()
            
            # 计算云端吞吐指标
            total_time = end_time - start_time
            avg_latency = total_time / len(sectors) if sectors else 0
            throughput = len(sectors) / total_time if total_time > 0 else 0
            success_rate = (success_count / len(sectors)) * 100 if sectors else 0
            
            print("\n" + "="*50)
            print("📊  GitHub Actions 云端吞吐压力测试评估报告")
            print("="*50)
            print(f"🔹 数据同步模式: {'全量历史同步' if self.data_limit > 1000 else '日常增量同步'}")
            print(f"🔹 目标获取板块数: {len(sectors)} 个")
            print(f"🔹 成功获取板块数: {success_count} 个")
            print(f"🔹 综合同步成功率: {success_rate:.2f}%")
            print(f"🔹 总运行耗时: {total_time:.2f} 秒")
            print(f"🔹 单板块平均耗时 (含随机延迟): {avg_latency:.2f} 秒/个")
            print(f"🔹 接口系统吞吐率: {throughput:.2f} 个板块/秒")
            print("="*50 + "\n")
