import asyncio
import json
import os
import random
import time
import sys
from loguru import logger
from playwright.async_api import async_playwright
from core.sector_catalog_builder import build_sector_catalog

class LocalBenchmark100:
    def __init__(self, data_limit: int = 1000000):
        self.output_dir = "data"
        os.makedirs(self.output_dir, exist_ok=True)
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        self.data_limit = data_limit  # 默认为 1000000 条（拉取全部历史数据）

    async def fetch_sector_api(self, context, sid: str, name: str) -> bool:
        page = await context.new_page()
        # 核心：板块行情没有复权机制，fqt 强制设为 0（不复权）解决东财部分指数行情网关报错 null 的问题
        url = (
            f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
            f"?secid={sid}"
            f"&ut=fa5fd1943c7b386f172d6893dbfba10b"
            f"&fields1=f1,f2,f3,f4,f5,f6"
            f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
            f"&klt=101"
            f"&fqt=0"
            f"&end=20500101"
            f"&lmt={self.data_limit}"
        )
        
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            raw_text = await page.evaluate("() => document.body.innerText")
            data_json = json.loads(raw_text)
            
            if not data_json or "data" not in data_json or data_json["data"] is None:
                return False
                
            payload = data_json["data"]
            klines = payload.get("klines", [])
            
            output_path = os.path.join(self.output_dir, f"{sid}_direct.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
                
            logger.success(f"🎯 [完成] 板块: {name} ({sid}) | 实拉记录数: {len(klines)}")
            return True
            
        except Exception:
            return False
        finally:
            await page.close()

    async def run_pipeline(self):
        logger.info("🧪 启动自愈型云端无头浏览器数据下载管线...")
        
        # =================【核心：基于方案二的动态板块生成】=================
        # 实时反向扫描全 A 股映射，动态构建最新最全的板块大名单，不再使用固定写死的 100 列表
        try:
            sectors = build_sector_catalog()
            logger.success(f"🏆 板块目录加载成功！动态发现全市场板块总数: {len(sectors)} 个。")
        except Exception as e:
            logger.critical(f"💥 动态板块目录生成失败: {e}，管线被迫中止。")
            sys.exit(1)
        # ====================================================================
        
        async with async_playwright() as p:
            # 启动无头浏览器
            browser = await p.chromium.launch(
                headless=True, 
                args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
            )
            
            context = await browser.new_context(
                user_agent=self.user_agent,
                viewport={"width": 800, "height": 600},
                locale="zh-CN",
                timezone_id="Asia/Shanghai"
            )
            
            # 🎲 【打乱洗牌】打乱下载顺序，瓦解东财的风控 IP 访问链审计
            random.shuffle(sectors)
            
            # 会话 Cookie 预热
            logger.info("⏳ 正在进行全局会话 Cookie 预热...")
            warmup_page = await context.new_page()
            try:
                # 访问列表洗牌后第一个板块的 HTML，诱导服务器写入基础 Cookies 凭证
                await warmup_page.goto(
                    f"https://quote.eastmoney.com/bk/{sectors[0]['sid']}.html", 
                    wait_until="domcontentloaded", 
                    timeout=25000
                )
                await asyncio.sleep(2.0)
                logger.success("🔑 Cookie 预热完毕，新浏览器上下文已持证上岗。")
            except Exception as e:
                logger.warning(f"⚠️ 会话预热失败: {e}")
            finally:
                await warmup_page.close()
            
            start_time = time.time()
            success_count = 0
            consecutive_failures = 0  # 连续失败计数器
            aborted = False           # 熔断退出标志
            
            # 顺序采集
            for i, item in enumerate(sectors):
                sid = item["sid"]
                name = item["name"]
                s_type = item["type"]
                
                if i > 0:
                    # 针对大吞吐全量历史数据，采用自适应控流（2.0s ~ 3.5s 延迟），保护会话 TCP 链接不被挂断
                    delay = random.uniform(2.0, 3.5) if self.data_limit > 1000 else random.uniform(1.2, 2.5)
                    await asyncio.sleep(delay)
                
                res = await self.fetch_sector_api(context, sid, name)
                if res:
                    success_count += 1
                    consecutive_failures = 0  # 只要有一次成功，清空两连败限制
                else:
                    consecutive_failures += 1
                    logger.error(
                        f"❌ [挂起] 板块: {name} ({sid}) [{s_type}] 网络通道被 WAF 强行重置。 "
                        f"(当前连续失败数: {consecutive_failures}/2)"
                    )
                    
                    # 🚨 连续 2 次下载挂死，断定当前Actions IP为脏IP，立刻执行“主动熔断暴死”
                    if consecutive_failures >= 2:
                        logger.critical(
                            f"🚨 [触发主动熔断] 同步板块时连续 2 次挂起失败！"
                            f"\n💥 当前云端公网 IP 已被 WAF 列为失信设备。继续同步无法产出任何后续数据。"
                            f"\n💥 立即执行非零状态退出，强制 Actions 中断报错，准备下次重试换取全新绿卡 IP！"
                        )
                        aborted = True
                        break
            
            end_time = time.time()
            await browser.close()
            
            # 报告打印
            total_time = end_time - start_time
            avg_latency = total_time / success_count if success_count > 0 else 0
            throughput = success_count / total_time if total_time > 0 else 0
            success_rate = (success_count / len(sectors)) * 100 if sectors else 0
            
            print("\n" + "="*50)
            print("📊  GitHub Actions 动态板块全量同步吞吐性能报告")
            print("="*50)
            print(f"🔹 熔断爆死退出: {'已触发 🚨' if aborted else '未触发 (全绿通关 🏆)'}")
            print(f"🔹 发现的板块总数: {len(sectors)} 个")
            print(f"🔹 成功抓取板块数: {success_count} 个")
            print(f"🔹 综合抓取成功率: {success_rate:.2f}%")
            print(f"🔹 总运行耗时: {total_time:.2f} 秒")
            print(f"🔹 有效单板块平均耗时 (含控流): {avg_latency:.2f} 秒/个")
            print(f"🔹 接口系统吞吐率: {throughput:.2f} 个板块/秒")
            print("="*50 + "\n")
            
            if aborted:
                sys.exit(1)
