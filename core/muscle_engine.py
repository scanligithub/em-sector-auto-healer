import asyncio
import json
import os
import random
import time
import sys
from loguru import loggerimport asyncio
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
        
        # =================【关键改进：基于方案二的动态板块生成】=================
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
from playwright.async_api import async_playwright

class LocalBenchmark100:
    def __init__(self, data_limit: int = 1000000):
        self.output_dir = "data"
        os.makedirs(self.output_dir, exist_ok=True)
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        self.data_limit = data_limit  # 默认为 1000000 条（拉取全部历史数据）
        
        # 从日志提取的固定 100 个目标板块列表
        self.sectors = [
            {"sid": "90.BK1373", "name": "电能综合服务"},
            {"sid": "90.BK1450", "name": "空调"},
            {"sid": "90.BK1327", "name": "分立器件"},
            {"sid": "90.BK1040", "name": "中药Ⅱ"},
            {"sid": "90.BK1035", "name": "美容护理"},
            {"sid": "90.BK1518", "name": "种子"},
            {"sid": "90.BK1296", "name": "视频媒体"},
            {"sid": "90.BK1320", "name": "逆变器"},
            {"sid": "90.BK1319", "name": "硅料硅片"},
            {"sid": "90.BK1505", "name": "其他农产品加工"},
            {"sid": "90.BK1279", "name": "非白酒"},
            {"sid": "90.BK1219", "name": "电视广播Ⅱ"},
            {"sid": "90.BK1609", "name": "城商行Ⅲ"},
            {"sid": "90.BK1580", "name": "肉制品"},
            {"sid": "90.BK1577", "name": "啤酒"},
            {"sid": "90.BK1377", "name": "火力发电"},
            {"sid": "90.BK1611", "name": "国有大型银行Ⅲ"},
            {"sid": "90.BK1380", "name": "水力发电"},
            {"sid": "90.BK1586", "name": "软饮料"},
            {"sid": "90.BK1283", "name": "银行"},
            {"sid": "90.BK1222", "name": "影视院线"},
            {"sid": "90.BK1425", "name": "胶黏剂及胶带"},
            {"sid": "90.BK1548", "name": "综合电商"},
            {"sid": "90.BK1585", "name": "乳品"},
            {"sid": "90.BK1500", "name": "医美耗材"},
            {"sid": "90.BK1476", "name": "化学工程"},
            {"sid": "90.BK0428", "name": "电力"},
            {"sid": "90.BK1318", "name": "光伏主材"},
            {"sid": "90.BK1322", "name": "其他电源设备Ⅲ"},
            {"sid": "90.BK1325", "name": "半导体材料"},
            {"sid": "90.BK1576", "name": "调味发酵品Ⅲ"},
            {"sid": "90.BK1245", "name": "照明设备Ⅱ"},
            {"sid": "90.BK1351", "name": "印染"},
            {"sid": "90.BK1250", "name": "煤炭开采"},
            {"sid": "90.BK1582", "name": "烘焙食品"},
            {"sid": "90.BK1581", "name": "预加工食品"},
            {"sid": "90.BK1612", "name": "农商行Ⅲ"},
            {"sid": "90.BK1511", "name": "肉鸡养殖"},
            {"sid": "90.BK1374", "name": "风力发电"},
            {"sid": "90.BK1563", "name": "自然景区"},
            {"sid": "90.BK1277", "name": "白酒Ⅱ"},
            {"sid": "90.BK1559", "name": "餐饮"},
            {"sid": "90.BK1213", "name": "商贸零售"},
            {"sid": "90.BK0457", "name": "电网设备"},
            {"sid": "90.BK1465", "name": "防水材料"},
            {"sid": "90.BK1619", "name": "其他金属新材料"},
            {"sid": "90.BK1355", "name": "运动服装"},
            {"sid": "90.BK1272", "name": "旅游及景区"},
            {"sid": "90.BK1034", "name": "其他电源设备Ⅱ"},
            {"sid": "90.BK1031", "name": "光伏设备"},
            {"sid": "90.BK1515", "name": "粮食种植"},
            {"sid": "90.BK1460", "name": "照明设备Ⅲ"},
            {"sid": "90.BK1561", "name": "旅游综合"},
            {"sid": "90.BK1281", "name": "休闲食品"},
            {"sid": "90.BK1295", "name": "其他数字媒体"},
            {"sid": "90.BK1608", "name": "中药Ⅲ"},
            {"sid": "90.BK1221", "name": "数字媒体"},
            {"sid": "90.BK1278", "name": "调味发酵品Ⅱ"},
            {"sid": "90.BK1583", "name": "零食"},
            {"sid": "90.BK1494", "name": "焦煤"},
            {"sid": "90.BK1251", "name": "个护用品"},
            {"sid": "90.BK1477", "name": "其他专业工程"},
            {"sid": "90.BK0438", "name": "食品饮料"},
            {"sid": "90.BK1579", "name": "保健品"},
            {"sid": "90.BK1298", "name": "文字媒体"},
            {"sid": "90.BK1510", "name": "其他养殖"},
            {"sid": "90.BK1496", "name": "洗护用品"},
            {"sid": "90.BK1253", "name": "医疗美容"},
            {"sid": "90.BK1321", "name": "火电设备"},
            {"sid": "90.BK1299", "name": "影视动漫制作"},
            {"sid": "90.BK1302", "name": "电池化学品"},
            {"sid": "90.BK1575", "name": "白酒Ⅲ"},
            {"sid": "90.BK1562", "name": "人工景区"},
            {"sid": "90.BK1552", "name": "超市"},
            {"sid": "90.BK1410", "name": "其他自动化设备"},
            {"sid": "90.BK1343", "name": "物业管理"},
            {"sid": "90.BK0482", "name": "一般零售"},
            {"sid": "90.BK1339", "name": "被动元件"},
            {"sid": "90.BK0475", "name": "银行Ⅱ"},
            {"sid": "90.BK1610", "name": "股份制银行Ⅲ"},
            {"sid": "90.BK1551", "name": "百货"},
            {"sid": "90.BK1623", "name": "钼"},
            {"sid": "90.BK1499", "name": "医美服务"},
            {"sid": "90.BK1268", "name": "互联网电商"},
            {"sid": "90.BK1547", "name": "跨境电商"},
            {"sid": "90.BK1498", "name": "品牌化妆品"},
            {"sid": "90.BK1271", "name": "酒店餐饮"},
            {"sid": "90.BK0427", "name": "公用事业"},
            {"sid": "90.BK1375", "name": "光伏发电"},
            {"sid": "90.BK1379", "name": "热力服务"},
            {"sid": "90.BK1300", "name": "院线"},
            {"sid": "90.BK1261", "name": "种植业"},
            {"sid": "90.BK1282", "name": "饮料乳品"},
            {"sid": "90.BK1606", "name": "线下药店"},
            {"sid": "90.BK0437", "name": "煤炭"},
            {"sid": "90.BK1280", "name": "食品加工"},
            {"sid": "90.BK1493", "name": "动力煤"},
            {"sid": "90.BK1291", "name": "电视广播Ⅲ"},
            {"sid": "90.BK1310", "name": "配电设备"},
            {"sid": "90.BK1311", "name": "输变电设备"}
        ]

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
            # 在无头模式下，带上预热成功的 Cookie 凭证直接打开 API
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            raw_text = await page.evaluate("() => document.body.innerText")
            data_json = json.loads(raw_text)
            
            if not data_json or "data" not in data_json or data_json["data"] is None:
                return False
                
            payload = data_json["data"]
            klines = payload.get("klines", [])
            
            # 紧凑存储降低 CI 磁盘 I/O 延迟
            output_path = os.path.join(self.output_dir, f"{sid}_direct.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
                
            logger.success(f"🎯 [完成] 板块: {name} ({sid}) | 历史天数: {len(klines)}")
            return True
            
        except Exception:
            return False
        finally:
            await page.close()

    async def run_pipeline(self):
        logger.info("🧪 启动 GitHub Actions 100 行业板块高保真全量抓取管线...")
        
        async with async_playwright() as p:
            # CI 环境下强制以无头模式启动
            browser = await p.chromium.launch(
                headless=True, 
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            
            context = await browser.new_context(
                user_agent=self.user_agent,
                viewport={"width": 800, "height": 600},
                locale="zh-CN",
                timezone_id="Asia/Shanghai"
            )
            
            # 🎲 【打乱洗牌】彻底随机重洗板块下载序列，破坏 WAF 时序统计
            sectors = list(self.sectors)
            logger.info("🎲 已执行随机洗牌，打破时序固定下载链...")
            random.shuffle(sectors)
            
            # 会话 Cookie 预热
            logger.info("⏳ 正在启动全局会话 Cookie 预热...")
            warmup_page = await context.new_page()
            try:
                await warmup_page.goto(
                    f"https://quote.eastmoney.com/bk/{sectors[0]['sid']}.html", 
                    wait_until="domcontentloaded", 
                    timeout=20000
                )
                await asyncio.sleep(2.0)
                logger.success("🔑 Cookie 会话预热成功，新浏览器会话已持证上岗。")
            except Exception as e:
                logger.warning(f"⚠️ 会话预热失败: {e}")
            finally:
                await warmup_page.close()
            
            start_time = time.time()
            success_count = 0
            consecutive_failures = 0  # 连续失败计数器
            aborted = False           # 熔断退出标志
            
            # 顺序直冲数据源
            for i, item in enumerate(sectors):
                sid = item["sid"]
                name = item["name"]
                
                # 白名单状态下直冲 0 延迟，但由于首发需要平滑建立连接，保留 0 延迟设定
                delay = 0 
                if delay > 0 and i > 0:
                    await asyncio.sleep(delay)
                
                res = await self.fetch_sector_api(context, sid, name)
                if res:
                    success_count += 1
                    consecutive_failures = 0  # 只要有一次成功，立刻清空连续失败记录
                else:
                    consecutive_failures += 1
                    logger.error(
                        f"❌ [失败] 板块: {name} ({sid}) 网络通道挂起或超时。 "
                        f"(当前连续失败数: {consecutive_failures}/2)"
                    )
                    
                    # ----------------- 【核心控制：连续 2 次失败主动熔断爆死】 -----------------
                    if consecutive_failures >= 2:
                        logger.critical(
                            f"🚨 [触发熔断] 在同步板块 '{name}' ({sid}) 时触发连续 2 次失败！"
                            f"\n💥 证实当前 IP 未通过滑块校验已被列为未授信设备。继续运行无任何数据产出价值。"
                            f"\n💥 立即执行中断暴死退出，以触发 GitHub Actions 重新分配新 IP 进行全新抓取！"
                        )
                        aborted = True
                        break  # 跳出采集
            
            end_time = time.time()
            await browser.close()
            
            total_time = end_time - start_time
            avg_latency = total_time / success_count if success_count > 0 else 0
            throughput = success_count / total_time if total_time > 0 else 0
            success_rate = (success_count / len(sectors)) * 100 if sectors else 0
            
            print("\n" + "="*50)
            print("📊  GitHub Actions 全量历史吞吐性能报告")
            print("="*50)
            print(f"🔹 熔断暴死退出: {'已触发 🚨' if aborted else '未触发 (全绿通关 🏆)'}")
            print(f"🔹 目标获取板块数: {len(sectors)} 个")
            print(f"🔹 成功获取板块数: {success_count} 个")
            print(f"🔹 综合同步成功率: {success_rate:.2f}%")
            print(f"🔹 总运行耗时: {total_time:.2f} 秒")
            print(f"🔹 有效单板块平均耗时: {avg_latency:.2f} 秒/个")
            print(f"🔹 接口系统吞吐率: {throughput:.2f} 个板块/秒")
            print("="*50 + "\n")
            
            # 如果触发了熔断，返回非 0 状态码，促使 GitHub Actions 判定为失败运行
            if aborted:
                sys.exit(1)
