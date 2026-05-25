import asyncio
import json
import os
import random
import time
import urllib.parse
from loguru import logger
from playwright.async_api import async_playwright

class MuscleEngine:
    def __init__(self, data_limit: int = 1000000):
        self.output_dir = "data"
        os.makedirs(self.output_dir, exist_ok=True)
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        self.data_limit = data_limit
        
        # 动态加载环境变量中的 Cloudflare Worker 代理域名
        self.cf_worker_url = os.environ.get("CF_WORKER_URL", "").strip()
        if self.cf_worker_url:
            self.cf_worker_url = self.cf_worker_url.replace("https://", "").replace("http://", "").rstrip("/")
            logger.info(f"🚀 [代理模式] 成功挂载 Cloudflare Worker 边缘代理网关: {self.cf_worker_url}")
        else:
            logger.warning("⚠️ [直连模式] 未检测到 CF_WORKER_URL 环境变量。管线将通过本地公网 IP 直接对抗东财 WAF。")

        # 核心防封控制阈值：每成功执行 15 次请求，强制主动重构上下文以清除长连接限制和 Cookie 累积特征
        self.rotation_threshold = 15

    async def get_active_sectors(self, context) -> list:
        """
        在已完成会话预热的 Context 下，在线获取前 100 个活跃行业板块列表
        """
        logger.info("📡 [列表获取] 正在从数据源在线获取前 100 个活跃行业板块列表...")
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
            
            if not raw_text:
                logger.error("❌ 获取板块列表返回空响应")
                return []
                
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

    async def run_warmup(self, context, target_sid: str):
        """
        对当前指定的浏览器上下文进行会话 Cookie 初始化预热
        """
        logger.info(f"⏳ 正在针对板块 {target_sid} 进行全局会话 Cookie 预热...")
        warmup_page = await context.new_page()
        try:
            await warmup_page.goto(
                f"https://quote.eastmoney.com/bk/{target_sid}.html", 
                wait_until="domcontentloaded", 
                timeout=25000
            )
            await asyncio.sleep(2.0)
            logger.success("🔑 Cookie 会话预热完毕，新上下文凭证已就绪。")
        except Exception as e:
            logger.warning(f"⚠️ 会话预热遇到阻碍: {e}")
        finally:
            await warmup_page.close()

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
        
        # 动态路由代理重写（如果配置了 CF_WORKER_URL）
        if self.cf_worker_url:
            encoded_target = urllib.parse.quote(url, safe="")
            url = f"https://{self.cf_worker_url}/?url={encoded_target}"
        
        try:
            logger.info(f"🌐 [API 请求] 正在发送数据请求 {sid}...")
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            raw_text = await page.evaluate("() => document.body.innerText")
            
            if not raw_text:
                logger.error(f"❌ 数据网关返回空响应 ({sid})")
                return False
            
            # 防御性 JSON 解析，提取由于 WAF 拦截产生的非 JSON 阻断 HTML
            try:
                data_json = json.loads(raw_text)
            except json.JSONDecodeError:
                logger.error(f"💥 [解析失败] 接口返回了非 JSON 格式。前 300 字特征:\n{raw_text[:300].strip()}")
                return False
            
            if not data_json or "data" not in data_json or data_json["data"] is None:
                return False
                
            payload = data_json["data"]
            klines = payload.get("klines", [])
            
            output_path = os.path.join(self.output_dir, f"{sid}_direct.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
                
            logger.success(f"🎯 [完成] 板块: {name} ({sid}) | 实拉记录数: {len(klines)}")
            return True
            
        except Exception as e:
            logger.error(f"💥 数据获取异常 ({sid}): {e}")
            return False
        finally:
            await page.close()

    async def run_factory(self, max_sectors: int = 100):
        logger.info("🧪 启动云端 headless 无头浏览器数据吞吐管线...")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True, 
                args=[
                    '--no-sandbox', 
                    '--disable-dev-shm-usage',
                    '--disable-gpu'
                ]
            )
            
            # 创建工作上下文
            context = await browser.new_context(
                user_agent=self.user_agent,
                viewport={"width": 800, "height": 600},
                locale="zh-CN",
                timezone_id="Asia/Shanghai"
            )
            
            # 1. 【首要动作：Cookie 预热】
            # 先进行会话预热，确保后续即使在直连模式下在线获取列表也能 100% 成功
            logger.info("⏳ 正在启动初始化全局会话 Cookie 预热...")
            await self.run_warmup(context, "90.BK1063")
            
            # 2. 【在线拉取 100 板块】
            sectors_all = await self.get_active_sectors(context)
            sectors = sectors_all[:max_sectors]
            
            if not sectors:
                logger.error("❌ 板块列表加载为空，同步中止。")
                await context.close()
                await browser.close()
                return
            
            start_time = time.time()
            success_count = 0
            success_count_in_session = 1  # 已执行 1 次 Warmup
            
            # 3. 顺序执行采集
            for i, item in enumerate(sectors):
                sid = item["sid"]
                name = item["name"]
                
                # ----------------- 【核心控制一：定期上下文新陈代谢】 -----------------
                # 只有直连模式才需要频繁重构。如果走 CF Worker 边缘代理，放宽重构阈值以最大化网络吞吐
                rotation_limit = 50 if self.cf_worker_url else self.rotation_threshold
                if success_count_in_session >= rotation_limit:
                    logger.warning(f"🔄 当前会话已成功请求 {rotation_limit} 次，执行主动重建...")
                    await context.close()
                    context = await browser.new_context(
                        user_agent=self.user_agent,
                        viewport={"width": 800, "height": 600},
                        locale="zh-CN",
                        timezone_id="Asia/Shanghai"
                    )
                    await self.run_warmup(context, sid)
                    success_count_in_session = 0
                
                if i > 0:
                    # 动态延迟控制：如果走 CF Worker 代理，可以使用 0.8s ~ 1.5s 的极高吞吐速度；
                    # 如果是本地直连全量，使用安全的 2.0s ~ 3.5s。
                    delay = random.uniform(0.8, 1.5) if self.cf_worker_url else (
                        random.uniform(2.0, 3.5) if self.data_limit > 1000 else random.uniform(1.2, 2.5)
                    )
                    await asyncio.sleep(delay)
                
                # 4. 执行 API 数据抓取
                res = await self.fetch_sector_api(context, sid, name)
                
                # ----------------- 【核心控制二：阶梯式指数退避自愈重试】 -----------------
                retry_count = 0
                max_retries = 3
                backoff_delays = [8.0, 30.0, 90.0]  # 三级降温退避时长
                
                while not res and retry_count < max_retries:
                    backoff_time = backoff_delays[retry_count]
                    logger.error(
                        f"🚨 [挂起/重置] 板块: {name} ({sid}) 网络通道异常。 "
                        f"触发第 {retry_count+1}/{max_retries} 次断点自愈..."
                    )
                    logger.warning(f"💤 正在启动物理降温，强制退避静默 {backoff_time} 秒...")
                    await asyncio.sleep(backoff_time)
                    
                    # 彻底销毁被污染、被限流的旧上下文
                    await context.close()
                    # 重建全新干净的浏览器上下文环境
                    context = await browser.new_context(
                        user_agent=self.user_agent,
                        viewport={"width": 800, "height": 600},
                        locale="zh-CN",
                        timezone_id="Asia/Shanghai"
                    )
                    await self.run_warmup(context, sid)
                    
                    logger.info(f"🔄 正在对板块: {name} ({sid}) 发起断点续传重试...")
                    res = await self.fetch_sector_api(context, sid, name)
                    success_count_in_session = 0
                    retry_count += 1
                
                if res:
                    success_count += 1
                    success_count_in_session += 1
                    
            end_time = time.time()
            await context.close()
            await browser.close()
            
            # 计算并展现云端吞吐性能报告
            total_time = end_time - start_time
            avg_latency = total_time / len(sectors) if sectors else 0
            throughput = len(sectors) / total_time if total_time > 0 else 0
            success_rate = (success_count / len(sectors)) * 100 if sectors else 0
            
            print("\n" + "="*50)
            print("📊  GitHub Actions 云端吞吐压力测试评估报告")
            print("="*50)
            print(f"🔹 运行模式: {'Cloudflare 边缘代理' if self.cf_worker_url else '云端 IP 直连'}")
            print(f"🔹 数据同步模式: {'全量历史同步' if self.data_limit > 1000 else '日常增量同步'}")
            print(f"🔹 目标获取板块数: {len(sectors)} 个")
            print(f"🔹 成功获取板块数: {success_count} 个")
            print(f"🔹 综合同步成功率: {success_rate:.2f}%")
            print(f"🔹 总运行耗时: {total_time:.2f} 秒")
            print(f"🔹 单板块平均耗时 (含随机延迟): {avg_latency:.2f} 秒/个")
            print(f"🔹 接口系统吞吐率: {throughput:.2f} 个板块/秒")
            print("="*50 + "\n")
