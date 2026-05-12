import asyncio
import json
import re
import os
import polars as pl
from loguru import logger

class MuscleEngine:
    def __init__(self, context):
        self.context = context
        # 因为不再需要渲染网页 DOM，纯网络并发可以大胆调高
        self.concurrency = 30 
        self.stolen_kline_url = ""

    async def fetch_dynamic_sector_list(self) -> list:
        """
        利用 Playwright 原生网络层拉取，无视 CSP 限制，共享浏览器 Cookie。
        """
        logger.info("💪 [Muscle] 正在使用底层 Browser API Context 拉取板块目录...")
        url = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:2,m:90+t:3,m:90+t:1&fields=f12"
        
        try:
            # 这里的 request.get 拥有极其纯正的浏览器血统
            resp = await self.context.request.get(
                url, 
                headers={"Referer": "https://quote.eastmoney.com/"},
                timeout=10000
            )
            # 由于我们没传 cb 参数，东财会非常配合地返回标准 JSON
            data = await resp.json()
            codes = [f"90.{x['f12']}" for x in data['data']['diff']]
            logger.success(f"💪 [Muscle] 目录扫描完成，共捕获 {len(codes)} 个板块。")
            return codes
        except Exception as e:
            logger.error(f"❌ 获取板块目录失败: {e}")
            return []

    async def prepare_hijack_template(self):
        """
        核心战术：打开白酒板块，偷走带有合法签名的原生 URL。
        """
        logger.info("💪 [Muscle] 正在执行单点劫持，窃取官方 API 模版...")
        page = await self.context.new_page()
        future_url = asyncio.get_event_loop().create_future()

        async def on_request(request):
            if "api/qt/stock/kline/get" in request.url and "secid=90.BK0896" in request.url:
                if not future_url.done():
                    future_url.set_result(request.url)

        page.on("request", on_request)

        try:
            # 进入白酒板块
            await page.goto("https://quote.eastmoney.com/bk/90.BK0896.html", wait_until="domcontentloaded", timeout=15000)
            # 等待猎物 URL 落网
            self.stolen_kline_url = await asyncio.wait_for(future_url, timeout=10.0)
            logger.success(f"💪 [Muscle] 窃取成功！获得原生母版 URL: {self.stolen_kline_url[:80]}...")
        except Exception as e:
            raise Exception(f"窃取原生 URL 失败，请重试: {e}")
        finally:
            await page.close()

    async def _fetch_single_sector_api(self, secid: str, semaphore: asyncio.Semaphore):
        """
        利用偷来的母版 URL，进行快速替换和并发拉取。
        没有任何 DOM 开销，速度极快。
        """
        async with semaphore:
            # 狸猫换太子：把母版 URL 里的白酒代码换成目标板块代码
            target_url = re.sub(r'secid=[^&]+', f'secid={secid}', self.stolen_kline_url)
            
            # 强制请求全量历史数据
            target_url = re.sub(r'lmt=\d+', 'lmt=100000', target_url)

            try:
                resp = await self.context.request.get(
                    target_url, 
                    headers={"Referer": "https://quote.eastmoney.com/"},
                    timeout=15000
                )
                text = await resp.text()
                
                # 剥离 JSONP 外壳
                match = re.search(r'\{.*\}', text, re.DOTALL)
                if not match: return []
                
                data = json.loads(match.group(0))
                klines_data = []
                
                if data.get("data") and data["data"].get("klines"):
                    for r in data["data"]["klines"]:
                        row = r.split(",")
                        klines_data.append({
                            "secid": secid, "date": row[0],
                            "open": float(row[1]), "close": float(row[2]),
                            "high": float(row[3]), "low": float(row[4]),
                            "volume": float(row[5]), "amount": float(row[6])
                        })
                return klines_data
            except Exception as e:
                logger.debug(f"⚠️ 拉取 {secid} 失败: {e}")
                return []

    async def fetch_all_sectors(self, sector_list: list):
        logger.info(f"💪 [Muscle] 启动底层原生并发群发，并发数: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        # 满血并发执行
        tasks = [self._fetch_single_sector_api(secid, semaphore) for secid in sector_list]
        
        for coro in asyncio.as_completed(tasks):
            res = await coro
            if res:
                all_results.extend(res)
                if len(all_results) % 5000 == 0:
                    logger.info(f"📊 已拉取 {len(all_results)} 条 K 线数据...")

        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            df.write_parquet("data/sector_klines_full.parquet")
            logger.success(f"💾 行动圆满结束！无视风控阻碍，完美落盘 {len(all_results)} 行数据！")
