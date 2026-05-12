import asyncio
import json
import re
import os
import polars as pl
from loguru import logger

class MuscleEngine:
    def __init__(self, context, main_page):
        self.context = context
        self.main_page = main_page
        self.concurrency = 5  # 浏览器多标签并发不宜过高，5 个同时渲染是最佳平衡点

    async def fetch_dynamic_sector_list(self) -> list:
        """
        利用 JSONP 注入技术，直接在可信的页面上下文中拉取板块目录。
        这完美绕过了 CORS 限制，且行为发生在东财原生 Domain 下。
        """
        logger.info("💪 [Hijacker] 正在原生环境中注入 JSONP 拉取全市场板块目录...")
        
        # 在浏览器上下文中动态创建一个 script 标签，执行 JSONP 请求
        script = """
        () => {
            return new Promise((resolve) => {
                const cbName = 'jsonp_clist_' + Date.now();
                window[cbName] = (data) => {
                    resolve(data);
                    delete window[cbName];
                };
                const script = document.createElement('script');
                // 拉取 行业、概念、地域 全部板块
                script.src = `https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:2,m:90+t:3,m:90+t:1&fields=f12&cb=${cbName}`;
                document.head.appendChild(script);
            });
        }
        """
        try:
            res = await self.main_page.evaluate(script)
            codes = [f"90.{x['f12']}" for x in res['data']['diff']]
            logger.success(f"💪 [Hijacker] 目录扫描完成，共捕获 {len(codes)} 个板块。")
            return codes
        except Exception as e:
            logger.error(f"❌ 获取板块目录失败: {e}")
            return []

    async def _hijack_single_sector(self, secid: str, semaphore: asyncio.Semaphore):
        """
        【被动流量劫持核心】
        打开一个新标签页 -> 拦截 response -> 拿到数据就立刻关掉标签页。
        完全不需要关心 ut、参数、加密算法。
        """
        async with semaphore:
            page = await self.context.new_page()
            future_data = asyncio.get_event_loop().create_future()

            async def on_response(response):
                # 幽灵监听：只关注 K 线数据的响应包
                if "api/qt/stock/kline/get" in response.url:
                    try:
                        text = await response.text()
                        # 东财返回的是 JSONP: jQuery12345({data: ...})
                        # 暴力破除外壳，直取内层 JSON
                        match = re.search(r'\{.*\}', text, re.DOTALL)
                        if match and not future_data.done():
                            future_data.set_result(json.loads(match.group(0)))
                    except Exception:
                        pass

            page.on("response", on_response)

            try:
                # 访问目标板块，触发网页自带的、完全合法的 K 线请求
                await page.goto(f"https://quote.eastmoney.com/bk/{secid}.html", wait_until="domcontentloaded", timeout=15000)
                
                # 等待幽灵监听器截获数据 (最多等 10 秒)
                data = await asyncio.wait_for(future_data, timeout=10.0)
                
                klines_data = []
                if data.get("data") and data["data"].get("klines"):
                    for r in data["data"]["klines"]:
                        row = r.split(",")
                        # 根据抓包分析：日期=row[0], 开盘=1, 收盘=2, 最高=3, 最低=4, 成交量=5, 成交额=6
                        klines_data.append({
                            "secid": secid,
                            "date": row[0],
                            "open": float(row[1]),
                            "close": float(row[2]),
                            "high": float(row[3]),
                            "low": float(row[4]),
                            "volume": float(row[5]),
                            "amount": float(row[6])
                        })
                return klines_data
            except Exception as e:
                logger.debug(f"⚠️ 劫持 {secid} 超时或失败，可能由于网络抖动。")
                return []
            finally:
                await page.close()

    async def hijack_all_sectors(self, sector_list: list):
        logger.info(f"💪 [Hijacker] 启动幽灵劫持网络，并发标签页数量: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        # 分批打印进度
        batch_size = 50
        for i in range(0, len(sector_list), batch_size):
            batch = sector_list[i:i+batch_size]
            tasks = [self._hijack_single_sector(secid, semaphore) for secid in batch]
            
            # 并发执行这批标签页的劫持
            batch_results = await asyncio.gather(*tasks)
            
            for res in batch_results:
                if res: all_results.extend(res)
            
            logger.info(f"📊 劫持进度: {min(i + batch_size, len(sector_list))} / {len(sector_list)} ...")

        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            df.write_parquet("data/sector_klines_full.parquet")
            logger.success(f"💾 幽灵劫持行动圆满结束！无视所有风控，完美落盘 {len(all_results)} 行数据！")
