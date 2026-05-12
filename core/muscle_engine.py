import asyncio
import json
import re
import os
import random
import polars as pl
from loguru import logger

class MuscleEngine:
    def __init__(self, context):
        self.context = context
        # 因为需要真实渲染网页 DOM 发起 JS，并发不能太高，5 是最佳平稳值
        self.concurrency = 5 

    async def fetch_dynamic_sector_list(self) -> list:
        """
        利用浏览器原生 Tab 跳转访问 API，享受 100% 真实的 Chromium 网络栈。
        解决 context.request 的 Shadow Ban 问题。
        """
        logger.info("💪 [Hijacker] 正在使用真实 Tab 导航被动捕获板块目录...")
        page = await self.context.new_page()
        try:
            # 这是一次 top-level 的页面跳转，彻底无视 CORS 和 CSP
            url = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:2,m:90+t:3,m:90+t:1&fields=f12"
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            
            # Chromium 会把纯 JSON 渲染在 body 或 pre 里
            content = await page.inner_text("body")
            data = json.loads(content)
            codes = [f"90.{x['f12']}" for x in data['data']['diff']]
            logger.success(f"💪 [Hijacker] 目录扫描完成，共捕获 {len(codes)} 个板块。")
            return codes
        except Exception as e:
            logger.error(f"❌ 获取板块目录失败: {e}")
            return []
        finally:
            await page.close()

    async def _hijack_single_sector(self, secid: str, semaphore: asyncio.Semaphore):
        """
        终极流量监听战术：
        打开官方网页，拦截官方 JS 发出的 K线响应，拿走 JSON，关掉网页。
        """
        async with semaphore:
            page = await self.context.new_page()
            
            # 性能优化：屏蔽图片、CSS、字体，但绝不屏蔽 JS 和 XHR！
            # 这会让东财网页加载极快，同时触发自带的 K 线请求
            async def route_intercept(route):
                if route.request.resource_type in ["image", "stylesheet", "font", "media"]:
                    await route.abort()
                else:
                    await route.continue_()
            await page.route("**/*", route_intercept)

            future_data = asyncio.get_event_loop().create_future()

            async def on_response(response):
                # 幽灵之耳：只听 K 线包
                if "api/qt/stock/kline/get" in response.url:
                    try:
                        text = await response.text()
                        match = re.search(r'\{.*\}', text, re.DOTALL)
                        if match and not future_data.done():
                            future_data.set_result(json.loads(match.group(0)))
                    except Exception:
                        pass

            page.on("response", on_response)

            try:
                # 真实翻页行为：直接访问目标板块网页
                # 此时，东财的原生 JS 会自动计算最新的 ut、拼装最高级的防伪参数，发起真实网络请求
                await page.goto(f"https://quote.eastmoney.com/bk/{secid}.html", wait_until="domcontentloaded", timeout=15000)
                
                # 等待东财自己的网络请求把数据送上门
                data = await asyncio.wait_for(future_data, timeout=10.0)
                
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
                logger.debug(f"⚠️ 监听 {secid} 超时 (网页卡顿或网络抖动)")
                return []
            finally:
                await page.close()

    async def hijack_all_sectors(self, sector_list: list):
        logger.info(f"💪 [Hijacker] 启动被动流量监听网络，并发 Tab 数量: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        batch_size = 50
        for i in range(0, len(sector_list), batch_size):
            batch = sector_list[i:i+batch_size]
            tasks = [self._hijack_single_sector(secid, semaphore) for secid in batch]
            
            batch_results = await asyncio.gather(*tasks)
            
            for res in batch_results:
                if res: all_results.extend(res)
            
            logger.info(f"📊 监听进度: {min(i + batch_size, len(sector_list))} / {len(sector_list)} ...")
            # 注入人类行为间隙
            await asyncio.sleep(random.uniform(1.0, 3.0))

        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            df.write_parquet("data/sector_klines_full.parquet")
            logger.success(f"💾 幽灵监听行动圆满结束！无视所有风控，完美落盘 {len(all_results)} 行数据！")
