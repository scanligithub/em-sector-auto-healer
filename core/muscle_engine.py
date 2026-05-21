import asyncio
import json
import os
from loguru import logger
import httpx

class MuscleEngine:
    def __init__(self):
        os.makedirs("data", exist_ok=True)
        # 精准模拟浏览器基础请求头
        # 1. 加入 "Connection": "close" 强制不复用 TCP 通道，防止 IIS 触发风控断连
        # 2. 补全 Accept/Accept-Language 规范
        self.headers = {
            "Host": "push2his.eastmoney.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "close", 
            "Referer": "https://quote.eastmoney.com/"
        }

    async def fetch_sector_kline(self, client: httpx.AsyncClient, sid: str) -> bool:
        url = (
            f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
            f"?secid={sid}"
            f"&ut=fa5fd1943c7b386f172d6893dbfba10b"
            f"&fields1=f1,f2,f3,f4,f5,f6"
            f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
            f"&klt=101"
            f"&fqt=1"
            f"&end=20500101"
            f"&lmt=1000000"
        )
        
        try:
            logger.info(f"🚀 [API 请求] 正在拉取板块 {sid} 的全量日K线数据...")
            # 传入当前请求头
            response = await client.get(url, headers=self.headers, timeout=15.0)
            
            if response.status_code != 200:
                logger.error(f"❌ 接口请求失败，HTTP 状态码: {response.status_code}")
                return False
                
            data_json = response.json()
            
            if not data_json or "data" not in data_json or data_json["data"] is None:
                logger.warning(f"⚠️ 板块 {sid} 未返回有效内容")
                return False
                
            payload = data_json["data"]
            name = payload.get("name", "未知")
            code = payload.get("code", "未知")
            dktotal = payload.get("dktotal", 0)
            klines = payload.get("klines", [])
            
            logger.success(
                f"🎯 [数据就绪] 板块: {name} ({code}) | "
                f"历史天数: {dktotal} | "
                f"实际拉取记录数: {len(klines)}"
            )
            
            output_path = f"data/{sid}_history.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=4)
            
            if klines:
                logger.info(f"📊 样本数据检验 -> [首条] {klines[0]} | [末条] {klines[-1]}")
            return True
            
        except httpx.HTTPError as e:
            logger.error(f"💥 网络请求异常 ({sid}): {e}")
            return False
        except Exception as e:
            logger.error(f"💥 数据解析异常 ({sid}): {e}")
            return False

    async def run_factory(self, sector_list):
        logger.info(f"🔬 初始化底层 HTTP/1.1 短连接异步客户端...")
        
        # 1. 显式约束 client 使用 http1=True，关闭可能产生冲突的 http2 协议
        # 2. 鉴于短连接特性，将并发数（max_connections）设为 5 即可实现稳定、温和的高速吞吐
        limits = httpx.Limits(max_keepalive_connections=0, max_connections=5)
        
        async with httpx.AsyncClient(limits=limits, http1=True, http2=False) as client:
            # 引入微小的延迟发射（100ms），避免高并发网络包瞬间堆叠导致防火墙误判
            tasks = []
            for i, sid in enumerate(sector_list):
                if i > 0:
                    await asyncio.sleep(0.1)
                tasks.append(self.fetch_sector_kline(client, sid))
                
            results = await asyncio.gather(*tasks)
            
        success_count = sum(1 for r in results if r)
        logger.info(f"🏁 本轮测试执行完毕。成功率: {success_count}/{len(sector_list)}")
