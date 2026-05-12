import asyncio
import json
import re
import os
import polars as pl
from loguru import logger
from curl_cffi.requests import AsyncSession

class MuscleEngine:
    def __init__(self, trust_context: dict):
        # 保存偷来的模板和通行证
        self.template_url = trust_context["url"]
        self.headers = {
            "User-Agent": trust_context["ua"],
            "Cookie": trust_context["cookies"],
            "Referer": "https://quote.eastmoney.com/"
        }
        self.concurrency = int(os.getenv("CONCURRENCY", 30))
        # 伪装底层 TLS 和 HTTP2 指纹
        self.impersonate = "chrome120"

    def _extract_json(self, text: str) -> dict:
        """核心剥壳器：无视 JSONP 包装，暴力提取真正的 JSON 数据"""
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        return {}

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle] 正在使用合法凭证极速拉取全市场板块目录...")
        url = (
            "https://push2.eastmoney.com/api/qt/clist/get"
            "?pn=1&pz=2000&po=1&np=1&fltt=2&invt=2&fid=f3"
            "&fs=m:90+t:2,m:90+t:3,m:90+t:1&fields=f12"
        )
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            try:
                resp = await session.get(url, headers=self.headers, timeout=15)
                # 剥离可能存在的 JSONP 外壳
                data = self._extract_json(resp.text)
                
                if not data or "data" not in data or "diff" not in data["data"]:
                    logger.error(f"❌ 目录解析失败，返回内容异常: {resp.text[:100]}")
                    return []
                    
                codes = [f"90.{x['f12']}" for x in data["data"]["diff"]]
                logger.success(f"💪 [Muscle] 目录扫描完成，共捕获 {len(codes)} 个板块。")
                return codes
            except Exception as e:
                logger.error(f"❌ 获取板块目录网络异常: {e}")
                return []

    async def _fetch_single_sector(self, session, secid: str, semaphore: asyncio.Semaphore):
        """单点高频拉取，使用信号量控制并发"""
        async with semaphore:
            # 狸猫换太子：替换板块代码，并将限制条数改为 10 万
            target_url = re.sub(r'secid=[^&]+', f'secid={secid}', self.template_url)
            target_url = re.sub(r'lmt=\d+', 'lmt=100000', target_url)

            # 极简重试机制
            for attempt in range(3):
                try:
                    resp = await session.get(target_url, headers=self.headers, timeout=15)
                    data = self._extract_json(resp.text)
                    
                    if data.get("data") and data["data"].get("klines"):
                        klines_data = []
                        for r in data["data"]["klines"]:
                            row = r.split(",")
                            klines_data.append({
                                "secid": secid, "date": row[0],
                                "open": float(row[1]), "close": float(row[2]),
                                "high": float(row[3]), "low": float(row[4]),
                                "volume": float(row[5]), "amount": float(row[6])
                            })
                        return klines_data
                except Exception:
                    await asyncio.sleep(1) # 被掐断就歇一秒
            
            logger.debug(f"⚠️ 拉取 {secid} 失败，已重试 3 次")
            return []

    async def fetch_all_sectors(self, sector_list: list):
        logger.info(f"💪 [Muscle] 启动狂暴并发群发，并发量: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        # 维持同一个高并发 Session 池
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single_sector(session, secid, semaphore) for secid in sector_list]
            
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res:
                    all_results.extend(res)
                    
        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            df.write_parquet("data/sector_klines_full.parquet")
            logger.success(f"💾 狂暴扫荡结束！完美伪装，成功落盘 {len(all_results)} 行数据！")
