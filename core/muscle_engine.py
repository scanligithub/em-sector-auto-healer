import asyncio
import json
import re
import os
import polars as pl
from loguru import logger
from curl_cffi.requests import AsyncSession

class MuscleEngine:
    def __init__(self, trust_context: dict):
        self.kline_template = trust_context["kline_url"]
        self.clist_template = trust_context["clist_url"]
        self.headers = {
            "User-Agent": trust_context["ua"],
            "Cookie": trust_context["cookies"],
            "Referer": "https://quote.eastmoney.com/"
        }
        self.concurrency = int(os.getenv("CONCURRENCY", 20)) # 降低一点并发，保护 CDN 节点
        self.impersonate = "chrome120"

    def _extract_json(self, text: str) -> dict:
        """💡 核心修复 2：终极防弹 JSONP 解析，免疫 502 HTML"""
        # 如果拿到的是 HTML 错误页，直接抛弃
        if "<html" in text.lower() or "502 bad gateway" in text.lower():
            return {}
            
        # 严格匹配 JSONP 格式: jQueryxxx({ ... })
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        if match:
            try: return json.loads(match.group(1))
            except: pass
            
        # Fallback 匹配纯 JSON
        try: return json.loads(text)
        except: pass
        
        return {}

    async def _safe_request(self, session, url: str) -> dict:
        """💡 核心修复 3：带有指数退避 (Exponential Backoff) 的安全请求网络栈"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                resp = await session.get(url, headers=self.headers, timeout=15)
                
                # 触发 CDN 熔断时主动抛出异常进入重试逻辑
                if resp.status_code in [502, 503, 504, 520, 522, 524]:
                    raise Exception(f"CDN Gateway Error: {resp.status_code}")
                    
                data = self._extract_json(resp.text)
                if data: return data
                
            except Exception as e:
                wait_time = 2 ** attempt  # 1s, 2s, 4s, 8s, 16s
                logger.debug(f"⚠️ 网络抖动/熔断 [{attempt+1}/{max_retries}] 等待 {wait_time}s 重试... ({e})")
                await asyncio.sleep(wait_time)
                
        return {}

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle] 正在使用官方合法凭证拉取全市场目录...")
        
        target_url = self.clist_template
        
        # 💡 终极防弹机制：如果雷达真的漏掉了 clist，直接从 kline 里扣出 ut 来继承！
        if not target_url:
            logger.warning("⚠️ 未能继承 clist 模板，正在使用 kline 签名进行原生构造...")
            ut_match = re.search(r'ut=([^&]+)', self.kline_template)
            ut = ut_match.group(1) if ut_match else "fa5fd1943c7b386f172d6893dbfba10b"
            target_url = (
                f"https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1"
                f"&fltt=2&invt=2&fid=f3&fs=m:90+t:2,m:90+t:3,m:90+t:1&fields=f12&ut={ut}"
            )
        else:
            # 狸猫换太子：扩大拉取条数，并强制拉取三大板块
            target_url = re.sub(r'pz=\d+', 'pz=2000', target_url)
            target_url = re.sub(r'fs=[^&]+', 'fs=m:90+t:2,m:90+t:3,m:90+t:1', target_url)
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            data = await self._safe_request(session, target_url)
            if not data or "data" not in data or not data["data"]:
                logger.error("❌ 目录解析彻底失败，请检查网络出口信誉度。")
                return []
                
            codes = [f"90.{x['f12']}" for x in data["data"]["diff"]]
            logger.success(f"💪 [Muscle] 目录扫描完成，共捕获 {len(codes)} 个板块。")
            return codes

    async def _fetch_single_sector(self, session, secid: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            if not self.kline_template: return []
            
            target_url = re.sub(r'secid=[^&]+', f'secid={secid}', self.kline_template)
            target_url = re.sub(r'lmt=\d+', 'lmt=100000', target_url)

            data = await self._safe_request(session, target_url)
                
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
            return []

    async def fetch_all_sectors(self, sector_list: list):
        logger.info(f"💪 [Muscle] 启动防弹并发扫荡，并发量: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single_sector(session, secid, semaphore) for secid in sector_list]
            
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res:
                    all_results.extend(res)
                    if len(all_results) % 10000 == 0:
                        logger.info(f"📊 稳定落盘中... 已拉取 {len(all_results)} 条 K 线数据")
                    
        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            df.write_parquet("data/sector_klines_full.parquet")
            logger.success(f"💾 工业级扫荡结束！成功抗击 CDN 熔断，落盘 {len(all_results)} 行数据！")
