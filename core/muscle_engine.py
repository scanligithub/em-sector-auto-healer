import asyncio
import json
import re
import os
import urllib.parse
import polars as pl
from loguru import logger
from curl_cffi.requests import AsyncSession

class MuscleEngine:
    def __init__(self, trust_context: dict):
        self.kline_template = trust_context.get("kline_url", "")
        self.clist_template = trust_context.get("clist_url", "")
        self.worker_url = os.getenv("CF_WORKER_URL", "").strip()
        
        self.headers = {
            "User-Agent": trust_context.get("ua", ""),
            "Cookie": trust_context.get("cookies", ""),
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
        }
        # 并发控制在 10 左右，保护 CF Worker，防止被东财反溯
        self.concurrency = int(os.getenv("CONCURRENCY", 10))
        self.impersonate = "chrome120"

    def _extract_json(self, text: str) -> dict:
        """工业级防弹解析，免疫 HTML/502，完美提取 JSONP"""
        if not text or "<html" in text.lower() or "bad gateway" in text.lower():
            return {}
        # 严格匹配 jQuery1234({...}) 格式
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        if match:
            try: return json.loads(match.group(1))
            except: pass
        # Fallback
        try: return json.loads(text)
        except: pass
        return {}

    def _route_through_worker(self, target_url: str) -> str:
        """核心枢纽：将请求包装，通过 Cloudflare Worker 中转"""
        if self.worker_url:
            encoded_target = urllib.parse.quote(target_url, safe='')
            return f"{self.worker_url}?url={encoded_target}"
        return target_url

    async def _safe_request(self, session, url: str) -> dict:
        """带指数退避和 Worker 路由的安全请求层"""
        routed_url = self._route_through_worker(url)
        max_retries = 5
        
        for attempt in range(max_retries):
            try:
                resp = await session.get(routed_url, headers=self.headers, timeout=20, verify=False)
                
                if resp.status_code in [502, 503, 504, 520, 522, 524]:
                    raise Exception(f"Gateway Error: {resp.status_code}")
                    
                data = self._extract_json(resp.text)
                if data: return data
                
            except Exception as e:
                wait_time = 2 ** attempt
                logger.debug(f"⚠️ 链路波动 [{attempt+1}/{max_retries}] 等待 {wait_time}s 重试... ({e})")
                await asyncio.sleep(wait_time)
                
        return {}

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info(f"💪 [Muscle] 正在通过 CF Worker 节点拉取全市场目录...")
        
        target_url = self.clist_template
        if not target_url:
            logger.warning("⚠️ 未能继承 clist 模板，正使用 kline 签名进行重构...")
            ut_match = re.search(r'ut=([^&]+)', self.kline_template)
            ut = ut_match.group(1) if ut_match else "fa5fd1943c7b386f172d6893dbfba10b"
            fs_param = urllib.parse.quote("m:90+t:2,m:90+t:3,m:90+t:1")
            target_url = (
                f"https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1"
                f"&fltt=2&invt=2&fid=f3&fs={fs_param}&fields=f12&ut={ut}"
            )
        else:
            fs_param = urllib.parse.quote("m:90+t:2,m:90+t:3,m:90+t:1")
            target_url = re.sub(r'pz=\d+', 'pz=2000', target_url)
            target_url = re.sub(r'fs=[^&]+', f'fs={fs_param}', target_url)
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            data = await self._safe_request(session, target_url)
            if not data or "data" not in data or not data["data"]:
                logger.error("❌ 目录解析彻底失败，Worker 节点可能被拦截。")
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
        logger.info(f"💪 [Muscle] 启动 CF 节点并发群发，并发量限制: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single_sector(session, secid, semaphore) for secid in sector_list]
            
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res:
                    all_results.extend(res)
                    if len(all_results) % 5000 == 0:
                        logger.info(f"📊 稳步落盘中... 已拉取 {len(all_results)} 条 K 线数据")
                    
        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            df.write_parquet("data/sector_klines_full.parquet")
            logger.success(f"💾 代理群发结束！利用 CF 骨干网成功抗击封锁，落盘 {len(all_results)} 行数据！")
