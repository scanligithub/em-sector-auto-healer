import asyncio
import json
import re
import os
import time
import random
import urllib.parse
import polars as pl
from loguru import logger
from curl_cffi.requests import AsyncSession

class MuscleEngine:
    # 💡 静态核心兜底库：防断流最后防线
    FALLBACK_SECTORS = [
        "90.BK0896", "90.BK1036", "90.BK0475", "90.BK0733", "90.BK0427",
        "90.BK1027", "90.BK0477", "90.BK0474", "90.BK0456", "90.BK0480"
    ]
    
    # 东财官方通用鉴权 Token (长期不变)
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        # 💡 安全解析 CF Worker URL
        raw_worker = os.getenv("CF_WORKER_URL", "").strip()
        if raw_worker and raw_worker.lower() not in ["none", "null"]:
            self.worker_url = raw_worker if raw_worker.startswith("http") else f"https://{raw_worker}"
            logger.info(f"🛡️ [Proxy] Worker 节点就绪: {self.worker_url}")
        else:
            self.worker_url = ""
            logger.warning("⚠️ [Proxy] 未配置 CF_WORKER_URL，将使用 GitHub Actions IP 直连 (极高危)")
            
        # 💡 最简净请求头：不要带乱七八糟的 Cookie，伪装成纯净请求
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }
        
        self.concurrency = int(os.getenv("CONCURRENCY", 15))
        # impersonate="chrome124" 完美接管 TLS 指纹，是零 DOM 模式的核心
        self.impersonate = "chrome124"

    def _extract_json_with_diag(self, text: str, secid: str) -> dict:
        """剥壳器：直接处理 API 返回的 JSONP"""
        if not text: return {"_err": "EMPTY"}
        if "安全验证" in text or "访问受限" in text: 
            return {"_err": "WAF_BLOCK"}
            
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        try:
            json_str = match.group(1) if match else text
            return json.loads(json_str)
        except:
            return {"_err": "PARSE_FAIL"}

    def _route_through_worker(self, target_url: str) -> str:
        """Cache-Buster + CF Worker 穿透路由"""
        base_url = re.sub(r'[&?]_cbuster=\d+', '', target_url)
        connector = "&" if "?" in base_url else "?"
        bust_url = f"{base_url}{connector}_cbuster={time.time_ns()}"
        
        if self.worker_url:
            return f"{self.worker_url}?url={urllib.parse.quote(bust_url, safe='')}"
        return bust_url

    async def _safe_request(self, session, url: str, secid: str = "LIST") -> dict:
        """带指数退避的强健请求层"""
        routed_url = self._route_through_worker(url)
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # 高并发下加入微小抖动，防止波峰打穿 Worker
                await asyncio.sleep(random.uniform(0.1, 0.5) * attempt)
                
                resp = await session.get(routed_url, headers=self.headers, timeout=25)
                
                if resp.status_code == 200:
                    data = self._extract_json_with_diag(resp.text, secid)
                    if "_err" not in data: 
                        return data
                
                logger.debug(f"⚠️ {secid} 响应异常 [Status: {resp.status_code}] | 重试 {attempt+1}")
            except Exception as e:
                logger.debug(f"🕒 {secid} 网络波动: {e} | 重试 {attempt+1}")
                
        return {}

    async def fetch_dynamic_sector_list(self) -> list:
        """
        零 DOM 模式抓取目录：直通 push2 API，分页获取
        """
        logger.info("💪 [Muscle] 直连 API：分页扫描全市场板块目录...")
        all_codes = set()
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            for pn in range(1, 6): # 1-5 页，每页 200 个，覆盖 1000 个板块
                fs_param = urllib.parse.quote("m:90+t:2,m:90+t:3")
                target_url = (
                    f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=200&po=1&np=1"
                    f"&fltt=2&invt=2&fid=f3&fs={fs_param}&fields=f12&ut={self.UT}"
                )
                
                data = await self._safe_request(session, target_url, f"LIST_P{pn}")
                if data and data.get("data") and data["data"].get("diff"):
                    for x in data["data"]["diff"]:
                        all_codes.add(f"90.{x['f12']}")
                    logger.debug(f"✅ 第 {pn} 页抓取成功")
                else:
                    logger.debug(f"⚠️ 第 {pn} 页无数据或到达尾页")
                await asyncio.sleep(0.3)

        if not all_codes:
            logger.warning("❌ API 目录直连扫描失败，启用本地静态核心库兜底！")
            return self.FALLBACK_SECTORS
            
        logger.success(f"💪 [Muscle] 目录扫描完成，共捕获 {len(all_codes)} 个唯一板块。")
        return list(all_codes)

    async def _fetch_single_sector(self, session, secid: str, semaphore: asyncio.Semaphore):
        """零 DOM 模式抓取单 K 线：直接拼凑官方标准接口"""
        async with semaphore:
            target_url = (
                f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                f"&klt=101&fqt=0&end=20500101&lmt=100000&ut={self.UT}"
            )
            
            data = await self._safe_request(session, target_url, secid)
            
            if data and data.get("data") and data["data"].get("klines"):
                res = []
                for r in data["data"]["klines"]:
                    row = r.split(",")
                    # 数据映射保护，容错处理
                    try:
                        res.append({
                            "secid": secid, 
                            "date": row[0],
                            "open": float(row[1]), 
                            "close": float(row[2]),
                            "high": float(row[3]), 
                            "low": float(row[4]),
                            "volume": float(row[5]), 
                            "amount": float(row[6])
                        })
                    except (IndexError, ValueError):
                        continue
                return res
            return []

    async def fetch_all_sectors(self, sector_list: list):
        """并发引擎主体"""
        logger.info(f"💪 [Muscle] 引擎预热完毕，并发限制: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single_sector(session, secid, semaphore) for secid in sector_list]
            
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res: 
                    all_results.extend(res)
                # 降低日志频率，每抓取一定数量播报一次
                if len(all_results) > 0 and len(all_results) % 50000 == 0:
                    logger.info(f"📊 内存池堆叠中: 已缓存 {len(all_results)} 条 K 线切片")
        
        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            # 使用 zstd 压缩，比 snappy 更适合金融时序数据，体积更小
            df.write_parquet("data/sector_klines_full.parquet", compression="zstd")
            logger.success(f"💾 工业级作业完成！成功抗击风控，落盘 {len(all_results)} 行底层数据。")
