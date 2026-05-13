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
    # 💡 核心板块静态库：即使接口全挂，也要保证最核心的数据不丢
    FALLBACK_SECTORS = [
        "90.BK0896", "90.BK1036", "90.BK0475", "90.BK0733", "90.BK0427",
        "90.BK1027", "90.BK0477", "90.BK0474", "90.BK0456", "90.BK0480"
    ]

    def __init__(self, trust_context: dict):
        self.kline_template = trust_context.get("kline_url", "")
        self.clist_template = trust_context.get("clist_url", "")
        raw_worker = os.getenv("CF_WORKER_URL", "").strip()
        self.worker_url = raw_worker if raw_worker.startswith("http") else f"https://{raw_worker}"
        
        self.headers = {
            "User-Agent": trust_context.get("ua", ""),
            "Cookie": trust_context.get("cookies", ""),
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }
        self.concurrency = int(os.getenv("CONCURRENCY", 10))
        self.impersonate = "chrome120"

    def _extract_json_with_diag(self, text: str, secid: str) -> dict:
        if not text: return {"_err": "EMPTY"}
        if "安全验证" in text: return {"_err": "WAF_BLOCK"}
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        try:
            json_str = match.group(1) if match else text
            return json.loads(json_str)
        except:
            return {"_err": "PARSE_FAIL"}

    def _route_through_worker(self, target_url: str) -> str:
        # 移除旧的 cbuster
        base_url = re.sub(r'[&?]_cbuster=\d+', '', target_url)
        bust_url = f"{base_url}&_cbuster={time.time_ns()}" if "?" in base_url else f"{base_url}?_cbuster={time.time_ns()}"
        if self.worker_url:
            return f"{self.worker_url}?url={urllib.parse.quote(bust_url, safe='')}"
        return bust_url

    async def _safe_request(self, session, url: str, secid: str = "LIST") -> dict:
        routed_url = self._route_through_worker(url)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 502 往往是并发冲突，增加随机避让
                await asyncio.sleep(random.uniform(0.3, 0.8) * attempt)
                resp = await session.get(routed_url, headers=self.headers, timeout=30)
                if resp.status_code == 200:
                    data = self._extract_json_with_diag(resp.text, secid)
                    if "_err" not in data: return data
                logger.debug(f"⚠️ {secid} 请求异常 [{resp.status_code}] | 重试 {attempt+1}")
            except Exception as e:
                logger.debug(f"🕒 {secid} 波动: {e}")
        return {}

    async def fetch_dynamic_sector_list(self) -> list:
        """分批抓取目录：解决超大规模请求导致的 502/Timeout 问题"""
        logger.info(f"💪 [Muscle] 开始分页扫描全市场目录 (探索模式)...")
        
        ut = "fa5fd1943c7b386f172d6893dbfba10b"
        if self.kline_template:
            ut_match = re.search(r'ut=([^&]+)', self.kline_template)
            if ut_match: ut = ut_match.group(1)

        all_codes = set()
        # 💡 关键改进：将 2000 个拆成 5 页，每页 200 个，降低单次响应压力
        async with AsyncSession(impersonate=self.impersonate) as session:
            for pn in range(1, 6): 
                fs_param = urllib.parse.quote("m:90+t:2,m:90+t:3")
                target_url = (
                    f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=200&po=1&np=1"
                    f"&fltt=2&invt=2&fid=f3&fs={fs_param}&fields=f12&ut={ut}"
                )
                data = await self._safe_request(session, target_url, f"LIST_P{pn}")
                if data and data.get("data") and data["data"].get("diff"):
                    for x in data["data"]["diff"]:
                        all_codes.add(f"90.{x['f12']}")
                    logger.debug(f"✅ 第 {pn} 页目录抓取成功")
                else:
                    logger.warning(f"⚠️ 第 {pn} 页目录抓取失败")
                await asyncio.sleep(0.5)

        if not all_codes:
            logger.warning("❌ 动态目录扫描全败，启用静态核心库兜底！")
            return self.FALLBACK_SECTORS
            
        logger.success(f"💪 [Muscle] 目录扫描完成，共捕获 {len(all_codes)} 个板块。")
        return list(all_codes)

    async def _fetch_single_sector(self, session, secid: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            if not self.kline_template: return []
            target_url = re.sub(r'secid=[^&]+', f'secid={secid}', self.kline_template)
            target_url = re.sub(r'lmt=\d+', 'lmt=100000', target_url)
            data = await self._safe_request(session, target_url, secid)
            if data and data.get("data") and data["data"].get("klines"):
                res = []
                for r in data["data"]["klines"]:
                    row = r.split(",")
                    res.append({
                        "secid": secid, "date": row[0],
                        "open": float(row[1]), "close": float(row[2]),
                        "high": float(row[3]), "low": float(row[4]),
                        "volume": float(row[5]), "amount": float(row[6])
                    })
                return res
            return []

    async def fetch_all_sectors(self, sector_list: list):
        logger.info(f"💪 [Muscle] 启动并发抓取，并发限制: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single_sector(session, secid, semaphore) for secid in sector_list]
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res: all_results.extend(res)
                if len(all_results) > 0 and len(all_results) % 10000 == 0:
                    logger.info(f"📊 进度监控: 已拉取 {len(all_results)} 条 K 线")
        
        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            df.write_parquet("data/sector_klines_full.parquet")
            logger.success(f"💾 抓取结束！成功落盘 {len(all_results)} 行数据。")
