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
    def __init__(self, trust_context: dict):
        self.kline_template = trust_context.get("kline_url", "")
        self.clist_template = trust_context.get("clist_url", "")
        
        # 环境变量处理：确保 URL 格式正确
        raw_worker = os.getenv("CF_WORKER_URL", "").strip()
        if raw_worker:
            self.worker_url = raw_worker if raw_worker.startswith("http") else f"https://{raw_worker}"
        else:
            self.worker_url = ""
            
        self.headers = {
            "User-Agent": trust_context.get("ua", ""),
            "Cookie": trust_context.get("cookies", ""),
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }
        # 并发控制：建议在 10-15 之间，过高会导致 Worker 502
        self.concurrency = int(os.getenv("CONCURRENCY", 10))
        self.impersonate = "chrome120"

    def _extract_json_with_diag(self, text: str, secid: str) -> dict:
        """带诊断功能的提取器：捕获 HTML 污染或空响应"""
        if not text:
            return {"_err": "EMPTY_RESPONSE"}
        
        # 诊断：是否被 WAF 拦截返回了 HTML
        if "<html" in text.lower() or "bad gateway" in text.lower():
            sample = text[:100].replace('\n', '')
            logger.warning(f"🔍 [Diag] {secid} 收到非 JSON 响应 (疑似拦截): {sample}")
            return {"_err": "HTML_POLLUTION", "_raw": sample}

        # 尝试匹配 JSONP 格式: jQuery123_456({...});
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        try:
            json_str = match.group(1) if match else text
            data = json.loads(json_str)
            
            if "data" not in data or data["data"] is None:
                return {"_err": "BUSINESS_EMPTY"}
            return data
        except Exception as e:
            sample = text[:100].replace('\n', '')
            logger.warning(f"🔍 [Diag] {secid} 解析失败. 样本: {sample} | 错误: {e}")
            return {"_err": "PARSE_ERROR", "_raw": sample}

    def _route_through_worker(self, target_url: str) -> str:
        """核心路由：强制注入 Cache-Buster 并通过 Worker 中转"""
        # 注入时间戳随机数，强制穿透 Cloudflare 边缘缓存，确保 Worker 计数增长
        connector = "&" if "?" in target_url else "?"
        bust_url = f"{target_url}{connector}_cbuster={time.time_ns()}"
        
        if self.worker_url:
            encoded_target = urllib.parse.quote(bust_url, safe='')
            return f"{self.worker_url}?url={encoded_target}"
        return bust_url

    async def _safe_request(self, session, url: str, secid: str = "LIST") -> dict:
        """工业级安全请求层：带指数退避和状态码诊断"""
        routed_url = self._route_through_worker(url)
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # 微量随机延迟，模拟真实行为
                await asyncio.sleep(random.uniform(0.1, 0.3))
                
                resp = await session.get(routed_url, headers=self.headers, timeout=25)
                
                if resp.status_code != 200:
                    raise Exception(f"HTTP_{resp.status_code}")

                data = self._extract_json_with_diag(resp.text, secid)
                if "_err" not in data:
                    return data
                
                # 如果是业务逻辑空，视为失败并重试
                if data["_err"] in ["EMPTY_RESPONSE", "HTML_POLLUTION"]:
                    raise Exception(data["_err"])
                
                return {} # 其他情况（如真的没数据）直接跳出
                
            except Exception as e:
                wait_time = (2 ** attempt) + random.random()
                if attempt < max_retries - 1:
                    logger.debug(f"🕒 链路波动 {secid}: {e} | {wait_time:.1f}s 后重试")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"❌ {secid} 最终请求失败: {e}")
                
        return {}

    async def fetch_dynamic_sector_list(self) -> list:
        """通过 Worker 拉取全市场板块目录"""
        logger.info(f"💪 [Muscle] 正在通过 Worker 节点拉取全市场目录...")
        
        target_url = self.clist_template
        if not target_url:
            # 备刷方案：如果没捕获到 clist，利用 kline 的签名构造一个
            ut_match = re.search(r'ut=([^&]+)', self.kline_template)
            ut = ut_match.group(1) if ut_match else "fa5fd1943c7b386f172d6893dbfba10b"
            fs_param = urllib.parse.quote("m:90+t:2,m:90+t:3,m:90+t:1")
            target_url = (
                f"https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1"
                f"&fltt=2&invt=2&fid=f3&fs={fs_param}&fields=f12&ut={ut}"
            )
        else:
            # 强制修改 pz 参数为 2000，一次抓完所有板块
            fs_param = urllib.parse.quote("m:90+t:2,m:90+t:3,m:90+t:1")
            target_url = re.sub(r'pz=\d+', 'pz=2000', target_url)
            target_url = re.sub(r'fs=[^&]+', f'fs={fs_param}', target_url)
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            data = await self._safe_request(session, target_url, "SECTOR_LIST")
            if not data or "data" not in data or not data["data"]:
                return []
                
            codes = [f"90.{x['f12']}" for x in data["data"]["diff"]]
            logger.success(f"💪 [Muscle] 目录扫描完成，共捕获 {len(codes)} 个板块。")
            return codes

    async def _fetch_single_sector(self, session, secid: str, semaphore: asyncio.Semaphore):
        """抓取单个板块的 K 线"""
        async with semaphore:
            if not self.kline_template: return []
            
            # 替换模板中的 secid 和 lmt
            target_url = re.sub(r'secid=[^&]+', f'secid={secid}', self.kline_template)
            target_url = re.sub(r'lmt=\d+', 'lmt=100000', target_url)

            data = await self._safe_request(session, target_url, secid)
                
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
        """启动并发抓取逻辑"""
        logger.info(f"💪 [Muscle] 启动并发抓取，并发限制: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single_sector(session, secid, semaphore) for secid in sector_list]
            
            # 使用 as_completed 实时获取结果
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res:
                    all_results.extend(res)
                    if len(all_results) % 5000 == 0:
                        logger.info(f"📊 已拉取 {len(all_results)} 条 K 线数据...")
                    
        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            # 使用 Snappy 压缩，兼顾性能与体积
            df.write_parquet("data/sector_klines_full.parquet", compression="snappy")
            logger.success(f"💾 抓取结束！成功落盘 {len(all_results)} 行数据至 Parquet。")
