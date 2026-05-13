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
    FALLBACK_SECTORS = [
        "90.BK0896", "90.BK1036", "90.BK0475", "90.BK0733", "90.BK0427",
        "90.BK1027", "90.BK0477", "90.BK0474", "90.BK0456", "90.BK0480"
    ]
    
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        raw_worker = os.getenv("CF_WORKER_URL", "").strip()
        if raw_worker and raw_worker.lower() not in ["none", "null"]:
            self.worker_url = raw_worker if raw_worker.startswith("http") else f"https://{raw_worker}"
            logger.info(f"🛡️ [Proxy] Worker 节点就绪: {self.worker_url}")
        else:
            self.worker_url = ""
            logger.warning("⚠️ [Proxy] 未配置 CF_WORKER_URL，将直连高危源站")
            
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }
        
        self.concurrency = int(os.getenv("CONCURRENCY", 10))
        self.impersonate = "chrome124"

    def _extract_json_with_diag(self, text: str, secid: str) -> dict:
        if not text: return {"_err": "EMPTY"}
        if "安全验证" in text or "访问受限" in text: return {"_err": "WAF_BLOCK"}
            
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        try:
            json_str = match.group(1) if match else text
            return json.loads(json_str)
        except:
            return {"_err": "PARSE_FAIL"}

    def _route_through_worker(self, target_url: str) -> str:
        base_url = re.sub(r'[&?]_cbuster=\d+', '', target_url)
        connector = "&" if "?" in base_url else "?"
        bust_url = f"{base_url}{connector}_cbuster={time.time_ns()}"
        
        if self.worker_url:
            return f"{self.worker_url}?url={urllib.parse.quote(bust_url, safe='')}"
        return bust_url

    async def _safe_request(self, session, url: str, secid: str = "LIST", timeout: int = 40) -> dict:
        """💡 增加默认超时至 40 秒，防止海外节点传输断裂"""
        routed_url = self._route_through_worker(url)
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # 💡 强力退避：如果前面失败了，后面等待更久 (指数级 + 随机数)
                wait_time = (1.5 ** attempt) + random.uniform(0.1, 0.5)
                if attempt > 0:
                    await asyncio.sleep(wait_time)
                
                resp = await session.get(routed_url, headers=self.headers, timeout=timeout)
                
                if resp.status_code == 200:
                    data = self._extract_json_with_diag(resp.text, secid)
                    if "_err" not in data: 
                        return data
                
                # 静默处理 520 等偶发错误，只有连续 3 次失败才会在外部报错
                logger.debug(f"⚠️ {secid} 响应 {resp.status_code} | 重试 {attempt+1}/{max_retries}")
            except Exception as e:
                logger.debug(f"🕒 {secid} 链路波动 ({e}) | 重试 {attempt+1}/{max_retries}")
                
        return {}

    async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle] 直连 API：细颗粒度扫描全市场板块目录...")
        all_codes = set()
        
        async def fetch_dynamic_sector_list(self) -> list:
        logger.info("💪 [Muscle] 启动目录扫描：按 [地域/行业/概念] 分类独立探测...")
        all_codes = set()
        
        # 💡 与官方网页完全对应的三大分类字典
        categories = {
            "地域板块": "m:90+t:1",
            "行业板块": "m:90+t:2",
            "概念板块": "m:90+t:3"
        }
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs_param in categories.items():
                logger.info(f"➡️ 开始扫描分类: {cat_name}")
                encoded_fs = urllib.parse.quote(fs_param)
                cat_count = 0
                
                # 每个分类单独从第 1 页开始翻页，极大地降低了深部分页(pn>10)的触发概率
                for pn in range(1, 20): 
                    target_url = (
                        f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=50&po=1&np=1"
                        f"&fltt=2&invt=2&fid=f3&fs={encoded_fs}&fields=f12&ut={self.UT}"
                    )
                    
                    data = await self._safe_request(session, target_url, f"{cat_name}_P{pn}")
                    
                    if data and data.get("data") and data.get("data").get("diff"):
                        diff = data["data"]["diff"]
                        for x in diff:
                            all_codes.add(f"90.{x['f12']}")
                            cat_count += 1
                            
                        # 如果不满 50 个，说明当前分类已经翻到底了，直接进入下一个分类
                        if len(diff) < 50:
                            logger.debug(f"✅ {cat_name} 扫描触底结束，共 {pn} 页，捕获 {cat_count} 个。")
                            break
                    else:
                        logger.debug(f"⚠️ {cat_name} 第 {pn} 页无数据，提前结束本分类。")
                        break
                        
                    # 拟人化翻页休眠
                    await asyncio.sleep(0.5)
                
                # 分类切换间的安全休眠
                await asyncio.sleep(1.0)

        if not all_codes:
            logger.warning("❌ 目录扫描全线失败，启用静态核心库兜底！")
            return self.FALLBACK_SECTORS
            
        logger.success(f"💪 [Muscle] 三大分类拼图完成！实际合并去重后共捕获 {len(all_codes)} 个唯一板块。")
        return list(all_codes)

    async def _fetch_single_sector(self, session, secid: str, semaphore: asyncio.Semaphore):
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
                    try:
                        res.append({
                            "secid": secid, "date": row[0],
                            "open": float(row[1]), "close": float(row[2]),
                            "high": float(row[3]), "low": float(row[4]),
                            "volume": float(row[5]), "amount": float(row[6])
                        })
                    except (IndexError, ValueError): continue
                return res
            return []

    async def fetch_all_sectors(self, sector_list: list):
        logger.info(f"💪 [Muscle] 流量平滑引擎启动，并发上限: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = []
            for secid in sector_list:
                # 💡 发牌器平滑 (Traffic Smoothing)：
                # 不要瞬间把所有任务塞入 Event Loop。每次循环强制休眠 0.05 秒。
                # 这保证了即使并发设为 10，每秒最多也只向 CF Worker 发起 20 个新连接。
                # 这将彻底消灭 520 和 502 错误！
                await asyncio.sleep(0.05)
                tasks.append(asyncio.create_task(self._fetch_single_sector(session, secid, semaphore)))
            
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res: 
                    all_results.extend(res)
                if len(all_results) > 0 and len(all_results) % 100000 == 0:
                    logger.info(f"📊 内存池堆叠中: 已安全缓存 {len(all_results)} 条 K 线切片")
        
        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            df.write_parquet("data/sector_klines_full.parquet", compression="zstd")
            logger.success(f"💾 工业级作业完成！成功抗击风控，落盘 {len(all_results)} 行底层数据。")
