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
    # 💡 极其重要的静态兜底：如果 API 暂时波动，保证核心板块不掉队
    # 包含：白酒、半导体、光伏、新能源车、人工智能、银行、证券、医药、军工、地产
    FALLBACK_SECTORS = [
        "90.BK0896", "90.BK1036", "90.BK0475", "90.BK1027", "90.BK0800",
        "90.BK0427", "90.BK0473", "90.BK0447", "90.BK0490", "90.BK0451"
    ]
    
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        raw_worker = os.getenv("CF_WORKER_URL", "").strip()
        self.worker_url = raw_worker if raw_worker.startswith("http") else f"https://{raw_worker}" if raw_worker else ""
        if not self.worker_url:
            logger.critical("🚨 未检测到 CF_WORKER_URL，全线代理模式无法启动！")
            
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }
        self.concurrency = int(os.getenv("CONCURRENCY", 10))
        self.impersonate = "chrome124"
        
        # Worker 错误统计
        self.worker_error_stats = {
            "total_requests": 0,
            "error_count": 0,
            "error_by_code": {},  # {status_code: count}
            "last_error": None,
            "last_error_time": None
        }

    def _route_url(self, target_url: str) -> str:
        """强制 100% 穿透 Worker"""
        # 注入毫秒级 Cache-Buster 确保穿透 CF 边缘缓存
        bust_url = f"{target_url}&_cb={time.time_ns()}" if "?" in target_url else f"{target_url}?_cb={time.time_ns()}"
        if self.worker_url:
            return f"{self.worker_url}?url={urllib.parse.quote(bust_url, safe='')}"
        return bust_url

    def _extract_json(self, text: str) -> dict:
        if not text: return {}
        # 兼容 JSONP 和 纯 JSON
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        try:
            return json.loads(match.group(1) if match else text)
        except:
            return {}

    async def check_worker_health(self) -> dict:
        """检查 Worker 健康状态，返回健康信息或 None"""
        if not self.worker_url:
            logger.warning("⚠️ 未配置 CF_WORKER_URL，跳过健康检查")
            return None
        try:
            health_url = f"{self.worker_url}?health"
            async with AsyncSession(impersonate=self.impersonate) as session:
                resp = await session.get(health_url, headers=self.headers, timeout=10)
                if resp.status_code == 200:
                    stats = json.loads(resp.text)
                    logger.success(f"💚 Worker 健康检查通过: {json.dumps(stats, ensure_ascii=False)}")
                    return stats
                else:
                    logger.error(f"💔 Worker 健康检查失败: HTTP {resp.status_code}")
                    return None
        except Exception as e:
            logger.error(f"💔 Worker 健康检查异常: {e}")
            return None

    async def _safe_request(self, session, url: str, label: str) -> dict:
        """核心请求器：只走代理，不成功便退避，并记录 Worker 错误统计"""
        routed = self._route_url(url)
        for attempt in range(3):
            try:
                # 增加请求间隔随机性
                await asyncio.sleep(random.uniform(0.5, 1.2) * attempt)
                resp = await session.get(routed, headers=self.headers, timeout=25)
                self.worker_error_stats["total_requests"] += 1
                if resp.status_code == 200:
                    data = self._extract_json(resp.text)
                    if data and data.get("data"): return data
                # 记录错误统计
                self.worker_error_stats["error_count"] += 1
                code_key = str(resp.status_code)
                self.worker_error_stats["error_by_code"][code_key] = \
                    self.worker_error_stats["error_by_code"].get(code_key, 0) + 1
                self.worker_error_stats["last_error"] = f"HTTP {resp.status_code}"
                self.worker_error_stats["last_error_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
                logger.debug(f"⚠️ {label} 尝试 {attempt+1} 失败: HTTP {resp.status_code}")
            except Exception as e:
                self.worker_error_stats["error_count"] += 1
                self.worker_error_stats["last_error"] = str(e)[:200]
                self.worker_error_stats["last_error_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
                logger.debug(f"🕒 {label} 网络波动: {e}")
        return {}

    def get_worker_error_summary(self) -> str:
        """生成 Worker 错误摘要报告"""
        stats = self.worker_error_stats
        if stats["total_requests"] == 0:
            return "📊 Worker 统计: 暂无请求记录"
        error_rate = (stats["error_count"] / stats["total_requests"]) * 100 if stats["total_requests"] > 0 else 0
        lines = [
            f"📊 Worker 错误统计摘要:",
            f"   总请求数: {stats['total_requests']}",
            f"   错误数: {stats['error_count']} ({error_rate:.1f}%)",
            f"   错误码分布: {json.dumps(stats['error_by_code'], ensure_ascii=False)}",
            f"   最后错误: {stats['last_error']} @ {stats['last_error_time']}"
        ]
        return "\n".join(lines)

    async def fetch_dynamic_sector_list(self) -> list:
        """
        Phase 1: 穿透代理扫描目录
        不再通过 Playwright，直接用 curl_cffi 打 API
        """
        logger.info("💪 [Phase 1] 启动全代理目录扫描...")
        all_codes = set()
        # 将地域(t:1)、行业(t:2)、概念(t:3) 分开扫描，降低单次载荷
        categories = {"地域": "m:90+t:1", "行业": "m:90+t:2", "概念": "m:90+t:3"}
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs in categories.items():
                logger.info(f"➡️ 正在扫描板块分类: {cat_name}")
                # 💡 每次只抓 250 条，分两页抓，确保 100% 成功率
                for pn in [1, 2, 3]:
                    url = (
                        f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                        f"&fltt=2&invt=2&fid=f3&fs={urllib.parse.quote(fs)}&fields=f12&ut={self.UT}"
                    )
                    data = await self._safe_request(session, url, f"LIST_{cat_name}_P{pn}")
                    if data and data.get("data") and data["data"].get("diff"):
                        for x in data["data"]["diff"]:
                            all_codes.add(f"90.{x['f12']}")
                        if len(data["data"]["diff"]) < 250: break
                    else:
                        break # 该分类抓取结束或失败
        
        if not all_codes:
            logger.warning("❌ 全代理扫描未果，启用【静态核心库】兜底运行")
            return self.FALLBACK_SECTORS
            
        logger.success(f"💪 [Phase 1] 扫描成功，共获取 {len(all_codes)} 个活跃板块。")
        return list(all_codes)

    async def _fetch_kline_worker(self, session, secid: str, semaphore: asyncio.Semaphore):
        """Phase 2: 高并发 K 线拉取"""
        async with semaphore:
            # 增加极短的抖动，防止批量请求在同一毫秒到达 Worker
            await asyncio.sleep(random.uniform(0.01, 0.1))
            url = (
                f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                f"&klt=101&fqt=0&end=20500101&lmt=100000&ut={self.UT}"
            )
            data = await self._safe_request(session, url, f"KLINE_{secid}")
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
        logger.info(f"💪 [Phase 2] 启动并发引擎 | Concurrency: {self.concurrency}")
        semaphore = asyncio.Semaphore(self.concurrency)
        all_results = []
        
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_kline_worker(session, sid, semaphore) for sid in sector_list]
            for coro in asyncio.as_completed(tasks):
                res = await coro
                if res:
                    all_results.extend(res)
                    if len(all_results) > 0 and len(all_results) % 50000 == 0:
                        logger.info(f"📊 数据规模监测: 已获取 {len(all_results)} 条 K 线")
        
        if all_results:
            os.makedirs("data", exist_ok=True)
            df = pl.DataFrame(all_results)
            df.write_parquet("data/sector_klines_full.parquet", compression="zstd")
            logger.success(f"💾 任务圆满完成！落盘 {len(all_results)} 行数据。")
