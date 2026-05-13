import asyncio
import json
import re
import os
import time
import random
import urllib.parse
from datetime import datetime, timedelta
import duckdb
from loguru import logger
from curl_cffi.requests import AsyncSession
from scrapling import Fetcher

class MuscleEngine:
    # 板块分类代码
    CATEGORIES = {"地域": "m:90%2Bt:1", "行业": "m:90%2Bt:2", "概念": "m:90%2Bt:3"}
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        # 1. 节点池化加载
        raw_env = os.getenv("CF_WORKER_URLS") or os.getenv("CF_WORKER_URL") or ""
        self.worker_pool = [u.strip() for u in raw_env.split(",") if u.strip()]
        if not self.worker_pool:
            raise RuntimeError("🚨 [Init] 未检测到 CF_WORKER_URLS，请检查 Secrets 配置。")
            
        # 单 Worker 建议并发不要超过 5
        self.concurrency = int(os.getenv("CONCURRENCY", 4))
        self.db_path = "data/sector_quant.db"
        self.impersonate = "chrome124"
        self.trust_context = {"cookies": {}, "headers": {}}
        # 统计审计
        self.stats = {"total_tasks": 0, "failed_tasks": 0, "codes": {}}
        
        os.makedirs("data", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        """DuckDB 工业级表结构"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sector_klines (
                secid VARCHAR, date DATE, open DOUBLE, close DOUBLE,
                high DOUBLE, low DOUBLE, volume DOUBLE, amount DOUBLE,
                PRIMARY KEY(secid, date)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sector_master (
                secid VARCHAR PRIMARY KEY, 
                last_update TIMESTAMP
            )
        """)

    async def build_trust_chain(self):
        """Phase 0: 建立信任链路 + 保底 Cookie 注入"""
        logger.info(f"🔑 [Phase 0] 启动信任链构建 (当前节点数: {len(self.worker_pool)})")
        try:
            response = await asyncio.to_thread(self._run_scrapling)
            cookies = response.cookies
            self.trust_context["cookies"] = {c['name']: c['value'] for c in cookies} if isinstance(cookies, list) else cookies
            
            # 💡 核心补丁：如果 Scrapling 没抓到 Cookie，注入保底伪装，防止 API 判定为非法匿名
            if not self.trust_context["cookies"]:
                self.trust_context["cookies"] = {
                    "qgqp_b_id": "".join(random.choices("0123456789abcdef", k=32)),
                    "st_pvi": str(int(time.time()))
                }
                logger.warning("⚠️ Scrapling 未获取原生 Cookie，已启用保底伪装指纹")
            
            self.trust_context["headers"] = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/center/hsbk.html",
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9"
            }
            logger.success(f"✅ 信任链就绪 | Cookie: {len(self.trust_context['cookies'])} 枚")
        except Exception as e:
            logger.error(f"⚠️ 信任链构建异常: {e}")

    def _run_scrapling(self):
        fetcher = Fetcher()
        return fetcher.get("https://quote.eastmoney.com/center/hsbk.html")

    def _route_url(self, target_url: str, use_cache: bool = False) -> str:
        """单/多节点自适应路由"""
        worker_base = random.choice(self.worker_pool)
        if not worker_base.startswith("http"): worker_base = f"https://{worker_base}"
        
        # 缓存键：盘后 1 小时，盘中 30 秒
        now = datetime.now()
        cache_window = 3600 if now.hour >= 16 or now.hour < 9 else 30
        suffix = f"&_ts={int(time.time()/cache_window)}" if use_cache else f"&_cb={time.time_ns()}"
        
        return f"{worker_base}?url={urllib.parse.quote(target_url + suffix, safe='')}"

    async def _safe_request(self, session, url: str, label: str, cache: bool = False) -> dict:
        """带指数退避和状态审计的请求器"""
        for attempt in range(3):
            routed = self._route_url(url, use_cache=cache)
            try:
                # 💡 指数退避：第一次失败等 2s，第二次等 6s
                if attempt > 0:
                    wait = 2 * (attempt ** 2) + random.uniform(0.1, 0.9)
                    await asyncio.sleep(wait)
                
                resp = await session.get(routed, headers=self.trust_context["headers"], 
                                         cookies=self.trust_context["cookies"], timeout=45)
                
                if resp.status_code == 200:
                    text = resp.text.strip()
                    # 识别 JSONP
                    if text.startswith("jQuery") or ("(" in text and text.endswith(");")):
                        match = re.search(r'\((.*)\)', text, re.DOTALL)
                        if match: text = match.group(1)
                    
                    data = json.loads(text)
                    if data and data.get("rc") == 0: return data
                    
                    # 业务错误诊断 (如 rc: 102)
                    if attempt == 2:
                        logger.debug(f"❌ {label} 业务失败 rc={data.get('rc')} | {text[:100]}")
                
                # 记录状态码分布
                code = str(resp.status_code)
                self.stats["codes"][code] = self.stats["codes"].get(code, 0) + 1
                
            except Exception as e:
                if attempt == 2:
                    logger.debug(f"🕒 {label} 最终重试失败: {str(e)[:100]}")
        
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        """Phase 1: 智能名录管理"""
        existing = self.conn.execute("SELECT secid FROM sector_master").fetchall()
        if existing and not force_reconcile:
            logger.success(f"✅ [Phase 1] 缓存命中使用 {len(existing)} 个板块")
            return [r[0] for r in existing]

        logger.info("📡 [Phase 1] 正在同步名录...")
        all_codes = set()
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs_code in self.CATEGORIES.items():
                cat_count = 0
                for pn in range(1, 12):
                    self.stats["total_tasks"] += 1
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={fs_code}&fields=f12&ut={self.UT}")
                    data = await self._safe_request(session, url, f"LIST_{cat_name}_P{pn}")
                    if data and data.get("data", {}).get("diff"):
                        items = data["data"]["diff"]
                        for x in items:
                            if x.get("f12"):
                                all_codes.add(f"90.{x['f12']}")
                                cat_count += 1
                        if len(items) < 250: break
                    else:
                        break
                logger.info(f"   ∟ [{cat_name}] 发现 {cat_count} 个")
        
        if all_codes:
            self.conn.execute("DELETE FROM sector_master")
            self.conn.executemany("INSERT INTO sector_master VALUES (?, ?)", [(c, datetime.now()) for c in all_codes])
            logger.success(f"✅ [Phase 1] 捕获 {len(all_codes)} 个编码")
            return list(all_codes)
        return [r[0] for r in existing] if existing else []

    async def sync_all_klines(self, sector_list: list):
        """Phase 2: 并发冲锋 + 串行补扫双模引擎"""
        init_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        res = self.conn.execute("SELECT secid, MAX(date) FROM sector_klines GROUP BY secid").fetchall()
        # 锚点逻辑：beg = last_date + 1 day
        anchors = {row[0]: (row[1] + timedelta(days=1)).strftime("%Y%m%d") for row in res if row[1]}
        
        logger.info(f"🚀 [Phase 2] 同步开始 | 目标: {len(sector_list)} | 并发: {self.concurrency}")
        
        failed_list = []
        semaphore = asyncio.Semaphore(self.concurrency)
        
        # 波次 1：抖动启动并发同步
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = []
            for i, sid in enumerate(sector_list):
                # 💡 抖动启动：均匀分散单 Worker 压力
                delay = i * 0.3
                task = self._fetch_with_stagger(session, sid, anchors.get(sid, "19900101"), semaphore, delay)
                tasks.append(task)
            
            for coro in asyncio.as_completed(tasks):
                sid, batch = await coro
                self.stats["total_tasks"] += 1
                if batch:
                    self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                else:
                    failed_list.append(sid)
        
        # 波次 2：串行降温补扫
        if failed_list:
            logger.warning(f"🔄 第一波次遗漏 {len(failed_list)} 个板块，静默 10s 后启动串行补扫...")
            await asyncio.sleep(10)
            async with AsyncSession(impersonate=self.impersonate) as session:
                for sid in failed_list:
                    # 💡 串行模式：给 Worker 彻底降温
                    await asyncio.sleep(1.5)
                    _, batch = await self._fetch_single(session, sid, anchors.get(sid, "19900101"), asyncio.Semaphore(1))
                    if batch:
                        self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                    else:
                        self.stats["failed_tasks"] += 1 # 补扫也失败才计入最终错误
        
        # 数据审计与落盘
        final_cnt = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        output_parquet = os.getenv("DATA_PATH", "data/sector_klines_full.parquet")
        self.conn.execute(f"COPY sector_klines TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        
        logger.success(f"📊 [Final] 同步完成 | 总行数: {final_cnt} | 本次新增 {final_cnt - init_cnt}")

    async def _fetch_with_stagger(self, session, secid, beg_date, sem, delay):
        """平滑启动函数"""
        await asyncio.sleep(min(delay, 20)) # 限制最大等待
        return await self._fetch_single(session, secid, beg_date, sem)

    async def _fetch_single(self, session, secid, beg_date, sem):
        async with sem:
            clean_beg = beg_date.replace("-", "")
            # 补全 push2his 必须的 fields1 参数
            url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                   f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                   f"&klt=101&fqt=0&end=20500101&beg={clean_beg}&lmt=50000&ut={self.UT}")
            
            data = await self._safe_request(session, url, f"K_{secid}", cache=True)
            if data and data.get("data", {}).get("klines"):
                rows = [(secid, k.split(',')[0], float(k.split(',')[1]), float(k.split(',')[2]), 
                         float(k.split(',')[3]), float(k.split(',')[4]), float(k.split(',')[5]), float(k.split(',')[6])) 
                        for k in data["data"]["klines"]]
                return secid, rows
            return secid, None
