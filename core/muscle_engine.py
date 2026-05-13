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
    UT = "fa5fd1943c7b386f172d6893dbfba10b"

    def __init__(self):
        # 1. 负载池与配置加载
        raw_env = os.getenv("CF_WORKER_URLS") or os.getenv("CF_WORKER_URL") or ""
        raw_urls = raw_env.split(",")
        self.worker_pool = [u.strip() for u in raw_urls if u.strip()]
        
        if not self.worker_pool:
            msg = "🚨 [Init] 未检测到 CF_WORKER_URLS，请检查 Secrets 配置。"
            logger.critical(msg)
            raise RuntimeError(msg)
            
        self.concurrency = int(os.getenv("CONCURRENCY", 8))
        self.db_path = "data/sector_quant.db"
        self.impersonate = "chrome124"
        self.trust_context = {"cookies": {}, "headers": {}}
        self.stats = {"total": 0, "errors": 0, "codes": {}}
        
        # 2. DuckDB 初始化
        os.makedirs("data", exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        """初始化核心表结构"""
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
        """Phase 0: 隔离线程构建信任态"""
        logger.info(f"🔑 [Phase 0] 建立信任链路 | 负载池: {len(self.worker_pool)} 节点")
        try:
            response = await asyncio.to_thread(self._run_scrapling)
            raw_cookies = response.cookies
            self.trust_context["cookies"] = {c['name']: c['value'] for c in raw_cookies} if isinstance(raw_cookies, list) else raw_cookies
            self.trust_context["headers"] = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/",
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9"
            }
            logger.success(f"✅ 信任链已就绪 (持有 {len(self.trust_context['cookies'])} 枚风控 Cookie)")
        except Exception as e:
            logger.error(f"⚠️ 信任链构建异常: {e} (系统将尝试裸奔模式)")

    def _run_scrapling(self):
        """同步 Fetcher 调用 (v0.3.x 兼容版)"""
        fetcher = Fetcher()
        return fetcher.get("https://quote.eastmoney.com/center/hsbk.html")

    def _route_url(self, target_url: str, use_cache: bool = False) -> str:
        """智能路由与缓存管理器"""
        now = datetime.now()
        # 盘后缓存 1 小时，盘中缓存 30 秒
        cache_window = 3600 if now.hour >= 16 or now.hour < 9 else 30
        
        if use_cache:
            target_url += f"&_ts={int(time.time() / cache_window)}"
        else:
            target_url += f"&_cb={time.time_ns()}"

        worker_base = random.choice(self.worker_pool)
        if not worker_base.startswith("http"): worker_base = f"https://{worker_base}"
        # 注意：这里我们使用 urllib.parse.quote 以支持 Worker 转发
        return f"{worker_base}?url={urllib.parse.quote(target_url, safe='')}"

    async def _safe_request(self, session, url: str, label: str, cache: bool = False) -> dict:
        """核心请求器：具备 JSONP 兼容解析与预览诊断功能"""
        routed = self._route_url(url, use_cache=cache)
        for attempt in range(3):
            try:
                if attempt > 0: await asyncio.sleep(random.uniform(0.5, 1.2) * attempt)
                
                resp = await session.get(routed, headers=self.trust_context["headers"], 
                                         cookies=self.trust_context["cookies"], timeout=25)
                self.stats["total"] += 1
                
                if resp.status_code == 200:
                    text = resp.text.strip()
                    # 识别并剥离 JSONP 外壳
                    if "(" in text and ")" in text:
                        match = re.search(r'\((.*)\)', text, re.DOTALL)
                        if match: text = match.group(1)
                    
                    try:
                        data = json.loads(text)
                        if data and data.get("data") is not None:
                            return data
                        logger.debug(f"⚠️ {label} 业务逻辑空回馈: {text[:100]}")
                    except json.JSONDecodeError:
                        logger.error(f"❌ {label} 响应无法解析为 JSON: {text[:100]}")
                
                self.stats["errors"] += 1
                self.stats["codes"][str(resp.status_code)] = self.stats["codes"].get(str(resp.status_code), 0) + 1
            except Exception as e:
                self.stats["errors"] += 1
                logger.debug(f"🕒 {label} 物理层抖动: {e}")
        return {}

    async def get_active_sectors(self, force_reconcile: bool = False) -> list:
        """Phase 1: 智能目录扫描 (具备 DB 避险能力)"""
        # 1. 尝试加载存量名录
        existing = self.conn.execute("SELECT secid FROM sector_master").fetchall()
        
        # 2. 判断是否需要发起网络请求
        if existing and not force_reconcile:
            logger.success(f"✅ [Phase 1] 缓存命中: 加载存量 {len(existing)} 个板块")
            return [r[0] for r in existing]

        logger.info(f"📡 [Phase 1] {'[强制重刷]' if force_reconcile else '[首次启动]'} 扫描活跃名录...")
        all_codes = set()
        categories = {"地域": "m:90+t:1", "行业": "m:90+t:2", "概念": "m:90+t:3"}
        
        async with AsyncSession(impersonate=self.impersonate) as session:
            for cat_name, fs in categories.items():
                cat_count = 0
                for pn in range(1, 12): # 增加扫描深度
                    # 补齐 fields 字段，防止某些 API 版本因字段缺失拒绝服务
                    url = (f"https://push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz=250&po=1&np=1"
                           f"&fltt=2&invt=2&fid=f3&fs={fs}&fields=f12,f14&ut={self.UT}")
                    
                    data = await self._safe_request(session, url, f"LIST_{cat_name}_P{pn}")
                    
                    if data and data.get("data", {}).get("diff"):
                        items = data["data"]["diff"]
                        for x in items:
                            if x.get("f12"):
                                all_codes.add(f"90.{x['f12']}")
                                cat_count += 1
                        if len(items) < 250: break
                    else:
                        break # 到达最后一页或失败
                logger.info(f"   ∟ 分类 [{cat_name}] 探测完毕: 发现 {cat_count} 个")
        
        # 3. 结果入库与避险
        if all_codes:
            self.conn.execute("DELETE FROM sector_master")
            self.conn.executemany("INSERT INTO sector_master VALUES (?, ?)", 
                                  [(c, datetime.now()) for c in all_codes])
            logger.success(f"✅ [Phase 1] 全量扫描成功: 捕获 {len(all_codes)} 个板块")
            return list(all_codes)
        else:
            if existing:
                logger.warning("⚠️ [Phase 1] 网络同步失败，已自动回滚至 DB 存量数据避险")
                return [r[0] for r in existing]
            logger.error("❌ [Phase 1] 名录扫描与避险全线失败")
            return []

    async def sync_all_klines(self, sector_list: list):
        """Phase 2: 工业级增量同步引擎"""
        initial_count = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        
        # 提取锚点：beg = last_date + 1 day
        res = self.conn.execute("SELECT secid, MAX(date) FROM sector_klines GROUP BY secid").fetchall()
        anchors = {row[0]: (row[1] + timedelta(days=1)).strftime("%Y%m%d") for row in res if row[1]}
        
        logger.info(f"🚀 [Phase 2] 并发同步启动 | 活跃板块: {len(sector_list)} | 存量锚点: {len(anchors)}")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        async with AsyncSession(impersonate=self.impersonate, max_clients=self.concurrency) as session:
            tasks = [self._fetch_single(session, sid, anchors.get(sid, "19900101"), semaphore) for sid in sector_list]
            for coro in asyncio.as_completed(tasks):
                batch = await coro
                if batch:
                    # DuckDB INSERT OR IGNORE 保证了即使 beg 日期重叠也能物理去重
                    self.conn.executemany("INSERT OR IGNORE INTO sector_klines VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
        
        # 数据审计与落盘
        final_count = self.conn.execute("SELECT count(*) FROM sector_klines").fetchone()[0]
        added = final_count - initial_count
        
        output_parquet = os.getenv("DATA_PATH", "data/sector_klines_full.parquet")
        self.conn.execute(f"COPY sector_klines TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        
        logger.success("📊 [Final Audit] 数据库同步作业完成")
        logger.info(f"   ∟ 总行数: {final_count} | 本次增量: {added} 行")
        logger.info(f"   ∟ 存储状态: {output_parquet} 已同步")

    async def _fetch_single(self, session, secid, beg_date, sem):
        async with sem:
            clean_beg = beg_date.replace("-", "")
            # 只请求必要的字段以降低带宽负载
            url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}"
                   f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt=101&fqt=0&end=20500101&beg={clean_beg}&ut={self.UT}")
            
            data = await self._safe_request(session, url, f"K_{secid}", cache=True)
            if data and data.get("data", {}).get("klines"):
                batch = []
                for k in data["data"]["klines"]:
                    r = k.split(',')
                    batch.append((secid, r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), float(r[6])))
                return batch
            return None
