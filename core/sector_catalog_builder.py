import concurrent.futures
import time
import gc
import re
import baostock as bs
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from datetime import datetime, timedelta

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0"
# 个股所属板块 API
STOCK_SECTOR_API = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&ut=fa5fd1943c09a822273714f23b58f2d0&pi=0&pz=100&po=1&np=1&fields=f12,f14&secid={secid}"
# 东财轻量级种子接口 (备用)
EM_SEED_API = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=6000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f12,f14"

def create_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=1)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": UA,
        "Referer": "https://quote.eastmoney.com/",
    })
    return session

def get_json(session: requests.Session, url: str, timeout=15, retries=2):
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(1)
    return None

def get_stock_seeds_from_baostock():
    """从 Baostock 获取今日全市场 A 股代码种子"""
    try:
        bs.login()
        for i in range(15):
            target_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            rs = bs.query_all_stock(day=target_date)
            stocks = []
            if rs.error_code == '0':
                while rs.next():
                    row = rs.get_row_data()
                    code, name = row[0], row[2] if len(row) > 2 else ""
                    if code.startswith(("sh.", "sz.", "bj.")) and name:
                        stocks.append((code, name.strip()))
                if stocks:
                    return stocks
    except: pass
    finally:
        try: bs.logout()
        except: pass
    return []

def get_stock_seeds_from_eastmoney(session: requests.Session):
    """从东财轻量级 API 获取备份个股种子"""
    data = get_json(session, EM_SEED_API)
    if not data: return []
    diff = data.get("data", {}).get("diff", [])
    items = list(diff.values()) if isinstance(diff, dict) else diff
    stocks = []
    for item in items:
        c, n = item.get("f12", ""), item.get("f14", "")
        if not c: continue
        prefix = "sh" if c.startswith("6") else "bj" if c.startswith(("4","8")) else "sz"
        stocks.append((f"{prefix}.{c}", n.strip()))
    return stocks

def fetch_stock_sector_relations(session: requests.Session, stock_info):
    """查询单只个股所属的所有板块"""
    bs_code, stock_name = stock_info
    pure_code = bs_code.split(".")[1]
    secid = f"1.{pure_code}" if bs_code.startswith("sh") else f"0.{pure_code}"
    
    data = get_json(session, STOCK_SECTOR_API.format(secid=secid))
    if not data: return []
    
    diff = data.get("data", {}).get("diff", [])
    items = list(diff.values()) if isinstance(diff, dict) else diff
    return [{"sector_code": x["f12"], "sector_name": x["f14"].strip()} 
            for x in items if x.get("f12", "").startswith("BK")]

def build_sector_catalog():
    """
    【方案二自愈升级版】
    通过全个股映射反推板块列表，并利用极速正则实现 0 网络阻断分类
    """
    session = create_session()
    # 双保险获取股票种子
    stocks = get_stock_seeds_from_baostock() or get_stock_seeds_from_eastmoney(session)
    if not stocks: 
        raise RuntimeError("严重错误: 无法获取任何个股种子，板块目录构建被迫终止。")

    sector_map = {}
    
    # 采用温和且极速的 80 线程进行反向扫描
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=80)
    try:
        futures = {executor.submit(fetch_stock_sector_relations, session, s): s for s in stocks}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="动态构建板块目录"):
            try:
                relations = future.result()
                for rel in relations:
                    code = rel["sector_code"]
                    if code not in sector_map:
                        sector_map[code] = {"code": code, "name": rel["sector_name"]}
            except: pass
    finally:
        session.close()
        try: del futures 
        except: pass
        gc.collect() 
        executor.shutdown(wait=False)

    # 🚀 方案一中沉淀下的硬正则极致分类划分器，无需任何网络请求，CPU 级计算零延迟
    region_pat = re.compile(r"BK014[5-9]|BK015|BK016|BK017|BK018|BK019")
    industry_pat = re.compile(r"BK042[7-9]|BK04[3-9]|BK0[5-8]|BK091[0-7]")

    all_sectors = []
    for code, info in sector_map.items():
        if region_pat.search(code):
            s_type = "Region"
        elif industry_pat.search(code):
            s_type = "Industry"
        else:
            s_type = "Concept"

        all_sectors.append({
            "sid": f"90.{code}",
            "name": info["name"],
            "type": s_type
        })

    return all_sectors
