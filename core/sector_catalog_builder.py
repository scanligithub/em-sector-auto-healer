import concurrent.futures
import time
import gc
import random
import os
import json
from collections import Counter
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
# 个股所属板块 API
STOCK_SECTOR_API = "https://push2.eastmoney.com/api/qt/slist/get?spt=3&ut=fa5fd1943c09a822273714f23b58f2d0&pi=0&pz=100&po=1&np=1&fields=f12,f14&secid={secid}"
# 东财轻量级种子接口
EM_SEED_API = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=6000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields=f12,f14"
# 板块详情接口 (仅作最后分类抢救兜底)
BASEINFO_API = "https://quote.eastmoney.com/newapi/baseinfo/90.{code}"


UNIVERSE_WORKERS = 80
BASEINFO_WORKERS = 80

# 官方分类映射表
BASEINFO_TYPE_MAP = {
    "1": "Region",      # 地域板块
    "2": "Industry",    # 行业板块
    "3": "Concept",     # 概念板块
}

def create_session() -> requests.Session:
    session = requests.Session()
    # 建立 TCP/TLS 通道复用
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=2)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": UA,
        "Referer": "https://quote.eastmoney.com/",
    })
    return session

def get_json(session: requests.Session, url: str, params=None, timeout=15, retries=2):
    for attempt in range(retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(1)
    return None

def get_stock_seeds_from_tdx():
    """从 Go TDX 导出的 stock_list.json 读取全量股票种子列表"""
    if os.path.exists("stock_list.json"):
        try:
            with open("stock_list.json", "r", encoding="utf-8") as f:
                data = json.load(f)
            stocks = []
            for item in data:
                code = item.get("code") or item.get("Code", "")
                name = item.get("code_name") or item.get("CodeName", "")
                if not code:
                    continue
                prefix = code[:2].lower()
                pure_num = code[2:]
                stocks.append((f"{prefix}.{pure_num}", name.strip()))
            if stocks:
                print(f"✅ [Catalog Builder] 成功从 TDX (stock_list.json) 载入 {len(stocks)} 只股票种子。")
                return stocks
        except Exception as e:
            print(f"⚠️ [Catalog Builder] 读取 stock_list.json 失败: {e}")
    return []

def get_stock_seeds_from_eastmoney(session: requests.Session):
    """东财备选种子接口"""
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
    """单只股票所属板块查询"""
    bs_code, stock_name = stock_info
    pure_code = bs_code.split(".")[1]
    secid = f"1.{pure_code}" if bs_code.startswith("sh") else f"0.{pure_code}"
    
    data = get_json(session, STOCK_SECTOR_API.format(secid=secid))
    if not data: return []
    
    diff = data.get("data", {}).get("diff", [])
    items = list(diff.values()) if isinstance(diff, dict) else diff
    return [{"sector_code": x["f12"], "sector_name": x["f14"].strip()} 
            for x in items if x.get("f12", "").startswith("BK")]

def build_sector_universe():
    """核心第一步：自下而上反推板块全集，并拦截成分映射"""
    session = create_session()
    # 优先从 Go TDX 种子获取，若无则自动降级使用东财种子 API
    stocks = get_stock_seeds_from_tdx() or get_stock_seeds_from_eastmoney(session)
    if not stocks: raise RuntimeError("无法获取任何个股种子")

    sector_map = {}
    components_mapping = []  # 拦截成分股映射数据
    
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=UNIVERSE_WORKERS)
    try:
        futures = {executor.submit(fetch_stock_sector_relations, session, s): s for s in stocks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="动态反推板块大名单"):
            stock_info = futures[future]
            bs_code, stock_name = stock_info
            # 将 sh.600000 转换为量化标准格式 SH600000
            std_stock_code = bs_code.upper().replace(".", "")
            
            try:
                relations = future.result()
                if not relations: continue
                for rel in relations:
                    code = rel["sector_code"]
                    if code not in sector_map:
                        sector_map[code] = {"code": code, "name": rel["sector_name"]}
                        
                    components_mapping.append({
                        "sector_id": f"90.{code}",  # 保证与 K 线的 sid 格式一致
                        "stock_id": std_stock_code,
                        "stock_name": stock_name
                    })
            except Exception: pass

    except Exception as e:
        print(f"[-] 扫描中断: {e}")
    finally:
        session.close()
        try: del futures 
        except: pass
        gc.collect() 
        executor.shutdown(wait=False)

    return sector_map, components_mapping

def fetch_single_dimension(session: requests.Session, fs_code, fid, po):
    """【官方方案二直连版】单维度前100名探测"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1, "pz": 100, "po": po, "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2, "invt": 2, "fid": fid,
        "fs": fs_code, "fields": "f12,f13,f14",
        "_cb": f"jQuery_{int(time.time() * 1000)}"
    }
    
    for attempt in range(2):
        try:
            resp = session.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                res = resp.json()
                if res and res.get('data') and res['data'].get('diff'):
                    items = res['data']['diff']
                    return list(items.values()) if isinstance(items, dict) else items
        except Exception:
            time.sleep(random.uniform(0.5, 1.5))
            
    return []

def scan_category_types(session: requests.Session, fs_code, label):
    """【官方方案二直连版】20维度正反序全向包抄"""
    print(f"[*] 正在对官方分类 [{label}] 执行 20 维度并发探测...")
    seen_codes = {}
    
    fids = [
        "f12", "f3", "f2", "f6", "f5", "f4", "f17", "f18", "f8", "f10",
        "f15", "f16", "f11", "f9", "f23", "f20", "f21", "f22", "f24", "f25"
    ]
    tasks = [(fid, po) for fid in fids for po in [1, 0]]

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(fetch_single_dimension, session, fs_code, fid, po): (fid, po) for fid, po in tasks}
        
        for future in concurrent.futures.as_completed(futures):
            try:
                items = future.result()
                if not items: continue
                for item in items:
                    code = item["f12"]
                    if code not in seen_codes:
                        seen_codes[code] = {
                            "code": code,
                            "market": item.get("f13", 90),
                            "name": item["f14"],
                            "type": label,
                        }
            except Exception:
                pass

    print(f"    [✓] [{label}] 扫描完成，捕获: {len(seen_codes)} 个官方唯一映射")
    return seen_codes

def fetch_baseinfo_type(session: requests.Session, code: str):
    """【官方方案二直连版】最后的详情页分类信息抢救"""
    params = {"_": int(time.time() * 1000)}
    data = get_json(session, BASEINFO_API.format(code=code), params=params)
    if not data: return None
    for key in ("Type111", "JYS", "Type182"):
        val = str(data.get(key, "")).strip()
        if val in BASEINFO_TYPE_MAP: return BASEINFO_TYPE_MAP[val]
    return None

def build_sector_catalog():
    """方案二构建完整板块目录"""
    # 1. 自下而上反推基础大名单
    universe_map, components_mapping = build_sector_universe()
    
    # 2. 官方三维度大目录扫描
    targets = {"Industry": "m:90 t:2", "Concept": "m:90 t:3", "Region": "m:90 t:1"}
    typed_map = {}
    
    session = create_session()
    try:
        for label, fs_code in targets.items():
            typed_map.update(scan_category_types(session, fs_code, label))
    except Exception as e:
        print(f"[-] 官方目录扫描受阻: {e}")

    # 3. 针对未分配类型的零散板块，启动详情页数据抢救
    missing_codes = [c for c in universe_map if c not in typed_map]
    if missing_codes:
        print(f"[*] 仍有 {len(missing_codes)} 个板块官方分类未就绪，启动 baseinfo 详情页抢救...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=BASEINFO_WORKERS) as executor:
            f_map = {executor.submit(fetch_baseinfo_type, session, c): c for c in missing_codes}
            for f in tqdm(concurrent.futures.as_completed(f_map), total=len(f_map), desc="官方分类最终补全"):
                code = f_map[f]
                try:
                    t = f.result()
                    if t:
                        typed_map[code] = {
                            "code": code,
                            "name": universe_map[code]["name"],
                            "type": t
                        }
                except: pass
                
    session.close()

    # 4. 最终汇总输出
    all_sectors = []
    regions = []
    industries = []
    concepts = []
    
    for code, info in universe_map.items():
        t = typed_map.get(code, {}).get("type", "Unknown")
        sector_obj = {
            "sid": f"90.{code}",
            "name": info["name"],
            "type": t
        }
        all_sectors.append(sector_obj)
        
        if t == "Region": regions.append(sector_obj)
        elif t == "Industry": industries.append(sector_obj)
        elif t == "Concept": concepts.append(sector_obj)
    
    # ==== 元数据落盘逻辑 ====
    os.makedirs("metadata", exist_ok=True)
    with open("metadata/components.json", "w", encoding="utf-8") as f:
        json.dump(components_mapping, f, ensure_ascii=False)
    with open("metadata/regions.json", "w", encoding="utf-8") as f:
        json.dump(regions, f, ensure_ascii=False)
    with open("metadata/industries.json", "w", encoding="utf-8") as f:
        json.dump(industries, f, ensure_ascii=False)
    with open("metadata/concepts.json", "w", encoding="utf-8") as f:
        json.dump(concepts, f, ensure_ascii=False)
        
    print(f"📦 [Metadata] 已成功截获并保存 {len(components_mapping)} 条成分股映射及三大分类名单。")
    
    counts = Counter(x["type"] for x in all_sectors)
    print(f"[+] 动态分类目录同步完毕 | 板块总数: {len(all_sectors)} | 分类统计: {dict(counts)}")
    return all_sectors
