import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor

def fetch_category(type_code, type_name):
    """拉取单一分类（地域、行业、概念）的板块名单"""
    url = f"https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:{type_code}+f:!50"
    try:
        res = requests.get(url, timeout=10).json()
        items = res.get("data", {}).get("diff", [])
        return [{"sid": str(i["f12"]), "name": str(i["f14"]), "type": type_name} for i in items if "f12" in i]
    except Exception as e:
        print(f"❌ 获取 {type_name} 失败: {e}")
        return []

def fetch_components(sector):
    """根据单个板块的 sid 拉取其包含的所有股票代码"""
    sid = sector["sid"]
    url = f"https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=b:{sid}+f:!50"
    try:
        res = requests.get(url, timeout=10).json()
        items = res.get("data", {}).get("diff", [])
        if not items:
            return []
        
        comps = []
        for i in items:
            # 自动添加沪深前缀 (f13: 1=SH, 0=SZ) 生成标准代码供量化使用 (如 SH600000)
            market = "SH" if i.get("f13") == 1 else "SZ"
            code = str(i.get("f12", ""))
            comps.append({"sector_id": sid, "stock_id": f"{market}{code}"})
        return comps
    except:
        return []

def build_sector_catalog():
    print("🌍 [Builder] 正在极速拉取三大类板块大名单...")
    regions = fetch_category(1, "地域")
    industries = fetch_category(2, "行业")
    concepts = fetch_category(3, "概念")
    
    all_sectors = regions + industries + concepts
    
    # 建立物理存放目录 (供 Artifact 上传使用)
    os.makedirs("metadata", exist_ok=True)
    
    # 1. 落地 3 个类型文件
    with open("metadata/regions.json", "w", encoding="utf-8") as f:
        json.dump(regions, f, ensure_ascii=False)
    with open("metadata/industries.json", "w", encoding="utf-8") as f:
        json.dump(industries, f, ensure_ascii=False)
    with open("metadata/concepts.json", "w", encoding="utf-8") as f:
        json.dump(concepts, f, ensure_ascii=False)
        
    print(f"⚡ [Builder] 开始并发获取 {len(all_sectors)} 个板块的内部成分股映射...")
    
    all_components = []
    # 2. 使用 20 线程并发裸抓成分股，速度极快（通常 3 秒内完成全部 1000+ 个板块）
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = executor.map(fetch_components, all_sectors)
        for res in results:
            all_components.extend(res)
            
    # 落地成分股映射关系（扁平化结构，完美适配 Polars 的 .join 操作）
    with open("metadata/components.json", "w", encoding="utf-8") as f:
        json.dump(all_components, f, ensure_ascii=False)
        
    print(f"✅ [Builder] 元数据提取完毕！成功生成映射关系 {len(all_components)} 条。")
    
    # 返回大名单给 Dispatcher 进行分发
    return all_sectors

if __name__ == "__main__":
    build_sector_catalog()
