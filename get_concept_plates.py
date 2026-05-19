import requests
import time
import random
import pandas as pd
from datetime import datetime

def fetch_plates(fs_code, plate_type, name, max_pages=8):
    subdomains = ["push2", "12.push2", "13.push2", "20.push2", "27.push2", 
                  "56.push2", "38.push2", "48.push2"]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/center/boardlist.html",
    }
    
    all_data = []
    page = 1
    
    print(f"🚀 正在抓取 【{name}】...")
    
    while page <= max_pages:
        random.shuffle(subdomains)
        for domain in subdomains:
            try:
                url = f"https://{domain}.eastmoney.com/api/qt/clist/get"
                params = {
                    "pn": page,
                    "pz": 100,
                    "po": 1,
                    "np": 1,
                    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    "fltt": 2,
                    "invt": 2,
                    "fid": "f3",
                    "fs": fs_code,
                    "fields": "f12,f13,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205",
                    "_": int(time.time() * 1000)
                }
                
                resp = requests.get(url, params=params, headers=headers, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get("data", {}).get("diff", [])
                    total = data.get("data", {}).get("total", 0)
                    
                    print(f"  【{name}】 第 {page} 页 → {len(items)} 条 (总计 {total})")
                    
                    for item in items:
                        secid = f"{item.get('f13', '')}.{item.get('f12', '')}"
                        all_data.append({
                            "secid": secid,
                            "code": item.get("f12"),
                            "name": item.get("f14"),
                            "type": plate_type,
                            "type_name": name,
                            "change_percent": item.get("f3"),
                        })
                    
                    if len(items) < 90 or len(all_data) >= total > 0:
                        print(f"✅ 【{name}】抓取完成！共 {len(all_data)} 个\n")
                        return all_data
                    break
            except Exception as e:
                continue
        
        page += 1
        time.sleep(random.uniform(0.8, 1.6))
    
    print(f"⚠️ 【{name}】抓取结束（可能不完整）共 {len(all_data)} 个\n")
    return all_data


def main():
    print("=== 开始抓取东方财富全板块（最终稳定版）===\n")
    start_time = time.time()
    
    all_plates = []
    
    # 依次抓取三种板块
    concept_data = fetch_plates("m:90+t:3", "concept", "概念板块", max_pages=8)
    all_plates.extend(concept_data)
    
    industry_data = fetch_plates("m:90+t:2", "industry", "行业板块", max_pages=5)
    all_plates.extend(industry_data)
    
    region_data = fetch_plates("m:90+t:1", "region", "地域板块", max_pages=3)
    all_plates.extend(region_data)
    
    df = pd.DataFrame(all_plates)
    
    # 防止空数据导致报错
    if df.empty:
        print("❌ 未抓取到任何数据！")
        return
    
    df = df.drop_duplicates(subset=['secid']).reset_index(drop=True)
    
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    df.to_csv(f"全板块列表_{ts}.csv", index=False, encoding="utf-8-sig")
    df.to_csv("全板块列表_最新.csv", index=False, encoding="utf-8-sig")
    
    print("="*70)
    print("🎉 抓取全部完成！最终统计：")
    
    if 'type_name' in df.columns:
        print(df.groupby('type_name').size())
    else:
        print("（无 type_name 列）")
    
    print(f"总计唯一板块: {len(df)} 个")
    print(f"总耗时: {time.time() - start_time:.1f} 秒")
    
    # 前10条预览
    print("\n前10条预览：")
    print(df.head(10)[["secid", "name", "type_name", "change_percent"]])


if __name__ == "__main__":
    main()
