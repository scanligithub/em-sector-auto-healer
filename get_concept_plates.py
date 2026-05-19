import requests
import time
import random
import pandas as pd
from datetime import datetime

def fetch_plates(fs_code, plate_type, name):
    """抓取指定类型的板块"""
    subdomains = [
        "push2", "12.push2", "13.push2", "20.push2", "27.push2", 
        "56.push2", "38.push2", "48.push2", "79.push2", "25.push2", 
        "62.push2", "67.push2", "80.push2", "40.push2"
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/center/boardlist.html",
        "Accept": "application/json, text/plain, */*",
    }
    
    session = requests.Session()
    all_data = []
    page = 1
    
    print(f"🚀 开始抓取 【{name}】...\n")
    
    while page <= 8:
        for attempt in range(5):
            random.shuffle(subdomains)
            for domain in subdomains:
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
                
                try:
                    resp = session.get(url, params=params, headers=headers, timeout=20)
                    if resp.status_code == 200:
                        data = resp.json()
                        items = data.get("data", {}).get("diff", [])
                        total = data.get("data", {}).get("total", 0)
                        
                        print(f"  第 {page} 页 | {domain} | 获取 {len(items)} 条 (总计 {total})")
                        
                        for item in items:
                            secid = f"{item.get('f13', '')}.{item.get('f12', '')}"
                            all_data.append({
                                "secid": secid,
                                "code": item.get("f12"),
                                "name": item.get("f14"),
                                "type": plate_type,
                                "type_name": name,
                                "price": item.get("f2"),
                                "change_percent": item.get("f3"),
                            })
                        
                        if len(items) < 90:
                            print(f"✅ 【{name}】抓取完成！共 {len(all_data)} 个\n")
                            return all_data
                        break
                except:
                    continue
            
            else:
                wait = random.uniform(2.5, 5.5)
                print(f"  第 {page} 页失败，等待 {wait:.1f}秒后重试...")
                time.sleep(wait)
                continue
            break
        
        page += 1
        time.sleep(random.uniform(1.0, 2.0))
    
    return all_data


def main():
    print("=== 开始抓取东方财富全板块 ===\n")
    
    all_plates = []
    
    # 1. 概念板块
    all_plates.extend(fetch_plates("m:90+t:3", "concept", "概念板块"))
    
    # 2. 行业板块
    all_plates.extend(fetch_plates("m:90+t:2", "industry", "行业板块"))
    
    # 3. 地域板块
    all_plates.extend(fetch_plates("m:90+t:1", "region", "地域板块"))
    
    # 转为 DataFrame 并保存
    df = pd.DataFrame(all_plates)
    
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    
    # 保存不同文件
    df.to_csv(f"全板块列表_{ts}.csv", index=False, encoding="utf-8-sig")
    df.to_excel(f"全板块列表_{ts}.xlsx", index=False)
    
    # 保存最新版本
    df.to_csv("全板块列表_最新.csv", index=False, encoding="utf-8-sig")
    df.to_excel("全板块列表_最新.xlsx", index=False)
    
    # 按类型统计
    print("="*60)
    print("🎉 抓取完成！统计结果：")
    print(df.groupby('type_name').size())
    print(f"总计板块数量: {len(df)} 个")
    print(f"文件已保存：全板块列表_{ts}.csv / .xlsx")
    
    # 预览
    print("\n前10个示例：")
    print(df.head(10)[["secid", "name", "type_name", "change_percent"]].to_string(index=False))


if __name__ == "__main__":
    main()
