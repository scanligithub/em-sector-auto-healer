import requests
import time
import random
import pandas as pd
from datetime import datetime

def get_all_concept_plates():
    subdomains = [
        "push2", "12.push2", "13.push2", "20.push2", "27.push2", 
        "56.push2", "38.push2", "48.push2", "79.push2", "25.push2", 
        "62.push2", "67.push2", "80.push2", "40.push2"
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/center/boardlist.html#boards2-90",
        "Accept": "application/json, text/plain, */*",
    }
    
    session = requests.Session()
    all_data = []
    page = 1
    max_pages = 8
    
    print("🚀 开始在 GitHub Actions 中抓取概念板块...\n")
    
    while page <= max_pages:
        page_success = False
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
                    "fs": "m:90+t:3",
                    "fields": "f12,f13,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205",
                    "_": int(time.time() * 1000)
                }
                
                try:
                    resp = session.get(url, params=params, headers=headers, timeout=20)
                    if resp.status_code == 200:
                        data = resp.json()
                        items = data.get("data", {}).get("diff", [])
                        total = data.get("data", {}).get("total", 0)
                        
                        print(f"第 {page} 页 | {domain} | 获取 {len(items)} 条 (总计 {total})")
                        
                        all_data.extend(items)
                        
                        if len(items) < 90:
                            print("✅ 抓取完成！")
                            return all_data
                        
                        page_success = True
                        break
                except:
                    continue
            
            if page_success:
                break
            
            wait = random.uniform(2.5, 5.5)
            print(f"第 {page} 页失败，等待 {wait:.1f}秒后重试...")
            time.sleep(wait)
        
        page += 1
        time.sleep(random.uniform(1.0, 2.2))
    
    return all_data


if __name__ == "__main__":
    all_items = get_all_concept_plates()
    
    if len(all_items) > 400:
        result = []
        seen = set()
        for item in all_items:
            secid = f"{item.get('f13', '')}.{item.get('f12', '')}"
            if secid in seen:
                continue
            seen.add(secid)
            result.append({
                "secid": secid,
                "code": item.get("f12"),
                "name": item.get("f14"),
                "price": item.get("f2"),
                "change_percent": item.get("f3"),
                "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        df = pd.DataFrame(result)
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"概念板块列表_{ts}"
        
        df.to_csv(f"{filename}.csv", index=False, encoding="utf-8-sig")
        df.to_excel(f"{filename}.xlsx", index=False)
        
        # 同时保存最新版本（方便直接使用）
        df.to_csv("概念板块列表_最新.csv", index=False, encoding="utf-8-sig")
        df.to_excel("概念板块列表_最新.xlsx", index=False)
        
        print(f"\n🎉 保存成功！共 {len(df)} 个概念板块")
        print(f"文件名：{filename}.csv")
    else:
        print("❌ 抓取数量不足")
