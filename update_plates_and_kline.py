import requests
import time
import random
import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm

SAVE_DIR = "板块日K线数据"
os.makedirs(SAVE_DIR, exist_ok=True)

def fetch_plates_with_pagination(fs_code, plate_type, name):
    """支持分页抓取板块"""
    print(f"🚀 开始抓取 【{name}】...")
    subdomains = ["push2", "12.push2", "13.push2", "20.push2", "27.push2", "56.push2"]
    all_data = []
    page = 1
    max_pages = 10
    
    while page <= max_pages:
        success = False
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
                    "fields": "f12,f13,f14,f3",
                    "_": int(time.time() * 1000)
                }
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                
                resp = requests.get(url, params=params, headers=headers, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get("data", {}).get("diff", [])
                    total = data.get("data", {}).get("total", 0)
                    
                    print(f"  【{name}】 第 {page} 页 → {len(items)} 条 (总计 {total})")
                    
                    for item in items:
                        secid = f"{item.get('f13')}.{item.get('f12')}"
                        all_data.append({
                            "secid": secid,
                            "code": item.get("f12"),
                            "name": item.get("f14"),
                            "type": plate_type,
                            "type_name": name,
                            "change_percent": item.get("f3")
                        })
                    
                    if len(items) < 90:
                        return all_data
                    success = True
                    break
            except:
                continue
        
        if not success:
            print(f"  【{name}】 第 {page} 页失败")
        
        page += 1
        time.sleep(random.uniform(1, 2))
    
    return all_data


def fetch_all_plates():
    """抓取全部板块"""
    print("=== 开始抓取板块列表 ===\n")
    
    all_data = []
    # 概念板块（最多）
    all_data.extend(fetch_plates_with_pagination("m:90+t:3", "concept", "概念板块"))
    # 行业板块
    all_data.extend(fetch_plates_with_pagination("m:90+t:2", "industry", "行业板块"))
    # 地域板块
    all_data.extend(fetch_plates_with_pagination("m:90+t:1", "region", "地域板块"))
    
    df = pd.DataFrame(all_data)
    df = df.drop_duplicates(subset=['secid']).reset_index(drop=True)
    
    df.to_csv("全板块列表_最新.csv", index=False, encoding="utf-8-sig")
    print(f"\n🎉 板块列表抓取完成！总计 {len(df)} 个")
    print(df.groupby('type_name').size())
    return df


# ==================== K线下载部分（保持不变） ====================
def download_kline(secid, name):
    subdomains = ["push2his", "12.push2his", "13.push2his", "20.push2his"]
    url = f"https://{random.choice(subdomains)}.eastmoney.com/api/qt/stock/kline/get"
    
    params = {
        "secid": secid, "klt": "101", "fqt": "1",
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "beg": "19900101", "end": "20500101",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b", "rtntype": 6
    }
    
    headers = {"User-Agent": "Mozilla/5.0"}
    
    for _ in range(3):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=25)
            if resp.status_code == 200:
                klines = resp.json().get("data", {}).get("klines", [])
                if klines:
                    df = pd.DataFrame([line.split(",") for line in klines])
                    df.columns = ["日期","开盘","收盘","最高","最低","成交量","成交额","振幅","涨跌幅","涨跌额","换手率"]
                    safe_name = str(name).replace("/", "_").replace("\\", "_")[:40]
                    df.to_csv(f"{SAVE_DIR}/{secid.replace('.', '_')}_{safe_name}.csv", index=False, encoding="utf-8-sig")
                    return len(klines)
        except:
            pass
        time.sleep(2)
    return 0


def main():
    start_time = time.time()
    
    df_plates = fetch_all_plates()
    
    print("\n🚀 开始下载全部板块日K线...")
    success = 0
    for idx, row in tqdm(df_plates.iterrows(), total=len(df_plates), desc="K线下载"):
        count = download_kline(row["secid"], row["name"])
        if count > 0:
            success += 1
        time.sleep(random.uniform(2.0, 4.0))
    
    print("\n🎉 全部任务完成！")
    print(f"板块列表: {len(df_plates)} 个")
    print(f"K线成功下载: {success} 个")
    print(f"总耗时: {(time.time() - start_time)/60:.1f} 分钟")


if __name__ == "__main__":
    main()
