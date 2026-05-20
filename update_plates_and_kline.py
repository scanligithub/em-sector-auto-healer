import requests
import time
import random
import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm

# ==================== 配置 ====================
SAVE_DIR = "板块日K线数据"
os.makedirs(SAVE_DIR, exist_ok=True)

def fetch_all_plates():
    """抓取全部板块列表"""
    print("🚀 开始抓取板块列表...")
    types = [
        ("m:90+t:3", "concept", "概念板块"),
        ("m:90+t:2", "industry", "行业板块"),
        ("m:90+t:1", "region", "地域板块")
    ]
    
    all_data = []
    for fs_code, plate_type, name in types:
        subdomains = ["push2", "12.push2", "13.push2", "20.push2"]
        for domain in subdomains:
            try:
                url = f"https://{domain}.eastmoney.com/api/qt/clist/get"
                params = {
                    "pn": 1, "pz": 500, "po": 1, "np": 1,
                    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    "fltt": 2, "invt": 2, "fid": "f3",
                    "fs": fs_code,
                    "fields": "f12,f13,f14,f3",
                    "_": int(time.time()*1000)
                }
                headers = {"User-Agent": "Mozilla/5.0"}
                
                resp = requests.get(url, params=params, headers=headers, timeout=15)
                if resp.status_code == 200:
                    items = resp.json().get("data", {}).get("diff", [])
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
                    print(f"✅ 【{name}】抓取完成: {len(items)} 个")
                    break
            except:
                continue
        time.sleep(1)
    
    df = pd.DataFrame(all_data)
    df = df.drop_duplicates(subset=['secid']).reset_index(drop=True)
    df.to_csv("全板块列表_最新.csv", index=False, encoding="utf-8-sig")
    print(f"🎉 板块列表总计: {len(df)} 个")
    return df


def download_kline(secid, name):
    """下载单个板块日K线"""
    subdomains = ["push2his", "12.push2his", "13.push2his", "20.push2his", "27.push2his"]
    url = f"https://{random.choice(subdomains)}.eastmoney.com/api/qt/stock/kline/get"
    
    params = {
        "secid": secid,
        "klt": "101",
        "fqt": "1",
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "beg": "19900101",
        "end": "20500101",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "rtntype": 6
    }
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
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
    return 0


def main():
    start_time = time.time()
    
    # 第一步：抓取板块列表
    df_plates = fetch_all_plates()
    
    # 第二步：下载K线
    print("\n🚀 开始下载全部板块日K线...")
    success = 0
    for idx, row in tqdm(df_plates.iterrows(), total=len(df_plates), desc="K线下载"):
        count = download_kline(row["secid"], row["name"])
        if count > 0:
            success += 1
        time.sleep(random.uniform(2.2, 4.0))   # 重要：限速
    
    print("\n" + "="*70)
    print("🎉 全部任务完成！")
    print(f"板块列表: {len(df_plates)} 个")
    print(f"K线成功下载: {success} 个")
    print(f"总耗时: {(time.time() - start_time)/60:.1f} 分钟")


if __name__ == "__main__":
    main()
