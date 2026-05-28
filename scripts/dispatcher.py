import json
import math
import sys
import os

# 导入目录构建器
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.sector_catalog_builder import build_sector_catalog

def main():
    attempt = int(os.environ.get("ATTEMPT_COUNT", "1"))
    
    # 1. 判定数据来源
    if attempt == 1:
        print("🌍 [Dispatcher] 检测到第 1 轮启动，开始构建全市场最新板块目录...")
        sectors = build_sector_catalog()
    else:
        print(f"🔄 [Dispatcher] 检测到第 {attempt} 轮接力，读取上一轮遗留的 pending_list...")
        if not os.path.exists("pending_list.json"):
            print("❌ pending_list.json 不存在！异常中止。")
            sys.exit(1)
        with open("pending_list.json", "r", encoding="utf-8") as f:
            sectors = json.load(f)
            
    total = len(sectors)
    print(f"📊 [Dispatcher] 待抓取板块总数: {total}")
    
    if total == 0:
        print("✅ [Dispatcher] 待抓取列表为空，写入空矩阵。")
        with open("matrix.json", "w") as f:
            json.dump([], f)
        return

    # 2. 分块逻辑 (最多20个组，每组最少20个)
    # 如果总数小于 20，就只分 1 个组
    chunk_size = 20
    max_jobs = 20
    num_chunks = min(max_jobs, math.ceil(total / chunk_size))
    
    # 动态平摊：让每个 job 数量尽可能均匀
    actual_chunk_size = math.ceil(total / num_chunks)
    
    matrix_indices = []
    os.makedirs("chunks", exist_ok=True)
    
    for i in range(num_chunks):
        chunk_data = sectors[i * actual_chunk_size : (i + 1) * actual_chunk_size]
        if chunk_data:
            with open(f"chunks/chunk_{i}.json", "w", encoding="utf-8") as f:
                json.dump(chunk_data, f, ensure_ascii=False)
            matrix_indices.append(i)
            
    print(f"🗂️ [Dispatcher] 成功将任务划分为 {len(matrix_indices)} 个并发组。")
    with open("matrix.json", "w") as f:
        json.dump(matrix_indices, f)

if __name__ == "__main__":
    main()
