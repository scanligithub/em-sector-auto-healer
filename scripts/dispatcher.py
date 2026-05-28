import json
import os
import sys

# 导入目录构建器
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.sector_catalog_builder import build_sector_catalog

def main():
    attempt = int(os.environ.get("ATTEMPT_COUNT", "1"))
    
    # 1. 数据源载入与初始化
    if attempt == 1:
        print("🌍 [Dispatcher] 第 1 轮启动：动态构建官方板块大名单...")
        raw_sectors = build_sector_catalog()
        # 初始化失败计数为 0
        sectors = []
        for item in raw_sectors:
            sectors.append({
                "sid": item["sid"],
                "name": item["name"],
                "type": item["type"],
                "fail_count": 0
            })
    else:
        print(f"🔄 [Dispatcher] 第 {attempt} 轮接力：读取上一轮遗留的 pending_list...")
        if not os.path.exists("pending_list.json"):
            print("❌ pending_list.json 不存在！异常中止。")
            sys.exit(1)
        with open("pending_list.json", "r", encoding="utf-8") as f:
            raw_sectors = json.load(f)
            
        # 【死信过滤器】自动剔除已经连续失败 3 次及以上的“毒药板块”
        sectors = [x for x in raw_sectors if x.get("fail_count", 0) < 3]
        dead_count = len(raw_sectors) - len(sectors)
        if dead_count > 0:
            print(f"🚨 [Dispatcher] 检测到并永久剔除已确诊的 [ 毒药/死信板块 ] 共 {dead_count} 个。")

    total = len(sectors)
    print(f"📊 [Dispatcher] 本轮待活跃下载板块数: {total}")
    
    if total == 0:
        print("✅ [Dispatcher] 待抓取列表为空，写入空矩阵。")
        with open("matrix.json", "w") as f:
            json.dump([], f)
        return

    # 2. 【核心创新：打散重叠填充至 400 算法】
    # 无论剩余多少个，只要小于 400 个，就循环自我复制拼接，强行凑齐 400 个送入 20 并发集群
    padded_sectors = []
    if total < 400:
        print(f"🧩 [Dispatcher] 板块数 {total} < 400，启动循环复制重叠机制填充至 400...")
        for i in range(400):
            # 使用 .copy() 确保字典引用独立
            item = sectors[i % total].copy()
            padded_sectors.append(item)
    else:
        padded_sectors = [x.copy() for x in sectors]

    # 3. 严格等分成 20 个 Chunk（每组正好 20 个）
    chunk_size = 20
    num_chunks = 20
    matrix_indices = []
    os.makedirs("chunks", exist_ok=True)
    
    for i in range(num_chunks):
        chunk_data = padded_sectors[i * chunk_size : (i + 1) * chunk_size]
        if chunk_data:
            with open(f"chunks/chunk_{i}.json", "w", encoding="utf-8") as f:
                json.dump(chunk_data, f, ensure_ascii=False)
            matrix_indices.append(i)
            
    print(f"🗂️ [Dispatcher] 任务已平摊至 {len(matrix_indices)} 个下载器节点，每个节点分配 20 个任务。")
    with open("matrix.json", "w") as f:
        json.dump(matrix_indices, f)

if __name__ == "__main__":
    main()
