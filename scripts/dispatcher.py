import json
import os
import sys
import math

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
            sectors = json.load(f)
            
        print(f"📝 本轮已完整加载上一轮遗留的 {len(sectors)} 个板块，无任何剔除过滤。")

    total = len(sectors)
    print(f"📊 [Dispatcher] 本轮待活跃下载板块数: {total}")
    
    if total == 0:
        print("✅ [Dispatcher] 待抓取列表为空，写入空矩阵。")
        with open("matrix.json", "w") as f:
            json.dump([], f)
        return

    # 创建分块数据存放目录
    os.makedirs("chunks", exist_ok=True)
    num_chunks = 20
    matrix_indices = []

    # 2. 核心分发策略：区分正常期与长尾期
    if total >= 400:
        # 【正常期】：动态计算步长，彻底解决 1012 个板块切片截断的 Bug
        chunk_size = math.ceil(total / num_chunks)
        print(f"📦 [Dispatcher] 处于正常期分发，动态 Chunk 大小: {chunk_size}，确保全量覆盖。")
        
        # 顺序平摊分块，无任何截断遗漏
        padded_sectors = [x.copy() for x in sectors]
        for i in range(num_chunks):
            chunk_data = padded_sectors[i * chunk_size : (i + 1) * chunk_size]
            if chunk_data:
                with open(f"chunks/chunk_{i}.json", "w", encoding="utf-8") as f:
                    json.dump(chunk_data, f, ensure_ascii=False)
                matrix_indices.append(i)
    else:
        # 【长尾期】：应用“动态收缩队列”与“轮转偏移分发”策略
        # 设定重叠因子为 5（即每个标的在 5 个不同的 IP 节点上同时并发推进，撞大运尝试）
        overlap_factor = 5
        
        # 动态收缩每个 Job 的队列长度，防止队列过长导致尾部被“提前熔断”饿死
        chunk_size = math.ceil((total * overlap_factor) / num_chunks)
        chunk_size = max(1, chunk_size)  # 确保队列最少有 1 个任务
        
        # 计算轮转偏移步长，保证 20 个 Job 队列的头部标的完全错开
        step = max(1, math.ceil(total / num_chunks))
        
        print(f"🧩 [Dispatcher] 处于长尾期分发。剩余板块: {total}，动态收缩队列长度: {chunk_size}，偏移步长: {step}")
        
        for i in range(num_chunks):
            # 基于当前节点索引计算独特的起始指针
            start_idx = (i * step) % total
            
            # 使用环形轮转算法装填任务
            chunk_data = []
            for j in range(chunk_size):
                idx = (start_idx + j) % total
                chunk_data.append(sectors[idx].copy())
                
            if chunk_data:
                with open(f"chunks/chunk_{i}.json", "w", encoding="utf-8") as f:
                    json.dump(chunk_data, f, ensure_ascii=False)
                matrix_indices.append(i)
            
    print(f"🗂️ [Dispatcher] 任务已均匀分发至 {len(matrix_indices)} 个下载器节点。")
    with open("matrix.json", "w") as f:
        json.dump(matrix_indices, f)

if __name__ == "__main__":
    main()
