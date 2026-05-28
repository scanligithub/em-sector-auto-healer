import glob
import json
import os

def main():
    # 1. 扫描当前工作流合并后的【真实物理成功数据集】
    success_sids = set()
    if os.path.exists("success_data_merged"):
        for fname in os.listdir("success_data_merged"):
            if fname.endswith(".json"):
                sid = fname.replace(".json", "")
                success_sids.add(sid)
                
    print(f"📊 [Aggregator] 经物理文件去重校验，本轮累计成功落地 {len(success_sids)} 个板块。")

    # 2. 扫描所有节点呈报的失败大名单
    failed_files = glob.glob("failed_list_*.json")
    merged_failed = {}
    
    for f in failed_files:
        with open(f, "r", encoding="utf-8") as file:
            chunk_failed = json.load(file)
            for item in chunk_failed:
                sid = item["sid"]
                # 核心：多节点重叠反馈时，继承并取最大的 fail_count（保障死信识别精确度）
                if sid not in merged_failed:
                    merged_failed[sid] = item
                else:
                    if item["fail_count"] > merged_failed[sid]["fail_count"]:
                        merged_failed[sid]["fail_count"] = item["fail_count"]

    # 3. 终极交叉对比剔除（即使被重叠退回，只要在任何一个节点落地成功，立刻从失败名单中永远抹去）
    final_pending = []
    for sid, item in merged_failed.items():
        if sid in success_sids:
            continue  # 已抓取成功，放行
        final_pending.append(item)

    # 4. 写入 pending_list.json 送往下一轮
    with open("pending_list.json", "w", encoding="utf-8") as f:
        json.dump(final_pending, f, ensure_ascii=False)
        
    print(f"COUNT={len(final_pending)}")

if __name__ == "__main__":
    main()
