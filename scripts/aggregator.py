import glob
import json
import os

def main():
    failed_files = glob.glob("failed_list_*.json")
    all_failed = []
    
    for f in failed_files:
        with open(f, "r", encoding="utf-8") as file:
            all_failed.extend(json.load(file))
            
    # 去重处理（理论上不需要，保险起见）
    unique_failed = {item['sid']: item for item in all_failed}.values()
    final_list = list(unique_failed)
    
    # 写入最终的未完成大名单，作为 Artifact 传给下一轮的 Dispatcher
    with open("pending_list.json", "w", encoding="utf-8") as f:
        json.dump(final_list, f, ensure_ascii=False)
        
    print(f"COUNT={len(final_list)}")

if __name__ == "__main__":
    main()
