import os
import json
from datetime import datetime

def format_size(bytes_size):
    return round(bytes_size / (1024 * 1024), 2)

def main():
    print("🔍 [QA Inspector] 正在执行全量数据深度质检...")
    dataset_dir = "final_dataset"
    metadata_dir = os.path.join(dataset_dir, "metadata")
    
    # 统计容器
    report_data = []

    # 1. 质检成分股映射表
    comp_path = os.path.join(metadata_dir, "components.json")
    if os.path.exists(comp_path):
        size = os.path.getsize(comp_path)
        with open(comp_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        rows = len(data)
        unique_stocks = len(set(x.get('stock_id') for x in data if x.get('stock_id')))
        anomalies = sum(1 for x in data if not x.get('stock_id') or not x.get('sector_id'))
        fields = ", ".join(data[0].keys()) if rows > 0 else ""
        report_data.append({
            "name": "components.json<br>(成分股映射)",
            "rows": rows,
            "targets": unique_stocks,
            "daterange": "---",
            "size": format_size(size),
            "anomalies": anomalies,
            "fields": fields
        })
        
    # 2. 质检三大分类大名单
    for cat in ["regions.json", "industries.json", "concepts.json"]:
        cat_path = os.path.join(metadata_dir, cat)
        if os.path.exists(cat_path):
            size = os.path.getsize(cat_path)
            with open(cat_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            rows = len(data)
            anomalies = sum(1 for x in data if not x.get('sid') or not x.get('name'))
            fields = ", ".join(data[0].keys()) if rows > 0 else ""
            
            # 中文别名转换
            alias = "地域" if "regions" in cat else "行业" if "industries" in cat else "概念"
            report_data.append({
                "name": f"{cat}<br>({alias}分类表)",
                "rows": rows,
                "targets": rows,
                "daterange": "---",
                "size": format_size(size),
                "anomalies": anomalies,
                "fields": fields
            })

    # 3. 质检 K 线分块数据
    kline_files = [f for f in os.listdir(dataset_dir) if f.endswith('.json')]
    total_kline_rows = 0
    total_kline_size = 0
    total_anomalies = 0
    min_date = "9999-99-99"
    max_date = "0000-00-00"
    
    for kf in kline_files:
        kf_path = os.path.join(dataset_dir, kf)
        total_kline_size += os.path.getsize(kf_path)
        try:
            with open(kf_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not data or 'klines' not in data:
                total_anomalies += 1
                continue
                
            klines = data['klines']
            if not klines:
                total_anomalies += 1
                continue
                
            total_kline_rows += len(klines)
            # 解析时间跨度 (东财格式首项即日期，如 "2023-01-01,10.0,...")
            first_date = klines[0].split(',')[0]
            last_date = klines[-1].split(',')[0]
            if first_date < min_date: min_date = first_date
            if last_date > max_date: max_date = last_date
            
        except Exception:
            total_anomalies += 1

    if min_date == "9999-99-99": min_date = "---"
    if max_date == "0000-00-00": max_date = "---"

    report_data.append({
        "name": "sector_klines.zip<br>(全量板块K线集合)",
        "rows": total_kline_rows,
        "targets": len(kline_files),
        "daterange": f"{min_date} ~ {max_date}",
        "size": format_size(total_kline_size),
        "anomalies": total_anomalies,
        # 根据东财接口 fields 拼装的标准定义
        "fields": "date, open, close, high, low, volume, amount, amplitude, pctChg, change, turnover"
    })

    # 4. 生成高度还原截图风格的 Markdown 看板
    md_content = "## 📊 数据质量深度质检报告\n\n"
    md_content += "### 📈 数据产物概览\n\n"
    md_content += "| 文件名 | 行数 | 标的数量 | 时间范围 | 大小 (MB) | 异常数 | 字段清单 |\n"
    md_content += "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n"
    
    for item in report_data:
        md_content += f"| **{item['name']}** | {item['rows']:,} | {item['targets']:,} | {item['daterange']} | {item['size']} | **{item['anomalies']}** | `{item['fields']}` |\n"

    # 在控制台打印，方便调试看日志
    print("\n" + md_content)
    
    # 核心：将 Markdown 写入 GitHub Actions 特有的全局环境变量文件，直接挂载到 Web UI 上！
    step_summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary_file:
        with open(step_summary_file, "a", encoding="utf-8") as f:
            f.write(md_content + "\n")
        print("✅ [QA Inspector] 质检看板已成功挂载至 GitHub Summary 墙！")

if __name__ == "__main__":
    main()
