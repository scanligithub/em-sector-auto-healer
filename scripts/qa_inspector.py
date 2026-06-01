import os
import json
from datetime import datetime

def format_size(bytes_size):
    return round(bytes_size / (1024 * 1024), 2)

def main():
    print("🔍 [QA Inspector] 正在执行全量数据深度质检与文档生成...")
    dataset_dir = "final_dataset"
    metadata_dir = os.path.join(dataset_dir, "metadata")
    os.makedirs(metadata_dir, exist_ok=True)
    
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
            first_date = klines[0].split(',')[0]
            last_date = klines[-1].split(',')[0]
            if first_date < min_date: min_date = first_date
            if last_date > max_date: max_date = last_date
            
        except Exception:
            total_anomalies += 1

    if min_date == "9999-99-99": min_date = "---"
    if max_date == "0000-00-00": max_date = "---"

    report_data.append({
        "name": "K线 JSON 集合<br>(各板块独立文件)",
        "rows": total_kline_rows,
        "targets": len(kline_files),
        "daterange": f"{min_date} ~ {max_date}",
        "size": format_size(total_kline_size),
        "anomalies": total_anomalies,
        "fields": "date, open, close, high, low, volume, amount, amplitude, pctChg, change, turnover"
    })

    # ==========================================
    # 4. 组装《数据质量报告与使用指南》长篇 Markdown
    # ==========================================
    
    md_content = "# 📊 板块数据质量深度质检与使用指南\n\n"
    md_content += f"> **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n\n"
    md_content += "## 📈 一、数据产物概览\n\n"
    md_content += "| 文件名 | 行数 | 标的数量 | 时间范围 | 大小 (MB) | 异常数 | 字段清单 |\n"
    md_content += "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n"
    for item in report_data:
        md_content += f"| **{item['name']}** | {item['rows']:,} | {item['targets']:,} | {item['daterange']} | {item['size']} | **{item['anomalies']}** | `{item['fields']}` |\n"

    # 用一个完整的三引号包裹后面所有的说明文档与代码示例，彻底杜绝编译错误
    md_content += """
## 📖 二、数据字典 (Data Dictionary)

### 1. K 线数据文件 (如 `90.BK1043.json`)
东方财富原始下发的数据格式中，`klines` 字段为一个由逗号分隔的字符串数组。切分后，各个位置对应的物理含义如下：
*   **0. date (日期)**: 交易日期 (如 `2024-03-01`)
*   **1. open (开盘价)**: 当日开盘指数
*   **2. close (收盘价)**: 当日收盘指数
*   **3. high (最高价)**: 当日最高指数
*   **4. low (最低价)**: 当日最低指数
*   **5. volume (成交量)**: 单位：手 (1手=100股)
*   **6. amount (成交额)**: 单位：元
*   **7. amplitude (振幅)**: 单位：%
*   **8. pctChg (涨跌幅)**: 单位：% (如 1.5 表示涨 1.5%)
*   **9. change (涨跌额)**: 指数变动绝对值
*   **10. turnover (换手率)**: 单位：%

*(注：系统抓取时设定了 `fqt=0` 不复权模式，以获取绝对真实的指数行情。)*

### 2. 板块成分股映射 (`metadata/components.json`)
关系型扁平数组，记录了全市场板块与股票的父子包含关系。
*   **`sector_id`**: 板块编码（带市场前缀，例如 `90.BK1043`，此编码与 K 线文件名完美对应）。
*   **`stock_id`**: 标准化个股代码（带市场前缀，例如 `SH600000`, `SZ000001`）。
*   **`stock_name`**: 个股中文简称，方便人类阅读和日志打印。

### 3. 三大分类名单 (`metadata/regions.json` 等)
*   **`sid`**: 板块唯一编码 (如 `90.BK1043`)。
*   **`name`**: 板块名称 (如 `银行业`)。
*   **`type`**: 板块类型标签 (`Region`, `Industry`, `Concept`)。

---

## 💻 三、量化投研使用指南 (Quick Start with Polars)

推荐使用 **Polars** 进行极速数据清洗和截面计算。以下提供将原始 JSON 转化为标准关系型 DataFrame 的标准范式。

### 1. 解析板块 K 线字符串
```python
import polars as pl
import json

# 读取单个板块数据
with open("90.BK1043.json", "r", encoding="utf-8") as f:
    data = json.load(f)

sector_name = data.get("name", "Unknown")
sector_id = data.get("code", "Unknown")

# 1. 拆解东财逗号分隔的字符串
rows = [x.split(',') for x in data["klines"]]
cols = ["date", "open", "close", "high", "low", "volume", "amount", "amplitude", "pctChg", "change", "turnover"]

# 2. 生成 Polars DataFrame，并极速转换数据类型
df = pl.DataFrame(rows, schema=cols, orient="row").with_columns([
    pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"),
    pl.col("open").cast(pl.Float64),
    pl.col("close").cast(pl.Float64),
    pl.col("high").cast(pl.Float64),
    pl.col("low").cast(pl.Float64),
    pl.col("volume").cast(pl.Float64),
    pl.col("amount").cast(pl.Float64),
    pl.col("pctChg").cast(pl.Float64)
])

# 3. 补充板块维度信息
df = df.with_columns([
    pl.lit(sector_id).alias("sector_id"),
    pl.lit(sector_name).alias("sector_name")
])

print(df.head())
