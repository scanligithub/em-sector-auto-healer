import asyncio
import sys
import baostock as bs
from datetime import datetime, timedelta
from loguru import logger
from core.brain_engine import BrainEngine
from core.muscle_engine import MuscleEngine

def get_seeds_from_baostock():
    """从 Baostock 稳健获取个股种子 (集成自 stockA)"""
    logger.info("📋 [BaoStock] 正在登录并提取全量个股种子...")
    bs.login()
    stocks = []
    try:
        # 获取最近一个交易日的股票列表
        for i in range(10):
            target_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            rs = bs.query_all_stock(day=target_date)
            if rs.error_code == '0' and len(rs.data) > 0:
                while rs.next():
                    row = rs.get_row_data()
                    if row[0].startswith(("sh.6", "sz.0", "sz.3")):
                        stocks.append(row[0])
                if stocks: break
        return stocks
    finally:
        bs.logout()

async def main():
    logger.info("🚀 [System] 融合版引擎启动 (BaoStock Seeds Mode)")
    
    # 1. 种子阶段：必须从 BaoStock 获取
    baostock_seeds = get_seeds_from_baostock()
    if not baostock_seeds:
        logger.error("❌ 无法从 BaoStock 获取个股种子，任务终止。")
        return

    # 2. 凭证阶段：由 BrainEngine 幽灵窃取
    brain = BrainEngine()
    trust_context = await brain.steal_trust_context()
    
    # 3. 引擎初始化
    muscle = MuscleEngine(trust_context)
    
    # 4. 发现阶段：利用种子反推板块及分类 (stockA 逻辑)
    sector_df = await muscle.discover_sectors_via_seeds(baostock_seeds)
    
    # 5. 下载阶段：工业级全量采集 (Healer 强度)
    if not sector_df.is_empty():
        await muscle.fetch_all_sectors(sector_df)
    else:
        logger.error("❌ 板块探测失败，请检查 Worker 连通性。")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
