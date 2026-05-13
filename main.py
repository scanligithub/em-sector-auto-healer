import asyncio
import sys
import os
from loguru import logger
from core.muscle_engine import MuscleEngine
from dotenv import load_dotenv

load_dotenv()

async def main():
    logger.info("🔥 [System] 工业级板块数据中台启动 | Scrapling + WorkerPool + DuckDB")
    
    engine = MuscleEngine()
    
    # 0. 构建信任链 (仅执行一次，建立 Browser Session 态)
    await engine.build_trust_chain()
    
    # 1. 扫描活跃板块 (高韧性分布式扫描)
    sector_list = await engine.fetch_dynamic_sector_list()
    
    if sector_list:
        # 2. 增量拉取数据 (池化路由 + 增量识别)
        await engine.fetch_all_sectors(sector_list)
        
        # 3. 输出汇总
        err_rate = (engine.stats['errors'] / engine.stats['total'] * 100) if engine.stats['total'] > 0 else 0
        logger.info(f"📊 任务统计: 总请求 {engine.stats['total']} | 错误率 {err_rate:.1f}% | 状态码分布: {engine.stats['codes']}")
    else:
        logger.error("❌ 目录扫描全线失败，任务终止")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
