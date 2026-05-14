import asyncio
import sys
from loguru import logger
from core.muscle_engine import MuscleEngine
from dotenv import load_dotenv

load_dotenv()

async def main():
    logger.info("🚀 [Nitro V5.0] 极速异步队列版启动")
    engine = MuscleEngine()
    
    await engine.build_trust_chain()
    
    sector_list = await engine.get_active_sectors(force_reconcile=True)
    
    if sector_list:
        await engine.sync_all_klines(sector_list)
        
        total = engine.stats.get('total_tasks', 0)
        failed = engine.stats.get('failed_tasks', 0)
        if total > 0:
            err_rate = (failed / total * 100)
            logger.info(f"📊 任务统计: 总任务 {total} | 业务失败数 {failed} | 失败率 {err_rate:.1f}% | 状态码分布: {engine.stats['codes']}")
    else:
        logger.error("❌ 未能获取板块名录")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
