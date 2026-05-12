import asyncio
import sys
from loguru import logger
from core.brain_engine import BrainEngine
from core.muscle_engine import MuscleEngine

async def main():
    logger.info("🚀 [System] 终极工业反风控引擎启动 (Exponential Backoff)")
    
    brain = BrainEngine()
    try:
        trust_context = await brain.steal_trust_context()
        muscle = MuscleEngine(trust_context)
        dynamic_sectors = await muscle.fetch_dynamic_sector_list()
        
        if dynamic_sectors:
            logger.info(f"🟢 [System] 对 {len(dynamic_sectors)} 个板块执行工业级并发抓取...")
            await muscle.fetch_all_sectors(dynamic_sectors)
        else:
            logger.error("❌ [System] 目录扫描失败，任务终止。")
            
    except Exception as e:
        logger.exception(f"🔥 [System] 系统严重异常: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
