import asyncio
import sys
from loguru import logger
from core.brain_engine import BrainEngine
from core.muscle_engine import MuscleEngine

async def main():
    logger.info("🚀 [System] 终极形态：浏览器被动流量劫持系统启动")
    
    brain = BrainEngine()
    try:
        # 1. 构建无懈可击的底层信誉环境
        context, main_page = await brain.build_trust_context()
        
        # 2. 移交环境给劫持引擎
        muscle = MuscleEngine(context, main_page)
        
        # 3. 获取全市场目录
        dynamic_sectors = await muscle.fetch_dynamic_sector_list()
        
        if dynamic_sectors:
            logger.info(f"🟢 [System] 准备对 {len(dynamic_sectors)} 个板块执行流量劫持...")
            # 执行全量劫持
            await muscle.hijack_all_sectors(dynamic_sectors)
        else:
            logger.error("❌ [System] 目录扫描失败，任务终止。")
            
    except Exception as e:
        logger.exception(f"🔥 [System] 系统严重异常: {e}")
    finally:
        await brain.close()
        logger.info("🛑 [System] 浏览器母体已安全销毁。")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
