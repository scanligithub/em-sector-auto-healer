import asyncio
import sys
from loguru import logger
from core.muscle_engine import MuscleEngine

async def main():
    logger.info("🚀 [System] Zero-DOM 极速纯享版引擎启动 | 纯 curl_cffi + CF Worker 驱动")
    
    try:
        # 直接实例化 MuscleEngine，彻底抛弃 BrainEngine 和前置信任链窃取
        muscle = MuscleEngine()
        
        # 1. 扫描目录 (动态分页 + 静态兜底)
        dynamic_sectors = await muscle.fetch_dynamic_sector_list()
        
        if dynamic_sectors:
            logger.info(f"🟢 [System] 目标确认: {len(dynamic_sectors)} 个板块，开始执行高并发压制...")
            # 2. 并发抓取并落盘
            await muscle.fetch_all_sectors(dynamic_sectors)
        else:
            logger.error("❌ [System] 目录扫描与兜底全线失败，任务终止。")
            
    except Exception as e:
        logger.exception(f"🔥 [System] 系统严重异常: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
