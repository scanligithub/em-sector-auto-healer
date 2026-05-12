import asyncio
import sys
from loguru import logger
from core.brain_engine import BrainEngine
from core.muscle_engine import MuscleEngine

async def main():
    logger.info("🚀 [System] 终极工业级混合引擎启动 (Playwright 洗白 + curl_cffi 狂暴)")
    
    brain = BrainEngine()
    try:
        # 1. 幽灵潜入：获取含有最新 Token 和 Cookie 的信誉凭证
        trust_context = await brain.steal_trust_context()
        
        # 2. 凭证交接给肌肉引擎
        muscle = MuscleEngine(trust_context)
        
        # 3. 极速拉取全市场板块目录
        dynamic_sectors = await muscle.fetch_dynamic_sector_list()
        
        if dynamic_sectors:
            logger.info(f"🟢 [System] 准备对 {len(dynamic_sectors)} 个板块执行极速并发抓取...")
            
            # 4. 执行全量拉取落盘
            await muscle.fetch_all_sectors(dynamic_sectors)
        else:
            logger.error("❌ [System] 目录扫描失败，任务终止。")
            
    except Exception as e:
        logger.exception(f"🔥 [System] 系统严重异常: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
