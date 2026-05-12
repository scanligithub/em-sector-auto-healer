import asyncio, sys, os
from dotenv import load_dotenv
from loguru import logger
from core.brain_engine import BrainEngine
from core.muscle_engine import MuscleEngine

load_dotenv()

async def main():
    logger.info("🚀 [System] AI 自愈板块抓取引擎启动")
    muscle = MuscleEngine()
    
    # 探针阶段
    if not await muscle.probe():
        logger.error("⚡ [System] 规则失效，启动 Brain Engine 进行导航链学习...")
        brain = BrainEngine()
        await brain.heal()
        muscle.reload_rules()
        if not await muscle.probe():
            logger.critical("❌ [System] 自愈失败，请检查环境！")
            return
            
    # 执行拉取
    logger.info("🟢 [System] 身份验证通过，开始执行业务...")
    dynamic_sectors = await muscle.fetch_dynamic_sector_list()
    if dynamic_sectors:
        await muscle.fetch_all_sectors(dynamic_sectors)
        logger.info("🎉 [System] 任务全部圆满完成。")

if __name__ == "__main__":
    if sys.platform == 'win32': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
