import asyncio, sys, os
from dotenv import load_dotenv
from loguru import logger
from core.brain_engine import BrainEngine
from core.muscle_engine import MuscleEngine

load_dotenv()

async def main():
    logger.info("🚀 [System] AI 浏览器原生抓取系统启动")
    
    # 1. 建立母体会话
    brain = BrainEngine()
    try:
        await brain.init_session()
        
        # 2. 嗅探最新 Token
        ut = await brain.discover_api_template()
        
        # 3. 实例化执行引擎，并寄生在 brain 的页面中
        muscle = MuscleEngine(brain.page)
        muscle.set_ut(ut)
        
        # 4. 原生验证
        if not await muscle.probe():
            logger.critical("❌ [System] 即使在原生浏览器环境下，身份仍被拒绝。")
            return
            
        # 5. 执行全量业务
        logger.info("🟢 [System] 进入 Browser-Native 拉取阶段...")
        dynamic_sectors = await muscle.fetch_dynamic_sector_list()
        
        if dynamic_sectors:
            # 只测试前 20 个，验证速度与稳定性
            await muscle.fetch_all_sectors(dynamic_sectors[:20])
            logger.info("🎉 [System] 浏览器原生抓取测试任务圆满完成。")
            
    finally:
        await brain.close()

if __name__ == "__main__":
    asyncio.run(main())
