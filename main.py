import asyncio
from loguru import logger
from core.muscle_engine import MuscleEngine
from dotenv import load_dotenv

load_dotenv()

async def main():
    logger.info("🎭 [Quant Interceptor] 浏览器流量劫持模式启动")
    engine = MuscleEngine()
    
    # 1. 获取名录
    sector_list = await engine.get_active_sectors()
    
    if not sector_list:
        # 如果 DB 为空，提供几个测试种子
        sector_list = ["90.BK1063", "90.BK0447", "90.BK0473"]
        logger.warning("⚠️ 数据库名录为空，进入种子测试模式")

    # 2. 执行劫持同步
    await engine.run_factory(sector_list)

if __name__ == "__main__":
    asyncio.run(main())
