import asyncio
import os
from loguru import logger
from core.muscle_engine import MuscleEngine
from dotenv import load_dotenv

load_dotenv()

async def main():
    logger.info("🧪 启动 V8-Pro 行为劫持压测...")
    engine = MuscleEngine()
    
    # 填入你想要测试的板块 ID
    sector_list = ["90.BK1063", "90.BK0447", "90.BK0473", "90.BK1026", "90.BK1037"]
    
    # 在正式环境中，这里可以从数据库获取全量 400+ 板块
    # sector_list = await engine.get_all_sector_ids() 

    await engine.run_factory(sector_list)

if __name__ == "__main__":
    # 强制设置并发为 1 以保证探测精度，稳定后可适度调至 2
    os.environ["CONCURRENCY"] = "1"
    asyncio.run(main())
