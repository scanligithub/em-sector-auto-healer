import asyncio
from loguru import logger
from core.muscle_engine import MuscleEngine
from dotenv import load_dotenv

load_dotenv()

async def main():
    logger.info("🧪 启动 V8-Pro 黄金 API 异步吞吐压测...")
    engine = MuscleEngine()
    
    # 压测目标板块组合（包含您指定的 BK1063、BK1026 等）
    sector_list = ["90.BK1063", "90.BK0447", "90.BK0473", "90.BK1026", "90.BK1037"]
    
    await engine.run_factory(sector_list)

if __name__ == "__main__":
    asyncio.run(main())
