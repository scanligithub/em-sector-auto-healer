import asyncio
import os
from loguru import logger
from core.muscle_engine import LocalBenchmark100

async def main():
    logger.info("🧪 启动行业数据全量同步管线...")
    
    # 强制开启全部历史获取（lmt=1000000）
    engine = LocalBenchmark100(data_limit=1000000)
    await engine.run_pipeline()

if __name__ == "__main__":
    asyncio.run(main())
