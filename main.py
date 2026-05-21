import asyncio
import os
from loguru import logger
from core.muscle_engine import MuscleEngine
from dotenv import load_dotenv

load_dotenv()

async def main():
    logger.info("🧪 启动 GitHub Actions 行业数据同步管线测试...")
    
    # 压测限制参数（可按需调大）：
    # 模式 A: 100 (默认) -> 增量日常同步（速度快，对云端 IP 极度安全稳定）
    # 模式 B: 1000000    -> 历史全量建库
    DATA_LIMIT = int(os.environ.get("DATA_LIMIT", "1000000"))
    
    # 本次 Actions 批量压测的板块上限
    MAX_SECTORS = int(os.environ.get("MAX_SECTORS", "100"))
    
    engine = MuscleEngine(data_limit=DATA_LIMIT)
    await engine.run_factory(max_sectors=MAX_SECTORS)

if __name__ == "__main__":
    asyncio.run(main())
