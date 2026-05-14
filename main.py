import asyncio
from loguru import logger
from core.muscle_engine import MuscleEngine
from dotenv import load_dotenv

load_dotenv()

async def main():
    logger.info("🧪 进入压力测试模式 - 全量 JSONP 注入劫持")
    engine = MuscleEngine()
    
    # 模拟板块名录（测试时可以填入 100+ 个板块观察性能）
    # 在实际压测中，你可以通过 engine.conn.execute(...) 读入完整的 400+ 板块
    sector_list = [
        "90.BK1063", "90.BK0447", "90.BK0473", "90.BK1026", 
        "90.BK1037", "90.BK0456", "90.BK0420", "90.BK0475",
        "90.BK0733", "90.BK0814", "90.BK1071", "90.BK0434",
        "90.BK1073", "90.BK0171", "90.BK1048", "90.BK1672"
    ]
    
    # 执行压力同步
    await engine.run_factory(sector_list)

if __name__ == "__main__":
    asyncio.run(main())
