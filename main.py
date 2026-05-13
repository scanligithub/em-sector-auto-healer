import asyncio
from datetime import datetime
from loguru import logger
from core.muscle_engine import MuscleEngine
from dotenv import load_dotenv

load_dotenv()

async def main():
    logger.info("🏢 [Quant DB v3] 工业级板块数据中台")
    engine = MuscleEngine()
    
    # 0. 认证
    await engine.build_trust_chain()
    
    # 1. 名录管理 (周日执行 Full Reconcile)
    is_sunday = datetime.now().weekday() == 6
    sector_list = await engine.get_active_sectors(force_reconcile=is_sunday)
    
    # 2. 增量同步
    if sector_list:
        await engine.sync_all_klines(sector_list)
        
        # 3. 汇总报告
        total = engine.stats['total']
        if total > 0:
            logger.info(f"📊 运行报告: 请求 {total} | 错误率 {(engine.stats['errors']/total)*100:.1f}%")

if __name__ == "__main__":
    asyncio.run(main())
