import asyncio
import sys
from loguru import logger
from core.muscle_engine import MuscleEngine
from dotenv import load_dotenv

load_dotenv()

async def main():
    logger.info("🚀 [System] 工业级 EM 数据中台 | 增量/池化/Stealth 模式")
    
    engine = MuscleEngine()
    
    # 0. 获取认证指纹
    await engine.build_trust_chain()
    
    # 1. 扫描板块目录
    sector_list = await engine.fetch_dynamic_sector_list()
    
    if sector_list:
        # 2. 增量抓取 K 线
        await engine.fetch_all_sectors(sector_list)
        
        # 3. 统计结果
        if engine.stats['total'] > 0:
            err_rate = (engine.stats['errors'] / engine.stats['total'] * 100)
            logger.info(f"📊 执行摘要: 总请求 {engine.stats['total']} | 错误率 {err_rate:.1f}%")
    else:
        logger.error("❌ 板块目录获取失败，请检查 Worker 状态或东财接口。")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
