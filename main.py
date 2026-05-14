import asyncio
from datetime import datetime
from loguru import logger
from core.muscle_engine import MuscleEngine
from dotenv import load_dotenv

load_dotenv()

async def main():
    logger.info("🏢 [Quant DB v3] 工业级板块数据中台启动")
    engine = MuscleEngine()
    
    # 0. 认证
    await engine.build_trust_chain()
    
    # 1. 名录管理 (周日执行全量校对)
    is_sunday = datetime.now().weekday() == 6
    sector_list = await engine.get_active_sectors(force_reconcile=is_sunday)
    
    # 2. 同步与审计报告
    if sector_list:
        await engine.sync_all_klines(sector_list)
        
        # 3. 错误统计汇总 (已修复统计字典的 Key)
        total = engine.stats.get('total_tasks', 0)
        failed = engine.stats.get('failed_tasks', 0)
        
        if total > 0:
            err_rate = (failed / total * 100)
            logger.info(f"📊 流量统计: 总任务数 {total} | 失败率 {err_rate:.1f}% | 状态码分布: {engine.stats['codes']}")
    else:
        logger.error("❌ 未能获取有效板块名录")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
