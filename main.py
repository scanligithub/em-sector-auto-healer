import asyncio
import sys
from datetime import datetime
from loguru import logger
from core.muscle_engine import MuscleEngine
from dotenv import load_dotenv

load_dotenv()

async def main():
    # 打印这行字，证明脚本真正跑起来了
    logger.info("🏢 [Quant DB v3] 工业级数据中台启动 (🔥全量压测模式)")
    
    engine = MuscleEngine()
    
    # 0. 预热浏览器指纹
    await engine.build_trust_chain()
    
    # 1. 获取名录 (压测模式下，强制刷新一遍目录，确保拿到最新 231 个)
    sector_list = await engine.get_active_sectors(force_reconcile=True)
    
    # 2. 启动极限压测
    if sector_list:
        await engine.sync_all_klines(sector_list)
        
        # 3. 输出压测报告
        total = engine.stats.get('total_tasks', 0)
        failed = engine.stats.get('failed_tasks', 0)
        
        if total > 0:
            err_rate = (failed / total * 100)
            logger.info(f"📊 压测总结: 总任务 {total} | 失败率 {err_rate:.1f}% | 状态码分布: {engine.stats['codes']}")
    else:
        logger.error("❌ 压测中止：未能获取有效板块名录")

# 👇 这两行是脚本的发动机，绝对不能漏掉！
if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
