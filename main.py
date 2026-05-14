import asyncio
import sys
from datetime import datetime
from loguru import logger
from core.muscle_engine import MuscleEngine
from dotenv import load_dotenv

load_dotenv()

async def main():
    # 只要看到这行日志，说明 Python 脚本真正开始运行了
    logger.info("🚀 [Nitro Mode] 极速数据中台启动 | 协议: HTTP/2 | 模式: 全量压测")
    
    engine = MuscleEngine()
    
    # 0. 认证
    await engine.build_trust_chain()
    
    # 1. 名录管理 (压测模式强制刷新)
    sector_list = await engine.get_active_sectors(force_reconcile=True)
    
    # 2. 启动同步
    if sector_list:
        await engine.sync_all_klines(sector_list)
        
        # 3. 统计汇总
        total = engine.stats.get('total_tasks', 0)
        failed = engine.stats.get('failed_tasks', 0)
        if total > 0:
            err_rate = (failed / total * 100)
            logger.info(f"📊 压测总结: 总任务 {total} | 失败率 {err_rate:.1f}% | 状态码分布: {engine.stats['codes']}")
    else:
        logger.error("❌ 未能获取有效板块名录，任务终止")

# 💡 核心：必须确保下面这三行在文件最底部，且没有缩进
if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
