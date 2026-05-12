import asyncio
import sys
from loguru import logger
from core.brain_engine import BrainEngine
from core.muscle_engine import MuscleEngine

async def main():
    logger.info("🚀 [System] 终极形态：混合原生引擎启动 (Traffic Hijack + Browser API)")
    
    brain = BrainEngine()
    try:
        # 1. 构建无懈可击的浏览器上下文 (登录态、Cookie、指纹)
        context, _ = await brain.build_trust_context()
        
        # 2. 移交环境给肌肉引擎
        muscle = MuscleEngine(context)
        
        # 3. 拉取最新的全量目录
        dynamic_sectors = await muscle.fetch_dynamic_sector_list()
        
        if dynamic_sectors:
            logger.info(f"🟢 [System] 准备对 {len(dynamic_sectors)} 个板块执行数据清洗...")
            
            # 4. 执行单点劫持，获取今天的官方合法链接
            await muscle.prepare_hijack_template()
            
            # 5. 执行极速拉取
            await muscle.fetch_all_sectors(dynamic_sectors)
        else:
            logger.error("❌ [System] 目录扫描失败，任务终止。")
            
    except Exception as e:
        logger.exception(f"🔥 [System] 系统异常: {e}")
    finally:
        await brain.close()
        logger.info("🛑 [System] 浏览器母体已安全销毁。")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
