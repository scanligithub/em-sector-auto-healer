import asyncio
import sys
from loguru import logger
from core.brain_engine import BrainEngine
from core.muscle_engine import MuscleEngine

async def main():
    logger.info("🚀 [System] 终极形态：纯被动流量劫持系统 (Passive Traffic Hijacker)")
    
    brain = BrainEngine()
    try:
        # 1. 构建无懈可击的浏览器上下文
        context = await brain.build_trust_context()
        
        # 2. 移交环境给监听引擎
        muscle = MuscleEngine(context)
        
        # 3. 真实 Tab 导航获取目录 (彻底摆脱 APIRequestContext 的 Timeout 黑洞)
        dynamic_sectors = await muscle.fetch_dynamic_sector_list()
        
        if dynamic_sectors:
            logger.info(f"🟢 [System] 准备对 {len(dynamic_sectors)} 个板块执行物理级流量监听...")
            # 4. 执行全量监听
            await muscle.hijack_all_sectors(dynamic_sectors)
        else:
            logger.error("❌ [System] 目录扫描失败，任务终止。")
            
    except Exception as e:
        logger.exception(f"🔥 [System] 系统严重异常: {e}")
    finally:
        await brain.close()
        logger.info("🛑 [System] 浏览器母体已安全销毁。")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
