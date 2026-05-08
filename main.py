import asyncio
from dotenv import load_dotenv
from loguru import logger
from core.brain_engine import BrainEngine
from core.muscle_engine import MuscleEngine

load_dotenv()

async def main():
    logger.info("🚀 [System] 东方财富 AI 自愈抓取引擎启动")
    
    muscle = MuscleEngine()
    
    # 状态 1: 发射探针
    is_valid = await muscle.probe()
    
    # 状态 2 & 3: 触发熔断与自愈
    if not is_valid:
        logger.error("⚡ [System] 警告：发现旧规则失效，触发熔断机制！")
        logger.info("⚡ [System] 正在移交控制权给 [Brain Engine] 进行 AI 自愈...")
        
        brain = BrainEngine()
        await brain.heal()
        
        # 自愈完成后，让肌肉引擎重新加载子弹
        muscle.reload_rules()
        logger.info("⚡ [System] 规则重载完毕，准备恢复作业。")
        
        # 再次探针确认
        if not await muscle.probe():
            logger.critical("❌ [System] 致命错误：AI 自愈后探针仍未通过，请检查网站是否发生重构！")
            return
            
    # 状态 4: 高并发正式抓取
    logger.info("🟢 [System] 权限校验完毕，进入正式拉取阶段...")
    
    # 测试环境：我们手动传入十几个知名板块的 secid 供测试
    test_sectors = [
        "90.BK0896", "90.BK0477", "90.BK0424", "90.BK0428", 
        "90.BK0473", "90.BK0731", "90.BK0727", "90.BK0475"
    ]
    
    await muscle.fetch_all_sectors(test_sectors)
    logger.info("🎉 [System] 今日自动化更新任务圆满完成！")

if __name__ == "__main__":
    # Windows 平台下避免 asyncio 报错
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(main())
