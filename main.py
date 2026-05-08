import asyncio
from dotenv import load_dotenv
from loguru import logger
from core.brain_engine import BrainEngine
from core.muscle_engine import MuscleEngine

load_dotenv()

async def main():
    logger.info("🚀 [System] 东方财富 AI 自愈抓取引擎启动")
    
    muscle = MuscleEngine()
    
    # 状态 1: 发射探针 (测试白酒板块的真实抓取权限)
    is_valid = await muscle.probe()
    
    # 状态 2 & 3: 触发熔断与自愈
    if not is_valid:
        logger.error("⚡ [System] 警告：发现旧规则失效(影子封杀)，触发熔断机制！")
        logger.info("⚡ [System] 正在移交控制权给 [Brain Engine] 进行 AI 自愈...")
        
        brain = BrainEngine()
        await brain.heal()
        
        # 自愈完成后，让肌肉引擎重新加载带有新 Token 的配置文件
        muscle.reload_rules()
        logger.info("⚡ [System] 规则重载完毕，准备恢复作业。")
        
        # 再次探针确认
        if not await muscle.probe():
            logger.critical("❌ [System] 致命错误：AI 自愈后探针仍未通过！请检查大模型输出或网页结构是否彻底重构。")
            return
            
    # 状态 4: 高并发正式抓取
    logger.info("🟢 [System] 权限校验彻底完毕，进入正式拉取阶段...")
    
    # 【新增功能】动态获取全市场最新近 900+ 个板块代码
    dynamic_sectors = await muscle.fetch_dynamic_sector_list()
    
    if len(dynamic_sectors) > 0:
        # 执行满血并发拉取
        await muscle.fetch_all_sectors(dynamic_sectors)
        logger.info("🎉 [System] 今日自动化更新任务圆满完成！")
    else:
        logger.error("❌ [System] 获取板块目录失败，任务终止。")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(main())
