import asyncio
import sys
import os
from dotenv import load_dotenv
from loguru import logger

# 核心模块导入
from core.brain_engine import BrainEngine
from core.muscle_engine import MuscleEngine

# 加载环境变量 (支持 GitHub Secrets 和本地 .env)
load_dotenv()

async def main():
    logger.info("🚀 [System] AI 自愈板块抓取引擎启动 (Autonomous Agent Mode)")
    
    # 实例化执行引擎 (肌肉)
    muscle = MuscleEngine()
    
    # ==========================================
    # 状态 1: 行为探针验证
    # ==========================================
    # 探针会尝试用现有规则拉取一次白酒板块数据
    is_valid = await muscle.probe()
    
    # ==========================================
    # 状态 2: 触发熔断与自愈 (如果探针失败)
    # ==========================================
    if not is_valid:
        logger.error("⚡ [System] 探针未通过：规则失效或遭遇影子封杀。触发自愈程序...")
        
        try:
            # 实例化训练引擎 (大脑)
            brain = BrainEngine()
            
            # 执行：传感器提纯 + AI 决策自愈
            # 这里会自动处理导航链模拟，并利用 LLM 提取最新 token
            await brain.heal()
            
            # 自愈后，肌肉引擎重载最新的 active_rules.json 攻略
            muscle.reload_rules()
            logger.info("⚡ [System] 攻略已重载。重新验证指纹...")
            
            # 再次发射探针，确保自愈结果有效
            if not await muscle.probe():
                logger.critical("❌ [System] 致命故障：AI 自愈后指纹仍被拒绝。可能网页协议发生了重大重构。")
                return
                
        except Exception as e:
            logger.error(f"❌ [System] 自愈过程崩溃: {e}")
            return
            
    # ==========================================
    # 状态 3: 全量业务执行 (如果验证通过)
    # ==========================================
    logger.info("🟢 [System] 身份指纹合法，进入全量数据并发拉取阶段...")
    
    try:
        # 1. 动态获取全市场最新的板块代码列表 (行业+概念+地域)
        # 这里会复用自愈得到的最新 Cookie 和 Referer
        dynamic_sectors = await muscle.fetch_dynamic_sector_list()
        
        if dynamic_sectors and len(dynamic_sectors) > 0:
            logger.info(f"📊 [System] 准备拉取全量历史 K 线，目标共计 {len(dynamic_sectors)} 个板块...")
            
            # 2. 开启高并发抓取引擎 (基于 curl_cffi + Polars)
            await muscle.fetch_all_sectors(dynamic_sectors)
            
            logger.info("🎉 [System] 东方财富全量板块数据更新任务圆满完成！")
            logger.info(f"💾 [System] Parquet 文件已成功落盘至 data 目录。")
        else:
            logger.error("❌ [System] 获取板块目录失败，可能是扫描器接口发生了变化。")
            
    except Exception as e:
        logger.exception(f"🔥 [System] 业务拉取阶段发生非预期错误: {e}")

if __name__ == "__main__":
    # 针对 Windows 平台的异步策略兼容 (解决 Actions 运行环境可能存在的 loop 冲突)
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    try:
        # 启动主程序
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("🛑 [System] 进程被用户中断。")
    except Exception as e:
        logger.critical(f"💀 [System] 进程因严重错误终止: {e}")
