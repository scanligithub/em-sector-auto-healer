import asyncio
import sys
import os
from dotenv import load_dotenv
from loguru import logger

# 核心模块导入
from core.brain_engine import BrainEngine
from core.muscle_engine import MuscleEngine

# 加载环境变量 (本地 .env 或 GitHub Secrets)
load_dotenv()

async def main():
    logger.info("🚀 [System] 东方财富 AI 自愈抓取引擎启动")
    
    # 实例化肌肉引擎 (负责高并发下载)
    muscle = MuscleEngine()
    
    # --- 状态 1: 发射探针 ---
    # 探针会测试当前 config/active_rules.json 是否能抓到真实的板块数据
    is_valid = await muscle.probe()
    
    # --- 状态 2 & 3: 触发熔断与自愈 ---
    if not is_valid:
        logger.error("⚡ [System] 警告：发现鉴权规则失效(影子封杀)，触发熔断机制！")
        logger.info("⚡ [System] 正在唤醒 [Brain Engine] 启动浏览器嗅探与 AI 逆向...")
        
        try:
            # 实例化大脑引擎 (负责破译密码)
            brain = BrainEngine()
            
            # 执行自愈流程：启动浏览器 -> 拦截流量 -> LLM 提取 -> 覆盖 JSON
            await brain.heal()
            
            # 自愈完成后，让肌肉引擎重新加载最新的 active_rules.json
            muscle.reload_rules()
            logger.info("⚡ [System] 规则重载完毕，准备重新执行任务。")
            
            # 再次发射探针，确保自愈结果真实有效
            if not await muscle.probe():
                logger.critical("❌ [System] 致命错误：AI 自愈后探针仍未通过！请检查大模型 API 额度或网页结构。")
                return
        except Exception as e:
            logger.error(f"❌ [System] 自愈过程发生异常: {e}")
            return
            
    # --- 状态 4: 全速并发下载阶段 ---
    logger.info("🟢 [System] 鉴权校验通过，进入全量数据拉取阶段...")
    
    # 1. 动态获取全市场最新的板块代码 (行业、概念、地域)
    dynamic_sectors = await muscle.fetch_dynamic_sector_list()
    
    if dynamic_sectors:
        logger.info(f"📊 [System] 准备抓取全量历史数据，共计 {len(dynamic_sectors)} 个板块...")
        
        # 2. 执行满血高并发抓取
        await muscle.fetch_all_sectors(dynamic_sectors)
        
        logger.info("🎉 [System] 东方财富全量板块历史更新任务圆满完成！")
        logger.info(f"💾 [System] 最终文件位置: data/sector_klines_full.parquet")
    else:
        logger.error("❌ [System] 获取板块目录失败，无法继续后续抓取。")

if __name__ == "__main__":
    # Windows 环境下的异步循环兼容性处理
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("🛑 [System] 任务被用户强制中断。")
    except Exception as e:
        logger.exception(f"🔥 [System] 系统崩溃: {e}")
