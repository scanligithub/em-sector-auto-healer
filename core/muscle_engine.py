import asyncio
import json
import re
import os
import time
import random
import urllib.parse
from loguru import logger
from curl_cffi.requests import AsyncSession

class MuscleEngine:
    def __init__(self, trust_context: dict):
        self.kline_template = trust_context.get("kline_url", "")
        self.clist_template = trust_context.get("clist_url", "")
        # 修正：确保 URL 格式正确，处理末尾斜杠
        raw_worker = os.getenv("CF_WORKER_URL", "").strip()
        self.worker_url = raw_worker if raw_worker.startswith("http") else f"https://{raw_worker}"
        
        self.headers = {
            "User-Agent": trust_context.get("ua", ""),
            "Cookie": trust_context.get("cookies", ""),
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }
        self.concurrency = int(os.getenv("CONCURRENCY", 10))
        self.impersonate = "chrome120"

    def _extract_json_with_diag(self, text: str, secid: str) -> dict:
        """带诊断功能的提取器"""
        if not text:
            return {"_err": "EMPTY_RESPONSE"}
        
        # 尝试匹配 JSONP
        match = re.search(r'^[^(]*\(\s*(\{.*\})\s*\)\s*;?\s*$', text, re.DOTALL)
        try:
            json_str = match.group(1) if match else text
            data = json.loads(json_str)
            # 即使是 JSON，也要检查业务数据是否存在
            if "data" not in data or data["data"] is None:
                return {"_err": "BUSINESS_EMPTY", "_raw": text[:200]}
            return data
        except Exception as e:
            # 记录异常样本的前 200 个字符
            sample = text[:200].replace('\n', '')
            logger.warning(f"🔍 [Diag] {secid} 解析失败. 样本: {sample} | 错误: {e}")
            return {"_err": "PARSE_ERROR", "_raw": sample}

    def _route_through_worker(self, target_url: str) -> str:
        """核心路由：强制注入 Cache-Buster 确保线性增长"""
        # 添加随机数防止东财或 CF 缓存
        connector = "&" if "?" in target_url else "?"
        bust_url = f"{target_url}{connector}_cbuster={time.time_ns()}"
        
        if self.worker_url and "workers.dev" in self.worker_url:
            encoded_target = urllib.parse.quote(bust_url, safe='')
            return f"{self.worker_url}?url={encoded_target}"
        
        logger.error("🚨 [Critical] CF_WORKER_URL 未配置或无效，正在直连（极度危险）")
        return bust_url

    async def _safe_request(self, session, url: str, secid: str = "LIST") -> dict:
        """带诊断和指数退避的请求层"""
        routed_url = self._route_through_worker(url)
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # 显式指定随机延迟，模拟人类行为
                await asyncio.sleep(random.uniform(0.1, 0.5))
                
                resp = await session.get(routed_url, headers=self.headers, timeout=20)
                
                # 状态码异常诊断
                if resp.status_code != 200:
                    logger.debug(f"⚠️ 状态码异常 {secid}: {resp.status_code} | 重试中...")
                    raise Exception(f"HTTP_{resp.status_code}")

                data = self._extract_json_with_diag(resp.text, secid)
                
                if "_err" not in data:
                    return data
                
                # 如果是业务逻辑空（被拦截），触发退避
                logger.debug(f"⚠️ 业务拦截 {secid}: {data['_err']} | 重试 [{attempt+1}/{max_retries}]")
                
            except Exception as e:
                wait_time = (2 ** attempt) + random.random()
                logger.debug(f"🕒 链路波动 {secid}: {e} | {wait_time:.1f}s 后重试")
                await asyncio.sleep(wait_time)
                
        return {}

    # ... (fetch_dynamic_sector_list 和 fetch_all_sectors 逻辑保持，但内部调用 _safe_request 传入 secid)
