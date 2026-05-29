import asyncio
import json
import os
import random
import sys
import urllib.request
import urllib.error
from loguru import logger

class MuscleEngine:
    def __init__(self, chunk_id: int):
        self.chunk_id = chunk_id
        self.output_dir = f"success_data_{chunk_id}"
        os.makedirs(self.output_dir, exist_ok=True)
        self.data_limit = 1000000

    async def fetch_sector(self, sid: str, name: str) -> bool:
        url = (
            f"https://push2his.eastmoney.com/api/qt/stock/kline/get"
            f"?secid={sid}&ut=fa5fd1943c7b386f172d6893dbfba10b"
            f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
            f"&klt=101&fqt=0&end=20500101&lmt={self.data_limit}"
        )
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/"
        }
        
        req = urllib.request.Request(url, headers=headers)
        
        try:
            # 内部定义阻塞式的读取函数
            def _perform_request():
                with urllib.request.urlopen(req, timeout=15) as response:
                    return response.read().decode('utf-8')
            
            # 利用 asyncio.to_thread 将阻塞的网络 I/O 异步化
            raw_text = await asyncio.to_thread(_perform_request)
            
            data = json.loads(raw_text)
            if not data or "data" not in data or data["data"] is None: 
                return False
            
            payload = data["data"]
            with open(os.path.join(self.output_dir, f"{sid}.json"), "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            return True
        except Exception:
            return False

    async def worker(self, worker_id: int, queue: asyncio.Queue, state: dict, lock: asyncio.Lock):
        """
        异步消费者协程
        """
        while not queue.empty():
            # 1. 检查全局熔断信号
            if state["circuit_broken"]:
                break

            # 2. 从队列中获取任务
            item = await queue.get()
            sid, name = item["sid"], item["name"]

            # 3. 错峰控流，避免 20 个协程同一微秒发起请求
            await asyncio.sleep(random.uniform(1.0, 3.0))

            if state["circuit_broken"]:
                queue.task_done()
                break

            # 4. 执行网络请求
            success = await self.fetch_sector(sid, name)

            # 5. 协程安全更新状态
            async with lock:
                ref_item = next((x for x in state["pending_list"] if x["sid"] == sid), None)

                if success:
                    if ref_item:
                        state["pending_list"].remove(ref_item)
                    state["consecutive_failures"] = 0
                    logger.success(f"🎯 [Job {self.chunk_id} | Worker {worker_id}] 成功: {name} ({sid})")
                else:
                    state["consecutive_failures"] += 1
                    if ref_item:
                        ref_item["fail_count"] += 1
                        logger.error(f"❌ [Job {self.chunk_id} | Worker {worker_id}] 失败: {name} ({sid}) | 单体累计失败: {ref_item['fail_count']}/3")

                    if state["consecutive_failures"] >= 2:
                        state["circuit_broken"] = True
                        logger.critical(f"🚨 [Job {self.chunk_id} | Worker {worker_id}] 触发熔断！中止本节点后续所有任务。")

            queue.task_done()

    async def run(self):
        # 1. 载入本节点的代办分块
        with open(f"chunks/chunk_{self.chunk_id}.json", "r", encoding="utf-8") as f:
            sectors = json.load(f)
            
        # 2. 初始化任务队列
        queue = asyncio.Queue()
        for item in sectors:
            await queue.put(item)

        # 3. 初始化全局共享状态与互斥锁
        state = {
            "consecutive_failures": 0,
            "circuit_broken": False,
            "pending_list": [x.copy() for x in sectors]
        }
        lock = asyncio.Lock()
        
        # 4. 建立轻量级预热请求
        try:
            def _warmup():
                warmup_url = f"https://quote.eastmoney.com/bk/{sectors[0]['sid']}.html"
                req = urllib.request.Request(warmup_url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    resp.read(1024) # 仅读取前1KB建立连接缓存
            await asyncio.to_thread(_warmup)
        except Exception:
            pass
            
        # 5. 并发启动 20 个 Worker 协程
        num_workers = 20
        workers = []
        for w_id in range(num_workers):
            task = asyncio.create_task(self.worker(w_id, queue, state, lock))
            workers.append(task)

        # 6. 等待所有 Worker 运行完毕
        await asyncio.gather(*workers)
            
        # 7. 退回未完成任务
        with open(f"failed_list_{self.chunk_id}.json", "w", encoding="utf-8") as f:
            json.dump(state["pending_list"], f, ensure_ascii=False)

if __name__ == "__main__":
    chunk_id = int(sys.argv[1])
    engine = MuscleEngine(chunk_id)
    asyncio.run(engine.run())
