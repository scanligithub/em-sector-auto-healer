export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const targetUrl = url.searchParams.get('url');

    // ========== 健康检查端点 ==========
    // GET /?health 返回 Worker 运行状态、最近错误统计
    if (url.searchParams.has('health')) {
      const stats = {
        status: 'ok',
        uptime: Date.now() - (env.START_TIME || Date.now()),
        total_requests: env.TOTAL_REQUESTS || 0,
        error_count: env.ERROR_COUNT || 0,
        last_error: env.LAST_ERROR || null,
        last_error_time: env.LAST_ERROR_TIME || null,
        target_host: 'push2.eastmoney.com / push2his.eastmoney.com',
        worker_version: '1.0.1'
      };
      return new Response(JSON.stringify(stats), {
        status: 200,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      });
    }

    if (!targetUrl) {
      return new Response('Missing target url parameter', { status: 400 });
    }

    // 初始化计数器（使用 env 持久化，在 Workers KV 不可用时回退到内存）
    if (!env.TOTAL_REQUESTS) env.TOTAL_REQUESTS = 0;
    if (!env.ERROR_COUNT) env.ERROR_COUNT = 0;
    if (!env.START_TIME) env.START_TIME = Date.now();
    env.TOTAL_REQUESTS++;

    // 构造发往东财的纯净请求
    const proxyRequest = new Request(targetUrl, {
      method: request.method,
      headers: request.headers,
      redirect: 'follow'
    });

    // 强行覆盖底层握手头，伪装得更像正常流量
    const targetUrlObj = new URL(targetUrl);
    proxyRequest.headers.set('Host', targetUrlObj.hostname);
    proxyRequest.headers.set('Origin', 'https://quote.eastmoney.com');
    proxyRequest.headers.set('Referer', 'https://quote.eastmoney.com/');
    
    // CF 默认会加一些 X-Forwarded-For，我们尽量清理掉爬虫特征
    proxyRequest.headers.delete('X-Forwarded-For');
    proxyRequest.headers.delete('CF-Connecting-IP');

    try {
      const response = await fetch(proxyRequest);
      
      // 将东财的响应原封不动传回 GitHub Actions
      const newResponse = new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers
      });
      
      // 允许跨域（方便本地测试）
      newResponse.headers.set('Access-Control-Allow-Origin', '*');
      return newResponse;
      
    } catch (e) {
      // 记录错误信息
      env.ERROR_COUNT++;
      env.LAST_ERROR = e.message;
      env.LAST_ERROR_TIME = new Date().toISOString();
      return new Response(JSON.stringify({ error: e.message }), { status: 502 });
    }
  }
};
