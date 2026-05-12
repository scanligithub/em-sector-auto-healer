export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const targetUrl = url.searchParams.get('url');

    if (!targetUrl) {
      return new Response('Missing target url parameter', { status: 400 });
    }

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
      return new Response(JSON.stringify({ error: e.message }), { status: 502 });
    }
  }
};
