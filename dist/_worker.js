// Crabify — Cloudflare Pages Advanced Mode Worker
// Proxies /api/* to the Python backend, serves static files for everything else

const BACKEND = 'https://3000-i8iktmfokg8w5lafzenkk-b32ec7bb.sandbox.novita.ai';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Accept',
          'Access-Control-Max-Age': '86400',
        },
      });
    }

    // Proxy all /api/* requests to the Python backend
    if (url.pathname.startsWith('/api/')) {
      const upstreamURL = BACKEND + url.pathname + url.search;
      
      try {
        const upstreamReq = new Request(upstreamURL, {
          method: request.method,
          headers: {
            'Content-Type': request.headers.get('Content-Type') || 'application/json',
            'Accept': 'application/json',
          },
          body: ['GET', 'HEAD'].includes(request.method) ? undefined : await request.text(),
        });

        const upstreamResp = await fetch(upstreamReq);
        const body = await upstreamResp.text();

        return new Response(body, {
          status: upstreamResp.status,
          headers: {
            'Content-Type': upstreamResp.headers.get('Content-Type') || 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Cache-Control': 'no-store',
          },
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: 'API proxy error', detail: err.message }), {
          status: 502,
          headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
          },
        });
      }
    }

    // For all other requests, serve static assets from the Pages deployment
    // Cloudflare Pages will handle this via its default static file serving
    return env.ASSETS.fetch(request);
  },
};
