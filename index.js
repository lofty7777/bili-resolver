import { connect } from 'cloudflare:sockets';

async function bypassFetch(url, options = {}) {
    const urlObj = new URL(url);
    const hostname = urlObj.hostname;
    const isHttps = urlObj.protocol === 'https:';
    const port = isHttps ? 443 : 80;
    const path = urlObj.pathname + urlObj.search;

    const headers = new Headers(options.headers || {});
    headers.set('Host', hostname);
    if (!headers.has('User-Agent')) headers.set('User-Agent', UA);
    if (!headers.has('Accept')) headers.set('Accept', 'application/json, text/plain, */*');
    headers.set('Accept-Encoding', 'identity');
    headers.set('Connection', 'close');

    const method = (options.method || 'GET').toUpperCase();
    let body = '';
    if (options.body && method !== 'GET' && method !== 'HEAD') {
        body = typeof options.body === 'string' ? options.body : JSON.stringify(options.body);
        headers.set('Content-Length', String(new TextEncoder().encode(body).length));
    }

    let reqString = `${method} ${path} HTTP/1.1\r\n`;
    for (const [key, value] of headers.entries()) {
        reqString += `${key}: ${value}\r\n`;
    }
    reqString += '\r\n';
    if (body) reqString += body;

    const socket = connect({ hostname, port }, { secureTransport: isHttps ? 'on' : 'off', allowHalfOpen: false });

    try {
        await socket.opened;
    } catch (e) {
        throw new Error(`Socket connect failed: ${e.message}`);
    }

    const writer = socket.writable.getWriter();
    const reader = socket.readable.getReader();

    try {
        await writer.write(new TextEncoder().encode(reqString));
        await writer.close();

        const chunks = [];
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            chunks.push(value);
        }

        const totalLength = chunks.reduce((acc, c) => acc + c.length, 0);
        const responseBytes = new Uint8Array(totalLength);
        let offset = 0;
        for (const chunk of chunks) {
            responseBytes.set(chunk, offset);
            offset += chunk.length;
        }

        // 在字节层面查找 \r\n\r\n（HTTP 头结束标志）
        let headerEndIndex = -1;
        for (let i = 0; i < responseBytes.length - 3; i++) {
            if (responseBytes[i] === 13 && responseBytes[i+1] === 10 && responseBytes[i+2] === 13 && responseBytes[i+3] === 10) {
                headerEndIndex = i;
                break;
            }
        }

        if (headerEndIndex === -1) {
            throw new Error(`Invalid HTTP response: header end not found (length=${responseBytes.length})`);
        }

        const headerBytes = responseBytes.slice(0, headerEndIndex);
        let bodyBytes = responseBytes.slice(headerEndIndex + 4);

        const headerText = new TextDecoder('utf-8', { fatal: false }).decode(headerBytes);
        const headerLines = headerText.split('\r\n');
        const statusMatch = headerLines[0].match(/^HTTP\/\d\.\d (\d+)/);
        const statusCode = statusMatch ? parseInt(statusMatch[1]) : 200;

        const responseHeaders = new Headers();
        for (let i = 1; i < headerLines.length; i++) {
            const colonIndex = headerLines[i].indexOf(':');
            if (colonIndex > 0) {
                responseHeaders.append(
                    headerLines[i].substring(0, colonIndex).trim(),
                    headerLines[i].substring(colonIndex + 1).trim()
                );
            }
        }

        // 处理 chunked 编码
        const transferEncoding = responseHeaders.get('transfer-encoding');
        if (transferEncoding && transferEncoding.toLowerCase().includes('chunked')) {
            const decodedChunks = [];
            let i = 0;
            while (i < bodyBytes.length) {
                let lineEnd = -1;
                for (let j = i; j < bodyBytes.length - 1; j++) {
                    if (bodyBytes[j] === 13 && bodyBytes[j+1] === 10) {
                        lineEnd = j;
                        break;
                    }
                }
                if (lineEnd === -1) break;

                const sizeStr = new TextDecoder().decode(bodyBytes.slice(i, lineEnd));
                const size = parseInt(sizeStr.trim(), 16);
                if (isNaN(size) || size === 0) break;

                const chunkStart = lineEnd + 2;
                const chunkEnd = chunkStart + size;
                decodedChunks.push(bodyBytes.slice(chunkStart, chunkEnd));
                i = chunkEnd + 2;
            }
            const totalLen = decodedChunks.reduce((sum, c) => sum + c.length, 0);
            const merged = new Uint8Array(totalLen);
            let off = 0;
            for (const c of decodedChunks) {
                merged.set(c, off);
                off += c.length;
            }
            bodyBytes = merged;
        }

        // 处理 gzip 编码
        const contentEncoding = responseHeaders.get('content-encoding');
        if (contentEncoding && contentEncoding.toLowerCase().includes('gzip')) {
            const ds = new DecompressionStream('gzip');
            const stream = new Blob([bodyBytes]).stream().pipeThrough(ds);
            const decompressed = await new Response(stream).arrayBuffer();
            bodyBytes = new Uint8Array(decompressed);
        }

        const bodyText = new TextDecoder('utf-8', { fatal: false }).decode(bodyBytes);

        return new Response(bodyText, { status: statusCode, headers: responseHeaders });
    } finally {
        try { writer.releaseLock(); } catch (e) {}
        try { reader.releaseLock(); } catch (e) {}
        try { socket.close(); } catch (e) {}
    }
}

/**
 * Bilibili Resolver & Proxy Worker
 * 
 * 双模式界面：视频 / 直播 切换
 * - 视频：原版逻辑 (1080P/720P/480P, Quest模式, 历史记录)
 * - 直播：v4.1 稳定版本 (CN/OV 节点检测)
 */

const VERSION = '20260609-029'; // 每次 push 时更新此版本号

const REFERER = 'https://www.bilibili.com/';
const LIVE_REFERER = 'https://live.bilibili.com/';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const UA_MOBILE = 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1';

let WORKER_ENV = {};

// 代理 Fetch 封装，统一附加 VERCEL_PROXY 和 PROXY_TOKEN
function proxiedFetch(originalUrl, options = {}) {
    let finalUrl = originalUrl;
    const isBiliApi = typeof originalUrl === 'string' && (originalUrl.includes('api.bilibili.com') || originalUrl.includes('api.live.bilibili.com'));

    const proxyBase = WORKER_ENV.VERCEL_PROXY;
    if (proxyBase && isBiliApi) {
        finalUrl = proxyBase + encodeURIComponent(originalUrl);
        const finalOptions = { ...options };
        const token = WORKER_ENV.PROXY_TOKEN;
        if (token) {
            finalOptions.headers = new Headers(options.headers || {});
            finalOptions.headers.set('x-proxy-token', token);
        }
        return fetch(finalUrl, finalOptions);
    }

    // 关键修改：B站 API 请求走 bypassFetch（Socket API），绕过 cf-* 标头注入
    if (isBiliApi) {
        return bypassFetch(finalUrl, options);
    }

    return fetch(finalUrl, options);
}

const ERROR_MAP = {
    '-400': '请求错误', '-403': '访问权限不足', '-404': '视频不存在',
    '-10403': '仅限港澳台地区', '62002': '视频不可见', '62004': '审核中'
};

// --- 反爬错误 ---
const ANTI_CRAWL_MSG = 'B 站风控拦截，请稍后重试';

class AntiCrawlError extends Error {
    constructor(message = ANTI_CRAWL_MSG) {
        super(message);
        this.name = 'AntiCrawlError';
    }
}

// --- 安全 JSON 抓取助手 ---
async function fetchBiliJson(url, options) {
    const res = await proxiedFetch(url, options);

    const text = await res.text();

    if (!res.ok) {
        if (res.status === 401 || text.includes('Unauthorized')) throw new Error('Vercel 代理鉴权失败：请检查 Cloudflare 上的 PROXY_TOKEN 是否与 Vercel 中的代码一致');
        if (res.status === 404 || text.includes('NOT_FOUND')) throw new Error('Vercel 代理地址失效：请检查 VERCEL_PROXY 变量结尾是否带了 /api/proxy?url=');
        throw new AntiCrawlError();
    }

    const contentType = res.headers.get('content-type') || '';
    if (!contentType.includes('application/json') || text.trim().startsWith('<')) {
        throw new AntiCrawlError();
    }

    let json;
    try {
        json = JSON.parse(text);
    } catch (e) {
        throw new AntiCrawlError();
    }

    if (json.code === -352) throw new AntiCrawlError();

    return json;
}

// --- Buvid ---
async function getBuvid() {
    try {
        const res = await proxiedFetch("https://api.bilibili.com/x/frontend/finger/spi", { headers: { "User-Agent": UA } });
        const json = await res.json();
        return json.data?.b_3 || "FE6D3664-927F-F75B-B7D4-733E5D4B263F69428infoc";
    } catch (e) { return "FE6D3664-927F-F75B-B7D4-733E5D4B263F69428infoc"; }
}

const FALLBACK_BUVID3 = "FE6D3664-927F-F75B-B7D4-733E5D4B263F69428infoc";

async function hmacSha256Hex(key, message) {
    const enc = new TextEncoder();
    const cryptoKey = await crypto.subtle.importKey(
        'raw', enc.encode(key), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
    );
    const sig = await crypto.subtle.sign('HMAC', cryptoKey, enc.encode(message));
    return Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, '0')).join('');
}

async function getAntiCrawlCookie() {
    let buvid3 = FALLBACK_BUVID3;
    let buvid4 = null;

    try {
        const res = await proxiedFetch("https://api.bilibili.com/x/frontend/finger/spi", { headers: { "User-Agent": UA } });
        const json = await res.json();
        if (json.data?.b_3) buvid3 = json.data.b_3;
        if (json.data?.b_4) buvid4 = json.data.b_4;
    } catch (e) { }

    let ticket = null;
    try {
        const ts = Math.floor(Date.now() / 1000);
        const hexsign = await hmacSha256Hex('XgwSnGZ1p', 'ts' + ts);
        const ticketUrl = `https://api.bilibili.com/bapis/bilibili.api.ticket.v1.Ticket/GenWebTicket?key_id=ec02&hexsign=${hexsign}&context[ts]=${ts}&csrf=`;
        const res = await proxiedFetch(ticketUrl, { method: 'POST', headers: { "User-Agent": UA } });
        const json = await res.json();
        if (json.data?.ticket) ticket = json.data.ticket;
    } catch (e) { }

    const parts = [`buvid3=${buvid3}`];
    if (buvid4) parts.push(`buvid4=${buvid4}`);
    if (ticket) parts.push(`bili_ticket=${ticket}`);
    return parts.join('; ');
}

const mixinKeyEncTab = [46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52];
const getMixinKey = (orig) => mixinKeyEncTab.map(n => orig[n]).join('').slice(0, 32);
async function md5(text) {
    const hashBuffer = await crypto.subtle.digest('MD5', new TextEncoder().encode(text));
    return Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');
}

async function getMixinKeyFromNav(cookie) {
    const json = await fetchBiliJson("https://api.bilibili.com/x/web-interface/nav", {
        headers: { 'User-Agent': UA, 'Referer': REFERER, 'Cookie': cookie }
    });
    const { img_url, sub_url } = json.data.wbi_img;
    return getMixinKey(img_url.split('/').pop().split('.')[0] + sub_url.split('/').pop().split('.')[0]);
}

async function signWbi(params, cookie, mixinKey) {
    const mixin_key = mixinKey !== undefined ? mixinKey : await getMixinKeyFromNav(cookie);
    const curr_params = { ...params, wts: Math.floor(Date.now() / 1000) };
    const query = Object.keys(curr_params).sort().map(k => `${k}=${encodeURIComponent(curr_params[k])}`).join('&');
    return query + `&w_rid=${await md5(query + mixin_key)}`;
}

const APP_KEYS = {
    ios: { appkey: 'YvirImLGlLANCLvM', appsec: 'JNlZNgfNGKZEpaDTkCdPQVXntXhuiJEM', platform: 'ios', ua: 'Bilibili/8.0.0 (bbcallen@gmail.com)' },
    tv: { appkey: '4409e2ce8ffd12b8', appsec: '59b43e04ad6965f34319062b478f83dd', platform: 'android', ua: 'Bilibili Freedoooooom/MOD' }
};

async function appSign(params, appkey, appsec) {
    const all = { ...params, appkey };
    const query = Object.keys(all).sort().map(k => `${k}=${encodeURIComponent(all[k])}`).join('&');
    return query + `&sign=${await md5(query + appsec)}`;
}

async function getPlayUrlWithFallback(bvid, cid, targetQn, cookie, mixinKey) {
    const qualities = [targetQn, 80, 64, 32].filter((v, i, a) => a.indexOf(v) === i && v <= targetQn);

    let mixin_key = mixinKey;
    let navAvailable = true;
    if (mixin_key === undefined) {
        try {
            mixin_key = await getMixinKeyFromNav(cookie);
        } catch (e) {
            navAvailable = false;
        }
    }

    let lastError = null;
    let sawAntiCrawl = false;

    const tryAppLine = async (qn, conf) => {
        const params = { bvid, cid: String(cid), qn: String(qn), fnval: '1', fnver: '0', fourk: '1', platform: conf.platform, ts: String(Math.floor(Date.now() / 1000)) };
        const signed = await appSign(params, conf.appkey, conf.appsec);
        const pData = await fetchBiliJson(`https://api.bilibili.com/x/player/playurl?${signed}`, {
            headers: { 'User-Agent': conf.ua }
        });
        if (pData.code === 0 && pData.data?.durl?.[0]?.url) {
            return { url: pData.data.durl[0].url, quality: pData.data.quality };
        }
        throw new Error(pData.message || ERROR_MAP[pData.code] || '取流失败');
    };

    const tryWebLine = async (qn) => {
        const signedQuery = await signWbi({ bvid, cid, qn, fnval: 1, try_look: 1, platform: 'html5', high_quality: 1 }, cookie, mixin_key);
        const pData = await fetchBiliJson(`https://api.bilibili.com/x/player/wbi/playurl?${signedQuery}`, {
            headers: { 'User-Agent': UA, 'Referer': REFERER, 'Cookie': cookie }
        });
        if (pData.code === 0 && pData.data?.durl?.[0]?.url) {
            return { url: pData.data.durl[0].url, quality: pData.data.quality };
        }
        throw new Error(pData.message || ERROR_MAP[pData.code] || '取流失败');
    };

    for (const qn of qualities) {
        const lines = [
            () => tryAppLine(qn, APP_KEYS.ios),
            () => tryAppLine(qn, APP_KEYS.tv),
        ];
        if (navAvailable) lines.push(() => tryWebLine(qn));

        for (const line of lines) {
            try {
                return await line();
            } catch (e) {
                if (e instanceof AntiCrawlError || e.name === 'AntiCrawlError') {
                    sawAntiCrawl = true;
                } else {
                    lastError = e.message;
                }
            }
        }
    }

    if (sawAntiCrawl && !lastError) throw new AntiCrawlError();
    throw new Error(lastError || (sawAntiCrawl ? ANTI_CRAWL_MSG : "视频解析失败"));
}

async function resolveVideo(bvid, qn, host) {
    const cookie = await getAntiCrawlCookie();

    const vData = await fetchBiliJson(`https://api.bilibili.com/x/web-interface/view?bvid=${bvid}`, {
        headers: { 'User-Agent': UA, 'Referer': REFERER, 'Cookie': cookie }
    });
    if (vData.code !== 0) throw new Error(ERROR_MAP[vData.code] || vData.message);

    const { cid, title, pic, owner } = vData.data;
    const videoStream = await getPlayUrlWithFallback(bvid, cid, qn || 116, cookie);

    const playableUrl = `${host}/proxy?url=${encodeURIComponent(videoStream.url)}&name=${encodeURIComponent(title)}`;
    const downloadUrl = `${host}/proxy?url=${encodeURIComponent(videoStream.url)}&name=${encodeURIComponent(title)}&dl=1`;

    return { title, pic, bvid, author: owner.name, playableUrl, downloadUrl, quality: videoStream.quality, isLive: false };
}

async function resolveLive(roomId, host) {
    const infoRes = await proxiedFetch(`https://api.live.bilibili.com/room/v1/Room/get_info?room_id=${roomId}`, {
        headers: { 'User-Agent': UA, 'Referer': LIVE_REFERER }
    });
    const infoData = await infoRes.json();
    if (infoData.code !== 0) throw new Error("直播间不存在");

    const { title, user_cover, keyframe, live_status, room_id: realRoomId, uid } = infoData.data;
    if (live_status !== 1 && live_status !== 2) throw new Error("主播未开播");

    const buvid = await getBuvid();
    const getHeaders = () => ({
        'User-Agent': UA_MOBILE,
        'Referer': `https://live.bilibili.com/${realRoomId}`,
        'Origin': 'https://live.bilibili.com',
        'Cookie': `buvid3=${buvid}`
    });

    const fetchStreamLegacy = async () => {
        const api = `https://api.live.bilibili.com/room/v1/Room/playUrl?cid=${realRoomId}&platform=h5&quality=3`;
        try {
            const res = await proxiedFetch(api, { headers: getHeaders() });
            const data = await res.json();
            if (data.data?.durl?.[0]?.url) {
                const url = data.data.durl[0].url;
                const isCN = url.includes('cn-');
                return { url, nodeType: isCN ? 'CN' : 'OV' };
            }
        } catch (e) { }
        return null;
    };

    const fetchStreamV2 = async () => {
        const api = `https://api.live.bilibili.com/xlive/web-room/v2/index/getRoomPlayInfo?room_id=${realRoomId}&protocol=0,1&format=0,1,2&codec=0,1&platform=h5&qn=150`;
        try {
            const res = await proxiedFetch(api, { headers: getHeaders() });
            const data = await res.json();
            const streams = data.data?.playurl_info?.playurl?.stream;
            if (!streams) return null;
            for (const s of streams) {
                if (s.format?.[0]?.codec?.[0]) {
                    const codecInfo = s.format[0].codec[0];
                    const urlInfos = codecInfo.url_info;
                    const cnNode = urlInfos.find(u => u.host.includes('cn-'));
                    if (cnNode) {
                        return { url: cnNode.host + codecInfo.base_url + cnNode.extra, nodeType: 'CN' };
                    }
                    return { url: urlInfos[0].host + codecInfo.base_url + urlInfos[0].extra, nodeType: 'OV' };
                }
            }
        } catch (e) { }
        return null;
    };

    let result = await fetchStreamLegacy();
    if (!result) result = await fetchStreamV2();
    if (!result) throw new Error("获取直播流失败");

    const playableUrl = `${host}/proxy?url=${encodeURIComponent(result.url)}&live=1&m3u8_direct=1`;
    const isHLS = result.url.includes('.m3u8');
    const formatStr = `${isHLS ? 'HLS' : 'FLV'} (${result.nodeType})`;

    return {
        title,
        pic: user_cover || keyframe,
        author: `UID:${uid}`,
        playableUrl,
        downloadUrl: result.url,
        quality: 0,
        isLive: true,
        format: formatStr,
        nodeType: result.nodeType
    };
}

const UI = (host) => `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="referrer" content="no-referrer">
    <title>Bilibili 解析</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+SC:wght@400;700;900&display=swap" rel="stylesheet">
    <style>
        body { background: #0f172a; font-family: 'Noto Sans SC', sans-serif; }
        .glass { background: rgba(30, 41, 59, 0.7); backdrop-filter: blur(20px); border: 1px solid rgba(255, 255, 255, 0.1); }
        .bg-gradient-mesh { background: radial-gradient(at 0% 0%, hsla(253,16%,7%,1) 0, transparent 50%), radial-gradient(at 50% 0%, hsla(225,39%,30%,1) 0, transparent 50%), radial-gradient(at 100% 0%, hsla(339,49%,30%,1) 0, transparent 50%); position: fixed; inset: 0; z-index: -1; }
        #bg-cover { position: fixed; inset: 0; z-index: -1; opacity: 0; transition: 1s; background-size: cover; background-position: center; filter: blur(30px) brightness(0.4); transform: scale(1.1); }
        .toast { position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%) translateY(100px); background: rgba(0,0,0,0.8); color: white; padding: 10px 20px; border-radius: 50px; transition: 0.3s; z-index: 100; }
        .toast.show { transform: translateX(-50%) translateY(0); }
        .toast.warn { background: rgba(180,80,0,0.9); }
        .mode-btn { transition: all 0.2s; }
        .mode-btn.active { background: linear-gradient(to right, #2563eb, #4f46e5); color: white; }
    </style>
</head>
<body class="text-slate-100 min-h-screen flex flex-col items-center justify-center p-4">
    <div class="bg-gradient-mesh"></div>
    <div id="bg-cover"></div>

    <div class="w-full max-w-lg relative z-10">
        <div class="text-center mb-6 space-y-1">
            <h1 class="text-4xl font-black tracking-tighter text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-pink-500">BILI PARSER</h1>
            <p class="text-xs font-bold text-slate-500 tracking-[0.4em] uppercase">v3.2</p>
        </div>

        <div class="flex justify-center mb-4">
            <div class="glass rounded-full p-1 flex gap-1">
                <button id="modeVideo" onclick="setMode('video')" class="mode-btn active px-4 py-2 rounded-full text-sm font-bold">📺 视频</button>
                <button id="modeLive" onclick="setMode('live')" class="mode-btn px-4 py-2 rounded-full text-sm font-bold text-slate-400 hover:text-white">📡 直播</button>
            </div>
        </div>

        <div class="glass rounded-3xl p-6 space-y-4">
            <div id="videoPanel" class="space-y-3">
                <input type="text" id="videoInput" placeholder="粘贴 BV号 / 视频链接..." 
                    class="w-full bg-slate-900/60 border border-slate-700/50 rounded-xl px-4 py-4 text-sm focus:ring-2 focus:ring-blue-500 outline-none text-center">
                
                <div class="flex gap-2">
                    <select id="videoQn" class="bg-slate-900/60 border border-slate-700/50 rounded-xl px-3 py-3 text-xs text-slate-300 w-1/3 text-center">
                        <option value="116">1080P+</option>
                        <option value="80" selected>1080P</option>
                        <option value="64">720P</option>
                        <option value="32">480P</option>
                    </select>
                    <button onclick="parseVideo()" class="flex-1 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 font-bold py-3 rounded-xl shadow-lg active:scale-95">解析视频</button>
                </div>

                <div class="flex justify-between items-center px-1 pt-1">
                    <span class="text-[10px] text-slate-500 font-bold tracking-widest">OPTIONS</span>
                    <label class="flex items-center gap-2 cursor-pointer">
                        <input type="checkbox" id="questMode" class="peer hidden">
                        <div class="w-3.5 h-3.5 rounded border border-slate-500 peer-checked:bg-blue-500 peer-checked:border-blue-500 flex items-center justify-center">
                            <svg class="w-2.5 h-2.5 text-white hidden peer-checked:block" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="4" d="M5 13l4 4L19 7"></path></svg>
                        </div>
                        <span class="text-xs text-slate-400 peer-checked:text-blue-400">Quest 兼容</span>
                    </label>
                </div>
            </div>

            <div id="livePanel" class="hidden space-y-3">
                <input type="text" id="liveInput" placeholder="输入直播房间号..." 
                    class="w-full bg-slate-900/60 border border-slate-700/50 rounded-xl px-4 py-4 text-sm focus:ring-2 focus:ring-pink-500 outline-none text-center">
                
                <button onclick="parseLive()" class="w-full bg-gradient-to-r from-pink-600 to-rose-600 hover:from-pink-500 font-bold py-3 rounded-xl shadow-lg active:scale-95">解析直播</button>
                
                <p class="text-[10px] text-slate-500 text-center">⚠️ OV 节点可能无法播放，需多尝试几次</p>
            </div>

            <div id="loader" class="hidden py-8 text-center"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-white"></div></div>

            <div id="result" class="hidden space-y-4 pt-4 border-t border-white/5">
                <video id="inlineVideo" class="hidden w-full rounded-xl bg-black" controls playsinline></video>
                <div class="flex gap-4 items-start">
                    <img id="resPic" referrerpolicy="no-referrer" class="w-28 h-16 object-cover rounded-lg shadow-md bg-slate-800 shrink-0">
                    <div class="min-w-0 flex-1 space-y-1">
                        <h3 id="resTitle" class="text-sm font-bold leading-tight line-clamp-2"></h3>
                        <p id="resAuthor" class="text-[10px] text-slate-400 truncate"></p>
                        <div class="flex items-center gap-2">
                            <span id="resTag" class="text-[10px] bg-pink-500/20 text-pink-300 px-1.5 py-0.5 rounded font-bold uppercase">VIDEO</span>
                            <span id="resQuality" class="text-[10px] bg-blue-500/20 text-blue-300 px-1.5 py-0.5 rounded">1080P</span>
                        </div>
                    </div>
                </div>
                <div class="relative">
                    <input id="link" readonly class="w-full bg-slate-900/40 border border-slate-700/50 rounded-xl px-4 py-3 text-xs text-slate-300 outline-none font-mono tracking-tight">
                    <button onclick="copyLink()" class="absolute right-2 top-2 bg-slate-700/50 hover:bg-slate-600 text-xs px-3 py-1 rounded-lg transition">复制</button>
                </div>
                <div class="grid grid-cols-2 gap-3">
                    <a id="btnPreview" target="_blank" class="flex items-center justify-center bg-slate-700/50 hover:bg-slate-700 py-3 rounded-xl text-sm font-bold transition">预览</a>
                    <a id="btnDownload" href="#" class="flex items-center justify-center bg-gradient-to-r from-pink-600 to-rose-600 hover:from-pink-500 hover:to-rose-500 py-3 rounded-xl text-sm font-bold shadow-lg transition transform hover:-translate-y-0.5 text-center">下载</a>
                </div>
            </div>
        </div>

        <div id="historyArea" class="hidden mt-6 glass rounded-3xl p-5">
            <h4 class="text-xs font-bold text-slate-500 uppercase mb-3 flex justify-between"><span>最近解析</span><span onclick="clearHistory()" class="cursor-pointer hover:text-white">清除</span></h4>
            <div id="historyList" class="space-y-2"></div>
        </div>

        <p class="text-center text-[10px] text-slate-600 mt-4"></p>
    </div>

    <div id="toast" class="toast">消息</div>

    <script>
        let currentMode = 'video';
        let currentPlayableUrl = '';
        let isCurrentLive = false;
        const PREFETCH_WINDOW_SIZE = 10;
        let hlsPreview = null;
        let seekAbortController = null;
        let levelFragments = new Map();
        let seekGeneration = 0;

        function setMode(mode) {
            currentMode = mode;
            document.querySelectorAll('.mode-btn').forEach(b => {
                b.classList.remove('active', 'text-white');
                b.classList.add('text-slate-400');
            });
            document.getElementById(mode === 'video' ? 'modeVideo' : 'modeLive').classList.add('active', 'text-white');
            document.getElementById(mode === 'video' ? 'modeVideo' : 'modeLive').classList.remove('text-slate-400');
            
            document.getElementById('videoPanel').style.display = mode === 'video' ? 'block' : 'none';
            document.getElementById('livePanel').style.display = mode === 'live' ? 'block' : 'none';
            document.getElementById('result').classList.add('hidden');
            document.getElementById('historyArea').classList.toggle('hidden', mode !== 'video' || !hasHistory());
        }

        function hasHistory() { return JSON.parse(localStorage.getItem('bili_history') || '[]').length > 0; }

        function showToast(msg, type='success') {
            const t = document.getElementById('toast');
            t.innerText = msg;
            t.className = 'toast show ' + (type === 'warn' ? 'warn' : '');
            setTimeout(() => t.classList.remove('show'), 2500);
        }


        function loadHistory() {
            const h = JSON.parse(localStorage.getItem('bili_history') || '[]');
            const list = document.getElementById('historyList'); const area = document.getElementById('historyArea');
            list.innerHTML = ''; if (h.length === 0 || currentMode !== 'video') { area.classList.add('hidden'); return; }
            area.classList.remove('hidden');
            h.forEach(item => {
                const div = document.createElement('div');
                div.className = 'flex items-center gap-3 p-2 hover:bg-white/5 rounded-lg cursor-pointer';
                div.onclick = () => { document.getElementById('videoInput').value = item.bvid; parseVideo(); };
                div.innerHTML = \`<div class="w-10 h-6 bg-slate-800 rounded bg-cover bg-center" style="background-image:url('\${item.pic}')"></div><p class="text-xs truncate text-slate-300 flex-1">\${item.title}</p>\`;
                list.appendChild(div);
            });
        }
        function saveHistory(data) {
            let h = JSON.parse(localStorage.getItem('bili_history') || '[]'); 
            h = h.filter(x => x.bvid !== data.bvid);
            h.unshift({ bvid: data.bvid, title: data.title, pic: data.pic });
            if (h.length > 5) h.pop(); 
            localStorage.setItem('bili_history', JSON.stringify(h)); 
            loadHistory();
        }
        function clearHistory() { localStorage.removeItem('bili_history'); loadHistory(); }
        loadHistory();

        function copyLink() {
            const link = document.getElementById('link').value;
            if(link) { navigator.clipboard.writeText(link); showToast('已复制'); }
        }

        async function parseVideo() {
            const input = document.getElementById('videoInput').value;
            const isQuest = document.getElementById('questMode').checked;
            const qn = isQuest ? 64 : document.getElementById('videoQn').value;
            if (!input) { showToast('请输入内容'); return; }
            
            document.getElementById('loader').classList.remove('hidden'); 
            document.getElementById('result').classList.add('hidden');
            document.getElementById('bg-cover').style.opacity = '0';

            try {
                const res = await fetch('/api/video?text=' + encodeURIComponent(input) + '&qn=' + qn);
                const data = await res.json();
                if (data.status === 'success') {
                    showResult(data, isQuest);
                    saveHistory(data);
                } else showToast(data.message);
            } catch (e) { showToast('请求失败'); } 
            finally { document.getElementById('loader').classList.add('hidden'); }
        }

        async function parseLive() {
            const input = document.getElementById('liveInput').value;
            if (!input) { showToast('请输入房间号'); return; }
            
            document.getElementById('loader').classList.remove('hidden'); 
            document.getElementById('result').classList.add('hidden');
            document.getElementById('bg-cover').style.opacity = '0';

            try {
                const res = await fetch('/api/live?room=' + encodeURIComponent(input));
                const data = await res.json();
                if (data.status === 'success') {
                    showResult(data, false);
                } else showToast(data.message);
            } catch (e) { showToast('请求失败'); } 
            finally { document.getElementById('loader').classList.add('hidden'); }
        }

        function getPreviewUrl(data) { return data.playableUrl; }
        function startInlinePreview(url) {
            const video = document.getElementById('inlineVideo');
            video.classList.remove('hidden');
            if (hlsPreview) { hlsPreview.destroy(); hlsPreview = null; }
            if (!window.Hls || !Hls.isSupported()) { video.src = url; return; }
            hlsPreview = new Hls({ enableWorker: true, startLevel: 0, autoStartLoad: true, maxBufferLength: 45, maxMaxBufferLength: 90, backBufferLength: 60, fragLoadingMaxRetry: 4, fragLoadingRetryDelay: 500 });
            hlsPreview.loadSource(url);
            hlsPreview.attachMedia(video);
            hlsPreview.on(Hls.Events.LEVEL_LOADED, (_event, data) => {
                const fragments = (data.details?.fragments || []).map(f => ({ start: f.start, duration: f.duration, url: f.url }));
                if (fragments.length) levelFragments.set(data.level, fragments);
            });
            const prefetch = (time) => {
                if (seekAbortController) seekAbortController.abort();
                seekAbortController = new AbortController();
                const fragments = Array.from(levelFragments.values())[0] || [];
                let index = fragments.findIndex(f => f.start <= time && time < f.start + f.duration);
                if (index < 0) index = fragments.findIndex(f => f.start >= time);
                if (index < 0) index = fragments.length - 1;
                const selected = fragments.slice(index, index + PREFETCH_WINDOW_SIZE);
                Promise.allSettled(selected.map(f => fetch(f.url, { cache: 'force-cache', signal: seekAbortController.signal }))).catch(() => {});
            };
            video.addEventListener('seeking', () => { hlsPreview.stopLoad(); hlsPreview.startLoad(video.currentTime); prefetch(video.currentTime); });
            hlsPreview.on(Hls.Events.ERROR, (_event, detail) => { if (detail.fatal && detail.type === Hls.ErrorTypes.NETWORK_ERROR) { hlsPreview.stopLoad(); hlsPreview.startLoad(video.currentTime); } });
        }

        function showResult(data, isQuest) {
            const pic = data.pic.replace('http:', 'https:');
            currentPlayableUrl = data.playableUrl;
            isCurrentLive = data.isLive;
            document.getElementById('resPic').src = pic;
            document.getElementById('bg-cover').style.backgroundImage = "url('" + pic + "')";
            document.getElementById('bg-cover').style.opacity = '0.4';
            document.getElementById('resTitle').innerText = data.title;
            document.getElementById('resAuthor').innerText = data.author;
            
            const tag = document.getElementById('resTag');
            const qn = document.getElementById('resQuality');
            const btnDl = document.getElementById('btnDownload');
            
            const btnPreview = document.getElementById('btnPreview');
            document.getElementById('link').value = data.playableUrl;
            btnPreview.href = data.playableUrl;
            if (!data.isLive) {
                btnPreview.onclick = (event) => { event.preventDefault(); startInlinePreview(getPreviewUrl(data)); };
            }
            if (data.isLive) {
                tag.innerText = 'LIVE';
                tag.className = 'text-[10px] bg-red-500/20 text-red-300 px-1.5 py-0.5 rounded font-bold uppercase';
                qn.innerText = data.format;
                btnDl.innerText = '复制直链';
                btnDl.href = '#';
                btnDl.onclick = (e) => { e.preventDefault(); navigator.clipboard.writeText(data.downloadUrl); showToast('直链已复制'); };
            } else {
                tag.innerText = 'VIDEO';
                tag.className = 'text-[10px] bg-pink-500/20 text-pink-300 px-1.5 py-0.5 rounded font-bold uppercase';
                const qnMap = { 116: '1080P+', 80: '1080P', 64: '720P', 32: '480P' };
                qn.innerText = isQuest ? 'Quest' : (qnMap[data.quality] || 'MP4');
                btnDl.innerText = '下载';
                btnDl.href = data.downloadUrl;
                btnDl.onclick = null;
            }
            
            document.getElementById('result').classList.remove('hidden');
        }
    </script>
</body>
</html>
`;

async function handleProxy(request, url, host) {
    const target = url.searchParams.get('url');
    const name = url.searchParams.get('name');
    const isDownload = url.searchParams.get('dl') === '1';
    if (!target) return new Response('Missing URL', { status: 400 });
    const isLive = url.searchParams.get('live') === '1' || target.includes('live-bvc');
    const m3u8Direct = url.searchParams.get('m3u8_direct') === '1';
    try {
        const targetUrl = new URL(target);
        if (!targetUrl.hostname.includes('bilivideo') && !targetUrl.hostname.includes('hdslb') && !targetUrl.hostname.includes('akamaized')) {
            return new Response('Forbidden', { status: 403 });
        }
    } catch (e) { return new Response('Invalid URL', { status: 400 }); }

    const isM3u8 = target.includes('.m3u8');

    const newHeaders = new Headers({
        'Referer': isLive ? LIVE_REFERER : REFERER,
        'User-Agent': isLive ? UA_MOBILE : UA,
        'Origin': isLive ? 'https://live.bilibili.com' : 'https://www.bilibili.com'
    });
    
    const forwardHeaders = ['Range'];
    for (const h of forwardHeaders) {
        if (request.headers.has(h)) newHeaders.set(h, request.headers.get(h));
    }

    try {
        newHeaders.set('Accept-Encoding', 'identity');
        let response;
        for (let attempt = 1; attempt <= 3; attempt += 1) {
            if (attempt > 1) {
                newHeaders.set('Cache-Control', 'no-cache');
                newHeaders.set('X-Proxy-Retry', String(attempt));
            }
            response = await fetch(target, { headers: newHeaders, cf: { cacheEverything: false } });
            if (response.status < 500 && response.status !== 403 && response.status !== 429) break;
            try { await response.body?.cancel(); } catch (e) {}
            await new Promise(resolve => setTimeout(resolve, attempt * 150));
        }

        const responseHeaders = new Headers();
        responseHeaders.set('Access-Control-Allow-Origin', '*');
        responseHeaders.set('Access-Control-Expose-Headers', 'Content-Type, Content-Length, Content-Range, Accept-Ranges, Cache-Control');
        responseHeaders.set('Cache-Control', response.ok ? 'public, max-age=86400, immutable' : 'no-store');

        if (isM3u8) {
            let m3u8Content = await response.text();
            const baseUrl = target.substring(0, target.lastIndexOf('/') + 1);
            m3u8Content = m3u8Content.split('\n').map(line => {
                line = line.trim();
                if (line && !line.startsWith('#')) {
                    let absoluteUrl = line.startsWith('http') ? line : baseUrl + line;
                    return `${host}/proxy?url=${encodeURIComponent(absoluteUrl)}`;
                }
                return line;
            }).join('\n');
            responseHeaders.set('Content-Type', 'application/vnd.apple.mpegurl');
            return new Response(m3u8Content, { status: 200, headers: responseHeaders });
        }

        const headersToCopy = ['Content-Type', 'Content-Length', 'Accept-Ranges', 'Content-Range', 'Cache-Control'];
        for (const h of headersToCopy) {
            if (response.headers.has(h)) responseHeaders.set(h, response.headers.get(h));
        }
        
        responseHeaders.set('Access-Control-Expose-Headers', headersToCopy.join(', '));
        if (name && isDownload) {
            responseHeaders.set("Content-Disposition", `attachment; filename="${encodeURIComponent(name)}.mp4"`);
        }

        if (response.status === 204 || response.status === 304) {
            return new Response(undefined, { status: response.status, headers: responseHeaders });
        }

        return new Response(response.body, { status: response.status, headers: responseHeaders });
    } catch (e) {
        if (e.name === 'AbortError') return new Response(null, { status: 499 });
        return new Response('Proxy Error: ' + e.message, { status: 502 });
    }
}

export default {
    async fetch(request, env, ctx) {
        WORKER_ENV = env || {};
        const url = new URL(request.url);
        const host = url.origin;
        const path = url.pathname;

        if (request.method === 'OPTIONS') {
            return new Response(null, { headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS', 'Access-Control-Allow-Headers': '*' } });
        }

        const liveMatch = path.match(/^\/live\/(\d+)$/);
        if (liveMatch) {
            try {
                const res = await resolveLive(liveMatch[1], host);
                return Response.redirect(res.downloadUrl, 302);
            } catch (e) {
                return new Response(`Error: ${e.message}`, { status: 500 });
            }
        }

        if (path === '/proxy') return handleProxy(request, url, host);
        if (path === '/' || path === '') return new Response(UI(host), { headers: { 'Content-Type': 'text/html;charset=UTF-8' } });

        if (path === '/v') return new Response(JSON.stringify({
            version: VERSION,
            VERCEL_PROXY: WORKER_ENV.VERCEL_PROXY ? '✅ 已配置' : '❌ 未配置',
            PROXY_TOKEN: WORKER_ENV.PROXY_TOKEN ? '✅ 已配置' : '❌ 未配置',
        }), { headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });

        const decodedPath = decodeURIComponent(path);
        const bvMatch = decodedPath.match(/(BV[a-zA-Z0-9]{10})/i);
        const b23Match = decodedPath.match(/b23\.tv\/([a-zA-Z0-9]+)/i);
        const liveRoomMatch = decodedPath.match(/live\.bilibili\.com\/(\d+)/i);

        if (!path.startsWith('/api/') && !path.startsWith('/proxy') && !path.startsWith('/v') && path !== '/') {
            if (liveRoomMatch) {
                try {
                    const res = await resolveLive(liveRoomMatch[1], host);
                    return Response.redirect(res.downloadUrl, 302);
                } catch (e) {
                    return new Response(JSON.stringify({ status: 'error', message: e.message }), { status: 200, headers: { 'Content-Type': 'application/json' } });
                }
            }

            if (bvMatch || b23Match) {
                let finalBvid = bvMatch ? bvMatch[1] : null;
                if (b23Match && !finalBvid) {
                    try {
                        const res = await fetch(`https://b23.tv/${b23Match[1]}`, { method: 'GET', redirect: 'manual' });
                        if (res.status >= 300 && res.status < 400) {
                            const loc = res.headers.get('location');
                            finalBvid = loc?.match(/(BV[a-zA-Z0-9]{10})/i)?.[1];
                            const shortLiveMatch = loc?.match(/live\.bilibili\.com\/(\d+)/i);
                            if (shortLiveMatch && !finalBvid) {
                                const resLive = await resolveLive(shortLiveMatch[1], host);
                                return Response.redirect(resLive.downloadUrl, 302);
                            }
                        }
                    } catch (e) {}
                }

                if (finalBvid) {
                    const qn = parseInt(url.searchParams.get('qn')) || 116;
                    try {
                        const res = await resolveVideo(finalBvid, qn, host);
                        return Response.redirect(res.downloadUrl, 302);
                    } catch (e) {
                        const message = (e instanceof AntiCrawlError || e.name === 'AntiCrawlError') ? ANTI_CRAWL_MSG : e.message;
                        return new Response(JSON.stringify({ status: 'error', message }), {
                            status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
                        });
                    }
                }
            }
        }

        if (path === '/api/video') {
            let text = url.searchParams.get('text');
            const qn = parseInt(url.searchParams.get('qn')) || 116;
            if (!text) return new Response(JSON.stringify({ status: 'error', message: 'Missing text' }), { status: 400 });

            const b23Match = text.match(/b23\.tv\/([a-zA-Z0-9]+)/);
            if (b23Match) {
                try {
                    const res = await fetch(`https://b23.tv/${b23Match[1]}`, { method: 'GET', redirect: 'manual' });
                    if (res.status >= 300 && res.status < 400) {
                        text = res.headers.get('location') || text;
                    }
                } catch (e) {
                }
            }

            const bvMatch = text.match(/(BV[a-zA-Z0-9]{10})/);
            if (!bvMatch) return new Response(JSON.stringify({ status: 'error', message: '无效的 BV 号' }), { status: 200, headers: { 'Content-Type': 'application/json' } });

            try {
                const res = await resolveVideo(bvMatch[1], qn, host);
                return new Response(JSON.stringify({ status: 'success', ...res }), {
                    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' }
                });
            } catch (e) {
                const message = (e instanceof AntiCrawlError || e.name === 'AntiCrawlError') ? ANTI_CRAWL_MSG : e.message;
                return new Response(JSON.stringify({ status: 'error', message }), {
                    status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
                });
            }
        }

        if (path === '/api/live') {
            let room = url.searchParams.get('room');
            if (!room) return new Response(JSON.stringify({ status: 'error', message: 'Missing room' }), { status: 400 });

            const b23Match = room.match(/b23\.tv\/([a-zA-Z0-9]+)/);
            if (b23Match) {
                try {
                    const res = await fetch(`https://b23.tv/${b23Match[1]}`, { method: 'GET', redirect: 'manual' });
                    if (res.status >= 300 && res.status < 400) {
                        room = res.headers.get('location') || room;
                    }
                } catch (e) {}
            }

            const roomId = room.match(/live\.bilibili\.com\/(\d+)/)?.[1] || room.match(/(?<![a-zA-Z])(\d+)(?![a-zA-Z])/)?.[1] || room.match(/(\d+)/)?.[1];
            if (!roomId) return new Response(JSON.stringify({ status: 'error', message: '无效的房间号' }), { status: 200, headers: { 'Content-Type': 'application/json' } });

            try {
                const res = await resolveLive(roomId, host);
                return new Response(JSON.stringify({ status: 'success', ...res }), {
                    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
                });
            } catch (e) {
                return new Response(JSON.stringify({ status: 'error', message: e.message }), {
                    status: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
                });
            }
        }

        return new Response('Not Found', { status: 404 });
    }
}

export { resolveVideo, getPlayUrlWithFallback, signWbi, getMixinKeyFromNav, appSign, getBuvid, getAntiCrawlCookie, AntiCrawlError, ANTI_CRAWL_MSG, fetchBiliJson };
