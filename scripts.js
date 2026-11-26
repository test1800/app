
/* =====================================================
   WORKER
===================================================== */
const WORKER_URLS = {
    crypto: [
        "https://coin.rxw.workers.dev/",
        "https://proxy.cors.dev/https://coin.rxw.workers.dev/"
    ],
    email:
        "https://email.rxw.workers.dev/",
    // First top dollar price: USDD + YEKUSD workers.
    usd: [
        "https://usdd.rxw.workers.dev/",
        "https://proxy.cors.dev/https://usdd.rxw.workers.dev/",
        "https://yekusd-rateusd.rxw.workers.dev/",
        "https://proxy.cors.dev/https://yekusd-rateusd.rxw.workers.dev/"
    ],
    // Second top dollar price: BahaUSD worker.
    yekusd: [
        "https://bahausd.rxw.workers.dev/",
        "https://proxy.cors.dev/https://bahausd.rxw.workers.dev/"
    ],
    signal: [
        "https://coinsignaltable.rxw.workers.dev/",
        "https://proxy.cors.dev/https://coinsignaltable.rxw.workers.dev/"
    ]
};
/* =====================================================
   API TRANSPORT / RELIABILITY LAYER (R01)
   - Full-response timeout (headers AND body)
   - Bounded concurrent network requests
   - Identical in-flight request de-duplication
   - Per-attempt diagnostics for troubleshooting
===================================================== */
const API_MAX_CONCURRENT_REQUESTS = 8;
const API_QUEUE_TIMEOUT_MS = 15000;
const API_DIAGNOSTIC_LIMIT = 100;
let apiActiveRequests = 0;
const apiRequestQueue = [];
const apiInFlightRequests = new Map();
const apiDiagnosticLog = [];
let apiNonIdempotentRequestSequence = 0;
try {
    const savedDiagnostics = JSON.parse(sessionStorage.getItem("financialDashboardApiDiagnostics") || "[]");
    if (Array.isArray(savedDiagnostics)) apiDiagnosticLog.push(...savedDiagnostics.slice(-API_DIAGNOSTIC_LIMIT));
} catch (e) {}

function recordApiDiagnostic(entry) {
    const item = {
        at: new Date().toISOString(),
        durationMs: Math.max(0, Number(entry?.durationMs) || 0),
        method: String(entry?.method || "GET"),
        url: String(entry?.url || "").slice(0, 500),
        status: entry?.status !== null && entry?.status !== undefined && Number.isFinite(Number(entry.status)) ? Number(entry.status) : null,
        outcome: String(entry?.outcome || "unknown"),
        detail: String(entry?.detail || "").slice(0, 240)
    };
    apiDiagnosticLog.push(item);
    if (apiDiagnosticLog.length > API_DIAGNOSTIC_LIMIT) apiDiagnosticLog.shift();
    try {
        sessionStorage.setItem("financialDashboardApiDiagnostics", JSON.stringify(apiDiagnosticLog));
    } catch (e) {}
}

function getApiDiagnostics() {
    return apiDiagnosticLog.slice();
}

function acquireApiRequestSlot() {
    if (apiActiveRequests < API_MAX_CONCURRENT_REQUESTS) {
        apiActiveRequests++;
        return Promise.resolve(makeReleaseApiSlot());
    }
    return new Promise((resolve, reject) => {
        const waiter = { resolve, reject, timer: null, cancelled: false };
        waiter.timer = setTimeout(() => {
            waiter.cancelled = true;
            reject(new Error(`API request queue timeout (${API_QUEUE_TIMEOUT_MS}ms)`));
        }, API_QUEUE_TIMEOUT_MS);
        apiRequestQueue.push(waiter);
    });
}

function makeReleaseApiSlot() {
    let released = false;
    return function releaseApiSlot() {
        if (released) return;
        released = true;
        while (apiRequestQueue.length) {
            const next = apiRequestQueue.shift();
            if (!next || next.cancelled) continue;
            clearTimeout(next.timer);
            next.resolve(makeReleaseApiSlot());
            return;
        }
        apiActiveRequests = Math.max(0, apiActiveRequests - 1);
    };
}

function apiRequestKey(url, options) {
    const method = String(options?.method || "GET").toUpperCase();
    const body = typeof options?.body === "string" ? options.body : "";
    let headers = "";
    try {
        if (options?.headers instanceof Headers) headers = Array.from(options.headers.entries()).sort().map(([k,v]) => `${k}:${v}`).join("|");
        else if (options?.headers && typeof options.headers === "object") headers = Object.entries(options.headers).sort(([a],[b]) => a.localeCompare(b)).map(([k,v]) => `${k}:${v}`).join("|");
    } catch (e) {}
    const requestContext = [options?.mode, options?.credentials, options?.redirect, options?.referrerPolicy, options?.integrity].map(v => String(v || "")).join("|");
    const base = `${method} ${String(url)} ${body} ${headers} ${requestContext}`;
    // Only safe/idempotent reads may share an in-flight request. Concurrent
    // POSTs (email/remote commands) must each reach the server independently.
    if (method !== "GET" && method !== "HEAD") return `${base} #${++apiNonIdempotentRequestSequence}`;
    return base;
}

function responseFromApiSnapshot(snapshot) {
    const noBodyStatus = [101, 204, 205, 304].includes(snapshot.status);
    const body = noBodyStatus ? null : snapshot.body.slice(0);
    return new Response(body, {
        status: snapshot.status,
        statusText: snapshot.statusText,
        headers: snapshot.headers
    });
}

function fetchWithTimeout(url, options = {}, timeout = PRICE_REQUEST_TIMEOUT) {
    const target = String(url || "").trim();
    if (!target) return Promise.reject(new Error("API URL is empty"));
    const requestOptions = options || {};
    const key = `${apiRequestKey(target, requestOptions)} timeout=${Math.max(1, Number(timeout) || PRICE_REQUEST_TIMEOUT)}`;
    const existing = apiInFlightRequests.get(key);
    const startedAt = Date.now();

    let shared = existing;
    if (!shared) {
        shared = (async () => {
            let releaseSlot = null;
            const controller = typeof AbortController === "function" ? new AbortController() : null;
            let timer = null;
            let timedOut = false;
            const timeoutError = new Error(`API timeout after ${timeout}ms`);
            try {
                try {
                    releaseSlot = await acquireApiRequestSlot();
                } catch (queueError) {
                    recordApiDiagnostic({url: target, method: requestOptions.method, outcome: "queue-timeout", durationMs: Date.now() - startedAt, detail: queueError?.message || queueError});
                    throw queueError;
                }
                const operation = (async () => {
                    const response = await fetch(target, {
                        ...requestOptions,
                        cache: requestOptions.cache || "no-store",
                        signal: controller ? controller.signal : requestOptions.signal
                    });
                    // Buffer the complete body before clearing the timeout. The old
                    // implementation cleared its timer after headers, not after JSON/text.
                    const body = await response.arrayBuffer();
                    const headers = [];
                    response.headers?.forEach((value, name) => headers.push([name, value]));
                    return {
                        body,
                        status: response.status,
                        statusText: response.statusText,
                        headers
                    };
                })();
                const timeoutPromise = new Promise((_, reject) => {
                    timer = setTimeout(() => {
                        timedOut = true;
                        try { controller?.abort(); } catch (e) {}
                        reject(timeoutError);
                    }, Math.max(1, Number(timeout) || PRICE_REQUEST_TIMEOUT));
                });
                const snapshot = await Promise.race([operation, timeoutPromise]);
                recordApiDiagnostic({url: target, method: requestOptions.method, status: snapshot.status, outcome: snapshot.status >= 200 && snapshot.status < 300 ? "http-ok" : "http-error", durationMs: Date.now() - startedAt, detail: snapshot.statusText});
                return snapshot;
            } catch (error) {
                recordApiDiagnostic({url: target, method: requestOptions.method, outcome: timedOut || error?.name === "AbortError" ? "timeout" : "network-error", durationMs: Date.now() - startedAt, detail: error?.message || error});
                throw error;
            } finally {
                if (timer) clearTimeout(timer);
                if (releaseSlot) releaseSlot();
            }
        })();
        apiInFlightRequests.set(key, shared);
        shared.finally(() => {
            if (apiInFlightRequests.get(key) === shared) apiInFlightRequests.delete(key);
        }).catch(() => {});
    }
    return shared.then(responseFromApiSnapshot);
}

window.getApiDiagnostics = getApiDiagnostics;

function buildApiProxyFallbacks(targetUrl) {
    const target = String(targetUrl || "").trim();
    if (!target) {
        return [];
    }
    return [
        target,
        `https://proxy.cors.dev/${target}`
    ].filter((url, index, list) => list.indexOf(url) === index);
}
async function fetchWithFallback(urls, options = {}, timeout = 7000, validateResponse = null) {
    const queue = Array.isArray(urls) ? urls : buildApiProxyFallbacks(urls);
    let lastError = null;
    const chainStartedAt = Date.now();
    const chainBudgetMs = Math.min(20000, Math.max(timeout, timeout * Math.min(queue.length || 1, 3)));
    for (const url of queue) {
        const remainingMs = chainBudgetMs - (Date.now() - chainStartedAt);
        if (remainingMs <= 0) break;
        try {
            const response = await fetchWithTimeout(url, { ...options, cache: "no-store" }, Math.min(timeout, remainingMs));
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            if (typeof validateResponse === "function") {
                const valid = await validateResponse(response.clone());
                if (valid === false) throw new Error("Response validation failed");
            } else {
                const type = String(response.headers.get("content-type") || "").toLowerCase();
                if (type.includes("json")) await response.clone().json();
                else {
                    const body = await response.clone().text();
                    if (!body.trim()) throw new Error("Empty response body");
                }
            }
            return response;
        } catch (error) {
            lastError = error;
            console.warn("API/proxy attempt failed; trying next source:", url, error);
            recordApiDiagnostic({url, outcome: "fallback", detail: error?.message || error});
        }
    }
    throw lastError || new Error("All API/proxy URLs unavailable");
}


async function fetchApiWithProxyFallback(apiUrl, options = {}, timeout = PRICE_REQUEST_TIMEOUT, validatePayload = null) {
    const target = String(apiUrl || "").trim();
    if (!target) throw new Error("API URL is missing");
    let lastError = null;

    const validate = async response => {
        const payload = await response.clone().json();
        if (typeof validatePayload === "function") {
            const valid = await validatePayload(payload);
            if (valid === false) throw new Error("API payload validation failed");
        }
        return payload;
    };

    // Android native bridge first. Promise-returning bridges are bounded too.
    try {
        if (window.AndroidNotifications && typeof window.AndroidNotifications.fetchExternalMarketJson === "function") {
            let nativeResult = window.AndroidNotifications.fetchExternalMarketJson(target);
            if (nativeResult && typeof nativeResult.then === "function") {
                nativeResult = await Promise.race([
                    Promise.resolve(nativeResult),
                    new Promise((_, reject) => setTimeout(() => reject(new Error("Native market bridge timeout")), Math.min(timeout, 5000)))
                ]);
            }
            if (nativeResult) {
                const nativeResponse = new Response(String(nativeResult), {
                    status: 200,
                    headers: { "Content-Type": "application/json" }
                });
                await validate(nativeResponse);
                return nativeResponse;
            }
        }
    } catch (error) {
        lastError = error;
        console.warn("Native market API request failed; trying browser sources:", target, error);
        recordApiDiagnostic({url: target, outcome: "native-fallback", detail: error?.message || error});
    }

    const browserUrls = buildApiProxyFallbacks(target);
    const chainStartedAt = Date.now();
    const chainBudgetMs = Math.min(16000, Math.max(timeout, timeout * Math.min(browserUrls.length || 1, 2)));
    for (const url of browserUrls) {
        const remainingMs = chainBudgetMs - (Date.now() - chainStartedAt);
        if (remainingMs <= 0) break;
        try {
            const response = await fetchWithTimeout(url, { ...options, cache: "no-store" }, Math.min(timeout, remainingMs));
            if (!response.ok) throw new Error(`API HTTP ${response.status}`);
            await validate(response);
            return response;
        } catch (error) {
            lastError = error;
            console.warn("API source failed validation/request:", url, error);
            recordApiDiagnostic({url, outcome: "fallback", detail: error?.message || error});
        }
    }
    throw lastError || new Error("All API/proxy URLs unavailable");
}


/* =====================================================
   INSTANT NATIVE WIDGET SYNC
   The home-screen widget receives the exact values already
   rendered by the dashboard instead of waiting for its own
   polling timer. This keeps Widget + Ultra Compact in lockstep.
===================================================== */
let nativeWidgetSyncFrame = 0;
function syncNativeWidgetSnapshot(){
    if(!window.AndroidNotifications || typeof window.AndroidNotifications.syncWidgetSnapshot !== "function") return;
    if(nativeWidgetSyncFrame) return;
    nativeWidgetSyncFrame=requestAnimationFrame(()=>{
        nativeWidgetSyncFrame=0;
        try{
            const text=id=>document.getElementById(id)?.textContent?.trim()||"-";
            const direction=id=>{
                const el=document.getElementById(id);
                if(!el)return 0;
                if(el.classList.contains("up"))return 1;
                if(el.classList.contains("down"))return -1;
                return 0;
            };
            let signalHtml="";
            let signalDailyChangePercent=null;
            let crossoverDailyChangePercent=null;
            try{
                const candidates=typeof rfkjUltraCandidates==="function"?rfkjUltraCandidates():[];
                if(candidates.length){
                    const index=(typeof rfkjUltraIndex==="number"?rfkjUltraIndex:0)%candidates.length;
                    const st=candidates[index];
                    const coin=String(st?.coin||document.querySelector(`#rfkjCoin${st?.index||1}`)?.value||"").trim();
                    const price=text("rfkjUltraPrice");
                    signalDailyChangePercent=st?.latestDailyChangePercent!=null && Number.isFinite(Number(st.latestDailyChangePercent))?Number(st.latestDailyChangePercent):null;
                    const arrow=id=>{
                        const el=document.getElementById(id);
                        const cls=el?.className||"";
                        const value=el?.textContent?.trim()||"→";
                        const color=cls.includes("up")?"#FFFFFF":cls.includes("down")?"#38BDF8":"#F8FAFC";
                        return `<font color="${color}">${value}</font>`;
                    };
                    if(coin) signalHtml=`${coin} ${price} ${arrow("rfkjUltraRF")}${arrow("rfkjUltraST")}${arrow("rfkjUltraKJ")}`;
                }
            }catch(e){}

            // If no Signal Matrix is active, feed the first active Crossover to the widget.
            // Crossover fallback intentionally has no direction arrows.
            if(!signalHtml){
                try{
                    const crossovers=Array.isArray(crossoverInstances)?crossoverInstances:[];
                    for(const cross of crossovers){
                        if(!cross || !cross.running) continue;
                        const coin=String(cross.activeCoin||cross.coin||"").trim().toUpperCase();
                        let price=Number(cross.latestPrice);
                        const daily=Number(cross.dailyChangePercent ?? cross.dailyChange ?? cross.changePercent);
                        crossoverDailyChangePercent=Number.isFinite(daily)?daily:null;
                        let priceText="";
                        if(Number.isFinite(price)&&price>0){
                            priceText=typeof crossoverPriceText==="function"?crossoverPriceText(price,coin):String(price);
                        }else{
                            priceText=document.querySelector(`#crossoverLive${cross.index} .crossover-live-price`)?.textContent?.trim()||"";
                        }
                        if(coin && priceText && priceText!=="-"){
                            signalHtml=`${coin} ${priceText}`;
                            break;
                        }
                    }
                }catch(e){}
            }

            let globalAsset={type:"",symbol:"",price:"-",direction:0,dailyChangePercent:null};
            try{
                const selected=JSON.parse(localStorage.getItem(GLOBAL_PULSE_COMPACT_ASSET_KEY)||"null");
                if(selected&&selected.type){
                    globalAsset.type=String(selected.type);
                    globalAsset.symbol=String(selected.symbol||"");
                    const item=typeof globalPulseCompactAssetPrice==="function"
                        ?globalPulseCompactAssetPrice(globalAsset.type,globalAsset.symbol):null;
                    if(item){globalAsset.price=String(item.price||"-");globalAsset.direction=Number(item.direction)||0;globalAsset.dailyChangePercent=item.dailyChangePercent!=null && Number.isFinite(Number(item.dailyChangePercent))?Number(item.dailyChangePercent):(Number(item.direction)||0); }
                }
            }catch(e){}

            const snapshot={
                usd:text("usdTxtPrice"),
                yekusd:text("yekusd"),
                crypto:text("cryptoValue"),
                nobitex:text("currentPrice"),
                exir:text("exirPrice"),
                bitpin:text("bitpinPrice"),
                nobitexDirection:direction("currentPrice"),
                exirDirection:direction("exirPrice"),
                bitpinDirection:direction("bitpinPrice"),
                signalHtml,
                signalDailyChangePercent,
                crossoverDailyChangePercent,
                signalUpdatedAt:signalHtml ? Date.now() : 0,
                globalAssetType:globalAsset.type,
                globalAssetSymbol:globalAsset.symbol,
                globalAssetPrice:globalAsset.price,
                globalAssetFlashDirection:globalAsset.direction,
                globalAssetDailyChangePercent:globalAsset.dailyChangePercent,
                updated:new Date().toLocaleTimeString("en-US",{hour12:false})
            };
            window.AndroidNotifications.syncWidgetSnapshot(JSON.stringify(snapshot));
        }catch(e){
            console.warn("Native widget sync failed:",e);
        }
    });
}

/* =====================================================
   TOP PURCHASES
===================================================== */
const TOP_PURCHASES_COUNT = 6;
/* =====================================================
   CRYPTO SIGNAL BOARD
===================================================== */
const CRYPTO_ICON_FILES = {
    BTC:  "file/btc.png",
    DOGE: "file/doge.png",
    ETH:  "file/eth.png",
    BNB:  "file/bnb.png",
    ADA:  "file/ada.png",
    SOL:  "file/sol.png",
    POL:  "file/pol.png",
    AVAX: "file/avax.png",
    GRAM:  "file/gram.png",
    TRX:  "file/trx.png"
};
/*
   All crypto icons are local files next to index.html.
   They are also copied into the browser Cache Storage so
   they can still be displayed after the network is lost.
*/
const CRYPTO_ICON_CACHE_NAME = "financial-dashboard-crypto-icons-v2";
const cryptoIconSources = {};
async function prepareCryptoIconCache() {
    if (!("caches" in window)) {
        Object.assign(cryptoIconSources, CRYPTO_ICON_FILES);
        return;
    }
    try {
        const cache = await caches.open(CRYPTO_ICON_CACHE_NAME);
        await Promise.all(
            Object.entries(CRYPTO_ICON_FILES).map(
                async ([coin, url]) => {
                    try {
                        let cached = await cache.match(url);
                        if (!cached) {
                            const response = await fetch(
                                url,
                                { cache: "no-store" }
                            );
                            if (response.ok) {
                                await cache.put(
                                    url,
                                    response.clone()
                                );
                                cached = response;
                            }
                        }
                        if (cached) {
                            const blob = await cached.blob();
                            cryptoIconSources[coin] =
                                URL.createObjectURL(blob);
                        } else {
                            cryptoIconSources[coin] = url;
                        }
                    }
                    catch (error) {
                        cryptoIconSources[coin] = url;
                    }
                }
            )
        );
    }
    catch (error) {
        Object.assign(cryptoIconSources, CRYPTO_ICON_FILES);
    }
}
function getCryptoIconSrc(coin) {
    return (
        cryptoIconSources[coin] ||
        CRYPTO_ICON_FILES[coin] ||
        ""
    );
}
/* =====================================================
   SIGNAL MEMORY
===================================================== */
const SIGNAL_CACHE_KEY =
    "financialDashboardCryptoSignals-v1";
let previousSignals = {};
let nativeSignalBaselineApplied = false;
let lastSignalRenderSignature = "";
function saveSignalCache(data) {
    try {
        localStorage.setItem(
            SIGNAL_CACHE_KEY,
            JSON.stringify(data)
        );
    }
    catch (error) {}
}
function loadSignalCache() {
    try {
        const raw =
            localStorage.getItem(
                SIGNAL_CACHE_KEY
            );
        if (!raw) {
            return null;
        }
        const data = JSON.parse(raw);
        if (
            !data ||
            typeof data !== "object"
        ) {
            return null;
        }
        return data;
    }
    catch (error) {
        return null;
    }
}
function getSignalNumericValue(value) {
    if (value && typeof value === "object") {
        const nested = value.signal ?? value.type ?? value.action ?? value.direction ?? value.value ?? value.score;
        if (nested !== undefined && nested !== value) return getSignalNumericValue(nested);
    }
    const text = String(value ?? "").trim().toUpperCase();
    if (text === "BUY" || text === "BULL" || text === "BULLISH" || text === "LONG" || text.includes("BUY")) return 1;
    if (text === "SELL" || text === "BEAR" || text === "BEARISH" || text === "SHORT" || text.includes("SELL")) return -1;
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : 0;
}
function getSignalClass(value, offline = false) {
    if (offline) return "signal-offline";
    const numericValue = getSignalNumericValue(value);
    if (numericValue === 1) return "signal-buy";
    if (numericValue === -1) return "signal-sell";
    return "signal-neutral";
}
function renderCryptoSignals(data, offline = false) {
    const grid = document.getElementById("signalGrid");
    if (!grid || !data) return;
    const signalCoins = [
        "BTC", "BNB", "ETH", "DOGE", "ADA",
        "SOL", "GRAM", "AVAX", "POL", "TRX"
    ];
    const values = signalCoins.map(coin => getSignalNumericValue(data[coin] ?? 0));
    const signature = JSON.stringify({ values, offline: !!offline });
    if (signature !== lastSignalRenderSignature) {
        const fragment = document.createDocumentFragment();
        signalCoins.forEach((coin, index) => {
            const value = values[index];
            const box = document.createElement("div");
            box.className = `signal-box ${getSignalClass(value, offline)}${offline ? " offline" : ""}`;
            box.dataset.coin = coin;
            const img = document.createElement("img");
            img.src = getCryptoIconSrc(coin);
            img.alt = coin;
            img.loading = "eager";
            img.decoding = "async";
            const name = document.createElement("div");
            name.className = "signal-name";
            name.textContent = coin;
            box.appendChild(img);
            box.appendChild(name);
            fragment.appendChild(box);
        });
        grid.replaceChildren(fragment);
        lastSignalRenderSignature = signature;
    }
    const info = document.getElementById("signalInfo");
    if (info) {
        const updated = data.updated ? new Date(data.updated).toLocaleTimeString() : "-";
        const nextInfo = `${data.timeframe || "-"} • ${updated}${offline ? " • Cached" : ""}`;
        if (info.textContent !== nextInfo) info.textContent = nextInfo;
    }
    previousSignals = {};
    signalCoins.forEach(coin => {
        previousSignals[coin] = getSignalNumericValue(data[coin] ?? 0);
    });
}
/* =====================================================
   LOAD CRYPTO SIGNALS
===================================================== */
let cryptoSignalsLoadInProgress = null;
let cryptoSignalsRetryAt = 0;
function loadCryptoSignals() {
    if (cryptoSignalsLoadInProgress) return cryptoSignalsLoadInProgress;
    if (Date.now() < cryptoSignalsRetryAt) return Promise.resolve(false);
    cryptoSignalsLoadInProgress = loadCryptoSignalsInternal();
    return cryptoSignalsLoadInProgress.finally(() => { cryptoSignalsLoadInProgress = null; });
}
async function loadCryptoSignalsInternal() {
    /*
       Always make the last known signal available first.
       This means the signal boxes do not disappear when
       the API is temporarily unavailable.
    */
    const cachedSignal =
        loadSignalCache();
    if (cachedSignal) {
        renderCryptoSignals(
            cachedSignal,
            !navigator.onLine
        );
    }
    try {
        const res = await fetchWithFallback(
            WORKER_URLS.signal.map(url => url + (url.includes("?") ? "&" : "?") + "t=" + Date.now()),
            { method: "GET", headers: { "Accept": "application/json" } },
            7000,
            async response => {
                const payload = await response.json();
                const keys = ["BTC", "BNB", "ETH", "DOGE", "ADA", "SOL", "GRAM", "AVAX", "POL", "TRX"];
                if (!payload || typeof payload !== "object" || Array.isArray(payload) || !keys.every(key => Object.prototype.hasOwnProperty.call(payload, key))) {
                    throw new Error("Signal Worker returned incomplete signal fields");
                }
                return true;
            }
        );
        if (!res.ok) {
            throw new Error(
                "Signal API HTTP " +
                res.status
            );
        }
        const data =
            await res.json();
        if (
            !data ||
            typeof data !== "object"
        ) {
            throw new Error(
                "Invalid signal response"
            );
        }
        /*
           Use the cached/previous signal as the
           comparison baseline so changes are still
           detected across page reloads.
        */
        let oldSignals = { ...previousSignals };
        if (!nativeSignalBaselineApplied &&
            window.AndroidNotifications &&
            typeof window.AndroidNotifications.getBackgroundSignalStateJson === "function") {
            try {
                const rawBaseline = window.AndroidNotifications.getBackgroundSignalStateJson();
                const nativeBaseline = rawBaseline ? JSON.parse(rawBaseline) : null;
                if (nativeBaseline && typeof nativeBaseline === "object" &&
                    Object.keys(nativeBaseline).length) {
                    oldSignals = nativeBaseline;
                }
            } catch (e) {}
            nativeSignalBaselineApplied = true;
        }
        const signalCoins = [
            "BTC",
            "BNB",
            "ETH",
            "DOGE",
            "ADA",
            "SOL",
            "GRAM",
            "AVAX",
            "POL",
            "TRX"
        ];
        const signalChanges = [];

        signalCoins.forEach(coin => {
            const value = getSignalNumericValue(data[coin] ?? 0);
            const oldValue = oldSignals[coin];
            /*
               Crypto Signal notifications are emitted only for a real BUY/SELL
               state. A transient/neutral/invalid value must never become a
               notification.
            */
            if (
                oldValue !== undefined &&
                oldValue !== value &&
                (value === 1 || value === -1)
            ) {
                signalChanges.push({ coin, value });
            }
        });

        /*
           Worker glitches commonly appear as simultaneous changes for two or
           more coins. Treat that whole poll as invalid for notification
           purposes. The UI/color rendering is untouched.
        */
        if (signalChanges.length === 1) {
            const change = signalChanges[0];
            const signalType = change.value > 0 ? "BUY" : "SELL";
            const signalIcon = change.value > 0 ? "🟢" : "🔴";
            sendDashboardNotification(
                `${signalIcon} ${change.coin} Crypto Signal`,
                `${signalIcon} ${change.coin} → ${signalType}`,
                getCryptoIconSrc(change.coin),
                signalType
            );
        }
        cryptoSignalsRetryAt = 0;
        saveSignalCache(data);
        renderCryptoSignals(data, false);
        clearPageIssue("signal-api");
        clearPageIssue("signal-cache");
    }
    catch (error) {
        cryptoSignalsRetryAt = Date.now() + (/429/.test(String(error?.message || error)) ? 60000 : 15000);
        const cached =
            loadSignalCache();
        if (cached) {
            renderCryptoSignals(cached, !navigator.onLine);
            setPageIssue("signal-api", "Signal API offline", "warn");
            setPageIssue("signal-cache", "Signal cached", "cache");
        } else {
            setPageIssue("signal-api", "Signal not received", "error");
        }
        console.warn("Signal API Error:", error);
    }
}
/* =====================================================
   DASHBOARD NOTIFICATION SYSTEM
===================================================== */
/*
Notification thresholds.
Based on ABSOLUTE plBank percentage.
*/
const notificationThresholds = [
    0.5,
    ...Array.from(
        { length: 1000 },
        (_, i) => i + 1
    )
];
/*
Each card has its own independent notification state.
*/
const notificationStates = Object.create(null);
const SIGN_CONFIRMATIONS_REQUIRED = 5;
/*
Same threshold can notify again
after 5 minutes.
*/
const NOTIFICATION_COOLDOWN = 5 * 60 * 1000;
/* =====================================================
REQUEST NOTIFICATION PERMISSION
===================================================== */
function requestDashboardNotificationPermission() {
    if (!("Notification" in window)) {
        console.warn(
            "Browser notifications are not supported."
        );
        return Promise.resolve("unsupported");
    }
    if (Notification.permission === "granted") {
        return Promise.resolve("granted");
    }
    if (Notification.permission === "denied") {
        console.warn(
            "Browser notification permission is denied."
        );
        return Promise.resolve("denied");
    }
    try {
        return Notification
            .requestPermission()
            .then(permission => {
                console.log(
                    "Notification permission:",
                    permission
                );
                return permission;
            })
            .catch(error => {
                console.warn(
                    "Notification permission request failed:",
                    error
                );
                return "error";
            });
    }
    catch (error) {
        console.warn(
            "Notification permission request failed:",
            error
        );
        return Promise.resolve("error");
    }
}
/* =====================================================
REQUEST PERMISSION ON USER GESTURE
===================================================== */
if (
    "Notification" in window &&
    Notification.permission === "default"
) {
    const requestPermissionOnUserGesture = () => {
        unlockDashboardNotificationSound();
        requestDashboardNotificationPermission();
    };
    window.addEventListener(
        "click",
        requestPermissionOnUserGesture,
        { once: true }
    );
    window.addEventListener(
        "touchstart",
        requestPermissionOnUserGesture,
        { once: true }
    );
    window.addEventListener(
        "keydown",
        requestPermissionOnUserGesture,
        { once: true }
    );
}
/* =====================================================
CARD COLOR
===================================================== */
function getCardColorFromPercent(percent) {
    const value =
        Number(percent);
    if (
        !Number.isFinite(value) ||
        value === 0
    ) {
        return "neutral";
    }
    return value > 0
        ? "green"
        : "red";
}
/* =====================================================
THRESHOLD
===================================================== */
function getThresholdFromPercent(percent) {
    const absolutePercent =
        Math.abs(
            Number(percent)
        );
    if (
        !Number.isFinite(
            absolutePercent
        )
    ) {
        return 0;
    }
    let currentThreshold = 0;
    for (
        const threshold
        of notificationThresholds
    ) {
        if (
            absolutePercent >=
            threshold
        ) {
            currentThreshold =
                threshold;
        }
        else {
            break;
        }
    }
    return currentThreshold;
}
/* =====================================================
CREATE INITIAL STATE
===================================================== */
function createNotificationState(
    cardId,
    percent
) {
    notificationStates[cardId] = {
        /*
           Confirmed direction.
           IMPORTANT:
           This changes only after
           5 consecutive readings confirm
           the new direction.
        */
        color:
            getCardColorFromPercent(
                percent
            ),
        threshold:
            getThresholdFromPercent(
                percent
            ),
        /*
           Candidate direction for
           positive <-> negative change.
        */
        signCandidateColor:
            null,
        /*
           Number of consecutive readings
           confirming the candidate direction.
        */
        signCandidateCount:
            0,
        /*
           Last notification time
           for each threshold.
        */
        thresholdNotificationTimes:
            Object.create(null),
        /*
           Last percentage value.
        */
        lastPercent:
            Number(percent)
    };
    return notificationStates[cardId];
}
/* =====================================================
GET CARD STATE
===================================================== */
function getNotificationState(
    cardId,
    percent
) {
    if (
        !notificationStates[cardId]
    ) {
        return createNotificationState(
            cardId,
            percent
        );
    }
    return notificationStates[cardId];
}
/* =====================================================
COLOR CHANGE MESSAGE
5 CONSECUTIVE CONFIRMATIONS
===================================================== */
function getColorChangeMessage(
    state,
    currentColor
) {
    /*
       Neutral does not confirm
       a direction change.
       Cancel pending confirmation.
    */
    if (
        currentColor === "neutral"
    ) {
        state.signCandidateColor =
            null;
        state.signCandidateCount =
            0;
        return null;
    }
    /*
       Current direction is already
       confirmed.
       Cancel any pending candidate.
    */
    if (
        currentColor ===
        state.color
    ) {
        state.signCandidateColor =
            null;
        state.signCandidateCount =
            0;
        return null;
    }
    /*
       New direction detected.
       Start or continue confirmation.
    */
    if (
        state.signCandidateColor !==
        currentColor
    ) {
        state.signCandidateColor =
            currentColor;
        state.signCandidateCount =
            1;
    }
    else {
        state.signCandidateCount++;
    }
    /*
       Need 5 consecutive readings.
    */
    if (
        state.signCandidateCount <
        SIGN_CONFIRMATIONS_REQUIRED
    ) {
        return null;
    }
    /*
       Five consecutive readings
       confirmed the new direction.
    */
    const previousColor =
        state.color;
    state.color =
        currentColor;
    state.signCandidateColor =
        null;
    state.signCandidateCount =
        0;
    /*
       RED -> GREEN
    */
    if (
        previousColor === "red"
        &&
        currentColor === "green"
    ) {
        return (
            "Investment is better than bank return"
        );
    }
    /*
       GREEN -> RED
    */
    if (
        previousColor === "green"
        &&
        currentColor === "red"
    ) {
        return (
            "Bank return is better than investment"
        );
    }
    return null;
}
/* =====================================================
THRESHOLD CROSSING
===================================================== */
function getCrossedThreshold(
    previousPercent,
    currentPercent
) {
    const previous =
        Number(previousPercent);
    const current =
        Number(currentPercent);
    if (
        !Number.isFinite(previous) ||
        !Number.isFinite(current)
    ) {
        return 0;
    }
    const previousAbs =
        Math.abs(previous);
    const currentAbs =
        Math.abs(current);
    /*
       Moving upward through thresholds.
    */
    if (
        currentAbs >
        previousAbs
    ) {
        for (
            const threshold
            of notificationThresholds
        ) {
            if (
                threshold > previousAbs &&
                threshold <= currentAbs
            ) {
                return threshold;
            }
        }
    }
    /*
       Moving downward through thresholds
       is also considered a crossing.
    */
    if (
        currentAbs <
        previousAbs
    ) {
        for (
            let i =
                notificationThresholds.length - 1;
            i >= 0;
            i--
        ) {
            const threshold =
                notificationThresholds[i];
            if (
                threshold < previousAbs &&
                threshold >= currentAbs
            ) {
                return threshold;
            }
        }
    }
    return 0;
}
/* =====================================================
THRESHOLD MESSAGE
===================================================== */
function getThresholdChangeMessage(
    cardId,
    crossedThreshold,
    percent
) {
    /*
       Top 6 Purchases and Last Period:
       disable threshold-crossing notifications only.
       Color-change notifications remain unchanged.
    */
    if (
        cardId === "sixMainPrice" ||
        cardId === "lastMainPrice"
    ) {
        return null;
    }
if (
        !crossedThreshold
    ) {
        return null;
    }
    const state =
        notificationStates[cardId];
    if (!state) {
        return null;
    }
    const now =
        Date.now();
    const key =
        String(crossedThreshold);
    const lastNotificationTime =
        state.thresholdNotificationTimes[key] || 0;
    /*
       Same threshold:
       wait according to
       NOTIFICATION_COOLDOWN.
    */
    if (
        now -
        lastNotificationTime <
        NOTIFICATION_COOLDOWN
    ) {
        return null;
    }
    /*
       Save notification time
       for THIS threshold only.
    */
    state.thresholdNotificationTimes[key] =
        now;
    /*
       Keep the sign of CURRENT percentage.
    */
    const sign =
        Number(percent) >= 0
            ? "+"
            : "-";
    return (
        getCardNotificationTitle(cardId) +
        ": Price " +
        sign +
        crossedThreshold +
        "% vs Bank"
    );
}
/* =====================================================
EVALUATE NOTIFICATION
===================================================== */
function evaluateDashboardNotification(
    cardId,
    percent
) {
    const numericPercent =
        Number(percent);
    if (
        !Number.isFinite(
            numericPercent
        )
    ) {
        return null;
    }
    /*
       First reading creates baseline.
       NEVER notify.
    */
    if (
        !notificationStates[cardId]
    ) {
        createNotificationState(
            cardId,
            numericPercent
        );
        return null;
    }
    const state =
        notificationStates[cardId];
    /* Reset the per-notification crossing direction. */
    state.lastNotificationDirection = null;
    const currentColor =
        getCardColorFromPercent(
            numericPercent
        );
    /*
       Positive <-> Negative
       requires 5 consecutive readings.
    */
    const colorMessage =
        getColorChangeMessage(
            state,
            currentColor
        );
    /*
       Find threshold crossing.
    */
    const crossedThreshold =
        getCrossedThreshold(
            state.lastPercent,
            numericPercent
        );
    let thresholdMessage =
        null;
    if (
        crossedThreshold
    ) {
        thresholdMessage =
            getThresholdChangeMessage(
                cardId,
                crossedThreshold,
                numericPercent
            );
        if (cardId === "overallMainPrice" && thresholdMessage) {
            const previousAbs = Math.abs(state.lastPercent);
            const currentAbs = Math.abs(numericPercent);
            state.lastNotificationDirection =
                currentAbs > previousAbs ? "up" :
                currentAbs < previousAbs ? "down" : null;
        }
    }
    let notificationMessage =
        null;
    /*
       Maximum GRAM notification
       per dashboard update.
       Confirmed color change
       has priority.
    */
    if (
        colorMessage
    ) {
        notificationMessage =
            colorMessage;
    }
    else if (
        thresholdMessage
    ) {
        notificationMessage =
            thresholdMessage;
    }
    /*
       IMPORTANT:
       DO NOT do:
       state.color = currentColor;
       here.
       state.color is changed only
       after 5 confirmed readings.
    */
    state.threshold =
        getThresholdFromPercent(
            numericPercent
        );
    state.lastPercent =
        numericPercent;
    return notificationMessage;
}
/* =====================================================
CARD TITLES
===================================================== */
function getCardNotificationTitle(
    cardId
) {
    if (
        cardId ===
        "overallMainPrice"
    ) {
        return "Overall";
    }
    if (
        cardId ===
        "sixMainPrice"
    ) {
        return "Top Purchases";
    }
    if (
        cardId ===
        "lastMainPrice"
    ) {
        return "Last Period";
    }
    return "Financial Dashboard";
}
/* =====================================================
NOTIFICATION ICON
===================================================== */
/*
   Positive notification:
   🟢
   Negative notification:
   🔴
   No image file is required.
*/
function getNotificationIconEmoji(message, crossingDirection = null) {
    const text =
        String(message || "");
    /*
       Positive color-change message
    */
    if (
        text.includes(
            "Investment is better than bank return"
        )
    ) {
        return "🟢";
    }
    /*
       Negative color-change message
    */
    if (
        text.includes(
            "Bank return is better than investment"
        )
    ) {
        return "🔴";
    }
    /*
       Overall threshold notification: the icon follows the
       direction in which the percentage crossed the threshold,
       not the sign of the percentage itself.
    */
    if (text.includes("Price +") || text.includes("Price -")) {
        if (crossingDirection === "up") return "🟢";
        if (crossingDirection === "down") return "🔴";
        return text.includes("Price +") ? "🟢" : "🔴";
    }
    /*
       Default
    */
    return "";
}
/* =====================================================
NOTIFICATION SOUND
===================================================== */
const DASHBOARD_NOTIFICATION_AUDIO_CACHE =
    "financial-dashboard-notification-audio-v1";
const DASHBOARD_NOTIFICATION_SOUND_URLS = {
    BUY: "./file/Buy.mp3",
    SELL: "./file/Sell.mp3",
    USD: "./file/Usd.mp3",
    RFKJ: "./file/signal.mp3",
    RFKJ_BLUE: "./file/matrisabi.mp3",
    RFKJ_WHITE: "./file/matrissefid.mp3",
    CROSSOVER: "./file/Crossover.mp3",
    GLOBAL_PULSE: "./file/global.mp3",
    GLOBAL_TIME: "./file/global.mp3",
    FEAR_GREED: "./file/global.mp3"
};
function getDashboardNotificationSoundURL(soundType) {
    const relativeURL = DASHBOARD_NOTIFICATION_SOUND_URLS[soundType];
    if (!relativeURL) return null;
    try {
        return new URL(relativeURL, document.baseURI).href;
    }
    catch (error) {
        return relativeURL;
    }
}
const dashboardNotificationSounds = Object.create(null);
let dashboardNotificationAudioReady = null;
let dashboardAudioContext = null;
function getNotificationSoundType(title, message, explicitType = null) {
    if (explicitType === "BUY" || explicitType === "SELL" || explicitType === "USD" || explicitType === "RFKJ" || explicitType === "RFKJ_BLUE" || explicitType === "RFKJ_WHITE" || explicitType === "CROSSOVER" || explicitType === "GLOBAL_PULSE" || explicitType === "GLOBAL_TIME" || explicitType === "FEAR_GREED") {
        return explicitType;
    }
    const text = `${title || ""} ${message || ""}`.toUpperCase();
    if (/\bSELL\b/.test(text)) return "SELL";
    if (/\bBUY\b/.test(text)) return "BUY";
    /* Dashboard percentage/color notifications use the USD sound. */
    return "USD";
}
async function cacheNotificationSound(soundType) {
    const relativeURL = DASHBOARD_NOTIFICATION_SOUND_URLS[soundType];
    if (!relativeURL) return null;
    const url = getDashboardNotificationSoundURL(soundType);
    if (!url) return null;
    try {
        if ("caches" in window) {
            const cache = await caches.open(DASHBOARD_NOTIFICATION_AUDIO_CACHE);
            let response = await cache.match(url);
            if (!response) {
                response = await fetch(url, { cache: "no-store" });
                if (!response.ok) {
                    throw new Error(`Audio HTTP ${response.status}`);
                }
                await cache.put(url, response.clone());
            }
            return URL.createObjectURL(await response.blob());
        }
    }
    catch (error) {
        console.warn(`Notification sound cache unavailable for ${soundType}:`, error);
    }
    return url;
}
async function prepareNotificationSound(soundType) {
    if (dashboardNotificationSounds[soundType]) {
        return dashboardNotificationSounds[soundType];
    }
    const source = await cacheNotificationSound(soundType);
    if (!source) return null;
    const audio = new Audio();
    audio.src = source;
    audio.preload = "auto";
    audio.autoplay = false;
    audio.loop = false;
    audio.muted = false;
    audio.volume = 1.0;
    audio.setAttribute("playsinline", "true");
    dashboardNotificationSounds[soundType] = audio;
    try {
        audio.load();
    }
    catch (error) {}
    return audio;
}
function unlockDashboardNotificationSound() {
    try {
        const AudioContextClass =
            window.AudioContext ||
            window.webkitAudioContext;
        if (AudioContextClass) {
            if (!dashboardAudioContext) {
                dashboardAudioContext = new AudioContextClass();
            }
            if (dashboardAudioContext.state === "suspended") {
                dashboardAudioContext.resume().catch(() => {});
            }
        }
        ["BUY", "SELL", "USD", "RFKJ", "RFKJ_BLUE", "RFKJ_WHITE", "CROSSOVER"].forEach(soundType => {
            const audio = dashboardNotificationSounds[soundType];
            if (!audio) return;
            try {
                audio.muted = true;
                audio.volume = 0;
                audio.currentTime = 0;
                const promise = audio.play();
                if (promise && typeof promise.then === "function") {
                    promise.then(() => {
                        try {
                            audio.pause();
                            audio.currentTime = 0;
                            audio.muted = false;
                            audio.volume = 1.0;
                        } catch (error) {}
                    }).catch(() => {
                        audio.muted = false;
                        audio.volume = 1.0;
                    });
                }
            }
            catch (error) {}
        });
    }
    catch (error) {}
}
async function prepareAllDashboardNotificationSounds() {
    if (!dashboardNotificationAudioReady) {
        dashboardNotificationAudioReady = Promise.all([
            prepareNotificationSound("BUY"),
            prepareNotificationSound("SELL"),
            prepareNotificationSound("USD"),
            prepareNotificationSound("RFKJ"),
            prepareNotificationSound("RFKJ_BLUE"),
            prepareNotificationSound("RFKJ_WHITE"),
            prepareNotificationSound("CROSSOVER"),
            prepareNotificationSound("GLOBAL_PULSE"),
            prepareNotificationSound("GLOBAL_TIME"),
            prepareNotificationSound("FEAR_GREED")
        ]).catch(() => null);
    }
    return dashboardNotificationAudioReady;
}
async function playDashboardNotificationSound(soundType = "USD") {
    const type = getNotificationSoundType("", "", soundType);
    try {
        if (dashboardAudioContext && dashboardAudioContext.state === "suspended") {
            await dashboardAudioContext.resume().catch(() => {});
        }
        const audio =
            dashboardNotificationSounds[type] ||
            await prepareNotificationSound(type);
        if (!audio) return false;
        audio.pause();
        audio.currentTime = 0;
        audio.muted = false;
        audio.volume = 1.0;
        const playPromise = audio.play();
        if (playPromise && typeof playPromise.then === "function") {
            await playPromise;
        }
        return true;
    }
    catch (error) {
        console.warn(`Notification sound playback blocked for ${type}:`, error);
        return false;
    }
}
/* Cache all notification sounds once. Later notifications use the cached copies. */
prepareAllDashboardNotificationSounds();
["click", "touchstart", "keydown", "pointerdown"].forEach(eventName => {
    window.addEventListener(eventName, () => {
        unlockDashboardNotificationSound();
        prepareAllDashboardNotificationSounds();
    }, { passive: true });
});
/* =====================================================
BACKGROUND NOTIFICATION RELIABILITY
===================================================== */
/*
   A normal web page cannot force arbitrary MP3 playback while it is
   backgrounded or suspended by the mobile browser. In that state the
   Service Worker notification is the reliable channel and the OS/browser
   supplies its own notification sound because silent=false.
*/
document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
        unlockDashboardNotificationSound();
        prepareAllDashboardNotificationSounds();
    }
});
/* =====================================================
SEND BROWSER NOTIFICATION
===================================================== */
const DASHBOARD_NOTIFICATION_BURST_DB = "financial-dashboard-notification-guard-v1";
const DASHBOARD_NOTIFICATION_BURST_STORE = "guard";
const DASHBOARD_NOTIFICATION_BURST_KEY = "last";
const DASHBOARD_NOTIFICATION_BURST_MS = 1500;

function acquireDashboardNotificationBurstSlot() {
    return new Promise(resolve => {
        try {
            const request = indexedDB.open(DASHBOARD_NOTIFICATION_BURST_DB, 1);
            request.onupgradeneeded = () => {
                try { request.result.createObjectStore(DASHBOARD_NOTIFICATION_BURST_STORE); } catch (e) {}
            };
            request.onerror = () => resolve(true);
            request.onsuccess = () => {
                try {
                    const db = request.result;
                    const tx = db.transaction(DASHBOARD_NOTIFICATION_BURST_STORE, "readwrite");
                    const store = tx.objectStore(DASHBOARD_NOTIFICATION_BURST_STORE);
                    const getReq = store.get(DASHBOARD_NOTIFICATION_BURST_KEY);
                    getReq.onsuccess = () => {
                        const now = Date.now();
                        const last = Number(getReq.result || 0);
                        if (last > 0 && now - last < DASHBOARD_NOTIFICATION_BURST_MS) {
                            resolve(false);
                            return;
                        }
                        store.put(now, DASHBOARD_NOTIFICATION_BURST_KEY);
                        resolve(true);
                    };
                    getReq.onerror = () => resolve(true);
                } catch (e) {
                    resolve(true);
                }
            };
        } catch (e) {
            resolve(true);
        }
    });
}

async function sendDashboardNotification(
    title,
    message,
    notificationImage = "./icon-192.png",
    soundType = "USD",
    crossingDirection = null
) {
    // Crossover alerts are independent events. They must not be suppressed
    // because another dashboard section notified at the same moment.
    if (String(soundType).toUpperCase() !== "CROSSOVER") {
        if (!(await acquireDashboardNotificationBurstSlot())) return false;
    }
    if (
        window.AndroidNotifications &&
        typeof window.AndroidNotifications.showNotification === "function"
    ) {
        try {
            const resolvedType =
                getNotificationSoundType(
                    title,
                    message,
                    soundType
                );
            const nativeIcon = getNotificationIconEmoji(message, crossingDirection);
            const nativeTitle = nativeIcon ? `${nativeIcon} ${title}` : title;
            window.AndroidNotifications.showNotification(
                resolvedType,
                nativeTitle,
                message
            );
            return true;
        }
        catch (nativeError) {
            console.warn(
                "Native Android notification bridge failed:",
                nativeError
            );
        }
    }
    if (!("Notification" in window)) {
        return false;
    }
    if (Notification.permission !== "granted") {
        console.log(
            "Notification skipped because permission is not granted:",
            title,
            message
        );
        return false;
    }
    try {
        const resolvedSoundType =
            getNotificationSoundType(
                title,
                message,
                soundType
            );
        const notificationIcon =
            getNotificationIconEmoji(message, crossingDirection);
        const notificationTitle =
            notificationIcon +
            (notificationIcon ? " " : "") +
            title;
        await prepareNotificationSound(resolvedSoundType);
        /* Play the user-selected MP3 first.
           Browser Notification API does not support arbitrary sound files. */
        const customSoundPlayed =
            await playDashboardNotificationSound(resolvedSoundType);
        if ("serviceWorker" in navigator) {
            const registration =
                await navigator.serviceWorker.ready;
            await registration.showNotification(
                notificationTitle,
                {
                    body: message,
                    icon: notificationImage,
                    badge: "./icon-192.png",
                    tag: `financial-dashboard-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
                    renotify: true,
                    requireInteraction: true,
                    silent: false,
                    vibrate: [220, 100, 220, 100, 220],
                    timestamp: Date.now(),
                    data: {
                        notificationType: resolvedSoundType,
                        soundUrl: DASHBOARD_NOTIFICATION_SOUND_URLS[resolvedSoundType] || null
                    }
                }
            );
            if (!customSoundPlayed) {
                console.warn(
                    `Custom notification sound could not be played for ${resolvedSoundType}.`
                );
            }
            return true;
        }
        const notification =
            new Notification(
                notificationTitle,
                {
                    body: message,
                    icon: "icon.png",
                    silent: false,
                    requireInteraction: true
                }
            );
        if (!customSoundPlayed) {
            console.warn(
                `Custom notification sound could not be played for ${resolvedSoundType}.`
            );
        }
        /* Do not auto-close the notification.
           It remains visible until the user dismisses it or the OS/browser decides otherwise. */
        return true;
    }
    catch (error) {
        console.warn(
            "Notification could not be displayed:",
            error
        );
        return false;
    }
}
/* =====================================================
CARD COLOR + NOTIFICATION HOOK
===================================================== */
function updateCardColor(
    cardId,
    percent
) {
    const card =
        document.getElementById(
            cardId
        );
    if (!card) {
        return;
    }
    const numericPercent =
        Number(percent);
    if (
        !Number.isFinite(
            numericPercent
        )
    ) {
        return;
    }
    /*
       Notification system uses the SAME
       percentage that the card already uses.
       No financial calculation is performed here.
    */
    const notificationMessage =
        evaluateDashboardNotification(
            cardId,
            numericPercent
        );
/* =============================================
   NOTIFICATION
   WINDOWS / MOBILE + EMAIL
============================================= */
if (
    notificationMessage
) {
    const notificationTitle =
        getCardNotificationTitle(
            cardId
        );
    /* =============================================
       WINDOWS / MOBILE NOTIFICATION
    ============================================= */
    sendDashboardNotification(
        notificationTitle,
        notificationMessage,
        "./icon-192.png",
        "USD",
        cardId === "overallMainPrice"
            ? (notificationStates[cardId]?.lastNotificationDirection || null)
            : null
    );
    /* =============================================
       EMAIL NOTIFICATION
    ============================================= */
    // sendEmailNotification( notificationTitle, notificationMessage);
}
    /* =================================================
       EXISTING COLOR LOGIC
       UNCHANGED
    ================================================= */
    const p =
        Math.max(
            -20,
            Math.min(
                20,
                numericPercent
            )
        );
    const t =
        Math.abs(p) / 20;
    if (p >= 0) {
        card.style.borderColor =
            `rgba(34,197,94,${0.15 + t * 0.55})`;
        card.style.boxShadow =
            `0 8px 30px rgba(34,197,94,${0.05 + t * 0.20})`;
    }
    else {
        card.style.borderColor =
            `rgba(239,68,68,${0.15 + t * 0.55})`;
        card.style.boxShadow =
            `0 8px 30px rgba(239,68,68,${0.05 + t * 0.20})`;
    }
}
/* =====================================================
   EMAIL NOTIFICATION
   Financial Dashboard → Cloudflare Worker
===================================================== */
/* =====================================================
   SEND EMAIL NOTIFICATION
===================================================== */
async function sendEmailNotification(
    title,
    message
) {
    if (!WORKER_URLS.email) {
        console.warn(
            "Email notification skipped: Worker URL is not configured."
        );
        return false;
    }
    try {
        const response = await fetchWithTimeout(
            WORKER_URLS.email,
            {
                method: "POST",
                headers: { "Content-Type": "application/json", "Accept": "application/json" },
                body: JSON.stringify({ title: String(title), message: String(message) })
            },
            8000
        );
        if (!response.ok) {
            console.warn(
                "Email notification failed:",
                response.status
            );
            return false;
        }
        console.log(
            "Email notification sent:",
            title,
            message
        );
        return true;
    }
    catch (error) {
        console.warn(
            "Email notification error:",
            error
        );
        return false;
    }
}
/* =====================================================
   HELPERS
===================================================== */
function $(id) {
    return document.getElementById(id);
}
function safeText(
    id,
    value
) {
    const element =
        $(id);
    if (element) {
        element.textContent =
            value;
    }
}
function animateLivePrice(id, value, previousValue) {
    const element = $(id);
    if (!element) return;
    const nextText = formatNumber(value);
    const previous = Number(previousValue);
    element.classList.remove("live-price-from-bottom", "live-price-from-top");
    if (Number.isFinite(previous) && Number(value) !== previous) {
        void element.offsetWidth;
        element.classList.add(Number(value) > previous ? "live-price-from-bottom" : "live-price-from-top");
    }
    element.textContent = nextText;
}
/* =====================================================
   BUY AGE
===================================================== */
function daysFromBuyToToday(
    buyTransaction,
    today
) {
    if (
        !buyTransaction ||
        buyTransaction.type !== "buy" ||
        !buyTransaction.date ||
        !today
    ) {
        return "-";
    }
    const startParts =
        String(buyTransaction.date)
            .split("/")
            .map(Number);
    const endParts =
        String(today)
            .split("/")
            .map(Number);
    if (
        startParts.length !== 3 ||
        endParts.length !== 3
    ) {
        return "-";
    }
    const startDate =
        jalaliToGregorian(
            startParts[0],
            startParts[1],
            startParts[2]
        );
    const endDate =
        jalaliToGregorian(
            endParts[0],
            endParts[1],
            endParts[2]
        );
    const days =
        Math.floor(
            (
                endDate -
                startDate
            ) /
            86400000
        );
    return (
        Math.max(
            0,
            days
        ) +
        "d"
    );
}
/* =====================================================
   CACHE
===================================================== */
const CACHE_KEYS = {
    DATA:
        "financialDashboardData",
    DATA_TIME:
        "financialDashboardDataSavedAt",
    NOBITEX_PRICE:
        "financialDashboardPrice",
    NOBITEX_TIME:
        "financialDashboardPriceTime",
    EXIR_PRICE:
        "financialDashboardExirPrice",
    EXIR_TIME:
        "financialDashboardExirPriceTime",
    BITPIN_PRICE:
        "financialDashboardBitpinPrice",
    BITPIN_TIME:
        "financialDashboardBitpinPriceTime",
    AVERAGE_PRICE:
        "financialDashboardAveragePrice",
    AVERAGE_TIME:
        "financialDashboardAveragePriceTime",
    USD_TXT_PRICE:
        "financialDashboardUsdTxtPrice",
    USD_TXT_TIME:
        "financialDashboardUsdTxtPriceTime",
    YEKUSD_PRICE:
        "financialDashboardYekusdPrice",
    YEKUSD_TIME:
        "financialDashboardYekusdPriceTime",
    CRYPTO_PRICE:
        "financialDashboardCryptoPrice"
};
/* =====================================================
   YEKUSD ONLINE / OFFLINE
===================================================== */
let yekusdOnline = false;
/* =====================================================
   SAVE YEKUSD
===================================================== */
function saveYekusdPrice(value) {
    if (parseValidTopDollarPrice(value) === null) return;
    try {
        localStorage.setItem(CACHE_KEYS.YEKUSD_PRICE, String(value));
        localStorage.setItem(CACHE_KEYS.YEKUSD_TIME, String(Date.now()));
    } catch (error) { console.warn("YEKUSD cache save failed:", error); }
}


/* =====================================================
   LOAD YEKUSD CACHE
===================================================== */
function loadCachedYekusdPrice() {
    return localStorage.getItem(
        CACHE_KEYS.YEKUSD_PRICE
    );
}
/* =====================================================
   YEKUSD CONNECTION COLOR
===================================================== */
function updateYekusdConnectionColor(
    online
) {
    const element =
        $("yekusd");
    if (!element) {
        return;
    }
    element.classList.toggle(
        "market-price-offline",
        !online
    );
}
/* =====================================================
   VALID TOP DOLLAR PRICE FETCH
   A source is accepted only when its body is a positive finite number.
   HTTP 200 with an empty/zero/non-numeric body advances to the next URL.
===================================================== */
function parseValidTopDollarPrice(raw) {
    const value = String(raw ?? "").trim();
    if (!value) return null;
    // Accept plain numbers, including thousands separators; reject JSON,
    // labels, NaN, Infinity, zero and negative values.
    const normalized = value.replace(/,/g, "");
    if (!/^(?:\d+\.?\d*|\.\d+)$/.test(normalized)) return null;
    const number = Number(normalized);
    return Number.isFinite(number) && number > 0 ? value : null;
}
async function fetchValidTopDollarPrice(urls) {
    let lastError = null;
    const queue = Array.isArray(urls) ? urls : [];
    const chainStartedAt = Date.now();
    const chainBudgetMs = 20000;
    for (const url of queue) {
        const remainingMs = chainBudgetMs - (Date.now() - chainStartedAt);
        if (remainingMs <= 0) break;
        try {
            const response = await fetchWithTimeout(url, { method: "GET", headers: { "Accept": "text/plain, application/json" }, cache: "no-store" }, Math.min(6500, remainingMs));
            if (!response.ok) {
                lastError = new Error(`HTTP ${response.status}`);
                continue;
            }
            const body = await response.text();
            const validValue = parseValidTopDollarPrice(body);
            if (validValue !== null) return validValue;
            lastError = new Error("Empty, zero or non-numeric price response");
            console.warn("Invalid top dollar price; trying next source:", url);
        } catch (error) {
            lastError = error;
            console.warn("Top dollar price source failed; trying next source:", url, error);
        }
    }
    throw lastError || new Error("All top dollar price sources unavailable");
}
/* =====================================================
   LOAD YEKUSD
===================================================== */
let yekusdLoadInProgress = null;
let yekusdRetryAt = 0;
function loadYekusd() {
    if (yekusdLoadInProgress) return yekusdLoadInProgress;
    if (Date.now() < yekusdRetryAt) return Promise.resolve(false);
    yekusdLoadInProgress = (async () => {
        const cachedFirst = loadCachedYekusdPrice();
        if (parseValidTopDollarPrice(cachedFirst) !== null) {
            safeText("yekusd", cachedFirst);
            updateYekusdConnectionColor(false);
        }
        try {
            const value = await fetchValidTopDollarPrice(WORKER_URLS.yekusd);
            safeText("yekusd", value);
            saveYekusdPrice(value);
            yekusdOnline = true;
            yekusdRetryAt = 0;
            clearPageIssue("yekusd");
            updateYekusdConnectionColor(true);
            syncNativeWidgetSnapshot();
        } catch (error) {
            yekusdOnline = false;
            yekusdRetryAt = Date.now() + (/429/.test(String(error?.message || error)) ? 60000 : 30000);
            const cached = loadCachedYekusdPrice();
            safeText("yekusd", parseValidTopDollarPrice(cached) !== null ? cached : "-");
            updateYekusdConnectionColor(false);
            setPageIssue("yekusd", parseValidTopDollarPrice(cached) !== null ? "YEKUSD cached" : "YEKUSD unavailable", parseValidTopDollarPrice(cached) !== null ? "cache" : "error");
            console.warn("YEKUSD Offline:", error);
        }
    })();
    return yekusdLoadInProgress.finally(() => { yekusdLoadInProgress = null; });
}


/* =====================================================
   USD TXT ONLINE / OFFLINE
===================================================== */
let usdTxtOnline = false;
/* =====================================================
   SAVE USD TXT
===================================================== */
function saveUsdTxtPrice(value) {
    if (parseValidTopDollarPrice(value) === null) return;
    try {
        localStorage.setItem(CACHE_KEYS.USD_TXT_PRICE, String(value));
        localStorage.setItem(CACHE_KEYS.USD_TXT_TIME, String(Date.now()));
    } catch (error) { console.warn("USD cache save failed:", error); }
}


/* =====================================================
   LOAD USD TXT CACHE
===================================================== */
function loadCachedUsdTxtPrice() {
    return localStorage.getItem(
        CACHE_KEYS.USD_TXT_PRICE
    );
}
/* =====================================================
   USD TXT CONNECTION COLOR
===================================================== */
function updateUsdTxtConnectionColor(
    online
) {
    const element =
        $("usdTxtPrice");
    if (!element) {
        return;
    }
    element.classList.toggle(
        "market-price-offline",
        !online
    );
}
/* =====================================================
   LOAD USD TXT
===================================================== */
let usdTxtLoadInProgress = null;
let usdTxtRetryAt = 0;
function loadUsdTxt() {
    if (usdTxtLoadInProgress) return usdTxtLoadInProgress;
    if (Date.now() < usdTxtRetryAt) return Promise.resolve(false);
    usdTxtLoadInProgress = (async () => {
        const cachedFirst = loadCachedUsdTxtPrice();
        if (parseValidTopDollarPrice(cachedFirst) !== null) {
            safeText("usdTxtPrice", cachedFirst);
            updateUsdTxtConnectionColor(false);
        }
        try {
            const value = await fetchValidTopDollarPrice(WORKER_URLS.usd);
            safeText("usdTxtPrice", value);
            saveUsdTxtPrice(value);
            usdTxtOnline = true;
            usdTxtRetryAt = 0;
            clearPageIssue("usd");
            updateUsdTxtConnectionColor(true);
            syncNativeWidgetSnapshot();
        } catch (error) {
            usdTxtOnline = false;
            usdTxtRetryAt = Date.now() + (/429/.test(String(error?.message || error)) ? 60000 : 30000);
            const cached = loadCachedUsdTxtPrice();
            safeText("usdTxtPrice", parseValidTopDollarPrice(cached) !== null ? cached : "-");
            updateUsdTxtConnectionColor(false);
            setPageIssue("usd", parseValidTopDollarPrice(cached) !== null ? "USD cached" : "USD unavailable", parseValidTopDollarPrice(cached) !== null ? "cache" : "error");
            console.warn("USD TXT Offline:", error);
        }
    })();
    return usdTxtLoadInProgress.finally(() => { usdTxtLoadInProgress = null; });
}


/* =====================================================
   MARKET PRICE CONNECTION COLORS
===================================================== */
function updateMarketPriceConnectionColors(
    nobitexOnline,
    exirOnline,
    bitpinOnline
) {
    const currentPrice =
        $("currentPrice");
    const exirPrice =
        $("exirPrice");
    const bitpinPrice =
        $("bitpinPrice");
    if (currentPrice) {
        currentPrice.classList.toggle(
            "market-price-offline",
            !nobitexOnline
        );
    }
    if (exirPrice) {
        exirPrice.classList.toggle(
            "market-price-offline",
            !exirOnline
        );
    }
    if (bitpinPrice) {
        bitpinPrice.classList.toggle(
            "market-price-offline",
            !bitpinOnline
        );
    }
}
/* =====================================================
   NUMBER FORMAT
===================================================== */
function formatNumber(value) {
    if (
        value === null ||
        value === undefined ||
        value === ""
    ) {
        return "-";
    }
    const number =
        Number(value);
    if (
        !Number.isFinite(number)
    ) {
        return "-";
    }
    return Math.round(number)
        .toLocaleString("en-US");
}
function formatMoneySigned(value) {
    const number =
        Number(value);
    if (
        !Number.isFinite(number)
    ) {
        return "-";
    }
    const rounded =
        Math.round(number);
    if (rounded > 0) {
        return (
            "+" +
            rounded.toLocaleString(
                "en-US"
            )
        );
    }
    return rounded.toLocaleString(
        "en-US"
    );
}
function formatPercent(value) {
    const number =
        Number(value);
    if (
        !Number.isFinite(number)
    ) {
        return "-";
    }
    return (
        number >= 0
            ? "+"
            : ""
    )
    +
    number.toFixed(2)
    +
    "%";
}
function setProfitLossClass(
    id,
    value
) {
    const element =
        $(id);
    if (!element) {
        return;
    }
    element.classList.remove(
        "profit",
        "loss"
    );
    const number =
        Number(value);
    if (
        !Number.isFinite(number)
    ) {
        return;
    }
    element.classList.add(
        number >= 0
            ? "profit"
            : "loss"
    );
}
/* =====================================================
   DASHBOARD CACHE / REQUEST
===================================================== */
const REMOTE_DATA_JSON_URL =
    "https://test1800.github.io/data.json";
const DATA_CACHE_MAX_AGE =
    5 * 60 * 1000;
const PRICE_REQUEST_TIMEOUT =
    10000;
let dashboardLoadInProgress =
    null;
function loadCachedDashboardData() {
    return loadLocalData();
}
function saveLocalData(data) {
    if (!isValidDashboardData(data)) {
        console.warn("Refusing to cache invalid data.json payload");
        return false;
    }
    try {
        // Keep only user data from data.json. Exchange API endpoints and runtime
        // configuration are defined in this HTML file, never in the data feed.
        const previous = loadLocalData();
        // Preserve the old bank-rate history in its own local cache before a
        // legacy R01 snapshot is compacted and its unrelated fields are dropped.
        if (Array.isArray(previous?.bankRatePeriods) &&
            typeof saveBankRatePeriodsCache === "function" &&
            typeof parseBankRatePeriods === "function") {
            try {
                const existingRates = parseBankRatePeriods(
                    JSON.parse(localStorage.getItem("financialDashboardBankRatePeriods_v1") || "null")
                );
                const legacyRates = parseBankRatePeriods(previous.bankRatePeriods);
                if (!existingRates.length && legacyRates.length) saveBankRatePeriodsCache(legacyRates);
            } catch (error) {}
        }
        const compact = {};
        if (Array.isArray(data.transactions)) {
            compact.transactions = data.transactions;
        } else if (Array.isArray(previous?.transactions)) {
            compact.transactions = previous.transactions;
        }
        if (data.cryptoHoldings && typeof data.cryptoHoldings === "object" && !Array.isArray(data.cryptoHoldings)) {
            compact.cryptoHoldings = data.cryptoHoldings;
        } else if (previous?.cryptoHoldings && typeof previous.cryptoHoldings === "object" && !Array.isArray(previous.cryptoHoldings)) {
            compact.cryptoHoldings = previous.cryptoHoldings;
        }
        localStorage.setItem(CACHE_KEYS.DATA, JSON.stringify(compact));
        localStorage.setItem(CACHE_KEYS.DATA_TIME, String(Date.now()));
        return true;
    } catch (error) {
        console.warn("Could not save dashboard data", error);
        return false;
    }
}
function isValidDashboardData(data) {
    if (!data || typeof data !== "object" || Array.isArray(data)) return false;
    const hasTransactions = Object.prototype.hasOwnProperty.call(data, "transactions") && Array.isArray(data.transactions);
    const hasCryptoHoldings = Object.prototype.hasOwnProperty.call(data, "cryptoHoldings") &&
        data.cryptoHoldings && typeof data.cryptoHoldings === "object" && !Array.isArray(data.cryptoHoldings);
    return hasTransactions || Boolean(hasCryptoHoldings);
}
function getDashboardDataCacheAge() {
    try {
        const savedAt = Number(localStorage.getItem(CACHE_KEYS.DATA_TIME));
        return Number.isFinite(savedAt) && savedAt > 0 ? Math.max(0, Date.now() - savedAt) : Infinity;
    } catch (error) { return Infinity; }
}


function loadLocalData() {
    try {
        const saved =
            localStorage.getItem(
                CACHE_KEYS.DATA
            );
        return saved
            ? JSON.parse(saved)
            : null;
    }
    catch (error) {
        return null;
    }
}
function saveNobitexPrice(price) {
    try {
        localStorage.setItem(
            CACHE_KEYS.NOBITEX_PRICE,
            String(price)
        );
        localStorage.setItem(
            CACHE_KEYS.NOBITEX_TIME,
            new Date().toISOString()
        );
    }
    catch (error) {}
}
function loadNobitexPrice() {
    try {
        const saved =
            localStorage.getItem(
                CACHE_KEYS.NOBITEX_PRICE
            );
        const price =
            Number(saved);
        return Number.isFinite(price) && price > 0
            ? Math.round(price)
            : null;
    }
    catch (error) {
        return null;
    }
}
function saveExirPrice(price) {
    const value = Number(price);
    if (!Number.isFinite(value) || value <= 0) return;
    try {
        localStorage.setItem(CACHE_KEYS.EXIR_PRICE, String(value));
        localStorage.setItem(CACHE_KEYS.EXIR_TIME, String(Date.now()));
    }
    catch (error) {}
}
function loadExirPrice() {
    try {
        const saved =
            localStorage.getItem(
                CACHE_KEYS.EXIR_PRICE
            );
        const price =
            Number(saved);
        return Number.isFinite(price) && price > 0
            ? price
            : null;
    }
    catch (error) {
        return null;
    }
}
/* EXIR cache is persistent and follows the same cache-first lifecycle as
   Nobitex and Bitpin. Do not clear it on page/app startup. */
function saveBitpinPrice(price) {
    try {
        localStorage.setItem(CACHE_KEYS.BITPIN_PRICE, String(price));
        localStorage.setItem(CACHE_KEYS.BITPIN_TIME, new Date().toISOString());
    }
    catch (error) {}
}
function loadBitpinPrice() {
    try {
        const saved =
            localStorage.getItem(
                CACHE_KEYS.BITPIN_PRICE
            );
        const price =
            Number(saved);
        return Number.isFinite(price) && price > 0
            ? price
            : null;
    }
    catch (error) {
        return null;
    }
}
function saveAveragePrice(price) {
    try {
        // Keep the persisted live-average cache compact: prices are whole integers.
        const roundedPrice = Math.round(Number(price));
        if (!Number.isFinite(roundedPrice) || roundedPrice <= 0) return;
        localStorage.setItem(
            CACHE_KEYS.AVERAGE_PRICE,
            String(roundedPrice)
        );
        localStorage.setItem(
            CACHE_KEYS.AVERAGE_TIME,
            new Date().toISOString()
        );
    }
    catch (error) {}
}
function loadAveragePrice() {
    try {
        const saved =
            localStorage.getItem(
                CACHE_KEYS.AVERAGE_PRICE
            );
        const price =
            Number(saved);
        return Number.isFinite(price)
            ? price
            : null;
    }
    catch (error) {
        return null;
    }
}
function getCachedPriceTime() {
    try {
        const saved =
            localStorage.getItem(
                CACHE_KEYS.NOBITEX_TIME
            );
        if (!saved) {
            return "";
        }
        const date =
            new Date(saved);
        if (
            Number.isNaN(
                date.getTime()
            )
        ) {
            return "";
        }
        return date.toLocaleString(
            "en-US",
            {
                year:
                    "numeric",
                month:
                    "2-digit",
                day:
                    "2-digit",
                hour:
                    "2-digit",
                minute:
                    "2-digit",
                second:
                    "2-digit",
                hour12:
                    false
            }
        );
    }
    catch (error) {
        return "";
    }
}
/* =====================================================
   CONNECTION STATUS / PAGE HEALTH
===================================================== */
const pageIssues = new Map();
function setPageIssue(key, message, type = "warn") {
    if (!message) pageIssues.delete(key);
    else pageIssues.set(key, { message: String(message), type });
    renderPageStatus();
    renderErrorBox();
}
function clearPageIssue(key) {
    pageIssues.delete(key);
    renderPageStatus();
    renderErrorBox();
}
function pageIssueLabel(key, issue) {
    const k = String(key || "");
    const m = String(issue?.message || "").toLowerCase();

    // Keep the error box compact: API/source name + short state.
    // These labels are intentionally short so an issue can be identified at a glance.
    if (k === "chart-history-sync") return "Chart";
    if (k === "chart-transactions") return "Chart Data";
    if (k === "coin-chart-history-sync") return "Coin History";
    if (k === "coin-live-api") return "Coin API";
    if (k === "coin-live-fallback") return "Coin Fallback";
    if (k === "connection") return "Internet Error";
    if (k === "data-json") return "Data JSON";
    if (k === "usd") return "USD API";
    if (k === "yekusd") return "YEKUSD API";
    if (k === "signal-api") return "Signal API";
    if (k === "signal-cache") return "Signal Cache";
    if (k === "crypto-portfolio") return "Portfolio API";
    if (k === "crypto-price") return "Crypto API";
    if (k === "fear-greed") return "F&G API";
    if (k === "portfolio-data") return "Portfolio Data";
    if (k === "nobitex") return "Nobitex API";
    if (k === "exir") return "Exir API";
    if (k === "bitpin") return "Bitpin API";
    if (k === "market-connection") return "Market API";
    if (k === "mexc-api") return "MEXC API";
    if (k === "mexc-cache") return "MEXC Cache";
    if (k.startsWith("mexc:")) return `MEXC ${k.slice(5)}`;
    if (k.startsWith("rfkj:")) return `Matrix #${k.split(":")[1] || "?"}`;
    if (k === "crossover-remote") return "Crossover API";
    if (k === "crossover-email-worker") return "Email API";
    if (k.startsWith("crossover:")) return `Crossover #${k.split(":")[1] || "?"}`;
    if (k === "global-gold") return "Gold API";
    if (k === "global-gold-change") return "Gold Change";
    if (k === "global-oil") return "Oil API";
    if (k === "global-economic") return "Economic API";
    if (k === "global-tsetmc") return "TSE API";
    if (k.startsWith("global-tsetmc:")) return `TSE ${k.slice(15)}`;
    if (k === "js") return "Page JavaScript";
    if (k === "promise") return "Background Request";
    if (k.startsWith("img:")) return `${k.slice(4)} Icon`;
    if (m.includes("nobitex")) return "Nobitex API";
    if (m.includes("exir")) return "Exir API";
    if (m.includes("bitpin")) return "Bitpin API";
    if (m.includes("mexc")) return "MEXC API";
    if (m.includes("tsetmc") || m.includes("tse")) return "TSE API";
    if (m.includes("matrix") || m.includes("signal")) return "Signal API";
    if (m.includes("crossover")) return "Crossover API";
    return "Page Error";
}
function pageIssueDescription(key, issue) {
    const k = String(key || "");
    const message = String(issue?.message || "");
    if (k === "connection") {
        return "Internet connection is offline; new data cannot be fetched from online sources.";
    }
    if (k === "coin-live-api") {
        return "The primary Coin price API did not return a valid price. The chart is using its dedicated latest-history fallback when available.";
    }
    if (k === "coin-live-fallback") {
        return "The Coin latest-history fallback API is unavailable or returned invalid data. The chart is retaining its last valid cached price when possible.";
    }
    if (k === "coin-chart-history-sync") {
        return "Coin chart history could not be synchronized from its dedicated history sources. Existing local history is retained.";
    }
    if (k === "chart-transactions") {
        return "The chart transaction feed could not be refreshed; its cached transaction snapshot is retained and will be retried.";
    }
    if (k === "usd") {
        return "A fresh USD price was not received; the previously cached value may be shown.";
    }
    if (k === "yekusd") {
        return "A fresh YEKUSD rate was not received; the previous value may be shown.";
    }
    if (k === "signal-api") {
        return "Fresh crypto signals were not received from the API.";
    }
    if (k === "signal-cache") {
        return "Crypto Signal is using the latest cached data; this section may not be real-time.";
    }
    if (k === "crypto-portfolio") {
        return "Fresh Crypto Portfolio data or prices were not received. The portfolio may be showing cached values or incomplete market data.";
    }
    if (k === "crypto-price") {
        return "The main crypto price feed is unavailable or using cached data. The displayed crypto price may not be real-time.";
    }
    if (k === "portfolio-data") {
        return "Portfolio holdings or required portfolio data are missing. Portfolio totals may be unavailable until valid data is loaded.";
    }
    if (k === "market-connection") {
        return "One or more live market price sources could not be reached. Affected prices may be stale or unavailable while the other sources continue normally.";
    }
    if (k === "mexc-cache") {
        return "Some crypto prices are being served from cache because the live MEXC market feed is unavailable. Cached prices may not be current.";
    }
    if (k.startsWith("mexc:")) {
        return `${k.slice(5)} live price is unavailable. The dashboard may be showing its last cached value or no current value for this asset.`;
    }
    if (k.startsWith("rfkj:")) {
        return "The Signal Matrix worker did not return valid data for this matrix/timeframe; its signals may be stale or unavailable.";
    }
    if (k === "crossover-remote") {
        return "The remote Crossover worker could not be synchronized; background Crossover monitoring may be unavailable while local monitoring continues.";
    }
    if (k.startsWith("crossover:")) {
        return "The Crossover price source did not return a valid current price; crossing checks for this panel may be unavailable.";
    }
    if (k === "global-gold") {
        return "The live Global Gold price source is unavailable; the displayed gold price may be stale or unavailable.";
    }
    if (k === "global-gold-change") {
        return "The 24-hour Global Gold change could not be calculated because intraday gold data is unavailable or incomplete.";
    }
    if (k === "global-oil") {
        return "The live Global Oil (WTI) price source is unavailable; the displayed oil price may be stale or unavailable.";
    }
    if (k === "global-economic") {
        return "The Economic Calendar feed is unavailable or returned no valid upcoming events; the last valid calendar data is retained when available.";
    }
    if (k === "global-tsetmc") {
        return "The TSETMC Worker could not provide valid current data; cached TSE values may be shown instead.";
    }
    if (k.startsWith("global-tsetmc:")) {
        return `${k.slice(15)} returned incomplete or invalid TSETMC data; its displayed values may be unavailable.`;
    }
    if (k === "js") {
        return "A JavaScript error occurred and part of the dashboard may not have updated correctly.";
    }
    if (k === "promise") {
        return "A background request failed and only that section may be out of date.";
    }
    if (k.startsWith("img:")) {
        return `${k.slice(4)} icon failed to load; this does not necessarily mean that section data is invalid.`;
    }
    return message || "This section needs attention.";
}
function renderPageStatus() {
    const element = $("connectionStatus");
    if (!element) return;
    const issues = Array.from(pageIssues.values());
    const livePriceWarning = $("liveErrorMessage");
    element.replaceChildren();
    const chip = document.createElement("span");
    chip.className = "connection-status-chip";
    if (!navigator.onLine) {
        chip.textContent = "● Internet connection is offline";
    }
    else if (issues.length) {
        chip.textContent =
            `● ${issues.length} item${issues.length === 1 ? "" : "s"} need attention`;
    }
    else {
        chip.textContent = "● Data is up to date";
    }
    element.appendChild(chip);
    if (livePriceWarning) element.appendChild(livePriceWarning);
    element.className =
        `connection-status visible ${
            (!navigator.onLine || issues.length)
                ? "offline"
                : "online"
        }`;
}
function renderErrorBox() {
    const errorBox = $("errorBox");
    if (!errorBox) return;
    const issues = Array.from(pageIssues.entries());
    if (!issues.length) {
        errorBox.innerHTML = "";
        errorBox.style.display = "none";
        return;
    }
    errorBox.style.display = "block";
    const statusIcon = type => {
        if (type === "error") return "🔴";
        if (type === "cache") return "🔵";
        if (type === "info") return "🟢";
        return "🟡";
    };
    const statusText = type => {
        if (type === "error") return "Error";
        if (type === "cache") return "Cache";
        if (type === "info") return "OK";
        return "Warn";
    };
    const rows = issues.map(([key, issue]) => {
        const label = pageIssueLabel(key, issue);
        const type = issue?.type || "warn";
        return `
            <div class="page-status-report-item ${type}" title="${label}">
                <span class="page-status-dot" aria-hidden="true">${statusIcon(type)}</span>
                <strong>${label}</strong>
                <span class="page-status-short">${statusText(type)}</span>
            </div>
        `;
    }).join("");
    errorBox.innerHTML = `
        <div class="error page-status-report">
            <div class="page-status-report-title">Issues</div>
            <div class="page-status-report-list">
                ${rows}
            </div>
        </div>
    `;
}
function showConnectionStatus(offline, message) {
    if (offline) {
        setPageIssue(
            "connection",
            message || "Offline",
            "warn"
        );
    }
    else {
        clearPageIssue("connection");
    }
}
window.addEventListener("error", event => {
    if (event?.target?.tagName === "IMG") {
        const coin = event.target.alt || "Image";
        setPageIssue(`img:${coin}`, `${coin} image unavailable`, "error");
    } else {
        setPageIssue("js", "Page error", "error");
    }
});
window.addEventListener("unhandledrejection", () => {
    setPageIssue("promise", "Request failed", "error");
});
window.addEventListener("offline", () => {
    setPageIssue("connection", "Internet offline", "warn");
    lastSignalRenderSignature = "";
    lastPortfolioRenderSignature = "";
});
window.addEventListener("online", () => {
    // The connection status is shown as OK briefly by the main online handler.
    // Repaint once when connectivity actually changes; normal refreshes do not repaint.
    lastSignalRenderSignature = "";
    lastPortfolioRenderSignature = "";
});
/* =====================================================
   LIVE ERROR MESSAGE
===================================================== */
function showLivePriceErrors(
    errors
) {
    const element =
        $("liveErrorMessage");
    if (!element) {
        return;
    }
    if (
        !Array.isArray(errors)
        ||
        errors.length === 0
    ) {
        element.textContent =
            "";
        element.classList.remove(
            "visible"
        );
        return;
    }
    const failedSources = new Set(
        errors.map(source => String(source).trim().toLowerCase())
    );
    element.textContent =
        ["nobitex", "exir", "bitpin"].every(source => failedSources.has(source))
            ? "All prices unavailable"
            : errors.join(" • ") + " unavailable";
    element.classList.add(
        "visible"
    );
}
/* =====================================================
   PRICE ARROW
===================================================== */
function setPriceArrow(
    id,
    currentPrice,
    previousPrice
) {
    const element =
        $(id);
    if (!element) {
        return;
    }
    element.classList.remove(
        "arrow-up",
        "arrow-down",
        "arrow-flat"
    );
    const current =
        Number(currentPrice);
    const previous =
        Number(previousPrice);
    if (
        !Number.isFinite(current)
        ||
        !Number.isFinite(previous)
    ) {
        element.textContent =
            "→";
        element.classList.add(
            "arrow-flat"
        );
        return;
    }
    if (
        current >
        previous
    ) {
        element.textContent =
            "↑";
        element.classList.add(
            "arrow-up"
        );
    }
    else if (
        current <
        previous
    ) {
        element.textContent =
            "↓";
        element.classList.add(
            "arrow-down"
        );
    }
    else {
        element.textContent =
            "→";
        element.classList.add(
            "arrow-flat"
        );
    }
}
/* =====================================================
   JALALI DATE
===================================================== */
function jalaliToGregorian(
    jy,
    jm,
    jd
) {
    jy -= 979;
    let days =
        365 * jy
        +
        Math.floor(
            jy / 33
        ) * 8
        +
        Math.floor(
            ((jy % 33) + 3) / 4
        );
    for (
        let i = 1;
        i < jm;
        i++
    ) {
        days +=
            i <= 6
                ? 31
                : 30;
    }
    days +=
        jd - 1;
    // The Jalali epoch is 79 days ahead of the Gregorian day count.
    // Omitting this offset shifts every transaction marker by about 79 days.
    days += 79;
    let gy =
        1600
        +
        400 *
        Math.floor(
            days / 146097
        );
    days %=
        146097;
    if (
        days > 36524
    ) {
        gy +=
            100 *
            Math.floor(
                --days / 36524
            );
        days %=
            36524;
        if (
            days >= 365
        ) {
            days++;
        }
    }
    gy +=
        4 *
        Math.floor(
            days / 1461
        );
    days %=
        1461;
    if (
        days > 365
    ) {
        gy +=
            Math.floor(
                (days - 1) / 365
            );
        days =
            (days - 1) % 365;
    }
    const gd =
        days + 1;
    const leap =
        (
            gy % 4 === 0
            &&
            gy % 100 !== 0
        )
        ||
        gy % 400 === 0;
    const monthDays = [
        0,
        31,
        leap
            ? 29
            : 28,
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31
    ];
    let day =
        gd;
    let month =
        1;
    while (
        day >
        monthDays[month]
    ) {
        day -=
            monthDays[month];
        month++;
    }
    return new Date(
        gy,
        month - 1,
        day
    );
}
function daysBetweenInclusive(
    start,
    end
) {
    const startParts =
        String(start)
            .split("/")
            .map(Number);
    const endParts =
        String(end)
            .split("/")
            .map(Number);
    if (
        startParts.length !== 3
        ||
        endParts.length !== 3
    ) {
        return 0;
    }
    const startDate =
        jalaliToGregorian(
            startParts[0],
            startParts[1],
            startParts[2]
        );
    const endDate =
        jalaliToGregorian(
            endParts[0],
            endParts[1],
            endParts[2]
        );
    const difference =
        Math.floor(
            (
                endDate -
                startDate
            )
            /
            86400000
        );
    return Math.max(
        0,
        difference + 1
    );
}
function getTodayJalali() {
    const now =
        new Date();
    const gy =
        now.getFullYear();
    const gm =
        now.getMonth() + 1;
    const gd =
        now.getDate();
    const gdm = [
        0,
        31,
        59,
        90,
        120,
        151,
        181,
        212,
        243,
        273,
        304,
        334
    ];
    let jy =
        gy > 1600
            ? 979
            : 0;
    const y =
        gy > 1600
            ? gy - 1600
            : gy - 621;
    let days =
        365 * y
        +
        Math.floor(
            (y + 3) / 4
        )
        -
        Math.floor(
            (y + 99) / 100
        )
        +
        Math.floor(
            (y + 399) / 400
        )
        -
        80
        +
        gd
        +
        gdm[gm - 1];
    jy +=
        33 *
        Math.floor(
            days / 12053
        );
    days %= 12053;
    jy +=
        4 *
        Math.floor(
            days / 1461
        );
    days %= 1461;
    if (
        days > 365
    ) {
        jy +=
            Math.floor(
                (days - 1) / 365
            );
        days =
            (days - 1) % 365;
    }
    let jm;
    let jd;
    if (
        days < 186
    ) {
        jm =
            1 +
            Math.floor(
                days / 31
            );
        jd =
            1 +
            days % 31;
    }
    else {
        jm =
            7 +
            Math.floor(
                (days - 186) / 30
            );
        jd =
            1 +
            (days - 186) % 30;
    }
    return (
        jy
        +
        "/"
        +
        String(jm)
            .padStart(2, "0")
        +
        "/"
        +
        String(jd)
            .padStart(2, "0")
    );
}
function dateNumber(date) {
    const parts =
        String(date)
            .split("/")
            .map(Number);
    if (
        parts.length !== 3
    ) {
        return 0;
    }
    return (
        parts[0] * 10000
        +
        parts[1] * 100
        +
        parts[2]
    );
}
/* =====================================================
   BANK RATE HISTORY
   Rates are effective from their Jalali "from" date.
   No single/fallback annual bank rate is used.
===================================================== */
const BANK_RATE_PERIODS_CACHE_KEY = "financialDashboardBankRatePeriods_v1";
function saveBankRatePeriodsCache(periods) {
    try {
        localStorage.setItem(BANK_RATE_PERIODS_CACHE_KEY, JSON.stringify(periods));
    } catch (error) {}
}
function parseBankRatePeriods(rawPeriods) {
    if (!Array.isArray(rawPeriods) || rawPeriods.length === 0) return [];

    const periods = rawPeriods
        .map((item) => ({
            from: String(item?.from || "").trim(),
            rate: Number(item?.rate)
        }))
        .filter((item) =>
            /^\d{4}\/\d{2}\/\d{2}$/.test(item.from) &&
            Number.isFinite(item.rate) &&
            item.rate >= 0
        )
        .sort((a, b) => dateNumber(a.from) - dateNumber(b.from));

    if (periods.length === 0) return [];

    for (let i = 1; i < periods.length; i++) {
        if (periods[i].from === periods[i - 1].from) {
            throw new Error(
                "Duplicate bankRatePeriods date: " + periods[i].from
            );
        }
    }

    return periods;
}

function loadBankRatePeriodsCache() {
    try {
        const cached = JSON.parse(localStorage.getItem(BANK_RATE_PERIODS_CACHE_KEY) || "null");
        const parsed = parseBankRatePeriods(cached);
        if (parsed.length) return parsed;
    } catch (error) {}
    // One-time migration for users upgrading from R01: keep their previously
    // cached bank-rate history without reading bankRatePeriods from new data.json.
    try {
        const legacy = loadLocalData();
        const migrated = parseBankRatePeriods(legacy?.bankRatePeriods);
        if (migrated.length) {
            saveBankRatePeriodsCache(migrated);
            return migrated;
        }
    } catch (error) {}
    return [];
}
function getActiveBankRate(periods, date) {
    if (!Array.isArray(periods) || periods.length === 0) return null;
    let active = periods[0];
    for (const period of periods) {
        if (dateNumber(period.from) <= dateNumber(date)) {
            active = period;
        } else {
            break;
        }
    }
    return active?.rate ?? null;
}

function formatBankRate(rate) {
    if (!Number.isFinite(rate)) return "--";
    return Number.isInteger(rate)
        ? rate + "%"
        : rate.toFixed(2) + "%";
}

function jalaliDayIndex(date) {
    const value = jalaliToGregorian(
        ...String(date).split("/").map(Number)
    );
    return Date.UTC(
        value.getFullYear(),
        value.getMonth(),
        value.getDate()
    ) / 86400000;
}

function compoundValue(
    principal,
    startDate,
    endDate,
    bankRatePeriods
) {
    if (principal <= 0) return 0;

    let value = principal;
    const startDay = jalaliDayIndex(startDate);
    const endDay = jalaliDayIndex(endDate);

    if (endDay < startDay) return value;

    for (let i = 0; i < bankRatePeriods.length; i++) {
        const period = bankRatePeriods[i];
        const periodStart = jalaliDayIndex(period.from);
        const nextPeriodStart =
            i + 1 < bankRatePeriods.length
                ? jalaliDayIndex(bankRatePeriods[i + 1].from)
                : Infinity;

        const segmentStart = Math.max(startDay, periodStart);
        const segmentEnd = Math.min(
            endDay,
            nextPeriodStart - 1
        );

        if (segmentEnd >= segmentStart) {
            const days = segmentEnd - segmentStart + 1;
            value *= Math.pow(
                1 + period.rate / 100 / 365,
                days
            );
        }
    }

    return value;
}
/* =====================================================
   PORTFOLIO FIFO
===================================================== */
function calculatePortfolio(
    transactions,
    currentPrice,
    today,
    annualRate
) {
    const lots = [];
    let realizedPurchase =
        0;
    let realizedBank =
        0;
    let buyCount =
        0;
    let sellCount =
        0;
    for (
        const transaction
        of transactions
    ) {
        const amount =
            Number(
                transaction.amount
            );
        const price =
            Number(
                transaction.price
            );
        if (
            !Number.isFinite(amount)
            ||
            !Number.isFinite(price)
            ||
            amount <= 0
            ||
            price < 0
        ) {
            continue;
        }
        if (
            transaction.type === "buy"
        ) {
            buyCount++;
            lots.push({
                amount,
                price,
                date:
                    transaction.date
            });
        }
        else if (
            transaction.type === "sell"
        ) {
            sellCount++;
            let remaining =
                amount;
            while (
                remaining > 0
                &&
                lots.length > 0
            ) {
                const lot =
                    lots[0];
                const used =
                    Math.min(
                        remaining,
                        lot.amount
                    );
                const purchaseCost =
                    used *
                    lot.price;
                const saleRevenue =
                    used *
                    price;
                const bankCost =
                    compoundValue(
                        purchaseCost,
                        lot.date,
                        transaction.date,
                        annualRate
                    );
                realizedPurchase +=
                    saleRevenue -
                    purchaseCost;
                realizedBank +=
                    saleRevenue -
                    bankCost;
                lot.amount -=
                    used;
                remaining -=
                    used;
                if (
                    lot.amount <=
                    0.0000001
                ) {
                    lots.shift();
                }
            }
        }
    }
    const currentQuantity =
        lots.reduce(
            (
                total,
                lot
            ) =>
                total +
                lot.amount,
            0
        );
    const paidAmount =
        lots.reduce(
            (
                total,
                lot
            ) =>
                total +
                lot.amount *
                lot.price,
            0
        );
    const averagePurchasePrice =
        currentQuantity > 0
            ?
            paidAmount /
            currentQuantity
            :
            0;
    let bankValue =
        0;
    for (
        const lot
        of lots
    ) {
        bankValue +=
            compoundValue(
                lot.amount *
                lot.price,
                lot.date,
                today,
                annualRate
            );
    }
    const currentValue =
        currentQuantity *
        currentPrice;
    const bankAdjustedPrice =
        currentQuantity > 0
            ?
            bankValue /
            currentQuantity
            :
            0;
    const unrealizedPurchaseAmount =
        currentValue -
        paidAmount;
    const unrealizedBankAmount =
        currentValue -
        bankValue;
    const plPurchase =
        paidAmount > 0
            ?
            (
                unrealizedPurchaseAmount /
                paidAmount
            )
            *
            100
            :
            0;
    const plBank =
        bankValue > 0
            ?
            (
                unrealizedBankAmount /
                bankValue
            )
            *
            100
            :
            0;
    return {
        currentQuantity,
        paidAmount,
        averagePurchasePrice,
        bankAdjustedPrice,
        currentPrice,
        currentValue,
        bankValue,
        unrealizedPurchaseAmount,
        unrealizedBankAmount,
        plPurchase,
        plBank,
        realizedPurchase,
        realizedBank,
        buyCount,
        sellCount
    };
}
/* =====================================================
   PERIOD ENGINE
===================================================== */
function buildPeriods(
    transactions
) {
    const periods = [];
    let current =
        null;
    for (
        const transaction
        of transactions
    ) {
        if (
            transaction.type === "buy"
        ) {
            if (
                current
                &&
                current.transactions.length > 0
            ) {
                periods.push(
                    current
                );
            }
            current = {
                transactions: [
                    transaction
                ],
                startDate:
                    transaction.date,
                endDate:
                    transaction.date
            };
        }
        else if (
            transaction.type === "sell"
        ) {
            if (current) {
                current.transactions.push(
                    transaction
                );
                current.endDate =
                    transaction.date;
            }
        }
    }
    if (
        current
        &&
        current.transactions.length > 0
    ) {
        periods.push(
            current
        );
    }
    return periods;
}
function getLastPeriods(
    transactions,
    count
) {
    const periods =
        buildPeriods(
            transactions
        );
    return periods
        .slice(-count)
        .flatMap(
            period =>
                period.transactions
        );
}
function getLastPeriod(
    transactions
) {
    const periods =
        buildPeriods(
            transactions
        );
    if (
        periods.length === 0
    ) {
        return [];
    }
    return periods[
        periods.length - 1
    ].transactions;
}
/* =====================================================
   TOP PURCHASES
===================================================== */
function getTop6HighestPricePurchases(
    transactions
) {
    return transactions
        .filter(
            transaction =>
                transaction.type === "buy"
        )
        .slice()
        .sort(
            (
                a,
                b
            ) =>
                Number(b.price)
                -
                Number(a.price)
        )
        .slice(
            0,
            TOP_PURCHASES_COUNT
        );
}
/* =====================================================
   DISPLAY SECTION
===================================================== */
function displaySection(
    result,
    prefix,
    previousLivePrice,
    age
) {
    safeText(
        prefix +
        "Average",
        formatNumber(
            result.averagePurchasePrice
        )
    );
    safeText(
        prefix +
        "Paid",
        formatNumber(
            result.paidAmount
        )
    );
    safeText(
        prefix +
        "Adjusted",
        formatNumber(
            result.bankAdjustedPrice
        )
    );
    safeText(
        prefix +
        "Live",
        formatNumber(
            result.currentPrice
        )
    );
    if(prefix==="overall"&&typeof crossoverSaveState==="function") crossoverSaveState();
    setPriceArrow(
        prefix +
        "PriceArrow",
        result.currentPrice,
        previousLivePrice
    );
    safeText(
        prefix +
        "Quantity",
        formatNumber(
            result.currentQuantity
        )
    );
    safeText(
        prefix +
        "Value",
        formatNumber(
            result.currentValue
        )
    );
    safeText(
        prefix +
        "Bank",
        formatNumber(
            result.bankValue
        )
    );
    safeText(
        prefix +
        "PLPurchase",
        formatPercent(
            result.plPurchase
        )
    );
    safeText(
        prefix +
        "PLBank",
        formatPercent(
            result.plBank
        )
    );
    safeText(
        prefix +
        "UnrealizedPurchase",
        formatMoneySigned(
            result.unrealizedPurchaseAmount
        )
    );
    safeText(
        prefix +
        "UnrealizedBank",
        formatMoneySigned(
            result.unrealizedBankAmount
        )
    );
    safeText(
        prefix +
        "RealizedPurchase",
        formatMoneySigned(
            result.realizedPurchase
        )
    );
    safeText(
        prefix +
        "RealizedBank",
        formatMoneySigned(
            result.realizedBank
        )
    );
    setProfitLossClass(
        prefix +
        "PLPurchase",
        result.plPurchase
    );
    setProfitLossClass(
        prefix +
        "PLBank",
        result.plBank
    );
    setProfitLossClass(
        prefix +
        "UnrealizedPurchase",
        result.unrealizedPurchaseAmount
    );
    setProfitLossClass(
        prefix +
        "UnrealizedBank",
        result.unrealizedBankAmount
    );
    setProfitLossClass(
        prefix +
        "RealizedPurchase",
        result.realizedPurchase
    );
    setProfitLossClass(
        prefix +
        "RealizedBank",
        result.realizedBank
    );
    const mainPrice =
        $(
            prefix +
            "MainPrice"
        );
    if (mainPrice) {
        mainPrice.classList.remove(
            "status-green",
            "status-red"
        );
        if (
            result.plBank >= 0
        ) {
            mainPrice.classList.add(
                "status-green"
            );
        }
        else {
            mainPrice.classList.add(
                "status-red"
            );
        }
    }
    const resultBox =
        $(
            prefix +
            "Result"
        );
    if (resultBox) {
        if (
            result.plBank >= 0
        ) {
            resultBox.textContent =
                "Investment return is better than bank";
            resultBox.style.color =
                "#34d399";
        }
        else {
            resultBox.textContent =
                "Bank return is better than investment";
            resultBox.style.color =
                "#f87171";
        }
    }
    const tradeCount =
        $(
            prefix +
            "Trades"
        );
    if (tradeCount) {
        tradeCount.innerHTML =
            result.buyCount
            +
            " IN  •  "
            +
            result.sellCount
            +
            " OUT  •  <span class=\"last-age-value\">"
            +
            age
            +
            "</span>";

        /* Last Period only:
           0-10d: static white, no wave.
           10-20d: white with subtle wave.
           20-30d: wave + smooth white-to-strong-red color progression.
           >=30d: strong red. */
        if (prefix === "last") {
            tradeCount.classList.remove(
                "last-age-red",
                "last-age-warn",
                "last-age-white",
                "last-age-wave"
            );

            const ageMatch =
                String(age ?? "").match(/(\d+)\s*d/i);
            const ageDays = ageMatch
                ? Number(ageMatch[1])
                : NaN;
            const ageValue = tradeCount.querySelector(".last-age-value");

            if (ageValue && Number.isFinite(ageDays)) {
                ageValue.style.color = "";
                ageValue.style.textShadow = "";

                if (ageDays < 10) {
                    tradeCount.classList.add("last-age-white");
                }
                else if (ageDays < 20) {
                    tradeCount.classList.add("last-age-white", "last-age-wave");
                }
                else if (ageDays < 30) {
                    const proximity = (ageDays - 20) / 10;
                    const red = Math.round(226 + 29 * proximity);
                    const green = Math.round(232 - 190 * proximity);
                    const blue = Math.round(240 - 185 * proximity);
                    const glow = (0.14 + 0.34 * proximity).toFixed(2);
                    tradeCount.classList.add("last-age-warn", "last-age-wave");
                    ageValue.style.color = `rgb(${red}, ${green}, ${blue})`;
                    ageValue.style.textShadow =
                        `0 0 ${3 + Math.round(5 * proximity)}px rgba(255,70,70,${glow})`;
                }
                else {
                    tradeCount.classList.add("last-age-red", "last-age-wave");
                }
            }
            else {
                tradeCount.classList.add("last-age-white");
            }
        }
    }
}
/* =====================================================
   LAST ACTION
===================================================== */
function displayLastAction(
    transactions
) {
    const badge =
        $("lastAction");
    if (
        !badge
        ||
        !transactions.length
    ) {
        return;
    }
    const last =
        transactions[
            transactions.length - 1
        ];
    if (
        last.type === "buy"
    ) {
        badge.textContent =
            "IN";
        badge.className =
            "action-badge action-buy";
    }
    else {
        badge.textContent =
            "OUT";
        badge.className =
            "action-badge action-sell";
    }
}
/* =====================================================
   NOBITEX API
   Direct public orderbook endpoint. data.json is never used for the URL.
===================================================== */
const NOBITEX_ORDERBOOK_API = "https://apiv2.nobitex.ir/v3/orderbook/USDTIRT";
const NOBITEX_PRICE_DIVISOR = 10; // Nobitex orderbook values are Rial; dashboard shows Toman.
function parseBestAskPrice(payload) {
    const asks = Array.isArray(payload?.asks) ? payload.asks :
        Array.isArray(payload?.ask) ? payload.ask : [];
    const values = asks.map(level => {
        const raw = Array.isArray(level) ? level[0] : (level?.price ?? level?.rate ?? level?.value);
        const normalized = typeof raw === "string" ? raw.replace(/[\s,٬،]/g, "") : raw;
        const price = Number(normalized);
        return Number.isFinite(price) && price > 0 ? price : null;
    }).filter(value => value !== null);
    return values.length ? Math.min(...values) : null;
}
async function getCurrentPrice() {
    const response =
        await fetchApiWithProxyFallback(
            NOBITEX_ORDERBOOK_API,
            {
                method: "GET",
                headers: {
                    "Accept": "application/json"
                },
                cache: "no-store"
            },
            PRICE_REQUEST_TIMEOUT,
            payload => {
                const value = parseBestAskPrice(payload);
                if (String(payload?.status || "ok").toLowerCase() !== "ok" || value === null) {
                    throw new Error("Invalid Nobitex asks payload");
                }
                return true;
            }
        );
    if (!response.ok) {
        throw new Error(
            "Unable to get Nobitex price"
        );
    }
    const data =
        await response.json();
    /*
       Public orderbook asks are [price, amount] rows. Choose the lowest
       positive ask regardless of the order returned by the provider.
       Convert Rial to Toman after parsing.
    */
    const rawPrice = parseBestAskPrice(data);
    const price = Number(rawPrice) / NOBITEX_PRICE_DIVISOR;
    if (
        !Number.isFinite(price)
        ||
        price <= 0
    ) {
        throw new Error(
            "Invalid Nobitex ask price"
        );
    }
    return price;
}
/* =====================================================
   EXIR API
   Direct public APIs. Ticker.last is primary; orderbook is the backup.
===================================================== */
const EXIR_ORDERBOOK_API = "https://api.exir.io/v2/orderbook?symbol=usdt-irt";
const EXIR_TICKER_API = "https://api.exir.io/v2/ticker?symbol=usdt-irt";
let nativeExirCallbacks = {};
let nativeExirSequence = 0;
window.__nativeExirPriceResult = function(callbackId, priceText) {
    const callback = nativeExirCallbacks[callbackId];
    if (!callback) return;
    delete nativeExirCallbacks[callbackId];
    callback(String(priceText || ""));
};

function getNativeExirPrice(apiUrl = EXIR_ORDERBOOK_API) {
    return new Promise((resolve, reject) => {
        if (!window.AndroidNotifications || typeof window.AndroidNotifications.fetchExirPriceAsync !== "function") {
            reject(new Error("Native Exir bridge unavailable"));
            return;
        }
        const callbackId = "exir_" + Date.now() + "_" + (++nativeExirSequence);
        const timer = setTimeout(() => {
            delete nativeExirCallbacks[callbackId];
            reject(new Error("Native Exir timeout"));
        }, 4500);
        nativeExirCallbacks[callbackId] = value => {
            clearTimeout(timer);
            const price = Number(String(value || "").replace(/[\s,٬،]/g, ""));
            if (Number.isFinite(price) && price > 0) resolve(price);
            else reject(new Error("Invalid native Exir price"));
        };
        try {
            window.AndroidNotifications.fetchExirPriceAsync(callbackId, apiUrl);
        } catch (error) {
            clearTimeout(timer);
            delete nativeExirCallbacks[callbackId];
            reject(error);
        }
    });
}

function exirNumber(raw) {
    if (raw === null || raw === undefined || raw === "") return null;
    let value = raw;
    if (typeof value === "string") {
        value = value.trim().replace(/[\s,٬،]/g, "");
        value = value.replace(/[۰-۹]/g, digit => String("۰۱۲۳۴۵۶۷۸۹".indexOf(digit)));
        value = value.replace(/[٠-٩]/g, digit => String("٠١٢٣٤٥٦٧٨٩".indexOf(digit)));
    }
    const number = Number(value);
    return Number.isFinite(number) && number > 0 ? number : null;
}

function extractExirPair(payload) {
    const roots = [payload, payload?.data, payload?.result, payload?.data?.result]
        .filter(value => value && typeof value === "object");
    for (const root of roots) {
        const pair = root["usdt-irt"] || root["USDT-IRT"] || root["USDT_IRT"] || root["usdt_irt"];
        if (pair && typeof pair === "object") return pair;
        if (Array.isArray(root.asks)) return root;
    }
    return null;
}

function extractExirLowestAsk(payload) {
    const pair = extractExirPair(payload);
    const asks = Array.isArray(pair?.asks) ? pair.asks : [];
    const values = asks.map(level => {
        const raw = Array.isArray(level) ? level[0] : (level?.price ?? level?.rate ?? level?.value);
        return exirNumber(raw);
    }).filter(value => value !== null);
    return values.length ? Math.min(...values) : null;
}

function extractExirTickerPrice(payload) {
    const roots = [payload, payload?.data, payload?.result, payload?.data?.result]
        .filter(value => value && typeof value === "object");
    for (const root of roots) {
        const pair = root["usdt-irt"] || root["USDT-IRT"] || root["USDT_IRT"] || root["usdt_irt"] || root;
        const price = exirNumber(pair?.last ?? pair?.close ?? pair?.price ?? pair?.lastPrice);
        if (price !== null) return price;
    }
    return null;
}

async function fetchExirDirectPrice(url, parser, timeoutMs = 5000) {
    const response = await fetchWithTimeout(url, {
        method: "GET",
        headers: { "Accept": "application/json" },
        cache: "no-store"
    }, timeoutMs);
    if (!response?.ok) throw new Error("EXIR HTTP " + (response?.status ?? "unknown"));
    const payload = await response.json();
    const price = parser(payload);
    if (!Number.isFinite(price) || price <= 0) throw new Error("Invalid EXIR response payload");
    return price;
}

async function fetchExirProxyPrice(url, parser, timeoutMs = 4500) {
    const proxyUrl = "https://proxy.cors.dev/" + url;
    const response = await fetchWithTimeout(proxyUrl, {
        method: "GET",
        headers: { "Accept": "application/json" },
        cache: "no-store"
    }, timeoutMs);
    if (!response?.ok) throw new Error("EXIR proxy HTTP " + (response?.status ?? "unknown"));
    const payload = await response.json();
    const price = parser(payload);
    if (!Number.isFinite(price) || price <= 0) throw new Error("Invalid EXIR proxy payload");
    return price;
}

/*
   EXIR LIVE ORDERBOOK (WebSocket)
   Keep the existing dashboard markup and cache keys unchanged.
   Initial snapshot + incremental asks updates are applied to a local orderbook;
   the lowest positive ask is the live USDT/IRT sell price.
*/
const EXIR_WS_URL = "wss://api.exir.io/stream";
const EXIR_WS_SYMBOL = "usdt-irt";
const EXIR_WS_CHANNEL = "orderbook:" + EXIR_WS_SYMBOL;

let exirSocket = null;
let exirPingTimer = null;
let exirReconnectTimer = null;
let exirReconnectDelay = 2000;
let exirAsks = new Map();
let exirHasSnapshot = false;
let exirSocketStarted = false;
let exirPendingResolvers = [];

function exirApplyLevels(levels) {
    if (!Array.isArray(levels)) return;
    for (const level of levels) {
        if (!Array.isArray(level) || level.length < 2) continue;
        const price = exirNumber(level[0]);
        const quantity = Number(level[1]);
        if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(quantity)) continue;
        if (quantity <= 0) exirAsks.delete(price);
        else exirAsks.set(price, quantity);
    }
}

function exirBestAsk() {
    let best = Infinity;
    for (const [price, quantity] of exirAsks) {
        if (quantity > 0 && price > 0 && price < best) best = price;
    }
    return Number.isFinite(best) ? best : null;
}

function exirPublishLivePrice(price) {
    if (!Number.isFinite(price) || price <= 0) return;

    saveExirPrice(price);

    // Update the existing dashboard value immediately, without changing its layout.
    const element = document.getElementById("exirPrice");
    if (element) {
        const previous = Number(sessionStorage.getItem("previousExirPrice"));
        animateLivePrice("exirPrice", price, Number.isFinite(previous) && previous > 0 ? previous : null);
        setPriceArrow("exirPriceArrow", price, Number.isFinite(previous) && previous > 0 ? previous : null);
        sessionStorage.setItem("previousExirPrice", String(price));
    }

    clearPageIssue("exir");
    if (typeof syncNativeWidgetSnapshot === "function") syncNativeWidgetSnapshot();

    const pending = exirPendingResolvers.splice(0);
    for (const resolve of pending) resolve(price);
}

function exirHandleWsMessage(raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.error || msg.success === false) {
        recordApiDiagnostic({
            url: EXIR_WS_URL,
            outcome: "exir-ws-error",
            detail: String(msg.error || "Subscription failed")
        });
        return;
    }

    if (msg.topic !== "orderbook" || msg.symbol !== EXIR_WS_SYMBOL) return;
    if (!msg.data || !Array.isArray(msg.data.asks)) return;

    if (msg.action === "partial") {
        exirAsks.clear();
        exirApplyLevels(msg.data.asks);
        exirHasSnapshot = true;
    } else if (["insert", "update", "delete"].includes(msg.action)) {
        if (!exirHasSnapshot) return;
        exirApplyLevels(msg.data.asks);
    } else {
        return;
    }

    const price = exirBestAsk();
    if (price !== null) exirPublishLivePrice(price);
}

function scheduleExirReconnect() {
    clearInterval(exirPingTimer);
    clearTimeout(exirReconnectTimer);
    exirReconnectTimer = setTimeout(() => {
        exirReconnectDelay = Math.min(exirReconnectDelay * 1.5, 30000);
        connectExirWebSocket();
    }, exirReconnectDelay);
}

function connectExirWebSocket() {
    clearTimeout(exirReconnectTimer);
    clearInterval(exirPingTimer);

    exirAsks.clear();
    exirHasSnapshot = false;

    try {
        exirSocket = new WebSocket(EXIR_WS_URL);
    } catch (error) {
        recordApiDiagnostic({url: EXIR_WS_URL, outcome: "exir-ws-connect-error", detail: error?.message || error});
        scheduleExirReconnect();
        return;
    }

    exirSocket.onopen = () => {
        exirReconnectDelay = 2000;
        exirSocket.send(JSON.stringify({op: "subscribe", args: [EXIR_WS_CHANNEL]}));

        exirPingTimer = setInterval(() => {
            if (exirSocket && exirSocket.readyState === WebSocket.OPEN) {
                exirSocket.send(JSON.stringify({op: "ping"}));
            }
        }, 30000);
    };

    exirSocket.onmessage = event => exirHandleWsMessage(event.data);

    exirSocket.onerror = () => {
        recordApiDiagnostic({url: EXIR_WS_URL, outcome: "exir-ws-error", detail: "WebSocket connection error"});
    };

    exirSocket.onclose = () => {
        clearInterval(exirPingTimer);
        exirHasSnapshot = false;
        scheduleExirReconnect();
    };
}

/* Primary EXIR source: the worker returns a plain numeric USDT/IRT price.
   If the worker is unavailable or returns an invalid value, fall back to the
   existing EXIR WebSocket orderbook. No dashboard calculations or markup change. */
const EXIR_WORKER_PRICE_URL = "https://tether.fridnika.workers.dev/";

async function getExirPrice() {
    // 1) Always retry the worker first on every normal price refresh.
    // This means recovery is automatic: as soon as the worker responds again,
    // its averaged/conversion price immediately becomes the displayed EXIR price.
    try {
        const response = await fetchWithTimeout(EXIR_WORKER_PRICE_URL, {
            method: "GET",
            headers: { "Accept": "text/plain" },
            cache: "no-store"
        }, 4500);

        if (!response || !response.ok) {
            throw new Error("EXIR worker HTTP " + (response?.status ?? "unknown"));
        }

        const rawPrice = (await response.text()).trim();
        // Worker returns a plain number, for example: 257747.
        if (!/^\d+(?:\.\d+)?$/.test(rawPrice)) {
            throw new Error("Invalid EXIR worker response");
        }

        const workerPrice = Number(rawPrice);
        if (!Number.isFinite(workerPrice) || workerPrice <= 0) {
            throw new Error("Invalid EXIR worker price");
        }

        exirPublishLivePrice(workerPrice);
        return workerPrice;
    } catch (workerError) {
        recordApiDiagnostic({
            url: EXIR_WORKER_PRICE_URL,
            outcome: "exir-worker-fallback",
            detail: workerError?.message || String(workerError)
        });
    }

    // 2) Worker failed: use the original EXIR REST API price.
    // Try ticker first, then the orderbook's lowest ask. Direct requests are
    // preferred; the existing CORS proxy/native bridge are only fallbacks.
    const exirRestSources = [
        { url: EXIR_TICKER_API, parser: extractExirTickerPrice, name: "ticker" },
        { url: EXIR_ORDERBOOK_API, parser: extractExirLowestAsk, name: "orderbook" }
    ];

    for (const source of exirRestSources) {
        try {
            let price;
            try {
                price = await fetchExirDirectPrice(source.url, source.parser, 5000);
            } catch (directError) {
                recordApiDiagnostic({
                    url: source.url,
                    outcome: "exir-rest-direct-fallback",
                    detail: directError?.message || String(directError)
                });
                try {
                    price = await fetchExirProxyPrice(source.url, source.parser, 4500);
                } catch (proxyError) {
                    if (window.AndroidNotifications &&
                        typeof window.AndroidNotifications.fetchExirPriceAsync === "function") {
                        price = await getNativeExirPrice(source.url);
                    } else {
                        throw proxyError;
                    }
                }
            }

            if (Number.isFinite(price) && price > 0) {
                exirPublishLivePrice(price);
                recordApiDiagnostic({
                    url: source.url,
                    outcome: "exir-rest-fallback-success",
                    detail: "EXIR " + source.name + " is active while the primary worker is unavailable."
                });
                return price;
            }
        } catch (restError) {
            recordApiDiagnostic({
                url: source.url,
                outcome: "exir-rest-source-failed",
                detail: restError?.message || String(restError)
            });
        }
    }

    // 3) Last resort: retain the existing live EXIR WebSocket orderbook.
    if (!exirSocketStarted) {
        exirSocketStarted = true;
        connectExirWebSocket();
    }

    const current = exirBestAsk();
    if (exirHasSnapshot && current !== null) {
        exirPublishLivePrice(current);
        return current;
    }

    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            exirPendingResolvers = exirPendingResolvers.filter(fn => fn !== resolve);
            reject(new Error("EXIR WebSocket snapshot timeout"));
        }, 15000);
        exirPendingResolvers.push(price => {
            clearTimeout(timeout);
            resolve(price);
        });
    });
}

/* =====================================================
   BITPIN API
   Direct public USDT/IRT orderbook endpoint. data.json is never used for the URL.
===================================================== */
const BITPIN_ORDERBOOK_API = "https://api.bitpin.org/api/v1/mth/orderbook/USDT_IRT/";
async function getBitpinPrice() {
    const response =
        await fetchApiWithProxyFallback(
            BITPIN_ORDERBOOK_API,
            {
                method: "GET",
                headers: {
                    "Accept": "application/json"
                },
                cache: "no-store"
            },
            PRICE_REQUEST_TIMEOUT,
            payload => {
                const value = parseBestAskPrice(payload);
                if (value === null) throw new Error("Invalid Bitpin asks payload");
                return true;
            }
        );
    if (!response.ok) {
        throw new Error(
            "Unable to get Bitpin price"
        );
    }
    const data =
        await response.json();
    // Support both array rows [price, amount] and object rows {price}.
    const price = parseBestAskPrice(data);
    if (
        !Number.isFinite(price)
        ||
        price <= 0
    ) {
        throw new Error(
            "Invalid Bitpin ask price"
        );
    }
    return price;
}
/* =====================================================
   MARKET SOURCE BACKOFF
===================================================== */
const MARKET_SOURCE_RETRY_MS = 15000;
const marketSourceRetryAt = { nobitex: 0, exir: 0, bitpin: 0 };
async function requestMarketSource(key, request){
    const retryAt=Number(marketSourceRetryAt[key]||0);
    if(Date.now()<retryAt)throw new Error(`${key} retry cooldown active`);
    try{
        const value=await request();
        marketSourceRetryAt[key]=0;
        return value;
    }catch(error){
        const retryDelay=/429/.test(String(error?.message||error))?60000:MARKET_SOURCE_RETRY_MS;
        marketSourceRetryAt[key]=Date.now()+retryDelay;
        throw error;
    }
}

/* =====================================================
   AVERAGE LIVE PRICE
===================================================== */
function calculateAverageLivePrice(
    nobitexPrice,
    exirPrice,
    bitpinPrice,
    nobitexOnline,
    exirOnline,
    bitpinOnline
) {
    let prices = [];
    if (
        nobitexOnline
        &&
        Number(nobitexPrice) > 0
    ) {
        prices.push(
            Number(nobitexPrice)
        );
    }
    if (
        exirOnline
        &&
        Number(exirPrice) > 0
    ) {
        prices.push(
            Number(exirPrice)
        );
    }
    if (
        bitpinOnline
        &&
        Number(bitpinPrice) > 0
    ) {
        prices.push(
            Number(bitpinPrice)
        );
    }
    /*
       ONLINE:
       save new average
    */
    if (
        prices.length > 0
    ) {
        const average =
            prices.reduce(
                (
                    total,
                    price
                ) =>
                    total + price,
                0
            )
            /
            prices.length;
        // The displayed live average and its persistent cache use the same integer value.
        const roundedAverage = Math.round(average);
        saveAveragePrice(roundedAverage);
        return roundedAverage;
    }
    /*
       OFFLINE:
       use cached average
    */
    const cachedAverage =
        loadAveragePrice();
    if (
        cachedAverage !== null
    ) {
        return cachedAverage;
    }
    return null;
}
/* =====================================================
   LOAD DASHBOARD DATA
===================================================== */
async function loadDashboardData(options = {}) {
    const cacheOnly = options && options.cacheOnly === true;
    if (
        dashboardLoadInProgress
    ) {
        return dashboardLoadInProgress;
    }
    dashboardLoadInProgress =
        (async () => {
            /*
               Top-of-page dollar prices are refreshed by the dedicated
               startup/refresh path before other dashboard market requests.
               Keep only the independent crypto-value request here.
            */
            void loadCryptoValue().catch(error =>
                console.warn("Crypto value refresh failed:", error)
            );
            // Migrate any R01 bank-rate cache before saveLocalData narrows the
            // snapshot to transactions and cryptoHoldings only.
            let bankRatePeriods = loadBankRatePeriodsCache();
            let data = loadCachedDashboardData();
            if (!isValidDashboardData(data)) data = null;
            let usingCachedData =
                Boolean(data);
            /* =================================================
               DATA.JSON
            ================================================= */
            const shouldRefreshRemoteData = !cacheOnly &&
                (!data || getDashboardDataCacheAge() >= DATA_CACHE_MAX_AGE);
            const dataRequest = shouldRefreshRemoteData
                ? fetchWithTimeout(REMOTE_DATA_JSON_URL, { method: "GET", cache: "no-store", headers: { "Accept": "application/json" } }, 7000)
                    .then(async response => {
                        if (!response.ok) throw new Error(`data.json HTTP ${response.status}`);
                        const freshData = await response.json();
                        if (!isValidDashboardData(freshData)) throw new Error("data.json returned an invalid dashboard structure");
                        saveLocalData(freshData);
                        clearPageIssue("data-json");
                        return freshData;
                    })
                    .catch(error => {
                        const hasCachedDashboard = Boolean(data);
                        setPageIssue("data-json", hasCachedDashboard ? "data.json cached; refresh failed" : "data.json unavailable", hasCachedDashboard ? "cache" : "error");
                        throw error;
                    })
                : null;
            if (!data) {
                if (cacheOnly) throw new Error("No saved dashboard snapshot is available yet.");
                try {
                    data = await dataRequest;
                    usingCachedData = false;
                } catch (error) {
                    throw new Error("Offline and no saved dashboard data is available.");
                }
            } else if (dataRequest) {
                // The saved snapshot is already visible; replace it with fresh data
                // when the TTL expires. On failure, retain the saved snapshot.
                try {
                    data = await dataRequest;
                    usingCachedData = false;
                } catch (error) {
                    usingCachedData = true;
                    console.warn("Using last valid dashboard snapshot:", error);
                }
            } else {
                usingCachedData = true;
            }

            // Restore the R00 source of truth for bank-adjusted performance:
            // use bankRatePeriods delivered by data.json, then persist it in
            // the dedicated R03 cache so later compact snapshots retain it.
            if (Array.isArray(data?.bankRatePeriods) && data.bankRatePeriods.length) {
                try {
                    const freshBankRatePeriods = parseBankRatePeriods(data.bankRatePeriods);
                    if (freshBankRatePeriods.length) {
                        bankRatePeriods = freshBankRatePeriods;
                        saveBankRatePeriodsCache(bankRatePeriods);
                    }
                } catch (error) {
                    console.warn("Could not refresh bank-rate history; retaining the last valid cache:", error);
                }
            }
            if (bankRatePeriods.length) {
                clearPageIssue("bank-rate-history");
            } else {
                setPageIssue(
                    "bank-rate-history",
                    "Bank-rate history is not configured; bank-adjusted figures currently exclude bank interest",
                    "warn"
                );
            }

            /*
               CRYPTO PORTFOLIO
               Independent MEXC market panel.
               It does not block the main dashboard.
            */
            if (data && data.cryptoHoldings) {
                saveCryptoHoldingsCache(data.cryptoHoldings);
            }
            loadCryptoPortfolio(
                data
            ).catch(
                error =>
                    console.warn(
                        "Crypto portfolio load error:",
                        error
                    )
            );
            // Exchange API URLs and Rial/Toman conversion are project config,
            // not user data. Never read them from data.json.
            /* =================================================
               USE CACHE FIRST
            ================================================= */
            let currentPrice =
                loadNobitexPrice();
            let exirPrice =
                loadExirPrice();
            let bitpinPrice =
                loadBitpinPrice();
            let usingCachedNobitex =
                currentPrice !== null;
            let usingCachedExir =
                exirPrice !== null;
            let usingCachedBitpin =
                bitpinPrice !== null;
            let nobitexOnline =
                false;
            let exirOnline =
                false;
            let bitpinOnline =
                false;
            const priceErrors = [];
            /*
               No hard-coded market price is allowed.
               A missing source remains null until a live API value arrives.
            */
            /* =================================================
               LIVE MARKET REQUESTS
               The first paint must NEVER wait for the network.
               cacheOnly renders the last saved prices immediately;
               the normal pass refreshes them in the background.
            ================================================= */
            if (!cacheOnly) {
                const results =
                    await Promise.allSettled([
                        requestMarketSource("nobitex", () => getCurrentPrice()),
                        requestMarketSource("exir", () => getExirPrice()),
                        requestMarketSource("bitpin", () => getBitpinPrice())
                    ]);
                const nobitexResult =
                    results[0];
                const exirResult =
                    results[1];
                const bitpinResult =
                    results[2];
                if (
                    nobitexResult.status ===
                    "fulfilled"
                ) {
                    clearPageIssue("nobitex");
                    currentPrice =
                        nobitexResult.value;
                    nobitexOnline =
                        true;
                    usingCachedNobitex =
                        false;
                    saveNobitexPrice(
                        currentPrice
                    );
                }
                else {
                    priceErrors.push(
                        "Nobitex"
                    );
                    setPageIssue("nobitex", "Nobitex unavailable", "error");
                }
                if (
                    exirResult.status ===
                    "fulfilled"
                ) {
                    clearPageIssue("exir");
                    exirPrice =
                        exirResult.value;
                    exirOnline =
                        true;
                    usingCachedExir =
                        false;
                    saveExirPrice(
                        exirPrice
                    );
                }
                else {
                    priceErrors.push(
                        "Exir"
                    );
                    /* Keep the last valid EXIR value visible when live EXIR
                       is unavailable, exactly like Nobitex/Bitpin. */
                    exirPrice = loadExirPrice();
                    usingCachedExir = exirPrice !== null;
                    setPageIssue(
                        "exir",
                        usingCachedExir ? "Exir cached" : "Exir unavailable",
                        usingCachedExir ? "cache" : "error"
                    );
                }
                if (
                    bitpinResult.status ===
                    "fulfilled"
                ) {
                    clearPageIssue("bitpin");
                    bitpinPrice =
                        bitpinResult.value;
                    bitpinOnline =
                        true;
                    usingCachedBitpin =
                        false;
                    saveBitpinPrice(
                        bitpinPrice
                    );
                }
                else {
                    priceErrors.push(
                        "Bitpin"
                    );
                    setPageIssue("bitpin", "Bitpin unavailable", "error");
                }
            }
            /* =================================================
               MARKET CONNECTION COLORS
            ================================================= */
            updateMarketPriceConnectionColors(
                nobitexOnline,
                exirOnline,
                bitpinOnline
            );
            /* =================================================
               TODAY
            ================================================= */
            const today =
                getTodayJalali();
            safeText(
                "today",
                today
            );
            /* =================================================
               MARKET PRICES
            ================================================= */
            animateLivePrice(
                "currentPrice",
                currentPrice,
                Number(sessionStorage.getItem("previousNobitexPrice"))
            );
            animateLivePrice(
                "exirPrice",
                exirPrice,
                Number(sessionStorage.getItem("previousExirPrice"))
            );
            animateLivePrice(
                "bitpinPrice",
                bitpinPrice,
                Number(sessionStorage.getItem("previousBitpinPrice"))
            );
            safeText(
                "bankRate",
                formatBankRate(
                    getActiveBankRate(
                        bankRatePeriods,
                        today
                    )
                )
            );
            showLivePriceErrors(
                priceErrors
            );
            const averageLivePrice =
                calculateAverageLivePrice(
                    currentPrice,
                    exirPrice,
                    bitpinPrice,
                    nobitexOnline,
                    exirOnline,
                    bitpinOnline
                );
            if (
                averageLivePrice === null
            ) {
                throw new Error(
                    "No valid live market price is available."
                );
            }
            /*
               MARKET API status must reflect market connectivity only.
               `usingCachedData` means the dashboard data.json came from
               local cache; it does NOT mean the market APIs are offline.
               The old logic incorrectly raised "Market API Warn" on every
               normal refresh whenever a cached dashboard snapshot existed.
            */
            if (!navigator.onLine) {
                const cachedTime =
                    getCachedPriceTime();
                let message =
                    "Internet offline • Market prices may use cache";
                if (cachedTime) {
                    message += " • Saved: " + cachedTime;
                }
                setPageIssue("market-connection", message, "warn");
            }
            else {
                /* Individual Nobitex / Exir / Bitpin issues are reported
                   by their own page-issue keys above. Do not manufacture a
                   generic Market API warning just because data.json is cached. */
                clearPageIssue("market-connection");
            }
            const updateTime =
                new Date()
                    .toLocaleTimeString(
                        "en-US",
                        {
                            hour:
                                "2-digit",
                            minute:
                                "2-digit",
                            second:
                                "2-digit",
                            hour12:
                                false
                        }
                    );
            safeText(
                "updateTime",
                "Last Update: " +
                updateTime
            );
            /* =================================================
               PREVIOUS PRICES
            ================================================= */
            const previousNobitex =
                Number(
                    sessionStorage.getItem(
                        "previousNobitexPrice"
                    )
                );
            const previousExir =
                Number(
                    sessionStorage.getItem(
                        "previousExirPrice"
                    )
                );
            const previousBitpin =
                Number(
                    sessionStorage.getItem(
                        "previousBitpinPrice"
                    )
                );
            setPriceArrow(
                "topPriceArrow",
                currentPrice,
                Number.isFinite(
                    previousNobitex
                )
                    ? previousNobitex
                    : null
            );
            setPriceArrow(
                "exirPriceArrow",
                exirPrice,
                Number.isFinite(
                    previousExir
                )
                    ? previousExir
                    : null
            );
            setPriceArrow(
                "bitpinPriceArrow",
                bitpinPrice,
                Number.isFinite(
                    previousBitpin
                )
                    ? previousBitpin
                    : null
            );
            try {
                sessionStorage.setItem(
                    "previousNobitexPrice",
                    String(
                        currentPrice
                    )
                );
                sessionStorage.setItem(
                    "previousExirPrice",
                    String(
                        exirPrice
                    )
                );
                sessionStorage.setItem(
                    "previousBitpinPrice",
                    String(
                        bitpinPrice
                    )
                );
            }
            catch (error) {}
            /* =================================================
               TRANSACTIONS
            ================================================= */
            const transactions =
                Array.isArray(
                    data.transactions
                )
                    ?
                    data.transactions
                        .filter(
                            transaction =>
                                transaction
                                &&
                                (
                                    transaction.type ===
                                    "buy"
                                    ||
                                    transaction.type ===
                                    "sell"
                                )
                        )
                        .slice()
                        .sort(
                            (
                                a,
                                b
                            ) =>
                                dateNumber(
                                    a.date
                                )
                                -
                                dateNumber(
                                    b.date
                                )
                        )
                    :
                    [];
            const previousAverageLivePrice =
                Number(
                    sessionStorage.getItem(
                        "previousAverageLivePrice"
                    )
                );
            /* =================================================
               BUY AGE
            ================================================= */
            /*
               OVERALL
               Oldest BUY from all transactions
            */
            const overallBuys =
                transactions.filter(
                    transaction =>
                        transaction.type ===
                        "buy"
                );
            const overallFirstBuy =
                overallBuys.length > 0
                    ? overallBuys[0]
                    : null;
            /*
               TOP PURCHASES
               Oldest BUY from selected Top Purchases
            */
            const sixTransactions =
                getTop6HighestPricePurchases(
                    transactions
                );
            const sixFirstBuy =
                sixTransactions.length > 0
                    ?
                    sixTransactions
                        .slice()
                        .sort(
                            (
                                a,
                                b
                            ) =>
                                dateNumber(
                                    a.date
                                )
                                -
                                dateNumber(
                                    b.date
                                )
                        )[0]
                    :
                    null;
            /*
               LAST PERIOD
            */
            const lastTransactions =
                getLastPeriod(
                    transactions
                );
            const lastFirstBuy =
                lastTransactions.length > 0
                    ?
                    lastTransactions.find(
                        transaction =>
                            transaction.type ===
                            "buy"
                    )
                    :
                    null;
            const overallAge =
                daysFromBuyToToday(
                    overallFirstBuy,
                    today
                );
            const sixAge =
                daysFromBuyToToday(
                    sixFirstBuy,
                    today
                );
            const lastAge =
                daysFromBuyToToday(
                    lastFirstBuy,
                    today
                );
            /* =================================================
               OVERALL
            ================================================= */
            const overall =
                calculatePortfolio(
                    transactions,
                    averageLivePrice,
                    today,
                    bankRatePeriods
                );
            displaySection(
                overall,
                "overall",
                Number.isFinite(
                    previousAverageLivePrice
                )
                    ? previousAverageLivePrice
                    : null,
                overallAge
            );
            updateCardColor(
                "overallMainPrice",
                overall.plBank
            );
            safeText(
                "topPurchasesTitle",
                TOP_PURCHASES_COUNT
            );
            /* =================================================
               TOP PURCHASES
            ================================================= */
            const six =
                calculatePortfolio(
                    sixTransactions,
                    averageLivePrice,
                    today,
                    bankRatePeriods
                );
            displaySection(
                six,
                "six",
                Number.isFinite(
                    previousAverageLivePrice
                )
                    ? previousAverageLivePrice
                    : null,
                sixAge
            );
/* =================================================
   TOP 6 PURCHASE PRICES
================================================= */
sixTransactions.forEach(
    (
        transaction,
        index
    ) => {
        safeText(
            "sixPurchase" +
            (index + 1),
            formatNumber(
                Number(
                    transaction.price
                )
            )
        );
    }
);
/* Clear unused positions */
for (
    let i =
        sixTransactions.length + 1;
    i <= TOP_PURCHASES_COUNT;
    i++
) {
    safeText(
        "sixPurchase" + i,
        "-"
    );
}
            updateCardColor(
                "sixMainPrice",
                six.plBank
            );
            /* =================================================
               LAST PERIOD
            ================================================= */
            const last =
                calculatePortfolio(
                    lastTransactions,
                    averageLivePrice,
                    today,
                    bankRatePeriods
                );
            displaySection(
                last,
                "last",
                Number.isFinite(
                    previousAverageLivePrice
                )
                    ? previousAverageLivePrice
                    : null,
                lastAge
            );
            updateCardColor(
                "lastMainPrice",
                last.plBank
            );
            displayLastAction(
                lastTransactions
            );
            /* =================================================
               SAVE PREVIOUS AVERAGE
            ================================================= */
            try {
                sessionStorage.setItem(
                    "previousAverageLivePrice",
                    String(
                        averageLivePrice
                    )
                );
            }
            catch (error) {}
            /* =================================================
               FINISH LOADING
            ================================================= */
            const loading =
                $("loading");
            if (loading) {
                loading.style.display =
                    "none";
            }
            const dashboard =
                $("dashboard");
            if (dashboard) {
                dashboard.style.display =
                    "grid";
            }
        })();
    try {
        return await dashboardLoadInProgress;
    }
    finally {
        syncNativeWidgetSnapshot();
        dashboardLoadInProgress =
            null;
    }
}
/* =====================================================
   GLOBAL PULSE • GOLD + WTI
===================================================== */
/* R01: public, browser-safe market feed. Biquote exposes XAUUSD and USOIL
 * without an API key and provides daily OHLC candles for a stable
 * previous-day reference. */
const GLOBAL_PULSE_COMMODITY_BATCH_URL =
    "https://biquote.io/api/latest?symbols=XAUUSD&symbols=USOIL";
const GLOBAL_PULSE_GOLD_OHLC_URL =
    "https://biquote.io/api/XAUUSD/ohlc?interval=1d&limit=3";
const GLOBAL_PULSE_OIL_OHLC_URL =
    "https://biquote.io/api/USOIL/ohlc?interval=1d&limit=3";
const GLOBAL_PULSE_COMMODITY_CACHE_KEY = "globalPulseCommodityCache-v2";
const GLOBAL_PULSE_REFRESH_MS =
    60 * 1000;
let globalPulseTimer = null;
function formatGlobalPulseNumber(value, decimals = 2){
    const number = Number(value);
    if (!Number.isFinite(number)) return "-";
    return number.toLocaleString("en-US", {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}
function setGlobalPulseChange(id, value){
    const element = $(id);
    if (!element) return;
    const number = Number(value);
    element.classList.remove("positive","negative","flat");
    if (!Number.isFinite(number)){
        element.textContent = "--";
        element.classList.add("flat");
        return;
    }
    const sign = number > 0 ? "+" : "";
    element.textContent = `${sign}${number.toFixed(2)}%`;
    element.classList.add(number > 0 ? "positive" : number < 0 ? "negative" : "flat");
}
const GLOBAL_PULSE_ALERT_LEVELS = [-10,-9,-8,-7,-6,-5,-4,-3,-2,-1,1,2,3,4,5,6,7,8,9,10];
const GLOBAL_PULSE_ALERT_COOLDOWN = 5 * 60 * 1000;
const GLOBAL_PULSE_ALERT_STATE_KEY = "globalPulseAlertState-v2";
let globalPulseAlertState = { gold: {previousPercent:null,alertTimes:{}}, oil: {previousPercent:null,alertTimes:{}} };

function globalPulseReadAlertState(){
    try{
        const raw=localStorage.getItem(GLOBAL_PULSE_ALERT_STATE_KEY);
        const data=raw?JSON.parse(raw):{};
        for(const commodity of ["gold","oil"]){
            if(data&&data[commodity]){
                globalPulseAlertState[commodity].previousPercent=Number.isFinite(Number(data[commodity].previousPercent))
                    ? Number(data[commodity].previousPercent) : null;
                Object.assign(globalPulseAlertState[commodity].alertTimes,data[commodity].alertTimes||{});
            }
        }
    }catch(e){}
}
function globalPulseSaveAlertState(){
    try{ localStorage.setItem(GLOBAL_PULSE_ALERT_STATE_KEY,JSON.stringify(globalPulseAlertState)); }catch(e){}
}
async function globalPulseCheckNotification(commodity, percent, currentPrice=null){
    const current=Number(percent);
    if(!Number.isFinite(current))return;
    const state=globalPulseAlertState[commodity] || (globalPulseAlertState[commodity]={previousPercent:null,alertTimes:{}});
    if(!Number.isFinite(state.previousPercent)){
        state.previousPercent=current;
        globalPulseSaveAlertState();
        return;
    }
    const old=state.previousPercent;
    if(old===current)return;
    const crossed=GLOBAL_PULSE_ALERT_LEVELS.filter(level =>
        (old<level && current>=level) || (old>level && current<=level)
    );
    state.previousPercent=current;
    for(const level of crossed){
        const key=String(level),now=Date.now(),last=Number(state.alertTimes[key]||0);
        if(now-last<GLOBAL_PULSE_ALERT_COOLDOWN)continue;
        state.alertTimes[key]=now;
        const up=level>0;
        const arrow=up?"↑":"↓";
        const color=up?"🟢":"🔴";
        const pctText=`${level>0?"+":""}${level.toFixed(0)}%`;
        const label=commodity==="oil"?"Oil":"Gold";
        const title=`${color} ${arrow} GLOBAL PULSE`;
        const priceText=Number.isFinite(Number(currentPrice)) ? ` • Price ${formatGlobalPulseNumber(Number(currentPrice))}` : "";
        const message=`GLOBAL PULSE • ${label} • ${pctText}${priceText}`;
        try{ await sendDashboardNotification(title,message,"./icon-192.png","GLOBAL_PULSE"); }
        catch(e){ console.warn("Global Pulse notification failed",e); }
    }
    globalPulseSaveAlertState();
}
async function fetchGlobalPulseJson(url, validatePayload = null){
    const freshUrl = url + (url.includes("?") ? "&" : "?") + "fresh=" + Date.now();
    const response = await fetchWithFallback(
        [freshUrl, `https://proxy.cors.dev/${freshUrl}`],
        { method: "GET", cache: "no-store", headers: { "Accept": "application/json" } },
        8000,
        async candidate => {
            const payload = await candidate.json();
            if (!payload || typeof payload !== "object") throw new Error("Global Pulse returned invalid JSON structure");
            if (typeof validatePayload === "function") {
                const valid = await validatePayload(payload);
                if (valid === false) throw new Error("Global Pulse data validation failed");
            }
            return true;
        }
    );
    return response.json();
}


const GLOBAL_PULSE_IRAN_TZ = "Asia/Tehran";
const GLOBAL_PULSE_SESSION_DEFS = {
    Sydney: { tz:"Australia/Sydney", open:[7,0], close:[16,0], stateId:"globalPulseSydneyState", timerId:"globalPulseSydneyTimer" },
    London: { tz:"Europe/London", open:[8,0], close:[17,0], stateId:"globalPulseLondonState", timerId:"globalPulseLondonTimer" },
    "New York": { tz:"America/New_York", open:[8,0], close:[17,0], stateId:"globalPulseNewYorkState", timerId:"globalPulseNewYorkTimer" }
};
const GLOBAL_PULSE_IRAN_OPEN_MIN = 8 * 60 + 45;
const GLOBAL_PULSE_IRAN_CLOSE_MIN = 12 * 60 + 30;

function globalPulseParts(date, timeZone){
    const parts = new Intl.DateTimeFormat("en-US", {
        timeZone, weekday:"short", year:"numeric", month:"2-digit", day:"2-digit",
        hour:"2-digit", minute:"2-digit", second:"2-digit", hour12:false
    }).formatToParts(date);
    const out={};
    for(const part of parts) if(part.type!=="literal") out[part.type]=part.value;
    return {
        year:Number(out.year), month:Number(out.month), day:Number(out.day),
        hour:Number(out.hour), minute:Number(out.minute), second:Number(out.second),
        weekday:out.weekday
    };
}
function globalPulsePad(n){return String(n).padStart(2,"0");}
function globalPulseCountdown(ms){
    const total=Math.max(0,Math.floor(ms/1000));
    const d=Math.floor(total/86400), h=Math.floor((total%86400)/3600), m=Math.floor((total%3600)/60), sec=total%60;
    return `${d?d+"d ":""}${globalPulsePad(h)}:${globalPulsePad(m)}:${globalPulsePad(sec)}`;
}
function globalPulseClockText(date,timeZone){
    return date.toLocaleTimeString("en-US",{hour:"2-digit",minute:"2-digit",second:"2-digit",hour12:false,timeZone});
}
function globalPulseIranState(now){
    const p=globalPulseParts(now,GLOBAL_PULSE_IRAN_TZ);
    const day=(new Date(`${p.year}-${globalPulsePad(p.month)}-${globalPulsePad(p.day)}T00:00:00Z`)).getUTCDay();
    const minute=p.hour*60+p.minute+p.second/60;
    const activeDays=[0,1,2,3,6]; // Sat–Wed
    const makeLocalTarget=(daysFromNow, minuteOfDay)=>{
        const base=new Date(Date.UTC(p.year,p.month-1,p.day));
        const d=new Date(base.getTime()+daysFromNow*86400000);
        const targetLocal=new Date(Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),d.getUTCDate(),Math.floor(minuteOfDay/60),minuteOfDay%60,0));
        let target=new Date(targetLocal);
        for(let i=0;i<3;i++){
            const actual=globalPulseParts(target,GLOBAL_PULSE_IRAN_TZ);
            const desired=minuteOfDay, actualMin=actual.hour*60+actual.minute;
            target=new Date(target.getTime()+(desired-actualMin)*60000);
        }
        return target;
    };
    if(activeDays.includes(day)){
        if(minute<GLOBAL_PULSE_IRAN_OPEN_MIN){
            const target=makeLocalTarget(0,GLOBAL_PULSE_IRAN_OPEN_MIN);
            const diff=target.getTime()-now.getTime();
            return {open:false,target,label:`OPEN IN ${globalPulseCountdown(diff)}`};
        }
        if(minute<GLOBAL_PULSE_IRAN_CLOSE_MIN){
            const target=makeLocalTarget(0,GLOBAL_PULSE_IRAN_CLOSE_MIN);
            const diff=target.getTime()-now.getTime();
            return {open:true,target,label:`CLOSE IN ${globalPulseCountdown(diff)}`};
        }
    }
    let days=1;
    for(let i=1;i<=7;i++){
        const candidate=(day+i)%7;
        if(activeDays.includes(candidate)){days=i;break;}
    }
    const target=makeLocalTarget(days,GLOBAL_PULSE_IRAN_OPEN_MIN);
    return {open:false,target,label:`OPEN IN ${globalPulseCountdown(target.getTime()-now.getTime())}`};
}
function globalPulseNotifyKey(date,event){
    const p=globalPulseParts(date,GLOBAL_PULSE_IRAN_TZ);
    return `iran_${p.year}${globalPulsePad(p.month)}${globalPulsePad(p.day)}_${event}`;
}
async function globalPulseCheckIranNotification(now){
    if(!globalPulseNotificationAllowed(now)) return;
    const state=globalPulseIranState(now);
    if(state.open || !state.target) return;
    const p=globalPulseParts(now,GLOBAL_PULSE_IRAN_TZ);
    const minute=p.hour*60+p.minute+p.second/60;
    const open=GLOBAL_PULSE_IRAN_OPEN_MIN;
    let event=null;
    // Opening alerts: 5 minutes and 1 minute before open.
    if(minute>=open-5 && minute<open-4) event="open5";
    else if(minute>=open-1.5 && minute<open) event="open1";
    // Closing alert: 5 minutes before the Iran market closes at 12:30 Tehran.
    else if(minute>=GLOBAL_PULSE_IRAN_CLOSE_MIN-5 && minute<GLOBAL_PULSE_IRAN_CLOSE_MIN-4) event="close5";
    if(!event) return;
    const key=globalPulseNotifyKey(now,event), storageKey="globalPulseIranNotice-v3";
    try{
        const last=localStorage.getItem(storageKey);
        const sent=new Set(last?last.split("|"):[]);
        if(sent.has(key)) return;
        let title, message;
        if(event==="close5"){
            title="🇮🇷 ↓ IRAN MARKET";
            message="Iran market closes in 5 minutes • 12:30 Tehran";
        }else{
            const mins=event==="open5"?5:1;
            title="🇮🇷 ↑ IRAN MARKET";
            message=`Iran market opens in ${mins} minute${mins===1?"":"s"} • 08:45 Tehran`;
        }
        await sendDashboardNotification(title,message,"./icon-192.png","GLOBAL_TIME");
        sent.add(key);
        localStorage.setItem(storageKey,Array.from(sent).slice(-12).join("|"));
    }catch(e){}
}
function globalPulseSessionBoundary(now,def){
    const p=globalPulseParts(now,def.tz);
    const weekdayIndex={Sun:0,Mon:1,Tue:2,Wed:3,Thu:4,Fri:5,Sat:6}[p.weekday];
    const localMinutes=p.hour*60+p.minute+p.second/60;
    const openMin=def.open[0]*60+def.open[1], closeMin=def.close[0]*60+def.close[1];
    const weekday=weekdayIndex>=1 && weekdayIndex<=5;
    if(weekday && localMinutes>=openMin && localMinutes<closeMin){
        const ms=((closeMin-localMinutes)*60000);
        const target=new Date(now.getTime()+ms);
        return {open:true,target,label:`CLOSE IN ${globalPulseCountdown(ms)}`};
    }
    // Countdown to the next weekday open, preserving the city's local clock and DST.
    let days=1;
    if(weekday && localMinutes<openMin) days=0;
    else if(weekdayIndex===5) days=3;
    else if(weekdayIndex===6) days=2;
    else if(weekdayIndex===0) days=1;
    const provisional=new Date(now.getTime()+days*86400000);
    const targetParts=globalPulseParts(provisional,def.tz);
    const base=Date.UTC(targetParts.year,targetParts.month-1,targetParts.day,def.open[0],def.open[1],0);
    // Find the UTC instant corresponding to the target local time by iterating around the provisional point.
    let target=new Date(base);
    for(let i=0;i<3;i++){
        const actual=globalPulseParts(target,def.tz);
        const desired=def.open[0]*60+def.open[1], actualMin=actual.hour*60+actual.minute;
        const delta=(desired-actualMin)*60000;
        target=new Date(target.getTime()+delta);
    }
    return {open:false,target,label:`OPEN IN ${globalPulseCountdown(target.getTime()-now.getTime())}`};
}

const GLOBAL_PULSE_ECONOMIC_URL = "https://www.economicium.com/data/economic-calendar.json";
let globalPulseEconomicEvents = [];
let globalPulseEconomicLoadedAt = 0;

/*
 * Economicium's public calendar is a JSON snapshot.  The current response is:
 *   { data: { events: [...] } }
 * but we also accept the flatter { events: [...] } form so a harmless API
 * schema change cannot turn the dashboard into "No upcoming event".
 * The feed explicitly says its release times are converted to UTC.
 */
function globalPulseEconomicDate(event){
    if(!event || typeof event!=="object") return null;

    const candidates = [
        event.datetime,
        event.datetime_utc,
        event.eventUtc,
        event.scheduled_at,
        event.timestamp,
        event.eventTimeUtc,
        event.dateTime
    ];
    for(const value of candidates){
        if(value==null || String(value).trim()==="") continue;
        const ms=Date.parse(String(value).trim());
        if(Number.isFinite(ms)) return new Date(ms);
    }

    const date=String(event.date||event.eventDate||event.releaseDate||"").trim();
    const time=String(event.time||event.eventTime||event.releaseTime||"").trim();
    if(!date) return null;

    /* Economicium publishes date/time already converted to UTC. */
    const normalizedTime=/^\d{1,2}:\d{2}(:\d{2})?$/.test(time)
        ? time.split(":").map((v,i)=>i===0?String(v).padStart(2,"0"):v).join(":")
        : "00:00:00";
    const iso=`${date}T${normalizedTime.length===5?normalizedTime+":00":normalizedTime}Z`;
    const ms=Date.parse(iso);
    return Number.isFinite(ms)?new Date(ms):null;
}
function globalPulseEconomicImpact(event){
    const impact=String(event?.impact||event?.importance||event?.impact_level||"").toLowerCase();
    return impact==="high"||impact==="red"?3:impact==="medium"||impact==="orange"?2:1;
}
function globalPulseEconomicRelevance(event){
    const text=`${event?.title||event?.name||event?.event||""} ${event?.country||event?.currency||""}`.toLowerCase();
    let score=0;
    const groups=[
        {w:["fomc","federal reserve","fed rate","interest rate","rate decision","central bank","ecb","boe","bank of canada","bank of japan","rba","rbnz","snb"],s:5},
        {w:["cpi","inflation","ppi","pce","consumer price","producer price"],s:5},
        {w:["nonfarm","nfp","payroll","employment","unemployment","jobless","jobs report","labor force"],s:5},
        {w:["gdp","retail sales","industrial production","pmi","ism","consumer confidence"],s:3},
        {w:["oil","crude","petroleum","energy","inventory","inventories","opec"],s:5},
        {w:["gold","precious metal","metals"],s:4},
        {w:["bitcoin","crypto","digital asset"],s:4}
    ];
    for(const g of groups) if(g.w.some(k=>text.includes(k))) score=Math.max(score,g.s);
    return score;
}
function globalPulseEconomicSummary(event){
    const text=String(event?.title||event?.name||event?.event||"").toLowerCase();
    if(/fomc|federal reserve|fed rate|rate decision|central bank|ecb|boe|bank of canada|bank of japan|rba|rbnz|snb/.test(text)) return "Rates → USD / Gold / Crypto";
    if(/cpi|inflation|pce|consumer price|ppi|producer price/.test(text)) return "Inflation → USD / Gold / Crypto";
    if(/nonfarm|nfp|payroll|employment|unemployment|jobless|jobs report|labor force/.test(text)) return "Jobs → USD / Gold / Crypto";
    if(/retail sales|gdp|industrial production|pmi|ism|consumer confidence/.test(text)) return "Growth → USD / Risk";
    if(/oil|crude|petroleum|energy|inventory|inventories|opec/.test(text)) return "Energy → Oil / Inflation";
    if(/gold|precious metal|metals/.test(text)) return "Metals → Gold";
    if(/bitcoin|crypto|digital asset/.test(text)) return "Crypto → Risk / USD";
    return "Macro market event";
}
function globalPulseEconomicFindNext(now=new Date()){
    let best=null;
    for(const event of globalPulseEconomicEvents){
        const date=globalPulseEconomicDate(event); if(!date) continue;
        const diff=date.getTime()-now.getTime();
        if(diff<=0) continue;
        const hours=diff/3600000;
        const impact=globalPulseEconomicImpact(event), relevance=globalPulseEconomicRelevance(event);
        const priority=(hours<=24?relevance*1.8:0)+(impact*0.35);
        const key={event,date,diff,priority,impact,relevance};
        if(!best || (hours<=24 && priority>best.priority+0.15) || (hours>24 && best.diff>24*3600000 && diff<best.diff)) best=key;
    }
    if(!best){
        for(const event of globalPulseEconomicEvents){
            const date=globalPulseEconomicDate(event); if(!date) continue;
            const diff=date.getTime()-now.getTime();
            if(diff>0&&(!best||diff<best.diff)) best={event,date,diff,priority:0,impact:globalPulseEconomicImpact(event),relevance:globalPulseEconomicRelevance(event)};
        }
    }
    return best;
}
function globalPulseEconomicUpdate(now=new Date()){
    const root=$("globalPulseEconomicState")?.closest(".global-pulse-economic");
    const state=$("globalPulseEconomicState"), timer=$("globalPulseEconomicTimer");
    const country=$("globalPulseEconomicCountry"), eventEl=$("globalPulseEconomicEvent"), summary=$("globalPulseEconomicSummary");
    if(!state||!timer) return;
    const next=globalPulseEconomicFindNext(now);
    if(!next){
        if(eventEl) eventEl.textContent="No upcoming event";
        if(country) country.textContent="—";
        if(summary) summary.textContent="—";
        timer.textContent="--:--:--";
        state.classList.remove("open","closed");
        if(root) root.classList.remove("event-high","event-medium","event-low","event-now");
        return;
    }
    const event=next.event;
    const impactText=String(event?.impact||event?.importance||event?.impact_level||"").toLowerCase();
    const impact=next.impact;
    const title=String(event?.title||event?.name||event?.event||"Economic Event").trim();
    const c=String(event?.country||event?.currency||"").trim();
    if(eventEl) eventEl.textContent=title;
    if(country) country.textContent=c||"—";
    if(summary) summary.textContent=globalPulseEconomicSummary(event);
    timer.textContent=globalPulseCountdown(next.diff);
    // Persist the currently displayed next event to native SharedPreferences as well.
    // This is the hand-off that lets the native notification engine keep the ECONOMIC
    // 5-minute alert working after the app is closed or the network disappears.
    try { if(window.AndroidNotifications?.setGlobalEconomicState) window.AndroidNotifications.setGlobalEconomicState(c,title,next.date.getTime()); } catch(e){}
    state.title=`${c} • ${title} • ${next.date.toISOString()}`;
    state.classList.remove("open","closed");
    if(root){
        root.classList.remove("event-high","event-medium","event-low","event-now");
        root.classList.add(impact===3?"event-high":impact===2?"event-medium":"event-low");
        if(next.diff<=60000) root.classList.add("event-now");
    }
}
function globalPulseEconomicExtractEvents(data){
    const candidates=[
        data?.data?.events,
        data?.events,
        data?.calendar?.events,
        data?.data?.calendar?.events,
        Array.isArray(data)?data:null
    ];
    for(const list of candidates){
        if(Array.isArray(list)) return list;
    }
    return [];
}
const GLOBAL_PULSE_ECONOMIC_CACHE_KEY = "globalPulseEconomicCache-v2";
let globalPulseEconomicLoadInProgress = null;
function globalPulseReadEconomicCache(){
    try{
        const data=JSON.parse(localStorage.getItem(GLOBAL_PULSE_ECONOMIC_CACHE_KEY)||"{}");
        return Array.isArray(data?.events)?data:{};
    }catch(e){return {};}
}
function globalPulseSaveEconomicCache(events){
    try{localStorage.setItem(GLOBAL_PULSE_ECONOMIC_CACHE_KEY,JSON.stringify({events,savedAt:Date.now()}));}catch(e){}
}
function globalPulseSetEconomicCacheMode(enabled){
    const root=$("globalPulseEconomicState")?.closest(".global-pulse-economic");
    if(root) root.classList.toggle("cache-mode",!!enabled);
}
async function loadGlobalPulseEconomicEvents(force=false){
    if (globalPulseEconomicLoadInProgress) return globalPulseEconomicLoadInProgress;
    if(!force&&globalPulseEconomicEvents.length&&Date.now()-globalPulseEconomicLoadedAt<600000) return;
    globalPulseEconomicLoadInProgress = loadGlobalPulseEconomicEventsInternal(force);
    try { return await globalPulseEconomicLoadInProgress; }
    finally { globalPulseEconomicLoadInProgress = null; }
}
async function loadGlobalPulseEconomicEventsInternal(force=false){
    if(!force&&globalPulseEconomicEvents.length&&Date.now()-globalPulseEconomicLoadedAt<600000) return;
    try{
        const separator=GLOBAL_PULSE_ECONOMIC_URL.includes("?")?"&":"?";
        const url=`${GLOBAL_PULSE_ECONOMIC_URL}${separator}_=${Date.now()}`;
        const response=await fetchWithFallback(
            [url, `https://proxy.cors.dev/${url}`],
            {method:"GET",cache:"no-store",mode:"cors",headers:{"Accept":"application/json"}},
            12000,
            async candidate=>{
                const payload=await candidate.json();
                const rows=globalPulseEconomicExtractEvents(payload);
                if(!rows.some(event=>globalPulseEconomicDate(event))) throw new Error("Economic calendar returned no valid event records");
                return true;
            }
        );
        if(!response.ok) throw new Error(`Economic calendar HTTP ${response.status}`);
        const data=await response.json();
        const events=globalPulseEconomicExtractEvents(data);
        const parsed=events.filter(e=>globalPulseEconomicDate(e));
        if(!parsed.length) throw new Error("Economic calendar returned no valid events");
        parsed.sort((a,b)=>globalPulseEconomicDate(a)-globalPulseEconomicDate(b));
        globalPulseEconomicEvents=parsed;
        globalPulseEconomicLoadedAt=Date.now();
        globalPulseSaveEconomicCache(parsed);
        globalPulseSetEconomicCacheMode(false);
        clearPageIssue("global-economic");
        globalPulseEconomicUpdate(new Date());
    }catch(error){
        console.warn("Economic calendar unavailable:",error);
        const cached=globalPulseReadEconomicCache();
        const cachedEvents=Array.isArray(cached.events)?cached.events.filter(e=>globalPulseEconomicDate(e)):[];
        if(cachedEvents.length){
            cachedEvents.sort((a,b)=>globalPulseEconomicDate(a)-globalPulseEconomicDate(b));
            globalPulseEconomicEvents=cachedEvents;
            globalPulseEconomicLoadedAt=Date.now();
            globalPulseSetEconomicCacheMode(true);
            setPageIssue("global-economic", `ECONOMIC is offline — showing cached calendar${cached.savedAt?` · cached ${new Date(cached.savedAt).toISOString()}`:""}`, "cache");
        }else{
            globalPulseSetEconomicCacheMode(false);
            setPageIssue("global-economic", `ECONOMIC API error: ${error?.message || "Economic calendar unavailable"}`, "error");
        }
        /* Keep the last valid live/cache calendar so its countdown continues. */
        globalPulseEconomicUpdate(new Date());
    }
}

const GLOBAL_PULSE_SLEEP_KEY = "globalPulseNightMode-v1";
function globalPulseSleepEnabled(){
    try { return localStorage.getItem(GLOBAL_PULSE_SLEEP_KEY) === "1"; } catch(e){ return false; }
}
function globalPulseIsQuietHours(now=new Date()){
    const h=now.getHours();
    return h>=22 || h<8;
}
function globalPulseSetSleep(enabled){
    try { localStorage.setItem(GLOBAL_PULSE_SLEEP_KEY, enabled ? "1" : "0"); } catch(e){}
    try { if(window.AndroidNotifications?.setGlobalNightMode) window.AndroidNotifications.setGlobalNightMode(!!enabled); } catch(e){}
    const btn=$("globalPulseSleepToggle");
    if(btn){ btn.classList.toggle("active",!!enabled); btn.textContent=enabled?"☽":"☾"; btn.title=enabled?"Night mode ON · 22:00–08:00 notifications muted":"Night mode OFF · Global notifications enabled"; }
}
function globalPulseNotificationAllowed(now=new Date()){
    return !(globalPulseSleepEnabled() && globalPulseIsQuietHours(now));
}
function initGlobalPulseSleepToggle(){
    let enabled=false;
    try { enabled=localStorage.getItem(GLOBAL_PULSE_SLEEP_KEY)==="1"; } catch(e){}
    const btn=$("globalPulseSleepToggle");
    if(btn) btn.addEventListener("click",()=>globalPulseSetSleep(!globalPulseSleepEnabled()));
    globalPulseSetSleep(enabled);
}

async function globalPulseCheckEconomicNotification(now){
    if(!globalPulseNotificationAllowed(now)) return;
    const next=globalPulseEconomicFindNext(now);
    if(!next || next.diff>5*60*1000 || next.diff<0) return;
    const key=`economic_${next.date.getTime()}`;
    try{
        const sent=new Set((localStorage.getItem("globalPulseEconomicNotice-v1")||"").split("|"));
        if(sent.has(key)) return;
        const title=String(next.event?.title||next.event?.name||next.event?.event||"Economic Event").trim();
        const country=String(next.event?.country||next.event?.currency||"").trim();
        await sendDashboardNotification(`📅 ECONOMIC${country?" · "+country:""}`,`${title} starts in 5 minutes`,"./icon-192.png","GLOBAL_TIME");
        sent.add(key); localStorage.setItem("globalPulseEconomicNotice-v1",Array.from(sent).slice(-30).join("|"));
    }catch(e){}
}

async function globalPulseCheckEconomicExactNotification(now){
    if(!globalPulseNotificationAllowed(now)) return;
    const next=globalPulseEconomicFindNext(now);
    if(!next || next.diff>30000 || next.diff<0) return;
    const key=`economic_exact_${next.date.getTime()}`;
    try{
        const sent=new Set((localStorage.getItem("globalPulseEconomicExactNotice-v1")||"").split("|"));
        if(sent.has(key)) return;
        const title=String(next.event?.title||next.event?.name||next.event?.event||"Economic Event").trim();
        const country=String(next.event?.country||next.event?.currency||"").trim();
        await sendDashboardNotification(`🔔 ECONOMIC${country?" · "+country:""}`,`${title} is now live`,"./icon-192.png","GLOBAL_TIME");
        sent.add(key); localStorage.setItem("globalPulseEconomicExactNotice-v1",Array.from(sent).slice(-30).join("|"));
    }catch(e){}
}

async function globalPulseCheckSessionNotifications(now){
    if(!globalPulseNotificationAllowed(now)) return;
    for(const [name,def] of Object.entries(GLOBAL_PULSE_SESSION_DEFS)){
        const state=globalPulseSessionBoundary(now,def);
        if(!state.target) continue;
        const diff=state.target.getTime()-now.getTime();
        if(diff<0 || diff>5*60*1000) continue;
        const p=globalPulseParts(state.target,def.tz);
        const dayKey=`${p.year}${globalPulsePad(p.month)}${globalPulsePad(p.day)}`;
        const boundaryType=state.open ? "close5" : "open5";
        const key=`global_${name}_${dayKey}_${boundaryType}`;
        try{
            const sent=new Set((localStorage.getItem("globalPulseSessionNotice-v1")||"").split("|"));
            if(sent.has(key)) continue;
            const title=state.open ? `🌐 ${name.toUpperCase()} CLOSE` : `🌐 ${name.toUpperCase()} OPEN`;
            const message=state.open ? `${name} market closes in 5 minutes` : `${name} market opens in 5 minutes`;
            await sendDashboardNotification(title,message,"./icon-192.png","GLOBAL_TIME");
            sent.add(key); localStorage.setItem("globalPulseSessionNotice-v1",Array.from(sent).slice(-60).join("|"));
        }catch(e){}
    }
}

function globalPulseUpdateMarketClock(){
    const now=new Date();
    globalPulseEconomicUpdate(now);
    safeText("globalPulseUtcTime",globalPulseClockText(now,"UTC"));
    safeText("globalPulseTehranTime",globalPulseClockText(now,GLOBAL_PULSE_IRAN_TZ));
    const iran=globalPulseIranState(now);
    const iranState=$("globalPulseIranState"), iranTimer=$("globalPulseIranTimer");
    if(iranState){
        iranState.classList.toggle("open",iran.open);
        iranState.classList.toggle("closed",!iran.open);
        iranState.classList.toggle("iran-alert",!!iran.target && iran.target.getTime()-now.getTime()>0 && iran.target.getTime()-now.getTime()<=300000);
        iranState.classList.toggle("near-boundary",!iran.open && !!iran.target && iran.target.getTime()-now.getTime()>0 && iran.target.getTime()-now.getTime()<=300000);
    }
    if(iranTimer) iranTimer.textContent=iran.label;
    const active=[];
    const upcoming=[{name:"IRAN",state:iran}];
    if(iran.open) active.push("IRAN");
    for(const [name,def] of Object.entries(GLOBAL_PULSE_SESSION_DEFS)){
        const state=globalPulseSessionBoundary(now,def);
        const el=$(def.stateId), timer=$(def.timerId);
        if(el){
            el.classList.toggle("open",state.open);
            el.classList.toggle("closed",!state.open);
            const boundaryDiff=state.target ? state.target.getTime()-now.getTime() : Infinity;
            el.classList.toggle("near-boundary",!state.open && boundaryDiff>0 && boundaryDiff<=300000);
        }
        if(timer) timer.textContent=state.label;
        if(state.open) active.push(name.toUpperCase());
        upcoming.push({name:name.toUpperCase(),state});
    }
    const activeEl=$("globalPulseActiveMarkets");
    if(activeEl) activeEl.textContent=active.length?`· ${active.join(" · ")}`:"· CLOSED";

    const urgent=$("globalPulseUrgentTimer");
    if(urgent){
        let best=null;
        for(const item of upcoming){
            if(!item.state?.target) continue;
            const diff=item.state.target.getTime()-now.getTime();
            if(diff>0 && diff<=300000 && (!best || diff<best.diff)){
                const opening=!item.state.open;
                best={...item,diff,opening};
            }
        }
        if(best){
            urgent.textContent=`⚡ ${best.name} ${best.opening?"OPEN":"CLOSE"} IN ${globalPulseCountdown(best.diff)}`;
            urgent.classList.add("active","pulse");
        }else{
            urgent.textContent="";
            urgent.classList.remove("active","pulse");
        }
    }
    globalPulseCheckIranNotification(now);
    globalPulseCheckSessionNotifications(now);
    globalPulseCheckEconomicNotification(now);
}

function startGlobalPulseMarketClock(){
    initGlobalPulseSleepToggle();
    globalPulseUpdateMarketClock();
    clearInterval(window.globalPulseMarketClockTimer);
    clearInterval(window.globalPulseInfoRotateTimer);
    clearInterval(window.globalPulseMobileClockTimer);
    window.globalPulseMarketClockTimer=setInterval(globalPulseUpdateMarketClock,1000);
}

const GLOBAL_PULSE_COMPACT_ASSET_KEY = "globalPulseCompactAsset-v1";
function globalPulseCompactAssetRead(){
    try{const v=JSON.parse(localStorage.getItem(GLOBAL_PULSE_COMPACT_ASSET_KEY)||"null");return v&&typeof v==="object"?v:{type:""};}catch(e){return {type:""};}
}
function globalPulseCompactAssetPrice(type,symbol=""){
    if(type==="gold"){
        const change=document.getElementById("globalGoldChange");
        const direction=change?.classList.contains("positive")?1:change?.classList.contains("negative")?-1:0;
        const pctText=(change?.textContent||"").replace(/[^0-9+-.]/g,"");
        const pct=pctText?Number(pctText):NaN;
        return {name:"GOLD",price:document.getElementById("globalGoldPrice")?.textContent?.trim()||"-",direction,dailyChangePercent:Number.isFinite(pct)?pct:direction};
    }
    if(type==="oil"){
        const change=document.getElementById("globalOilChange");
        const direction=change?.classList.contains("positive")?1:change?.classList.contains("negative")?-1:0;
        const pctText=(change?.textContent||"").replace(/[^0-9+-.]/g,"");
        const pct=pctText?Number(pctText):NaN;
        return {name:"OIL",price:document.getElementById("globalOilPrice")?.textContent?.trim()||"-",direction,dailyChangePercent:Number.isFinite(pct)?pct:direction};
    }
    if(type==="tse"){
        const row=Array.from(document.querySelectorAll("#globalTsetmcCards .global-pulse-tsetmc-card")).find(el=>String(el.dataset.tsetmcSymbol||"").trim()===String(symbol||"").trim());
        if(row){
            const price=row.querySelector(".global-pulse-tsetmc-group:first-child .global-pulse-tsetmc-price")?.textContent?.trim()||"-";
            const pct=row.querySelector(".global-pulse-tsetmc-group:first-child .global-pulse-tsetmc-percent");
            const direction=pct?.classList.contains("positive")?1:pct?.classList.contains("negative")?-1:0;
            const pctText=(pct?.textContent||"").replace(/[^0-9+-.]/g,"");
            const dailyChangePercent=pctText?Number(pctText):NaN;
            return {name:String(symbol||"TSE"),price,direction,dailyChangePercent:Number.isFinite(dailyChangePercent)?dailyChangePercent:direction};
        }
    }
    return {name:"",price:"-",direction:0};
}
function globalPulseRefreshCompactAssetDisplay(){
    const value=globalPulseCompactAssetRead();
    const wrap=document.getElementById("rfkjUltraGlobalAsset"), price=document.getElementById("rfkjUltraGlobalAssetPrice"), values=wrap?.closest(".live-price-values");
    if(!wrap||!price)return;
    const has=!!value.type, item=has?globalPulseCompactAssetPrice(value.type,value.symbol):{name:"",price:"-",direction:0};
    const key=has?`${value.type}:${String(value.symbol||"").trim()}`:"";
    const previousKey=String(price.dataset.globalCompactKey||"");
    const previousPrice=String(price.dataset.globalCompactPrice||"");
    const changed=has && key===previousKey && previousPrice && previousPrice!==String(item.price||"");
    wrap.hidden=!has; price.textContent=item.price||"-";
    price.classList.remove("global-compact-up","global-compact-down","global-compact-flat","daily-positive","daily-negative","daily-neutral");
    const daily=Number(item.dailyChangePercent);
    price.classList.add(!Number.isFinite(daily)||daily===0?"daily-neutral":daily>0?"daily-positive":"daily-negative");
    if(changed && item.direction!==0){
        // Force a fresh animation only when the selected asset price actually changes.
        void price.offsetWidth;
        price.classList.add(item.direction>0?"global-compact-up":"global-compact-down");
    }else{
        price.classList.add("global-compact-flat");
    }
    price.dataset.globalCompactKey=key;
    price.dataset.globalCompactPrice=String(item.price||"");
    if(values) values.classList.toggle("has-global-asset",has);
}
function globalPulseCompactAssetWrite(type,symbol=""){
    const current=globalPulseCompactAssetRead();
    const nextType=type||"";
    const nextSymbol=String(symbol||"").trim();
    const sameActive=current.type===nextType && (nextType!=="tse" || String(current.symbol||"").trim()===nextSymbol);
    const value=sameActive?{type:"",symbol:""}:{type:nextType,symbol:nextSymbol};
    try{localStorage.setItem(GLOBAL_PULSE_COMPACT_ASSET_KEY,JSON.stringify(value));}catch(e){}
    try{window.AndroidNotifications?.setGlobalCompactAsset?.(value.type,value.symbol);}catch(e){}
    document.querySelectorAll(".global-pulse-compact-select").forEach(btn=>{
        btn.classList.toggle("active",btn.dataset.globalCompactAsset===value.type && (value.type!=="tse" || btn.dataset.globalCompactSymbol===value.symbol));
    });
    globalPulseRefreshCompactAssetDisplay();
}
let globalPulseCompactButtonsReady=false;
function initGlobalPulseCompactAssetButtons(){
    if(!globalPulseCompactButtonsReady){
        globalPulseCompactButtonsReady=true;
        document.addEventListener("click",event=>{
            // The TSETMC edit pencil also uses the compact-button visual class.
            // Handle it first in capture phase so the compact-asset handler cannot
            // swallow the click before the symbol editor opens.
            const editBtn=event.target.closest?.(".global-tsetmc-edit[data-tsetmc-edit]");
            if(editBtn){
                event.preventDefault();
                event.stopPropagation();
                globalPulseTsetmcOpenEditor(editBtn.dataset.tsetmcEdit);
                return;
            }
            const btn=event.target.closest?.(".global-pulse-compact-select");
            if(!btn)return;
            event.preventDefault(); event.stopPropagation();
            globalPulseCompactAssetWrite(btn.dataset.globalCompactAsset||"",btn.dataset.globalCompactSymbol||"");
        },true);
    }
    // Restore the persisted selection without toggling it off.
    // IMPORTANT: globalPulseCompactAssetWrite() is a toggle action, so calling it
    // during init would deactivate the saved item after every page refresh.
    const current=globalPulseCompactAssetRead();
    const currentType=String(current.type||"");
    const currentSymbol=String(current.symbol||"").trim();
    document.querySelectorAll(".global-pulse-compact-select").forEach(btn=>{
        btn.classList.toggle("active",
            btn.dataset.globalCompactAsset===currentType &&
            (currentType!=="tse" || String(btn.dataset.globalCompactSymbol||"").trim()===currentSymbol)
        );
    });
    globalPulseRefreshCompactAssetDisplay();
}
const GLOBAL_PULSE_TSE_WORKER_URL = "https://tsetmc.rxw1.workers.dev/";
const GLOBAL_PULSE_TSE_FALLBACK_URL = "https://ropmwamrcvvuwrvajxdr.supabase.co/functions/v1/dynamic-service/";
const GLOBAL_PULSE_TSE_ACTIVE_API_KEY = "globalPulseTsetmcActiveApi-v1";
let globalPulseTsetmcActiveApiIndex = 0; // 0=Worker, 1=Supabase
const GLOBAL_PULSE_TSE_CACHE_KEY = "globalPulseTsetmcCache-v1";
const GLOBAL_PULSE_TSE_ALERT_STATE_KEY = "globalPulseTsetmcAlertState-v1";
const GLOBAL_PULSE_TSE_REFRESH_MS = 10 * 1000;
const GLOBAL_PULSE_TSE_START_MIN = 8 * 60 + 45;
const GLOBAL_PULSE_TSE_END_MIN = 18 * 60;
const GLOBAL_PULSE_TSE_ACTIVE_DAYS = new Set([0,1,2,3,6]); // Sat-Wed in Tehran
const GLOBAL_PULSE_TSE_DEFAULT_RESULTS = [];
let globalPulseTsetmcTimer = null;
let globalPulseTsetmcLoopEnabled = false;
let globalPulseTsetmcLoadInProgress = false;
let globalPulseTsetmcFailureCount = 0;
let globalPulseTsetmcLastFetchAt = 0;
let globalPulseTsetmcAlertState = {};

const GLOBAL_PULSE_TSE_SLOTS_KEY = "globalPulseTsetmcSlots-v1";
const GLOBAL_PULSE_TSE_API_BASE = "https://cdn.tsetmc.com/api";
let globalPulseTsetmcEditorSlot = 1;
let globalPulseTsetmcEditorCandidate = null;
let globalPulseTsetmcEditorResults = [];
const globalPulseTsetmcQuoteCache = new Map();
const globalPulseTsetmcQuoteInFlight = new Map();

function globalPulseTsetmcReadConfig(){
    try{
        const raw=JSON.parse(localStorage.getItem(GLOBAL_PULSE_TSE_SLOTS_KEY)||"null");
        const slots=Array.isArray(raw?.slots)?raw.slots.slice(0,4):[];
        while(slots.length<4)slots.push(null);
        return {count:Math.max(1,Math.min(4,Number(raw?.count)||1)),slots:slots.map(x=>x&&typeof x==="object"?x:null)};
    }catch(e){return {count:1,slots:[null,null,null,null]};}
}
function globalPulseTsetmcGetSlots(){
    const config=globalPulseTsetmcReadConfig();
    return config.slots.slice(0,config.count);
}
function globalPulseTsetmcSyncNative(){
    try{
        const config=globalPulseTsetmcReadConfig();
        const list=config.slots.slice(0,config.count).map((item,index)=>item?({
            slot:index+1,symbol:String(item.symbol||""),name:String(item.name||""),insCode:String(item.insCode||"")
        }):null).filter(item=>item&&item.symbol&&/^\d+$/.test(item.insCode));
        if(window.AndroidNotifications&&typeof window.AndroidNotifications.setTsetmcWatchlistJson==="function"){
            window.AndroidNotifications.setTsetmcWatchlistJson(JSON.stringify(list));
        }
    }catch(e){console.warn("TSETMC native watchlist sync failed",e);}
}
function globalPulseTsetmcResetCrossoverForSlotChanges(previous,next){
    try{
        if(typeof crossoverInstances==="undefined"||!Array.isArray(crossoverInstances))return;
        let changed=false;
        for(let slot=1;slot<=4;slot++){
            const before=previous.slots[slot-1],after=next.slots[slot-1];
            const wasActive=slot<=previous.count,isActive=slot<=next.count;
            const beforeCode=wasActive?String(before?.insCode||""):"";
            const afterCode=isActive?String(after?.insCode||""):"";
            if(beforeCode===afterCode)continue;
            const asset=`TSE|SLOT${slot}`;
            const oldSymbol=String(before?.symbol||"").trim().toUpperCase();
            const legacyAsset=oldSymbol?`TSE|${oldSymbol}`:"";
            crossoverInstances.forEach((state,index)=>{
                if(!state)return;
                let mapped=false;
                if(legacyAsset&&String(state.coin||"").toUpperCase()===legacyAsset){state.coin=asset;mapped=true;}
                if(legacyAsset&&String(state.activeCoin||"").toUpperCase()===legacyAsset){state.activeCoin=asset;mapped=true;}
                if(mapped){changed=true;crossoverUpdateUI(index+1);}
                const monitored=String(state.running?(state.activeCoin||state.coin):state.coin||"").toUpperCase();
                if(monitored!==asset)return;
                state.previousPrice=null;state.latestPrice=null;state.latestUpdated=null;
                state.dailyChangePercent=null;state.priceDirection=0;state.wasOffline=false;
                crossoverUpdateUI(index+1);
                if(state.running)setTimeout(()=>void crossoverFetchOne(index+1,false),0);
                changed=true;
            });
        }
        if(changed)crossoverSaveState();
    }catch(error){console.warn("TSETMC Crossover slot refresh failed",error);}
}
function globalPulseTsetmcSaveConfig(config){
    const previous=globalPulseTsetmcReadConfig();
    try{localStorage.setItem(GLOBAL_PULSE_TSE_SLOTS_KEY,JSON.stringify(config));}catch(e){}
    globalPulseTsetmcSyncNative();
    globalPulseTsetmcResetCrossoverForSlotChanges(previous,config);
    globalPulseRenderTsetmc(readGlobalPulseTsetmcCache().results||[]);
    try{window.dispatchEvent(new Event("crossover-assets-updated"));}catch(e){}
    globalPulseTsetmcLastFetchAt=0;
    void loadGlobalPulseTsetmc(true);
}
function globalPulseTsetmcSetCount(count){
    const config=globalPulseTsetmcReadConfig();
    config.count=Math.max(1,Math.min(4,Number(count)||1));
    globalPulseTsetmcSaveConfig(config);
    document.querySelectorAll(".global-tsetmc-slot-btn").forEach(btn=>btn.classList.toggle("active",Number(btn.dataset.tsetmcCount)<=config.count));
}
const GLOBAL_PULSE_TSE_API_TIMEOUT_MS = Math.max(20000, Number(PRICE_REQUEST_TIMEOUT)||10000);
const GLOBAL_PULSE_TSE_API_HOSTS = ["https://cdn.tsetmc.com/api", "https://cdn10.tsetmc.com/api"];
const globalPulseTsetmcInstrumentInfoCache = new Map();
async function globalPulseTsetmcFetchJson(path,validator){
    const cleanPath=String(path||"").replace(/^\/+/,"");
    let lastError=null;
    for(const base of GLOBAL_PULSE_TSE_API_HOSTS){
        const url=`${base}/${cleanPath}`;
        try{
            const response=await fetchApiWithProxyFallback(url,{method:"GET",headers:{"Accept":"application/json"},cache:"no-store"},GLOBAL_PULSE_TSE_API_TIMEOUT_MS,async clonedResponse=>{
                try{
                    const payload=await clonedResponse.json();
                    if(typeof validator==="function")return validator(payload);
                    return payload&&typeof payload==="object";
                }catch(e){return false;}
            });
            return await response.json();
        }catch(error){lastError=error;console.warn("TSETMC API host failed; trying alternate host:",url,error);}
    }
    throw lastError||new Error("TSETMC API unavailable on all hosts");
}
function globalPulseTsetmcUnwrapInstrumentInfo(payload){
    if(!payload||typeof payload!=="object")return null;
    const candidates=[
        payload.instrumentIdentity,payload.instrumentInfo,payload.instrument,
        payload.data?.instrumentIdentity,payload.data?.instrumentInfo,payload.data?.instrument,
        payload.result?.instrumentIdentity,payload.result?.instrumentInfo,payload.result?.instrument,
        payload.data,payload.result,payload
    ];
    for(const item of candidates){
        if(!item||typeof item!=="object"||Array.isArray(item))continue;
        const nested=item.instrument&&typeof item.instrument==="object"?item.instrument:null;
        const symbol=globalPulseTsetmcPickField(item,["lVal18AFC","symbol","symbolName","Symbol","symbolFa"])
            ||globalPulseTsetmcPickField(nested,["lVal18AFC","symbol","symbolName","Symbol","symbolFa"]);
        if(symbol)return nested?{...item,...nested}:item;
    }
    return null;
}
async function globalPulseTsetmcGetInstrumentInfo(insCode){
    const code=String(insCode||"").trim();
    if(!/^\d{8,}$/.test(code))throw new Error("کد عددی معتبر TSETMC نیست");
    const cached=globalPulseTsetmcInstrumentInfoCache.get(code);
    if(cached&&Date.now()-cached.at<24*60*60*1000)return cached.value;
    let raw=null,lastError=null;
    // The user's verified TSETMC endpoint returns an instrumentSearch array.
    // Match the exact InsCode; do not rely on InstrumentIdentity response wrappers.
    try{
        const lookup=await globalPulseTsetmcFetchJson(
            `Shareholder/GetInstrumentShareHolderLast/${encodeURIComponent(code)}`,
            payload=>globalPulseTsetmcExtractSearchRows(payload).some(row=>
                String(globalPulseTsetmcPickField(row,["insCode","InsCode","instrumentCode","InstrumentCode"])||"").trim()===code
            )
        );
        const exactRow=globalPulseTsetmcExtractSearchRows(lookup).find(row=>
            String(globalPulseTsetmcPickField(row,["insCode","InsCode","instrumentCode","InstrumentCode"])||"").trim()===code
        );
        if(exactRow)raw=exactRow;
        else throw new Error("کد دقیق در پاسخ instrumentSearch پیدا نشد");
    }catch(error){lastError=error;console.warn("TSETMC exact instrumentSearch lookup failed; trying identity endpoint:",code,error);}
    // Keep standard TSETMC identity endpoints as fallback only.
    if(!raw){
        try{
            const identity=await globalPulseTsetmcFetchJson(
                `Instrument/GetInstrumentIdentity/${encodeURIComponent(code)}`,
                data=>!!(data?.instrumentIdentity||data?.data?.instrumentIdentity||data?.result?.instrumentIdentity)
            );
            raw=globalPulseTsetmcUnwrapInstrumentInfo(identity);
        }catch(error){lastError=error;console.warn("TSETMC identity endpoint failed; trying instrument info:",code,error);}
    }
    // InstrumentInfo is a second, independent source for the same identity fields.
    if(!raw){
        try{
            const info=await globalPulseTsetmcFetchJson(
                `Instrument/GetInstrumentInfo/${encodeURIComponent(code)}`,
                data=>!!(data?.instrumentInfo||data?.data?.instrumentInfo||data?.result?.instrumentInfo)
            );
            raw=globalPulseTsetmcUnwrapInstrumentInfo(info);
        }catch(error){lastError=error;console.warn("TSETMC instrument info endpoint failed:",code,error);}
    }
    if(!raw)throw lastError||new Error("پاسخ هویت نماد از TSETMC خالی است");
    const symbol=String(globalPulseTsetmcPickField(raw,["lVal18AFC","symbol","symbolName","Symbol","symbolFa"])||"").trim();
    const name=String(globalPulseTsetmcPickField(raw,["lVal30","name","companyName","Name","company_name"])||symbol).trim();
    const returnedCode=String(globalPulseTsetmcPickField(raw,["insCode","InsCode","instrumentCode"])||code).trim();
    if(returnedCode!==code)throw new Error("کد برگشتی API با کد واردشده مطابقت ندارد");
    if(!symbol||/^\d+$/.test(symbol))throw new Error("API نام واقعی نماد را برنگرداند");
    const value={insCode:code,symbol,name:name||symbol,flowTitle:String(globalPulseTsetmcPickField(raw,["flowTitle","FlowTitle"])||""),flow:Number(globalPulseTsetmcPickField(raw,["flow","Flow"])||0)};
    globalPulseTsetmcInstrumentInfoCache.set(code,{value,at:Date.now()});
    return value;
}
function globalPulseTsetmcExtractSearchRows(payload){
    if(Array.isArray(payload))return payload;
    if(!payload||typeof payload!=="object")return [];
    for(const key of ["instrumentSearch","InstrumentSearch","instrument","Instrument","data","Data","results","Results","result","Result"]){
        const value=payload[key];
        if(Array.isArray(value))return value;
        if(value&&typeof value==="object"){
            for(const nestedKey of ["instrumentSearch","instrument","data","results","result"]){
                if(Array.isArray(value[nestedKey]))return value[nestedKey];
            }
        }
    }
    return [];
}
function globalPulseTsetmcPickField(row,keys){
    for(const key of keys){
        const value=row?.[key];
        if(value!==undefined&&value!==null&&String(value).trim()!=="")return value;
    }
    return "";
}
async function globalPulseTsetmcSearch(query){
    const text=String(query||"").trim();
    if(text.length<2)throw new Error("حداقل دو حرف از نام یا نماد را وارد کن");
    const encoded=encodeURIComponent(text);
    const data=await globalPulseTsetmcFetchJson(
        `Instrument/GetInstrumentSearch/${encoded}`,
        payload=>(Array.isArray(payload)||(payload&&typeof payload==="object"))
    );
    const rows=globalPulseTsetmcExtractSearchRows(data);
    const normalized=[];
    const seen=new Set();
    for(const row of rows){
        if(!row||typeof row!=="object")continue;
        const rawCode=globalPulseTsetmcPickField(row,["insCode","InsCode","instrumentCode","InstrumentCode","inscode"]);
        const rawSymbol=globalPulseTsetmcPickField(row,["lVal18AFC","symbol","symbolName","name","symbolFa","Symbol","SymbolName"]);
        const rawName=globalPulseTsetmcPickField(row,["lVal30","name","companyName","Name","CompanyName"])||rawSymbol;
        const insCode=String(rawCode).trim(),symbol=String(rawSymbol).trim();
        if(!/^\d+$/.test(insCode)||!symbol)continue;
        const key=insCode+"|"+symbol;
        if(seen.has(key))continue;
        seen.add(key);
        normalized.push({
            insCode,
            symbol,
            name:String(rawName).trim(),
            flowTitle:String(globalPulseTsetmcPickField(row,["flowTitle","FlowTitle","flowName"])||""),
            flow:Number(globalPulseTsetmcPickField(row,["flow","Flow"])||0)
        });
        if(normalized.length>=30)break;
    }
    return normalized;
}
function globalPulseTsetmcUnwrapQuoteInfo(payload){
    if(!payload||typeof payload!=="object")return null;
    const candidates=[payload.closingPriceInfo,payload.data?.closingPriceInfo,payload.result?.closingPriceInfo,payload.data,payload.result,payload];
    for(const item of candidates){
        if(item&&typeof item==="object"&&!Array.isArray(item)){
            const hasPrice=["pDrCotVal","pClosing","priceYesterday","pl","pc","py","last","close"].some(key=>item[key]!==undefined&&item[key]!==null&&String(item[key]).trim()!=="");
            if(hasPrice)return item;
        }
    }
    return null;
}
function globalPulseTsetmcNumber(row,keys){
    for(const key of keys){
        const raw=row?.[key];
        if(raw===undefined||raw===null||String(raw).trim()==="")continue;
        const value=Number(String(raw).replace(/,/g,""));
        if(Number.isFinite(value)&&value>0)return value;
    }
    return NaN;
}
function globalPulseTsetmcSignedNumber(row,keys){
    for(const key of keys){
        const raw=row?.[key];
        if(raw===undefined||raw===null||String(raw).trim()==="")continue;
        const value=Number(String(raw).replace(/,/g,""));
        if(Number.isFinite(value))return value;
    }
    return NaN;
}
function globalPulseTsetmcPercent(price,previous,fallback,change){
    const p=Number(price),y=Number(previous),f=Number(fallback),c=Number(change);
    if(Number.isFinite(p)&&p>0&&Number.isFinite(y)&&y>0)return ((p-y)/y)*100;
    if(Number.isFinite(c)&&Number.isFinite(y)&&y>0)return (c/y)*100;
    return Number.isFinite(f)?f:NaN;
}
async function globalPulseTsetmcFetchWorkerQuote(instrument,resolved){
    try{
        const response=await fetchGlobalPulseTsetmcWithFailover(Date.now());
        const rows=Array.isArray(response?.data?.results)?response.data.results:[];
        const code=String(instrument?.insCode||"");
        const knownSymbol=String(resolved?.symbol||instrument?.symbol||"").trim().toUpperCase();
        const match=rows.find(row=>{
            if(!row||typeof row!=="object")return false;
            const rowCode=String(globalPulseTsetmcPickField(row,["insCode","InsCode","instrumentCode","code","symbolCode"])||"").trim();
            if(rowCode&&rowCode===code)return true;
            const rowSymbol=String(globalPulseTsetmcPickField(row,["symbol","lVal18AFC","symbolName"])||"").trim().toUpperCase();
            return !!knownSymbol&&!/^\d+$/.test(knownSymbol)&&rowSymbol===knownSymbol;
        });
        if(!match)return null;
        const last=globalPulseTsetmcNumber(match,["lastPrice","pDrCotVal","pl","last"]);
        const close=globalPulseTsetmcNumber(match,["closingPrice","pClosing","pc","close"]);
        if(!Number.isFinite(last)&&!Number.isFinite(close))return null;
        const actualLast=Number.isFinite(last)?last:close;
        const actualClose=Number.isFinite(close)?close:actualLast;
        const previous=globalPulseTsetmcNumber(match,["priceYesterday","py","yesterday"]);
        const lastPctRaw=globalPulseTsetmcSignedNumber(match,["lastPricePercent","dailyChangePercent","priceChangePercent","changePercent","percent"]);
        const closePctRaw=globalPulseTsetmcSignedNumber(match,["closingPricePercent","closeChangePercent"]);
        const lastPct=globalPulseTsetmcPercent(actualLast,previous,lastPctRaw,NaN);
        const closePct=globalPulseTsetmcPercent(actualClose,previous,closePctRaw,NaN);
        const symbol=String(globalPulseTsetmcPickField(match,["symbol","lVal18AFC","symbolName"])||resolved?.symbol||instrument?.symbol||code).trim();
        const name=String(globalPulseTsetmcPickField(match,["name","lVal30","companyName"])||resolved?.name||symbol).trim();
        return {insCode:code,symbol,name,lastPrice:actualLast,lastPricePercent:lastPct,closingPrice:actualClose,closingPricePercent:closePct,updatedAt:Date.now(),status:"",quoteSource:"worker"};
    }catch(error){console.warn("TSETMC Worker quote fallback failed:",instrument?.insCode,error);return null;}
}
async function globalPulseTsetmcFetchQuote(instrument){
    if(!instrument||!/^[0-9]+$/.test(String(instrument.insCode||"")))throw new Error("TSETMC instrument code is missing");
    const code=String(instrument.insCode),cached=globalPulseTsetmcQuoteCache.get(code);
    if(cached&&Date.now()-cached.at<4500){
        const storedSymbol=String(instrument.symbol||"").trim(),storedName=String(instrument.name||"").trim();
        return {...cached.value,symbol:storedSymbol&&!/^\d+$/.test(storedSymbol)?storedSymbol:cached.value.symbol,name:storedName&&!/^\d+$/.test(storedName)&&!storedName.startsWith("کد TSETMC")?storedName:cached.value.name};
    }
    if(globalPulseTsetmcQuoteInFlight.has(code))return globalPulseTsetmcQuoteInFlight.get(code);
    const task=(async()=>{
        let resolved={insCode:code,symbol:String(instrument.symbol||""),name:String(instrument.name||instrument.symbol||"")};
        try{
            resolved=await globalPulseTsetmcGetInstrumentInfo(code);
            instrument.symbol=resolved.symbol;instrument.name=resolved.name;
            const config=globalPulseTsetmcReadConfig();
            const saved=config.slots.find(item=>item&&String(item.insCode)===code);
            if(saved){saved.symbol=resolved.symbol;saved.name=resolved.name;try{localStorage.setItem(GLOBAL_PULSE_TSE_SLOTS_KEY,JSON.stringify(config));}catch(e){}globalPulseTsetmcSyncNative();}
        }catch(metaError){console.warn("TSETMC instrument metadata unavailable; continuing with quote request:",code,metaError);}

        let info=null,quoteSource="live",quoteError=null;
        try{
            const data=await globalPulseTsetmcFetchJson(`ClosingPrice/GetClosingPriceInfo/${encodeURIComponent(code)}`,payload=>{
                const quote=globalPulseTsetmcUnwrapQuoteInfo(payload);
                return !!quote&&(globalPulseTsetmcNumber(quote,["pDrCotVal","pClosing","priceYesterday","pl","pc","py"])>0);
            });
            info=globalPulseTsetmcUnwrapQuoteInfo(data);
        }catch(error){quoteError=error;console.warn("TSETMC live quote endpoint failed:",code,error);}

        // Some responses carry the Persian symbol/name in the embedded instrument object.
        const embedded=info?.instrument||info?.instrumentInfo||{};
        const embeddedSymbol=String(globalPulseTsetmcPickField(embedded,["lVal18AFC","symbol","symbolName"])||globalPulseTsetmcPickField(info,["lVal18AFC","symbol","symbolName"])||"").trim();
        const embeddedName=String(globalPulseTsetmcPickField(embedded,["lVal30","name","companyName"])||globalPulseTsetmcPickField(info,["lVal30","name","companyName"])||"").trim();
        if(embeddedSymbol&&!/^\d+$/.test(embeddedSymbol))resolved={...resolved,symbol:embeddedSymbol,name:embeddedName||embeddedSymbol};
        else if(embeddedName)resolved={...resolved,name:embeddedName};

        let lastPrice=globalPulseTsetmcNumber(info,["pDrCotVal","pl","last","lastPrice"]);
        let close=globalPulseTsetmcNumber(info,["pClosing","pc","close","closingPrice"]);
        let yesterday=globalPulseTsetmcNumber(info,["priceYesterday","py","yesterday"]);
        let priceChange=globalPulseTsetmcSignedNumber(info,["priceChange","pricechange"]);
        let fallback=globalPulseTsetmcSignedNumber(info,["priceChangePercent","changePercent","percent"]);
        if(!Number.isFinite(yesterday)&&Number.isFinite(priceChange)&&priceChange!==0){
            const anchor=Number.isFinite(lastPrice)?lastPrice:close;
            if(Number.isFinite(anchor)&&anchor-priceChange>0)yesterday=anchor-priceChange;
        }

        // If the live snapshot is incomplete, request the latest daily record.
        if(!Number.isFinite(lastPrice)&&!Number.isFinite(close)){
            try{
                const history=await globalPulseTsetmcFetchJson(`ClosingPrice/GetClosingPriceDailyList/${encodeURIComponent(code)}/1`,payload=>Array.isArray(payload?.closingPriceDaily)||Array.isArray(payload?.data?.closingPriceDaily));
                const list=Array.isArray(history?.closingPriceDaily)?history.closingPriceDaily:(Array.isArray(history?.data?.closingPriceDaily)?history.data.closingPriceDaily:[]);
                const latest=list[0]||{};
                if(latest&&Object.keys(latest).length){
                    info=latest;quoteSource="daily-fallback";
                    lastPrice=globalPulseTsetmcNumber(info,["pDrCotVal","pl","last","lastPrice"]);
                    close=globalPulseTsetmcNumber(info,["pClosing","pc","close","closingPrice"]);
                    yesterday=globalPulseTsetmcNumber(info,["priceYesterday","py","yesterday"]);
                    priceChange=globalPulseTsetmcSignedNumber(info,["priceChange","pricechange"]);
                    fallback=globalPulseTsetmcSignedNumber(info,["priceChangePercent","changePercent","percent"]);
                    if(!Number.isFinite(yesterday)&&Number.isFinite(priceChange)&&priceChange!==0){const anchor=Number.isFinite(lastPrice)?lastPrice:close;if(Number.isFinite(anchor)&&anchor-priceChange>0)yesterday=anchor-priceChange;}
                }
            }catch(historyError){console.warn("TSETMC daily quote fallback failed:",code,historyError);}
        }

        if(!Number.isFinite(lastPrice)&&!Number.isFinite(close)){
            const workerRow=await globalPulseTsetmcFetchWorkerQuote(instrument,resolved);
            if(workerRow){
                globalPulseTsetmcQuoteCache.set(code,{value:workerRow,at:Date.now()});
                return workerRow;
            }
        }
        if(!Number.isFinite(lastPrice))lastPrice=close;
        if(!Number.isFinite(close))close=lastPrice;
        if(!Number.isFinite(lastPrice)||lastPrice<=0){
            const workerRow=await globalPulseTsetmcFetchWorkerQuote(instrument,resolved);
            if(workerRow){globalPulseTsetmcQuoteCache.set(code,{value:workerRow,at:Date.now()});return workerRow;}
            throw new Error(`${resolved.symbol||instrument.symbol||code} price unavailable${quoteError?`: ${quoteError.message||quoteError}`:""}`);
        }
        const lastPct=globalPulseTsetmcPercent(lastPrice,yesterday,fallback,priceChange);
        const closePct=globalPulseTsetmcPercent(close,yesterday,fallback,priceChange);
        const value={insCode:code,symbol:String(resolved.symbol||instrument.symbol||code),name:String(resolved.name||instrument.name||resolved.symbol||instrument.symbol||code),lastPrice,lastPricePercent:Number.isFinite(lastPct)?lastPct:NaN,closingPrice:Number.isFinite(close)&&close>0?close:lastPrice,closingPricePercent:Number.isFinite(closePct)?closePct:NaN,updatedAt:Date.now(),status:String(info?.instrumentState?.cEtavalTitle||""),quoteSource};
        if(value.symbol&&!/^\d+$/.test(value.symbol)){
            instrument.symbol=value.symbol;instrument.name=value.name;
            const config=globalPulseTsetmcReadConfig(),saved=config.slots.find(item=>item&&String(item.insCode)===code);
            if(saved){saved.symbol=value.symbol;saved.name=value.name;try{localStorage.setItem(GLOBAL_PULSE_TSE_SLOTS_KEY,JSON.stringify(config));}catch(e){}globalPulseTsetmcSyncNative();}
        }
        globalPulseTsetmcQuoteCache.set(code,{value,at:Date.now()});return value;
    })();
    globalPulseTsetmcQuoteInFlight.set(code,task);
    try{return await task;}finally{globalPulseTsetmcQuoteInFlight.delete(code);}
}
function globalPulseTsetmcFindInstrument(symbol){
    const key=String(symbol||"").trim().toUpperCase();
    if(!key)return null;
    const configured=globalPulseTsetmcGetSlots().filter(Boolean);
    const found=configured.find(item=>String(item.symbol||"").trim().toUpperCase()===key);
    if(found)return found;
    const cached=readGlobalPulseTsetmcCache();
    const row=(cached?.results||[]).find(item=>String(item.symbol||"").trim().toUpperCase()===key&&String(item.insCode||""));
    return row?{insCode:String(row.insCode),symbol:String(row.symbol),name:String(row.name||row.symbol)}:null;
}
function globalPulseTsetmcEnsureEditor(){
    let modal=document.getElementById("globalTsetmcEditor");
    if(modal)return modal;
    modal=document.createElement("div");modal.id="globalTsetmcEditor";modal.className="global-tsetmc-editor-backdrop";modal.hidden=true;
    modal.innerHTML=`<div class="global-tsetmc-editor" role="dialog" aria-modal="true" aria-labelledby="globalTsetmcEditorTitle"><div class="global-tsetmc-editor-head"><strong id="globalTsetmcEditorTitle">TSETMC • Select symbol</strong><button type="button" data-tsetmc-close aria-label="Close">×</button></div><div class="global-tsetmc-editor-body"><label for="globalTsetmcSearchInput">نام/نماد شرکت یا کد عددی TSETMC را وارد کن</label><div class="global-tsetmc-search-row"><input id="globalTsetmcSearchInput" type="text" autocomplete="off" placeholder="مثلاً فولاد، رانیز یا 13386281759834677"><button type="button" id="globalTsetmcSearchButton">جستجو</button></div><div id="globalTsetmcSearchStatus" class="global-tsetmc-search-status">برای پیدا کردن نماد، جستجو را بزن.</div><div id="globalTsetmcSearchResults" class="global-tsetmc-search-results"></div></div><div class="global-tsetmc-editor-actions"><button type="button" id="globalTsetmcRemoveButton" class="remove">حذف نماد</button><span></span><button type="button" data-tsetmc-close class="cancel">انصراف</button><button type="button" id="globalTsetmcConfirmButton" class="confirm" disabled>تأیید و ثبت</button></div></div>`;
    document.body.appendChild(modal);
    const close=()=>{modal.hidden=true;globalPulseTsetmcEditorCandidate=null;};
    modal.querySelectorAll("[data-tsetmc-close]").forEach(btn=>btn.addEventListener("click",close));
    modal.addEventListener("click",event=>{if(event.target===modal)close();});
    const search=()=>globalPulseTsetmcRunEditorSearch();
    modal.querySelector("#globalTsetmcSearchButton").addEventListener("click",search);
    modal.querySelector("#globalTsetmcSearchInput").addEventListener("keydown",event=>{if(event.key==="Enter"){event.preventDefault();search();}});
    modal.querySelector("#globalTsetmcConfirmButton").addEventListener("click",()=>{
        const candidate=globalPulseTsetmcEditorCandidate;if(!candidate)return;
        const config=globalPulseTsetmcReadConfig();
        config.slots[globalPulseTsetmcEditorSlot-1]=candidate;
        globalPulseTsetmcSaveConfig(config);close();
    });
    modal.querySelector("#globalTsetmcRemoveButton").addEventListener("click",()=>{
        const config=globalPulseTsetmcReadConfig();config.slots[globalPulseTsetmcEditorSlot-1]=null;
        globalPulseTsetmcSaveConfig(config);close();
    });
    return modal;
}
async function globalPulseTsetmcRunEditorSearch(){
    const modal=globalPulseTsetmcEnsureEditor();
    const input=modal.querySelector("#globalTsetmcSearchInput"),status=modal.querySelector("#globalTsetmcSearchStatus"),host=modal.querySelector("#globalTsetmcSearchResults"),confirm=modal.querySelector("#globalTsetmcConfirmButton");
    const query=String(input.value||"").trim();
    globalPulseTsetmcEditorCandidate=null;confirm.disabled=true;host.innerHTML="";
    if(!query){status.textContent="نام نماد یا کد عددی TSETMC را وارد کن.";return;}
    if(/^\d{8,}$/.test(query)){
        status.textContent="در حال شناسایی نام نماد و دریافت قیمت با کد مستقیم TSETMC...";
        let candidate;
        try{
            candidate=await globalPulseTsetmcGetInstrumentInfo(query);
        }catch(error){
            console.warn("TSETMC direct code could not be resolved:",query,error);
            status.textContent=`کد وارد شد، اما TSETMC نام واقعی نماد را برنگرداند؛ ثبت انجام نشد. خطا: ${error?.message||"API unavailable"}`;
            return;
        }
        let quote=null,quoteError=null;
        try{quote=await globalPulseTsetmcFetchQuote(candidate);}catch(error){quoteError=error;console.warn("TSETMC preview quote failed:",query,error);}
        if(quote){candidate={...candidate,...quote};}
        globalPulseTsetmcEditorCandidate=candidate;
        const button=document.createElement("button");button.type="button";button.className="global-tsetmc-result selected";
        const title=document.createElement("strong");title.textContent=`${candidate.symbol} — ${candidate.name}`;
        const detail=document.createElement("span");
        detail.textContent=quote?`کد ${query} تأیید شد • آخرین قیمت ${formatGlobalPulseNumber(quote.lastPrice,0)} • تغییر ${globalPulseTsetmcPercentText(quote.lastPricePercent)}`:`کد ${query} و نام نماد تأیید شد؛ قیمت فعلاً در دسترس نیست (${quoteError?.message||"API quote unavailable"}).`;
        button.append(title,detail);button.addEventListener("click",()=>{globalPulseTsetmcEditorCandidate=candidate;confirm.disabled=false;});
        host.appendChild(button);confirm.disabled=false;
        status.textContent=quote?"نام و اطلاعات قیمت از TSETMC دریافت شد؛ برای ثبت تأیید کن.":"نام واقعی نماد پیدا شد، اما قیمت دریافت نشد؛ می‌توانی نماد را ثبت کنی و خطای قیمت در کارت مشخص خواهد شد.";
        globalPulseTsetmcEditorResults=[candidate];return;
    }
    status.textContent="در حال جستجو در TSETMC...";
    try{
        const results=await globalPulseTsetmcSearch(query);globalPulseTsetmcEditorResults=results;
        if(!results.length){status.textContent="نمادی پیدا نشد. نام یا نماد را دقیق‌تر وارد کن یا کد عددی TSETMC را وارد کن.";return;}
        status.textContent=`${results.length} نتیجه پیدا شد؛ نماد موردنظر را انتخاب کن.`;
        results.forEach((item,index)=>{
            const button=document.createElement("button");button.type="button";button.className="global-tsetmc-result";
            const title=document.createElement("strong");title.textContent=item.symbol;
            const detail=document.createElement("span");detail.textContent=`${item.name}${item.flowTitle?` • ${item.flowTitle}`:""}`;
            button.append(title,detail);button.addEventListener("click",()=>{globalPulseTsetmcEditorCandidate=item;host.querySelectorAll(".global-tsetmc-result").forEach(el=>el.classList.remove("selected"));button.classList.add("selected");confirm.disabled=false;});
            host.appendChild(button);if(results.length===1)button.click();
        });
    }catch(error){status.textContent=`خطا در دریافت اطلاعات TSETMC: ${error?.message||"اتصال برقرار نشد"}`;}
}
function globalPulseTsetmcOpenEditor(slot){
    globalPulseTsetmcEditorSlot=Math.max(1,Math.min(4,Number(slot)||1));
    const modal=globalPulseTsetmcEnsureEditor(),config=globalPulseTsetmcReadConfig(),current=config.slots[globalPulseTsetmcEditorSlot-1];
    modal.querySelector("#globalTsetmcEditorTitle").textContent=`TSETMC • Slot ${globalPulseTsetmcEditorSlot}`;
    modal.querySelector("#globalTsetmcSearchInput").value=current?.symbol||"";
    modal.querySelector("#globalTsetmcSearchStatus").textContent=current?`نماد فعلی: ${current.symbol} — برای تغییر، جستجو کن.`:"نام/نماد شرکت یا کد عددی TSETMC را وارد کن و جستجو را بزن.";
    modal.querySelector("#globalTsetmcSearchResults").innerHTML="";
    modal.querySelector("#globalTsetmcConfirmButton").disabled=true;
    modal.hidden=false;setTimeout(()=>modal.querySelector("#globalTsetmcSearchInput").focus(),50);
}
function initGlobalPulseTsetmcControls(){
    const config=globalPulseTsetmcReadConfig();
    document.querySelectorAll(".global-tsetmc-slot-btn").forEach(btn=>{
        btn.classList.toggle("active",Number(btn.dataset.tsetmcCount)<=config.count);
        if(!btn.dataset.bound){btn.dataset.bound="1";btn.addEventListener("click",()=>globalPulseTsetmcSetCount(btn.dataset.tsetmcCount));}
    });
    const host=$("globalTsetmcCards");
    if(host&&!host.dataset.editBound){
        host.dataset.editBound="1";
        host.addEventListener("click",event=>{const btn=event.target.closest("[data-tsetmc-edit]");if(btn){event.preventDefault();event.stopPropagation();globalPulseTsetmcOpenEditor(btn.dataset.tsetmcEdit);}});
    }
    globalPulseTsetmcSyncNative();
    globalPulseRenderTsetmc(readGlobalPulseTsetmcCache().results||[]);
}


function globalPulseTsetmcIsLiveWindow(now=new Date()){
    const p=globalPulseParts(now,GLOBAL_PULSE_IRAN_TZ);
    const day=(new Date(Date.UTC(p.year,p.month-1,p.day))).getUTCDay();
    const minute=p.hour*60+p.minute;
    return GLOBAL_PULSE_TSE_ACTIVE_DAYS.has(day) && minute>=GLOBAL_PULSE_TSE_START_MIN && minute<GLOBAL_PULSE_TSE_END_MIN;
}
function readGlobalPulseTsetmcCache(){
    try{
        const data=JSON.parse(localStorage.getItem(GLOBAL_PULSE_TSE_CACHE_KEY)||"{}");
        if(data&&typeof data==="object"&&Array.isArray(data.results)&&data.results.length) return data;
    }catch(e){}
    return {results:[],updatedAt:0,fallback:true};
}
function saveGlobalPulseTsetmcCache(data){
    try{localStorage.setItem(GLOBAL_PULSE_TSE_CACHE_KEY,JSON.stringify(data));}catch(e){}
}
function readGlobalPulseTsetmcAlertState(){
    try{
        const data=JSON.parse(localStorage.getItem(GLOBAL_PULSE_TSE_ALERT_STATE_KEY)||"{}");
        globalPulseTsetmcAlertState=data&&typeof data==="object"?data:{};
    }catch(e){globalPulseTsetmcAlertState={};}
}
function saveGlobalPulseTsetmcAlertState(){
    try{localStorage.setItem(GLOBAL_PULSE_TSE_ALERT_STATE_KEY,JSON.stringify(globalPulseTsetmcAlertState));}catch(e){}
}
function globalPulseTsetmcEscape(value){
    return String(value??"").replace(/[&<>"']/g,char=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[char]));
}
function globalPulseTsetmcPercentText(value){
    const n=Number(value);
    if(!Number.isFinite(n))return "--";
    return `${n>0?"+":""}${n.toFixed(2)}%`;
}
function globalPulseTsetmcPercentClass(value){
    const n=Number(value);
    return !Number.isFinite(n)?"flat":n>0?"positive":n<0?"negative":"flat";
}
function globalPulseRenderTsetmc(results){
    const host=$("globalTsetmcCards");
    if(!host)return;
    const config=globalPulseTsetmcReadConfig();
    const rows=Array.isArray(results)?results.filter(row=>row&&String(row.insCode||"")):[];
    const byCode=new Map(rows.map(row=>[String(row.insCode),row]));
    host.dataset.count=String(config.count);
    host.classList.toggle("is-three-cards",config.count===3);
    host.style.gridTemplateColumns=config.count===1?"minmax(0,1fr)":"repeat(2,minmax(0,1fr))";
    const displaySlots=config.slots.slice(0,config.count);
    host.innerHTML=displaySlots.map((instrument,index)=>{
        const row=instrument?byCode.get(String(instrument.insCode))||{}:{};
        const symbol=String(instrument?.symbol||"").trim();
        const name=String(instrument?.name||symbol||"TSE").trim();
        const lastPrice=Number(row.lastPrice),lastPct=Number(row.lastPricePercent);
        const closePrice=Number(row.closingPrice),closePct=Number(row.closingPricePercent);
        const iconText=symbol?symbol.slice(0,2):"TSE";
        const iconClass=index%2===0?"gold":"oil";
        const safeSymbol=globalPulseTsetmcEscape(symbol),safeName=globalPulseTsetmcEscape(name),safeIcon=globalPulseTsetmcEscape(iconText);
        return `<article class="global-pulse-card global-pulse-tsetmc-card"${symbol?` data-tsetmc-symbol="${safeSymbol}"`:""} data-tsetmc-slot="${index+1}">
            <div class="global-pulse-card-top">
                <div class="global-pulse-asset">
                    <span class="global-pulse-icon ${iconClass}">${safeIcon}</span>
                    <div class="global-tsetmc-asset-copy">
                        <div class="global-pulse-name">${safeSymbol||"TSE"} ${symbol?`<button type="button" class="global-pulse-compact-select" data-global-compact-asset="tse" data-global-compact-symbol="${safeSymbol}" aria-label="Show ${safeSymbol} in Ultra Compact" title="Show ${safeSymbol} in Ultra Compact">◎</button>`:""}<button type="button" class="global-pulse-compact-select global-tsetmc-edit" data-tsetmc-edit="${index+1}" aria-label="Set TSETMC symbol ${index+1}" title="Set or change symbol">✎</button></div>
                        <div class="global-pulse-symbol">${symbol?safeName:"Select symbol"} · TSETMC</div>
                    </div>
                </div>
                <div class="global-pulse-card-actions"><span class="global-pulse-badge">TSE</span></div>
            </div>
            <div class="global-pulse-tsetmc-values">
                <div class="global-pulse-tsetmc-group"><span class="global-pulse-tsetmc-label">LAST</span><span class="global-pulse-tsetmc-price">${Number.isFinite(lastPrice)&&lastPrice>0?formatGlobalPulseNumber(lastPrice,0):"-"}</span><span class="global-pulse-tsetmc-percent ${globalPulseTsetmcPercentClass(lastPct)}">${globalPulseTsetmcPercentText(lastPct)}</span></div>
                <div class="global-pulse-tsetmc-group"><span class="global-pulse-tsetmc-label">CLOSE</span><span class="global-pulse-tsetmc-price">${Number.isFinite(closePrice)&&closePrice>0?formatGlobalPulseNumber(closePrice,0):"-"}</span><span class="global-pulse-tsetmc-percent ${globalPulseTsetmcPercentClass(closePct)}">${globalPulseTsetmcPercentText(closePct)}</span></div>
            </div>
        </article>`;
    }).join("");
    globalPulseRefreshCompactAssetDisplay();
    initGlobalPulseCompactAssetButtons();
    try{window.dispatchEvent(new Event("crossover-assets-updated"));}catch(e){}
}

async function globalPulseCheckTsetmcNotification(row){
    const symbol=String(row?.symbol||"").trim();
    const current=Number(row?.lastPricePercent);
    if(!symbol||!Number.isFinite(current)||!globalPulseTsetmcIsLiveWindow())return;
    const state=globalPulseTsetmcAlertState[symbol] || (globalPulseTsetmcAlertState[symbol]={previousPercent:null,alertTimes:{}});
    if(!Number.isFinite(state.previousPercent)){state.previousPercent=current;return;}
    const old=Number(state.previousPercent);
    if(old===current)return;
    const crossed=GLOBAL_PULSE_ALERT_LEVELS.filter(level => (old<level&&current>=level)||(old>level&&current<=level));
    state.previousPercent=current;
    for(const level of crossed){
        const key=String(level),now=Date.now(),last=Number(state.alertTimes?.[key]||0);
        if(now-last<GLOBAL_PULSE_ALERT_COOLDOWN)continue;
        state.alertTimes[key]=now;
        const up=current>old, arrow=up?"↑":"↓", color=up?"🟢":"🔴";
        const pctText=`${level>0?"+":""}${level}%`;
        const price=Number(row?.lastPrice);
        const priceText=Number.isFinite(price)?` • Price ${formatGlobalPulseNumber(price,0)}`:"";
        const title=`${color} ${arrow} GLOBAL PULSE`;
        const message=`GLOBAL PULSE • ${symbol} • ${pctText}${priceText}`;
        try{await sendDashboardNotification(title,message,"./icon-192.png","GLOBAL_PULSE");}catch(e){}
    }
    saveGlobalPulseTsetmcAlertState();
}
function readGlobalPulseTsetmcActiveApi(){
    try{
        const value=Number(localStorage.getItem(GLOBAL_PULSE_TSE_ACTIVE_API_KEY));
        globalPulseTsetmcActiveApiIndex=(value===1)?1:0;
    }catch(e){globalPulseTsetmcActiveApiIndex=0;}
    return globalPulseTsetmcActiveApiIndex;
}
function saveGlobalPulseTsetmcActiveApi(index){
    globalPulseTsetmcActiveApiIndex=index===1?1:0;
    try{localStorage.setItem(GLOBAL_PULSE_TSE_ACTIVE_API_KEY,String(globalPulseTsetmcActiveApiIndex));}catch(e){}
}
let globalPulseTsetmcFetchInProgress = null;
function fetchGlobalPulseTsetmcWithFailover(requestStartedAt){
    if (globalPulseTsetmcFetchInProgress) return globalPulseTsetmcFetchInProgress;
    globalPulseTsetmcFetchInProgress = fetchGlobalPulseTsetmcWithFailoverInternal(requestStartedAt);
    return globalPulseTsetmcFetchInProgress.finally(() => { globalPulseTsetmcFetchInProgress = null; });
}
async function fetchGlobalPulseTsetmcWithFailoverInternal(requestStartedAt){
    const bases=[GLOBAL_PULSE_TSE_WORKER_URL,GLOBAL_PULSE_TSE_FALLBACK_URL];
    const active=readGlobalPulseTsetmcActiveApi();
    const order=[active,active===0?1:0];
    let firstError=null;
    for(const index of order){
        const base=bases[index];
        const url=`${base}?t=${requestStartedAt}&fresh=${requestStartedAt}`;
        try{
            const response=await fetchApiWithProxyFallback(
                url,
                {method:"GET",headers:{"Accept":"application/json"},cache:"no-store"},
                PRICE_REQUEST_TIMEOUT,
                payload => {
                    const rows = Array.isArray(payload?.results) ? payload.results.filter(row => row && String(row.symbol || "").trim()) : [];
                    if (!payload?.ok || !rows.length || !rows.some(row => Number(row.lastPrice) > 0)) throw new Error("TSETMC payload has no valid market rows");
                    return true;
                }
            );
            if(!response.ok)throw new Error(`HTTP ${response.status}`);
            const data=await response.json();
            const results=Array.isArray(data?.results)
                ? data.results.filter(row=>row&&String(row.symbol||"").trim())
                : [];
            if(!data?.ok || !results.length)throw new Error("TSETMC source returned no valid results");
            if(index!==active)saveGlobalPulseTsetmcActiveApi(index);
            return {data,sourceIndex:index};
        }catch(error){
            if(!firstError)firstError=error;
        }
    }
    throw firstError||new Error("TSETMC sources unavailable");
}
async function loadGlobalPulseTsetmc(force=false){
    if(globalPulseTsetmcLoadInProgress)return false;
    readGlobalPulseTsetmcAlertState();
    const cached=readGlobalPulseTsetmcCache();
    if(Array.isArray(cached.results))globalPulseRenderTsetmc(cached.results);
    const now=Date.now();
    if(!force&&now-globalPulseTsetmcLastFetchAt<GLOBAL_PULSE_TSE_REFRESH_MS-2000)return true;
    globalPulseTsetmcLoadInProgress=true;
    try{
        const instruments=globalPulseTsetmcGetSlots().filter(item=>item&&item.insCode&&item.symbol);
        if(!instruments.length){
            globalPulseTsetmcLastFetchAt=Date.now();
            globalPulseRenderTsetmc([]);
            clearPageIssue("global-tsetmc");
            return true;
        }
        const results=[],failed=[];
        let freshCount=0;
        const cachedByCode=new Map((cached.results||[]).filter(row=>row&&row.insCode).map(row=>[String(row.insCode),row]));
        for(const instrument of instruments){
            try{
                const row=await globalPulseTsetmcFetchQuote(instrument);
                results.push(row);freshCount++;
                clearPageIssue(`global-tsetmc:${row.symbol}`);
                await globalPulseCheckTsetmcNotification(row);
            }catch(error){
                failed.push(instrument.symbol);
                const stale=cachedByCode.get(String(instrument.insCode));
                if(stale)results.push(stale);
                setPageIssue(`global-tsetmc:${instrument.symbol}`,`${instrument.symbol} TSETMC data unavailable: ${error?.message||"API error"}`,"error");
            }
        }
        if(!freshCount)throw new Error("No selected TSETMC symbol returned valid live data");
        globalPulseTsetmcLastFetchAt=Date.now();
        saveGlobalPulseTsetmcCache({results,updatedAt:Date.now()});
        globalPulseRenderTsetmc(results);
        if(failed.length)setPageIssue("global-tsetmc",`Some TSETMC symbols are unavailable: ${failed.join(", ")}`,"warn");
        else clearPageIssue("global-tsetmc");
        globalPulseTsetmcFailureCount=0;
        syncNativeWidgetSnapshot();
        return true;
    }catch(error){
        console.warn("TSETMC CDN refresh failed:",error);
        if(Array.isArray(cached.results)&&cached.results.length){
            globalPulseRenderTsetmc(cached.results);
            setPageIssue("global-tsetmc",`TSETMC API unavailable; cached data from ${cached.updatedAt?new Date(cached.updatedAt).toLocaleString():"an earlier response"} is being shown`,"cache");
        }else{
            globalPulseRenderTsetmc([]);
            setPageIssue("global-tsetmc",`TSETMC API error: ${error?.message||"current data unavailable"}`,"error");
        }
        globalPulseTsetmcFailureCount++;
        return false;
    }finally{globalPulseTsetmcLoadInProgress=false;}
}
function startGlobalPulseTsetmc(){
    if(globalPulseTsetmcTimer)clearTimeout(globalPulseTsetmcTimer);
    globalPulseTsetmcLoopEnabled = true;
    const tick = async () => {
        const succeeded = await loadGlobalPulseTsetmc(true);
        const delay = succeeded
            ? GLOBAL_PULSE_TSE_REFRESH_MS
            : Math.min(60000, GLOBAL_PULSE_TSE_REFRESH_MS * Math.pow(2, Math.min(Math.max(0, globalPulseTsetmcFailureCount - 1), 3)));
        if (globalPulseTsetmcLoopEnabled) globalPulseTsetmcTimer = setTimeout(tick, delay);
    };
    void tick();
}

const GLOBAL_PULSE_FREEZE_KEY = "globalPulseCommodityFreeze-v1";
function globalPulseCommodityIsOpen(now=new Date()){
    const d=now.getUTCDay();
    return d!==0 && d!==6;
}
function readGlobalPulseFreeze(){
    try{
        const v=JSON.parse(localStorage.getItem(GLOBAL_PULSE_FREEZE_KEY)||"{}");
        return v&&typeof v==="object"?v:{};
    }catch(e){ return {}; }
}
function saveGlobalPulseFreeze(v){
    try{ localStorage.setItem(GLOBAL_PULSE_FREEZE_KEY,JSON.stringify(v)); }catch(e){}
}

function readGlobalPulseCommodityCache(){
    try{
        const data=JSON.parse(localStorage.getItem(GLOBAL_PULSE_COMMODITY_CACHE_KEY)||"{}");
        return data&&typeof data==="object"?data:{};
    }catch(e){return {};}
}
function saveGlobalPulseCommodityCache(data){
    try{localStorage.setItem(GLOBAL_PULSE_COMMODITY_CACHE_KEY,JSON.stringify(data));}catch(e){}
}
function globalPulseCommodityValue(data,symbol){
    const row=data?.[symbol] || data?.symbols?.[symbol];
    const price = Number(row?.mid);
    return Number.isFinite(price) && price > 0 ? price : null;
}
function globalPulseCommodityDayChange(data,symbol){
    const row=data?.[symbol] || data?.symbols?.[symbol];
    const value=Number(row?.dayDiffPercent);
    return Number.isFinite(value) ? value : null;
}
function globalPulseCommodityBars(data){
    const bars=Array.isArray(data?.bars)?data.bars:
        Array.isArray(data?.data?.bars)?data.data.bars:
        Array.isArray(data?.data)?data.data:
        Array.isArray(data?.result?.bars)?data.result.bars:
        Array.isArray(data)?data:[];
    return bars.map(b=>{
        const rawTime=Array.isArray(b)?b[0]:(b?.openTime??b?.timestamp??b?.time??b?.date??"");
        let t=Number(rawTime);
        if(Number.isFinite(t)&&t>0){if(t<100000000000)t*=1000;}
        else t=Date.parse(String(rawTime));
        const close=Number(Array.isArray(b)?b[4]:(b?.close??b?.c??b?.price));
        const open=Number(Array.isArray(b)?b[1]:(b?.open??b?.o));
        const rawOpen=Array.isArray(b)?false:(b?.isOpen===true||String(b?.isOpen||"").toLowerCase()==="true");
        return {t,close,open,isOpen:rawOpen};
    }).filter(b=>Number.isFinite(b.t)&&Number.isFinite(b.close)&&b.close>0)
      .sort((a,b)=>a.t-b.t);
}
function globalPulsePreviousDailyClose(data){
    const bars=globalPulseCommodityBars(data);
    if(bars.length<2)return null;
    const latest=bars[bars.length-1];
    const completed=bars.filter(b=>b.t<latest.t || !latest.isOpen);
    if(completed.length<2)return null;
    const previous=completed[completed.length-2];
    return Number.isFinite(previous.close)&&previous.close>0?previous.close:null;
}
function globalPulseApplyCommodity(idPrice,idChange,price,change,cacheKey){
    if(Number.isFinite(price)) safeText(idPrice,formatGlobalPulseNumber(price));
    if(Number.isFinite(change)) setGlobalPulseChange(idChange,change);
    else if(cacheKey){
        const cached=readGlobalPulseCommodityCache()[cacheKey];
        if(Number.isFinite(Number(cached?.price))) safeText(idPrice,formatGlobalPulseNumber(cached.price));
        if(Number.isFinite(Number(cached?.change))) setGlobalPulseChange(idChange,cached.change);
    }
}
let globalPulseLoadInProgress = null;
async function loadGlobalPulseInternal(){
    loadGlobalPulseEconomicEvents(false);
    const cached=readGlobalPulseCommodityCache();
    let live=false;
    let batch=null;
    try{
        batch=await fetchGlobalPulseJson(GLOBAL_PULSE_COMMODITY_BATCH_URL);
        live=true;
    }catch(error){
        console.warn("Global Pulse commodity APIs unavailable:",error);
    }

    const goldPrice=globalPulseCommodityValue(batch,"XAUUSD");
    const oilPrice=globalPulseCommodityValue(batch,"USOIL");
    // Use Biquote's own dayDiffPercent for both the displayed value and
    // notification decisions. This keeps them on exactly the same API field.
    let goldChange=globalPulseCommodityDayChange(batch,"XAUUSD");
    let oilChange=globalPulseCommodityDayChange(batch,"USOIL");
    let effectiveGoldPrice=goldPrice;
    let effectiveOilPrice=oilPrice;

    // If the aggregate feed is incomplete, try each already-configured OHLC
    // endpoint independently before falling back to cached values.
    if (!Number.isFinite(effectiveGoldPrice) || !Number.isFinite(goldChange)) {
        try {
            const goldBarsPayload = await fetchGlobalPulseJson(GLOBAL_PULSE_GOLD_OHLC_URL, payload => globalPulseCommodityBars(payload).length >= 2);
            const bars = globalPulseCommodityBars(goldBarsPayload);
            const latest = bars[bars.length - 1], previous = bars[bars.length - 2];
            if (latest && previous && latest.close > 0 && previous.close > 0) {
                effectiveGoldPrice = latest.close;
                goldChange = ((latest.close - previous.close) / previous.close) * 100;
            }
        } catch (error) { console.warn("Gold OHLC fallback failed:", error); }
    }
    if (!Number.isFinite(effectiveOilPrice) || !Number.isFinite(oilChange)) {
        try {
            const oilBarsPayload = await fetchGlobalPulseJson(GLOBAL_PULSE_OIL_OHLC_URL, payload => globalPulseCommodityBars(payload).length >= 2);
            const bars = globalPulseCommodityBars(oilBarsPayload);
            const latest = bars[bars.length - 1], previous = bars[bars.length - 2];
            if (latest && previous && latest.close > 0 && previous.close > 0) {
                effectiveOilPrice = latest.close;
                oilChange = ((latest.close - previous.close) / previous.close) * 100;
            }
        } catch (error) { console.warn("Oil OHLC fallback failed:", error); }
    }

    if(Number.isFinite(effectiveGoldPrice)&&Number.isFinite(goldChange)){
        cached.gold={price:effectiveGoldPrice,change:goldChange,updatedAt:Date.now()};
        saveGlobalPulseCommodityCache(cached);
        globalPulseApplyCommodity("globalGoldPrice","globalGoldChange",effectiveGoldPrice,goldChange,"gold");
        clearPageIssue("global-gold");
        clearPageIssue("global-gold-change");
        globalPulseCheckNotification("gold",goldChange,effectiveGoldPrice);
    }else if(Number.isFinite(Number(cached?.gold?.price))){
        globalPulseApplyCommodity("globalGoldPrice","globalGoldChange",null,null,"gold");
        setPageIssue("global-gold",`Gold is using cached data${cached.gold.updatedAt?` · ${new Date(cached.gold.updatedAt).toISOString()}`:""}`,"cache");
        clearPageIssue("global-gold-change");
    }else{
        setPageIssue("global-gold","Gold live price unavailable and no cached value exists","error");
    }

    if(Number.isFinite(effectiveOilPrice)&&Number.isFinite(oilChange)){
        cached.oil={price:effectiveOilPrice,change:oilChange,updatedAt:Date.now()};
        saveGlobalPulseCommodityCache(cached);
        globalPulseApplyCommodity("globalOilPrice","globalOilChange",effectiveOilPrice,oilChange,"oil");
        clearPageIssue("global-oil");
        globalPulseCheckNotification("oil",oilChange,effectiveOilPrice);
    }else if(Number.isFinite(Number(cached?.oil?.price))){
        globalPulseApplyCommodity("globalOilPrice","globalOilChange",null,null,"oil");
        setPageIssue("global-oil",`Oil is using cached data${cached.oil.updatedAt?` · ${new Date(cached.oil.updatedAt).toISOString()}`:""}`,"cache");
    }else{
        setPageIssue("global-oil","Oil live price unavailable and no cached value exists","error");
    }

    globalPulseRefreshCompactAssetDisplay();
    try{window.dispatchEvent(new Event("crossover-assets-updated"));}catch(e){}
    // Each commodity's own validated source path clears or sets its status.
    // A successful aggregate HTTP response must not hide a missing/invalid symbol.
    syncNativeWidgetSnapshot();
}
async function loadGlobalPulse(){
    if (globalPulseLoadInProgress) return globalPulseLoadInProgress;
    globalPulseLoadInProgress = loadGlobalPulseInternal();
    try { return await globalPulseLoadInProgress; }
    finally { globalPulseLoadInProgress = null; }
}
function startGlobalPulse(){
    globalPulseReadAlertState();
    initGlobalPulseTsetmcControls();
    initGlobalPulseCompactAssetButtons();
    startGlobalPulseMarketClock();
    startGlobalPulseTsetmc();
    if (globalPulseTimer) clearTimeout(globalPulseTimer);
    const tick = async () => {
        await loadGlobalPulse();
        globalPulseTimer = setTimeout(tick, GLOBAL_PULSE_REFRESH_MS);
    };
    void tick();
}

/* =====================================================
   AUTO REFRESH
===================================================== */
let countdownTimer =
    null;
function startAutoRefresh() {
    clearInterval(
        countdownTimer
    );
    let seconds =
        10;
    safeText(
        "updateCountdown",
        seconds + "s"
    );
    countdownTimer =
        setInterval(
            async () => {
                seconds--;
                if (
                    seconds <= 0
                ) {
                    seconds =
                        10;
                    try {
                        // Keep the two top-of-page dollar prices at the front
                        // of each refresh cycle; do not wait for other APIs.
                        void Promise.allSettled([
                            loadUsdTxt(),
                            loadYekusd()
                        ]);
                        await loadDashboardData();
                        await loadCryptoSignals();
                    }
                    catch (error) {
                        console.error(
                            error
                        );
                    }
                }
                safeText(
                    "updateCountdown",
                    seconds + "s"
                );
            },
            1000
        );
}
/* =====================================================
   CRYPTO PRICE
===================================================== */
let cryptoOnline =
    false;
let lastCryptoPrice =
    null;
function saveCryptoPrice(value) {
    try {
        const number = Number(value);
        if (Number.isFinite(number) && number > 0) {
            localStorage.setItem(
                CACHE_KEYS.CRYPTO_PRICE,
                String(number)
            );
        }
    } catch (error) {}
}
function loadCachedCryptoPrice() {
    try {
        const saved = localStorage.getItem(
            CACHE_KEYS.CRYPTO_PRICE
        );
        const number = Number(saved);
        return Number.isFinite(number) && number > 0 ? number : null;
    } catch (error) {
        return null;
    }
}
function renderCachedCryptoPrice() {
    const cached = loadCachedCryptoPrice();
    if (cached === null) {
        return null;
    }

    const cryptoText = cached.toLocaleString(
        "en-US",
        {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }
    );
    safeText("cryptoValue", cryptoText);
    const cryptoElement = document.getElementById("cryptoValue");
    if (cryptoElement) {
        cryptoElement.classList.remove("crypto-live", "up", "down", "market-price-offline");
        cryptoElement.classList.add("crypto-cached");
    }

    const mini = document.getElementById("cryptoMini");
    if (mini) {
        mini.textContent = cached.toLocaleString(
            "en-US",
            {
                minimumFractionDigits: 0,
                maximumFractionDigits: 0
            }
        );
        mini.classList.remove("crypto-live");
        mini.classList.add("crypto-cached");
    }

    lastCryptoPrice = cached;
    return cached;
}

let cryptoValueLoadInProgress = null;
let cryptoValueRetryAt = 0;
function loadCryptoValue() {
    if (cryptoValueLoadInProgress) return cryptoValueLoadInProgress;
    if (Date.now() < cryptoValueRetryAt) return Promise.resolve(false);
    cryptoValueLoadInProgress = loadCryptoValueInternal();
    return cryptoValueLoadInProgress.finally(() => { cryptoValueLoadInProgress = null; });
}
async function loadCryptoValueInternal() {
    const element =
        $("cryptoValue");

    // Show the last valid Crypto value immediately on refresh/open.
    // The live API request below can then replace it when a fresh value arrives.
    renderCachedCryptoPrice();

    try {
const response = await fetchWithFallback(
    WORKER_URLS.crypto,
    { method: "GET", headers: { "Accept": "text/plain" } },
    7000,
    async candidate => {
        const raw = (await candidate.text()).trim().replace(/,/g, "");
        const value = Number(raw);
        if (!raw || !Number.isFinite(value) || value <= 0) throw new Error("Crypto Worker returned an invalid positive price");
        return true;
    }
);
        if (!response.ok) {
            throw new Error(
                "Crypto worker unavailable"
            );
        }
        const text =
            await response.text();
        const value = Number(String(text).trim().replace(/,/g, ""));
        if (!Number.isFinite(value) || value <= 0) {
            throw new Error("Invalid crypto value (must be positive and finite)");
        }

        // Keep the last valid value available for the next refresh/open.
        saveCryptoPrice(value);

        const cryptoText =
            value.toLocaleString(
                "en-US",
                {
                    minimumFractionDigits:
                        2,
                    maximumFractionDigits:
                        2
                }
            );
        safeText(
            "cryptoValue",
            cryptoText
        );
        const cryptoElement =
            $("cryptoValue");
        if (
            lastCryptoPrice !== null
            &&
            cryptoElement
        ) {
            if (
                value >
                lastCryptoPrice
            ) {
                cryptoElement.classList.add(
                    "up"
                );
                cryptoElement.classList.remove(
                    "down"
                );
            }
            else if (
                value <
                lastCryptoPrice
            ) {
                cryptoElement.classList.add(
                    "down"
                );
                cryptoElement.classList.remove(
                    "up"
                );
            }
            setTimeout(
                () => {
                    cryptoElement.classList.remove(
                        "up",
                        "down"
                    );
                },
                350
            );
        }
        lastCryptoPrice =
            value;
        const mini =
            document.getElementById(
                "cryptoMini"
            );
        if (mini) {
            // Ultra Compact: total Crypto is always a whole number
            // with thousands separators; keep the main Crypto value unchanged.
            mini.textContent =
                value.toLocaleString("en-US", {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 0
                });
        }
        cryptoOnline = true;
        cryptoValueRetryAt = 0;
        clearPageIssue("crypto-price");
        if (element) {
            element.classList.remove(
                "market-price-offline",
                "crypto-cached",
                "up",
                "down"
            );
            element.classList.add("crypto-live");
        }
        if (mini) {
            mini.classList.remove("crypto-cached");
            mini.classList.add("crypto-live");
        }
        syncNativeWidgetSnapshot();
    } catch (error) {
        cryptoValueRetryAt = Date.now() + (/429/.test(String(error?.message || error)) ? 60000 : 15000);
        cryptoOnline =
            false;
        if (element) {
            element.classList.add(
                "market-price-offline"
            );
        }
        setPageIssue("crypto-price", "Crypto price cached", "cache");
        console.warn("Crypto Price Offline:", error);
    }
}
/* =====================================================
   ONLINE / OFFLINE
===================================================== */
window.addEventListener(
    "offline",
    () => {
        setPageIssue("connection", "Offline • Using cache", "cache");
    }
);
window.addEventListener(
    "online",
    async () => {
        setPageIssue("connection", "Online • Refreshing", "info");
        setTimeout(() => {
            clearPageIssue("connection");
        }, 2000);
        try {
            void Promise.allSettled([
                loadUsdTxt(),
                loadYekusd()
            ]);
            await loadDashboardData();
        }
        catch (error) {
            console.error(
                error
            );
        }
    }
);
window.addEventListener("DOMContentLoaded", renderPageStatus);
window.addEventListener("DOMContentLoaded", () => {
    loadFearGreedIndex();
    setInterval(loadFearGreedIndex, FEAR_GREED_REFRESH_MS);
});
/* =====================================================
   CRYPTO PORTFOLIO
   MEXC FUTURES KLINE
===================================================== */
const MEXC_KLINE_BASE =
    "https://api.mexc.com/api/v1/contract/kline/";
const MEXC_SPOT_TICKER =
    "https://api.mexc.com/api/v3/ticker/price";

/*
   Unified crypto price failover.
   Source order is sticky per coin:
   1) MEXC spot ticker (primary)
   2) Binance spot ticker
   3) CoinGecko simple/price
   4) Nestex CG ticker
   Once a source works, subsequent requests use that source directly until
   it fails. We do NOT walk the whole chain on every refresh.
*/
const CRYPTO_PRICE_FAILOVER_KEY = "financialCryptoPriceFailover-v1";
const CRYPTO_PRICE_FAILOVER_RETRY_MS = 30 * 1000;
const CRYPTO_PRICE_SOURCES = [
    {
        id: "mexc",
        url: coin => `https://api.mexc.com/api/v3/ticker/24hr?symbol=${encodeURIComponent(cryptoSpotSymbol(coin))}`
    },
    {
        id: "binance",
        url: coin => `https://api.binance.com/api/v3/ticker/24hr?symbol=${encodeURIComponent(cryptoSpotSymbol(coin))}`
    },
    {
        id: "coingecko",
        url: coin => `https://api.coingecko.com/api/v3/simple/price?ids=${encodeURIComponent(CRYPTO_COINGECKO_IDS[coin] || "")}&vs_currencies=usd&include_24hr_change=true`
    },
    {
        id: "nestex",
        url: coin => `https://api.nestex.one/cg/tickers/${encodeURIComponent(String(coin).toLowerCase())}_USDT`
    }
];
const CRYPTO_COINGECKO_IDS = {
    BTC: "bitcoin",
    BNB: "binancecoin",
    ETH: "ethereum",
    DOGE: "dogecoin",
    ADA: "cardano",
    SOL: "solana",
    POL: "polygon-ecosystem-token",
    TRX: "tron",
    AVAX: "avalanche-2"
};
const MEXC_CORS_PROXY =
    "https://proxy.cors.dev/";

function cryptoSpotSymbol(asset) {
    const raw = String(asset || "").replace(/[-_\/]/g, "").toUpperCase();
    return raw.endsWith("USDT") ? raw : `${raw}USDT`;
}
function cryptoFailoverReadState() {
    try {
        const raw = localStorage.getItem(CRYPTO_PRICE_FAILOVER_KEY);
        const state = raw ? JSON.parse(raw) : {};
        return state && typeof state === "object" ? state : {};
    } catch (e) {
        return {};
    }
}
function cryptoFailoverWriteState(state) {
    try {
        localStorage.setItem(CRYPTO_PRICE_FAILOVER_KEY, JSON.stringify(state));
    } catch (e) {}
}
function cryptoFailoverStateFor(coin) {
    const key = String(coin || "").toUpperCase();
    const all = cryptoFailoverReadState();
    const item = all[key] && typeof all[key] === "object" ? all[key] : {};
    return {
        sourceIndex: Number.isInteger(Number(item.sourceIndex))
            ? Math.max(0, Math.min(CRYPTO_PRICE_SOURCES.length - 1, Number(item.sourceIndex)))
            : 0,
        retryAt: Number(item.retryAt) || 0
    };
}
function cryptoFailoverSave(coin, state) {
    const all = cryptoFailoverReadState();
    all[String(coin || "").toUpperCase()] = state;
    cryptoFailoverWriteState(all);
}
async function cryptoFetchSource(source, coin) {
    const apiUrl = source.url(coin);
    const requestOptions = { method: "GET", headers: { "Accept": "application/json" }, cache: "no-store" };
    let payload = null;

    const parseAndValidate = async response => {
        if (!response.ok) throw new Error(`${source.id.toUpperCase()} HTTP ${response.status}`);
        const candidate = await response.clone().json();
        let price = null;
        if (source.id === "mexc" || source.id === "binance") price = Number(candidate.price ?? candidate.lastPrice);
        else if (source.id === "coingecko") price = Number(candidate?.[CRYPTO_COINGECKO_IDS[coin]]?.usd);
        else if (source.id === "nestex") price = Number(candidate.last_price ?? candidate.lastPrice);
        if (!Number.isFinite(price) || price <= 0) throw new Error(`${source.id} returned invalid price`);
        return candidate;
    };

    if (source.id === "mexc" || source.id === "binance") {
        const exchangeUrls = [apiUrl, `https://proxy.cors.dev/${apiUrl}`];
        let lastError = null;
        for (const url of exchangeUrls) {
            try {
                const candidate = await fetchWithTimeout(url, requestOptions, 6500);
                payload = await parseAndValidate(candidate);
                break;
            } catch (error) { lastError = error; }
        }
        if (!payload) throw (lastError || new Error(`${source.id} unavailable`));
    } else {
        const response = await fetchApiWithProxyFallback(apiUrl, requestOptions, 8000, candidate => {
            const price = source.id === "coingecko"
                ? Number(candidate?.[CRYPTO_COINGECKO_IDS[coin]]?.usd)
                : Number(candidate?.last_price ?? candidate?.lastPrice);
            if (!Number.isFinite(price) || price <= 0) throw new Error(`${source.id} returned invalid price`);
            return true;
        });
        payload = await response.json();
    }

    let price = null, previousClose = null, changePct = null;
    if (source.id === "mexc" || source.id === "binance") {
        price = Number(payload.lastPrice ?? payload.price);
        previousClose = Number(payload.prevClosePrice);
        // Use only the percentage explicitly returned by the exchange API.
        changePct =
            payload.priceChangePercent == null ||
            String(payload.priceChangePercent).trim() === ""
                ? null
                : Number(payload.priceChangePercent);
    }
    else if (source.id === "coingecko") {
        const coinData = payload?.[CRYPTO_COINGECKO_IDS[coin]];
        price = Number(coinData?.usd);
        // CoinGecko supplies its own 24h percentage when requested.
        changePct =
            coinData?.usd_24h_change == null
                ? null
                : Number(coinData.usd_24h_change);
    }
    else if (source.id === "nestex") {
        price = Number(payload.last_price ?? payload.lastPrice);
        // oldltp24h is a reference price, not an API percentage.
        // Do not derive a percentage from it.
    }
    if (!Number.isFinite(price) || price <= 0) throw new Error(`${source.id} returned invalid price`);
    return {
        price,
        previousClose: Number.isFinite(previousClose) && previousClose > 0 ? previousClose : null,
        changePct: Number.isFinite(changePct) ? changePct : null,
        source: source.id,
        time: Date.now(),
        offline: false
    };
}


const CRYPTO_MEXC_PROBE_MS = 30 * 1000;

async function getUnifiedCryptoPrice(coin, options = {}) {
    const normalized = String(coin || "").replace(/[-_\/]/g, "").replace(/USDT$/i, "").toUpperCase();
    if (!normalized || !/^[A-Z0-9]{2,20}$/.test(normalized)) {
        throw new Error(`Unsupported crypto asset: ${normalized || "unknown"}`);
    }

    let state = cryptoFailoverStateFor(normalized);
    const now = Date.now();
    const preferPrimary = options && options.preferPrimary === true;
    if (state.retryAt > now) {
        throw new Error(`Crypto price sources are in recovery cooldown for ${Math.ceil((state.retryAt-now)/1000)}s`);
    }

    /*
       MEXC is the authoritative primary source. The Crypto Portfolio calls
       this with preferPrimary=true, so every portfolio refresh probes MEXC
       first even if an older localStorage state selected a fallback.
       This prevents a stale fallback state (for example DOGE -> CoinGecko)
       from surviving after MEXC has recovered.
    */
    if (preferPrimary) {
        try {
            const primary = await cryptoFetchSource(CRYPTO_PRICE_SOURCES[0], normalized);
            cryptoFailoverSave(normalized, { sourceIndex: 0, retryAt: 0 });
            return primary;
        } catch (error) {
            console.warn(`Primary MEXC probe failed for ${normalized}:`, error);
            state = cryptoFailoverStateFor(normalized);
            // If MEXC failed, continue with the last known fallback.
            if (state.sourceIndex === 0) {
                state.sourceIndex = 1;
            }
            cryptoFailoverSave(normalized, state);
        }
    } else if (state.sourceIndex > 0) {
        /*
           Non-portfolio consumers also periodically re-check MEXC while a
           fallback is active. Once MEXC answers with a valid price, the
           selected source immediately returns to index 0.
        */
        const lastProbe = Number(state.lastMexcProbeAt) || 0;
        if (now - lastProbe >= CRYPTO_MEXC_PROBE_MS) {
            cryptoFailoverSave(normalized, { ...state, lastMexcProbeAt: now });
            try {
                const primary = await cryptoFetchSource(CRYPTO_PRICE_SOURCES[0], normalized);
                cryptoFailoverSave(normalized, { sourceIndex: 0, retryAt: 0 });
                return primary;
            } catch (error) {
                console.warn(`MEXC recovery probe failed for ${normalized}:`, error);
                state = cryptoFailoverStateFor(normalized);
            }
        }
    }

    const availableSources = CRYPTO_PRICE_SOURCES.filter(source => source.id !== "coingecko" || !!CRYPTO_COINGECKO_IDS[normalized]);

    /*
       A retry pause must never block the selected fallback itself. If MEXC
       was already probed recently, use the active fallback until the next
       probe window instead of getting stuck on a stale source state.
    */
    if (state.retryAt > now && state.sourceIndex > 0) {
        const source = availableSources[Math.min(state.sourceIndex, availableSources.length - 1)];
        return await cryptoFetchSource(source, normalized);
    }

    const startIndex = Math.max(0, Math.min(availableSources.length - 1, state.sourceIndex));
    for (let index = startIndex; index < availableSources.length; index++) {
        const source = availableSources[index];
        try {
            const result = await cryptoFetchSource(source, normalized);
            cryptoFailoverSave(normalized, { sourceIndex: index, retryAt: 0 });
            return result;
        } catch (error) {
            console.warn(`Crypto price source ${source.id} failed for ${normalized}:`, error);
            /*
               Move the active source only after it actually fails.
               The next refresh therefore starts directly from the next
               source instead of checking the failed source again.
            */
            state.sourceIndex = Math.min(index + 1, availableSources.length - 1);
            cryptoFailoverSave(normalized, state);
        }
    }

    /*
       All remaining sources failed. After a short recovery pause, start
       again from the primary rather than probing the whole chain on every poll.
    */
    cryptoFailoverSave(normalized, {
        sourceIndex: 0,
        retryAt: Date.now() + CRYPTO_PRICE_FAILOVER_RETRY_MS
    });
    throw new Error(`All crypto price sources unavailable for ${normalized}`);
}
/*
   Holdings are read ONLY from data.json.
   There is intentionally no crypto holding data
   hard-coded in index.html.
*/
const CRYPTO_PORTFOLIO_REFRESH_MS = 30 * 1000;
const CRYPTO_PORTFOLIO_ICONS =
    CRYPTO_ICON_FILES;
let cryptoPortfolioLoadInProgress = null;
let cryptoPortfolioLastLoadTime = 0;
let cryptoPortfolioRefreshTimer = null;
let lastPortfolioRenderSignature = "";
function cryptoPortfolioCacheKey(symbol) {
    return "financialDashboardMexc_" + symbol;
}
function saveCryptoPortfolioCache(symbol, item) {
    try {
        localStorage.setItem(
            cryptoPortfolioCacheKey(symbol),
            JSON.stringify({
                price: item.price,
                previousClose: item.previousClose,
                changePct: item.changePct,
                time: item.time
            })
        );
    }
    catch (error) {}
}
function loadCryptoPortfolioCache(symbol) {
    try {
        const raw =
            localStorage.getItem(
                cryptoPortfolioCacheKey(symbol)
            );
        if (!raw) {
            return null;
        }
        const item =
            JSON.parse(raw);
        if (
            !item ||
            !Number.isFinite(Number(item.price)) || Number(item.price) <= 0
        ) {
            return null;
        }
        return item;
    }
    catch (error) {
        return null;
    }
}
const CRYPTO_HOLDINGS_CACHE_KEY = "financialDashboardCryptoHoldings_v1";
function saveCryptoHoldingsCache(holdings) {
    try {
        if (holdings && typeof holdings === "object") {
            localStorage.setItem(
                CRYPTO_HOLDINGS_CACHE_KEY,
                JSON.stringify(holdings)
            );
        }
    } catch (error) {}
}
function loadCryptoHoldingsCache() {
    try {
        const raw = localStorage.getItem(CRYPTO_HOLDINGS_CACHE_KEY);
        if (!raw) return null;
        const holdings = JSON.parse(raw);
        return holdings && typeof holdings === "object" ? holdings : null;
    } catch (error) {
        return null;
    }
}
function getCachedCryptoPortfolioMarketData(holdings) {
    const marketData = {};
    Object.keys(holdings || {}).forEach(coin => {
        const symbol = holdings[coin] && holdings[coin].symbol
            ? holdings[coin].symbol
            : coin + "_USDT";
        const cached = loadCryptoPortfolioCache(symbol);
        if (cached) {
            marketData[coin] = { ...cached, offline: !navigator.onLine };
        }
    });
    return marketData;
}
function formatCryptoAmount(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) {
        return "-";
    }
    return number.toLocaleString(
        "en-US",
        {
            maximumFractionDigits: 8
        }
    );
}
function cryptoPriceDecimals(asset) {
    // Normalize every form used by Signal Matrix (BTC, BTC/USDT, BTC_USDT, etc.)
    // so a decimal asset can never fall through to an integer/general formatter.
    const raw = String(asset || "").trim().toUpperCase();
    const key = raw.replace(/([/_-]?USDT|[/_-]?USD)$/i, "");
    return ({
        BTC: 0, BNB: 2, ETH: 1, DOGE: 5, ADA: 4, SOL: 2,
        POL: 5, AVAX: 3, GRAM: 3, TRX: 5
    })[key];
}
function formatCryptoAssetPrice(value, asset, withDollar = true) {
    const number = Number(value);
    if (!Number.isFinite(number) || number <= 0) return "-";
    const assetKey = String(asset || "").trim().toUpperCase();
    // USD quote itself is always displayed as a whole number everywhere.
    if (assetKey === "USD") return (withDollar ? "$" : "") + Math.round(number).toLocaleString("en-US", { maximumFractionDigits: 0 });
    const decimals = cryptoPriceDecimals(asset);
    // Signal Matrix / Ultra Compact must preserve the configured decimals exactly.
    const options = decimals === undefined
        ? { minimumFractionDigits: number < 1 ? 4 : 2, maximumFractionDigits: number < 1 ? 6 : 2 }
        : { minimumFractionDigits: decimals, maximumFractionDigits: decimals };
    const text = number.toLocaleString("en-US", options);
    return withDollar ? "$" + text : text;
}
function formatCryptoUsd(value, asset) {
    return formatCryptoAssetPrice(value, asset, true);
}
function formatCryptoUsdValue(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) {
        return "-";
    }
    return "$" + Math.round(number).toLocaleString("en-US", { maximumFractionDigits: 0 });
}
function formatCryptoPct(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) {
        return "-";
    }
    return (
        number >= 0 ? "+" : ""
    ) +
    number.toFixed(2) +
    "%";
}
function getCryptoPortfolioClass(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) {
        return "flat";
    }
    if (number > 0) {
        return "up";
    }
    if (number < 0) {
        return "down";
    }
    return "flat";
}
function extractMexcKlineCloses(payload) {
    if (
        !payload ||
        payload.success === false
    ) {
        throw new Error(
            "Invalid MEXC response"
        );
    }
    const data =
        payload.data;
    if (
        data &&
        Array.isArray(data.close)
    ) {
        return data.close
            .map(Number)
            .filter(
                value =>
                    Number.isFinite(value) &&
                    value > 0
            );
    }
    if (
        Array.isArray(data)
    ) {
        return data
            .map(row => {
                if (Array.isArray(row)) {
                    return Number(
                        row[4]
                    );
                }
                if (
                    row &&
                    typeof row === "object"
                ) {
                    return Number(
                        row.close ??
                        row.c ??
                        row[4]
                    );
                }
                return NaN;
            })
            .filter(
                value =>
                    Number.isFinite(value) &&
                    value > 0
            );
    }
    throw new Error(
        "MEXC close data not found"
    );
}
async function fetchMexcKlines(
    symbol,
    interval,
    start,
    end
) {
    let lastError = null;
    const directUrl =
        new URL(
            MEXC_KLINE_BASE +
            symbol
        );
    directUrl.searchParams.set(
        "interval",
        interval
    );
    directUrl.searchParams.set(
        "start",
        String(start)
    );
    directUrl.searchParams.set(
        "end",
        String(end)
    );
    const requestUrls = [
        directUrl.toString(),
        MEXC_CORS_PROXY + directUrl.toString()
    ];
    for (const requestUrl of requestUrls) {
        try {
            const response =
                await fetchWithTimeout(
                    requestUrl,
                    {
                        cache: "no-store"
                    },
                    10000
                );
            if (!response.ok) {
                throw new Error(
                    "MEXC HTTP " +
                    response.status
                );
            }
            const payload =
                await response.json();
            return extractMexcKlineCloses(
                payload
            );
        }
        catch (error) {
            lastError = error;
        }
    }
    throw (
        lastError ||
        new Error(
            "MEXC unavailable"
        )
    );
}
async function getMexcCryptoData(symbol) {
    const coin = String(symbol || "")
        .replace(/[-_\\/]/g, "")
        .replace(/USDT$/i, "")
        .toUpperCase();

    const live = await getUnifiedCryptoPrice(coin, { preferPrimary: true });

    /*
       Daily percentage must come directly from the active API source.
       Do not calculate it from current/previous prices and do not reuse an
       old cached percentage when the active API does not provide one.
    */
    const previousClose =
        live.previousClose == null ? null : Number(live.previousClose);
    const changePct =
        live.changePct == null ? null : Number(live.changePct);

    return {
        price: live.price,
        previousClose: Number.isFinite(previousClose) && previousClose > 0 ? previousClose : null,
        changePct: Number.isFinite(changePct) ? changePct : null,
        time: live.time,
        source: live.source,
        offline: false
    };
}

function renderCryptoPortfolio(
    holdings,
    marketData
) {
    const grid =
        $("cryptoPortfolioGrid");
    if (!grid) {
        return;
    }
    let totalValue = 0;
    let previousTotalValue = 0;
    let totalChangeComplete = true;
    const coins =
        Object.keys(holdings || {});
    // DOGE and BNB are swapped only in display order.
    const dogeIndex = coins.indexOf("DOGE");
    const bnbIndex = coins.indexOf("BNB");
    if (dogeIndex !== -1 && bnbIndex !== -1) {
        [coins[dogeIndex], coins[bnbIndex]] =
        [coins[bnbIndex], coins[dogeIndex]];
    }
    // AVAX is displayed after POL and before GRAM.
    const avaxIndex = coins.indexOf("AVAX");
    const polIndex = coins.indexOf("POL");
    if (avaxIndex !== -1 && polIndex !== -1) {
        coins.splice(avaxIndex, 1);
        const newPolIndex = coins.indexOf("POL");
        coins.splice(newPolIndex + 1, 0, "AVAX");
    }
    const html =
        coins.map(
            coin => {
                const holding =
                    holdings[coin] ||
                    {};
                const amount =
                    Number(
                        holding.amount
                    );
                const market =
                    marketData[coin];
                if (
                    !market ||
                    !Number.isFinite(
                        Number(market.price)
                    )
                ) {
                    if (Number.isFinite(amount) && amount > 0) {
                        totalChangeComplete = false;
                    }
                    return `
                        <div class="crypto-portfolio-item flat">
                            <div class="crypto-portfolio-item-top">
                                <div class="crypto-portfolio-coin">
                                    <img
                                        src="${CRYPTO_PORTFOLIO_ICONS[coin] || ""}"
                                        alt="${coin}"
                                    >
                                    <div>
                                        <div class="crypto-portfolio-name">${coin}</div>
                                        <div class="crypto-portfolio-amount">
                                            ${formatCryptoAmount(amount)}
                                        </div>
                                    </div>
                                </div>
                                <div class="crypto-portfolio-change">-</div>
                            </div>
                            <div class="crypto-portfolio-price-row">
                                <div class="crypto-portfolio-price">-</div>
                                <div class="crypto-portfolio-value">-</div>
                            </div>
                        </div>
                    `;
                }
                const price =
                    Number(
                        market.price
                    );
                const value =
                    Number.isFinite(amount)
                        ? amount * price
                        : 0;
                totalValue += value;
                // Total Value change is a weighted portfolio return. Each
                // asset's 24h return itself must come directly from its API.
                const assetChangePct =
                    market.changePct == null ? NaN : Number(market.changePct);
                if (
                    Number.isFinite(amount) && amount > 0 &&
                    Number.isFinite(assetChangePct) && assetChangePct > -100
                ) {
                    previousTotalValue += value / (1 + assetChangePct / 100);
                } else if (Number.isFinite(amount) && amount > 0) {
                    totalChangeComplete = false;
                }
                const className =
                    getCryptoPortfolioClass(
                        market.changePct
                    );
                return `
                    <div class="crypto-portfolio-item ${className}${market.offline ? " offline" : ""}">
                        <div class="crypto-portfolio-item-top">
                            <div class="crypto-portfolio-coin">
                                <img
                                    src="${CRYPTO_PORTFOLIO_ICONS[coin] || ""}"
                                    alt="${coin}"
                                >
                                <div>
                                    <div class="crypto-portfolio-name">${coin}</div>
                                    <div class="crypto-portfolio-amount">
                                        ${formatCryptoAmount(amount)}
                                    </div>
                                </div>
                            </div>
                            <div class="crypto-portfolio-change">
                                ${formatCryptoPct(market.changePct)}
                            </div>
                        </div>
                        <div class="crypto-portfolio-price-row">
                            <div class="crypto-portfolio-price">
                                ${formatCryptoUsd(price, coin)}
                            </div>
                            <div class="crypto-portfolio-value">
                                ${formatCryptoUsdValue(value)}
                            </div>
                        </div>
                    </div>
                `;
            }
        ).join("");
    const renderSignature = JSON.stringify(
        (coins || []).map(coin => {
            const market = marketData[coin] || null;
            return {
                coin,
                amount: holdings[coin]?.amount,
                price: market?.price ?? null,
                previousClose: market?.previousClose ?? null,
                changePct: market?.changePct ?? null,
                offline: !!market?.offline
            };
        })
    );
    if (renderSignature !== lastPortfolioRenderSignature) {
        grid.innerHTML = html || `<div class="crypto-portfolio-item flat">No crypto holdings</div>`;
        lastPortfolioRenderSignature = renderSignature;
    }
    safeText(
        "cryptoPortfolioTotal",
        formatCryptoUsdValue(
            totalValue
        )
    );
    // Derive the portfolio's weighted 24h return from the API-provided
    // percentage for each holding. No per-coin percentage is synthesized.
    const totalChange = $("cryptoPortfolioTotalChange");
    if (totalChange) {
        const totalPct =
            totalChangeComplete && previousTotalValue > 0
                ? ((totalValue / previousTotalValue) - 1) * 100
                : null;
        totalChange.textContent = formatCryptoPct(totalPct);
        totalChange.classList.remove("profit", "loss");
        if (Number.isFinite(totalPct)) {
            if (totalPct > 0) totalChange.classList.add("profit");
            else if (totalPct < 0) totalChange.classList.add("loss");
        }
    }
}
const FEAR_GREED_API =
    "https://api.alternative.me/fng/?limit=1";
const FEAR_GREED_CACHE_KEY =
    "financialDashboardFearGreedIndex";
const FEAR_GREED_REFRESH_MS =
    5 * 60 * 1000;
const FEAR_GREED_NOTIFICATION_STATE_KEY =
    "financialDashboardFearGreedNotificationState";
let fearGreedLoadInProgress = null;

function getFearGreedBand(value) {
    const v = Math.max(0, Math.min(100, Number(value)));
    if (v <= 24) return { key: "extreme-fear", label: "Extreme Fear", icon: "🔴" };
    if (v <= 44) return { key: "fear", label: "Fear", icon: "🟠" };
    if (v <= 55) return { key: "neutral", label: "Neutral", icon: "⚪" };
    if (v <= 75) return { key: "greed", label: "Greed", icon: "🟢" };
    return { key: "extreme-greed", label: "Extreme Greed", icon: "🔥" };
}

function loadFearGreedNotificationState() {
    try {
        const raw = localStorage.getItem(FEAR_GREED_NOTIFICATION_STATE_KEY);
        return raw ? JSON.parse(raw) : null;
    } catch (error) {
        return null;
    }
}

function saveFearGreedNotificationState(item) {
    try {
        localStorage.setItem(FEAR_GREED_NOTIFICATION_STATE_KEY, JSON.stringify(item));
    } catch (error) {}
}

async function notifyFearGreedBandChange(previousItem, currentItem) {
    const currentValue = Number(currentItem?.value);
    if (!Number.isFinite(currentValue)) return;
    const currentBand = getFearGreedBand(currentValue);
    const previousValue = Number(previousItem?.value);
    const previousBand = Number.isFinite(previousValue)
        ? getFearGreedBand(previousValue)
        : null;

    // First valid reading establishes the baseline; only a band change alerts.
    if (!previousBand) {
        saveFearGreedNotificationState({
            value: currentValue,
            band: currentBand.key,
            updatedAt: Date.now()
        });
        return;
    }
    if (previousBand.key === currentBand.key) {
        saveFearGreedNotificationState({
            value: currentValue,
            band: currentBand.key,
            updatedAt: Date.now()
        });
        return;
    }

    const title = `${currentBand.icon} Fear & Greed`;
    const message =
        `${previousBand.icon} ${previousBand.label} → ${currentBand.icon} ${currentBand.label}` +
        ` • ${Math.round(currentValue)}`;
    try {
        await sendDashboardNotification(
            title,
            message,
            "./icon-192.png",
            "FEAR_GREED"
        );
    } catch (error) {
        console.warn("Fear & Greed notification failed:", error);
    }
    saveFearGreedNotificationState({
        value: currentValue,
        band: currentBand.key,
        updatedAt: Date.now()
    });
}

function saveFearGreedCache(item) {
    try {
        localStorage.setItem(
            FEAR_GREED_CACHE_KEY,
            JSON.stringify(item)
        );
    } catch (error) {}
}

function loadFearGreedCache() {
    try {
        const raw = localStorage.getItem(FEAR_GREED_CACHE_KEY);
        if (!raw) return null;
        const item = JSON.parse(raw);
        if (!item || !Number.isFinite(Number(item.value))) return null;
        return item;
    } catch (error) {
        return null;
    }
}

function renderFearGreedIndex(item, offline = false) {
    const gauge = document.getElementById("fearGreedMini");
    const valueEl = document.getElementById("fearGreedValue");
    if (!gauge || !item) return;
    const value = Math.max(0, Math.min(100, Number(item.value)));
    const classification = item.value_classification || "Unknown";
    if (valueEl) valueEl.textContent = String(Math.round(value));
    gauge.style.setProperty("--fear-greed-position", `${value}%`);
    gauge.setAttribute(
        "aria-label",
        `Fear and Greed Index ${value}: ${classification}`
    );
    gauge.title =
        `Fear and Greed Index: ${value} • ${classification}` +
        (offline ? " • Cached" : "");
}

async function loadFearGreedIndex() {
    if (fearGreedLoadInProgress) return fearGreedLoadInProgress;
    const cached = loadFearGreedCache();
    if (cached) {
        renderFearGreedIndex(cached, true);
        setPageIssue(
            "fear-greed",
            `Fear & Greed is using cached data${cached.cachedAt ? ` · ${new Date(cached.cachedAt).toISOString()}` : ""}`,
            "cache"
        );
    }
    fearGreedLoadInProgress = (async () => {
        try {
            const response = await fetchApiWithProxyFallback(
                FEAR_GREED_API,
                {
                    method: "GET",
                    headers: { "Accept": "application/json" },
                    cache: "no-store"
                },
                6000,
                payload => {
                    const v = Number(payload?.data?.[0]?.value);
                    if (!Number.isFinite(v) || v < 0 || v > 100) throw new Error("Invalid Fear & Greed payload");
                    return true;
                }
            );
            const data = await response.json();
            const item = data?.data?.[0];
            const value = Number(item?.value);
            if (!item || !Number.isFinite(value) || value < 0 || value > 100) {
                throw new Error("Invalid Fear and Greed Index response");
            }
            const normalized = {
                value,
                value_classification: item.value_classification || "Unknown",
                timestamp: item.timestamp || null,
                time_until_update: item.time_until_update || null,
                cachedAt: Date.now()
            };
            const previousNotificationState = loadFearGreedNotificationState();
            await notifyFearGreedBandChange(previousNotificationState, normalized);
            saveFearGreedCache(normalized);
            renderFearGreedIndex(normalized, false);
            clearPageIssue("fear-greed");
        } catch (error) {
            if (cached) {
                renderFearGreedIndex(cached, true);
                setPageIssue(
                    "fear-greed",
                    `Fear & Greed is offline — showing cached data${cached.cachedAt ? ` · ${new Date(cached.cachedAt).toISOString()}` : ""}`,
                    "cache"
                );
            } else {
                setPageIssue(
                    "fear-greed",
                    "Fear & Greed unavailable",
                    "error"
                );
            }
            console.warn("Fear and Greed Index request failed:", error);
        }
    })();
    try {
        await fearGreedLoadInProgress;
    } finally {
        fearGreedLoadInProgress = null;
    }
}

async function loadCryptoPortfolio(
    data
) {
    if (cryptoPortfolioLoadInProgress) {
        return cryptoPortfolioLoadInProgress;
    }
    const now = Date.now();
    if (
        cryptoPortfolioLastLoadTime > 0 &&
        now - cryptoPortfolioLastLoadTime < CRYPTO_PORTFOLIO_REFRESH_MS
    ) {
        return;
    }
    cryptoPortfolioLoadInProgress = (async () => {
        let sourceData = data;
        let holdings = null;
        // 1) Use data.json data when it is already available.
        if (
            sourceData &&
            sourceData.cryptoHoldings &&
            typeof sourceData.cryptoHoldings === "object"
        ) {
            holdings = sourceData.cryptoHoldings;
        }
        // 2) Offline fallback: use the dashboard's saved data.
        if (!holdings) {
            const cachedDashboardData = loadCachedDashboardData();
            if (
                cachedDashboardData &&
                cachedDashboardData.cryptoHoldings &&
                typeof cachedDashboardData.cryptoHoldings === "object"
            ) {
                holdings = cachedDashboardData.cryptoHoldings;
            }
        }
        // 3) Dedicated Crypto Portfolio cache.
        if (!holdings) {
            holdings = loadCryptoHoldingsCache();
        }
        // 4) Independent data.json request. This is deliberately separate
        //    from loadDashboardData(), so one dashboard API failure cannot
        //    make the Crypto Portfolio disappear.
        if (!holdings) {
            try {
                const response = await fetchWithTimeout(
                    REMOTE_DATA_JSON_URL + "?t=" + Date.now(),
                    { cache: "no-store" },
                    5000
                );
                if (response.ok) {
                    const freshData = await response.json();
                    if (
                        freshData &&
                        freshData.cryptoHoldings &&
                        typeof freshData.cryptoHoldings === "object"
                    ) {
                        sourceData = freshData;
                        holdings = freshData.cryptoHoldings;
                        saveLocalData(freshData);
                    }
                }
            }
            catch (error) {
                console.warn(
                    "Independent data.json Crypto Portfolio request failed:",
                    error
                );
            }
        }
        // Never overwrite the visible Portfolio with an empty object.
        // If there is no previous data at all, leave the existing UI intact.
        if (
            !holdings ||
            typeof holdings !== "object" ||
            Object.keys(holdings).length === 0
        ) {
            setPageIssue("portfolio-data", "Portfolio data missing", "error");
            console.warn("Crypto Portfolio: no holdings available yet.");
            return;
        }
        clearPageIssue("portfolio-data");
        cryptoPortfolioLastLoadTime = Date.now();
        saveCryptoHoldingsCache(holdings);
        // Show cached prices immediately.
        const cachedMarketData =
            getCachedCryptoPortfolioMarketData(holdings);
        renderCryptoPortfolio(
            holdings,
            cachedMarketData
        );
        const coins = Object.keys(holdings);
        const marketData = { ...cachedMarketData };
        await Promise.all(
            coins.map(async coin => {
                const symbol =
                    holdings[coin] && holdings[coin].symbol
                        ? holdings[coin].symbol
                        : coin + "_USDT";
                try {
                    const liveData =
                        await getMexcCryptoData(symbol);
                    marketData[coin] = liveData;
                    saveCryptoPortfolioCache(symbol, liveData);
                    clearPageIssue(`mexc:${coin}`);
                }
                catch (error) {
                    const cached =
                        loadCryptoPortfolioCache(symbol);
                    if (cached) {
                        marketData[coin] = { ...cached, offline: !navigator.onLine };
                        setPageIssue(`mexc:${coin}`, `${coin} price cached`, "cache");
                    } else {
                        setPageIssue(`mexc:${coin}`, `${coin} price unavailable`, "error");
                    }
                    console.warn("MEXC portfolio error:", coin, error);
                }
            })
        );
        renderCryptoPortfolio(holdings, marketData);
        const liveCount = Object.values(marketData).filter(item => item && !item.offline).length;
        const cachedCount = Object.values(marketData).filter(item => item && item.offline).length;
        if (liveCount > 0) clearPageIssue("mexc-api");
        if (cachedCount > 0) setPageIssue("mexc-cache", `${cachedCount} crypto prices cached`, "cache");
        else clearPageIssue("mexc-cache");
    })();
    try {
        await cryptoPortfolioLoadInProgress;
    }
    finally {
        cryptoPortfolioLoadInProgress = null;
    }
}
/* =====================================================
   APP PIN LOCK
   Native app_locked is the authoritative state for Activity lifetime.
   sessionStorage is only a convenience for the same WebView session.
   Manual refresh: native state remains unlocked => no PIN.
   New Activity after closing/reopening: native state starts locked => PIN.
===================================================== */
(function initializeAppPinLock() {
    const APP_PIN = "4181";
    const PIN_SESSION_KEY = "financialAppPinUnlocked";
    const pinOverlay = document.getElementById("appPinLock");
    const pinBoxes = Array.from(
        document.querySelectorAll("#appPinBoxes .app-pin-box")
    );
    const pinError = document.getElementById("appPinError");
    const pinLoader = document.getElementById("appPinLoader");
    const pinKeypad = document.getElementById("appPinKeypad");
    const pinEntryModal = document.getElementById("appPinEntryModal");
    const pinEntryInput = document.getElementById("appPinEntryInput");
    const pinEntryCancel = document.getElementById("appPinEntryCancel");
    const pinEntrySubmit = document.getElementById("appPinEntrySubmit");
    const pinEntryHint = document.getElementById("appPinEntryHint");

    if (!pinOverlay || !pinKeypad || pinBoxes.length !== 4) return;

    let enteredPin = "";
    let focusedPinIndex = 0;
    let suppressNextPinClickUntil = 0;
    const isLgWebOsTv = /web0s|webos|netcast|lg browser|lg tv/i.test(String(navigator.userAgent || ""));
    let tvPinKeyAt = 0;
    let tvPinKeyType = "";

    function nativeAvailable() {
        return !!(
            window.AndroidNotifications &&
            typeof window.AndroidNotifications.isAppLocked === "function" &&
            typeof window.AndroidNotifications.setAppLocked === "function"
        );
    }

    function nativeLocked() {
        try {
            return nativeAvailable()
                ? !!window.AndroidNotifications.isAppLocked()
                : true;
        } catch (error) { return true; }
    }

    function setNativeLockState(locked) {
        try {
            if (window.AndroidNotifications && typeof window.AndroidNotifications.setAppLocked === "function") {
                window.AndroidNotifications.setAppLocked(locked);
            }
        } catch (error) {}
    }

    function isSessionUnlocked() {
        try { return sessionStorage.getItem(PIN_SESSION_KEY) === "1"; }
        catch (error) { return false; }
    }

    function markSessionUnlocked() {
        try { sessionStorage.setItem(PIN_SESSION_KEY, "1"); }
        catch (error) {}
    }

    function renderPin() {
        pinBoxes.forEach((box, index) => {
            const active = index < enteredPin.length;
            box.textContent = active ? "●" : "";
            box.classList.toggle("active", active);
        });
    }

    function renderPinFocus() {
        const cells = Array.from(pinKeypad.querySelectorAll("button.app-pin-key"));
        cells.forEach((button) => button.classList.remove("pin-nav-focus"));
        const selected = cells[focusedPinIndex];
        if (selected && !selected.classList.contains("spacer")) {
            selected.classList.add("pin-nav-focus");
        }
    }

    function setPinLoading(loading) {
        if (pinLoader) pinLoader.classList.toggle("visible", loading);
        if (pinKeypad) pinKeypad.style.pointerEvents = loading ? "none" : "";
    }

    function resetPin() {
        setPinLoading(false);
        enteredPin = "";
        renderPin();
    }

    function showPin() {
        resetPin();
        focusedPinIndex = 0;
        renderPinFocus();
        if (pinError) pinError.textContent = "";
        pinOverlay.classList.remove("pin-unlocked");
        pinOverlay.setAttribute("aria-hidden", "false");
        if (!isLgWebOsTv) {
            try { pinOverlay.focus({ preventScroll: true }); } catch (error) { try { pinOverlay.focus(); } catch (_) {} }
        }
    }

    function hidePin() {
        pinOverlay.classList.add("pin-unlocked");
        pinOverlay.setAttribute("aria-hidden", "true");
        markSessionUnlocked();
        setNativeLockState(false);
    }

    function checkPin() {
        setPinLoading(false);
        if (enteredPin === APP_PIN) {
            if (pinError) pinError.textContent = "";
            hidePin();
            return true;
        }
        if (pinError) pinError.textContent = "Incorrect PIN";
        resetPin();
        focusedPinIndex = 0;
        renderPinFocus();
        window.setTimeout(() => { if (pinError) pinError.textContent = ""; }, 1800);
        return false;
    }

    function addDigit(digit) {
        if (enteredPin.length >= 4) return;
        enteredPin += digit;
        renderPin();
        // Show the animated indicator for the actual PIN check, without adding a timer.
        if (enteredPin.length === 4) {
            setPinLoading(true);
            // Let the browser paint the spinner before running the synchronous check.
            requestAnimationFrame(() => requestAnimationFrame(() => {
                if (!pinOverlay.classList.contains("pin-unlocked") && enteredPin.length === 4) {
                    checkPin();
                } else {
                    setPinLoading(false);
                }
            }));
        }
    }

    function deleteDigit() {
        if (!enteredPin.length) return;
        enteredPin = enteredPin.slice(0, -1);
        if (pinError) pinError.textContent = "";
        renderPin();
    }

    function openPinEntryModal() {
        if (!pinEntryModal || !pinEntryInput) return;
        pinEntryInput.value = "";
        if (pinEntryHint) pinEntryHint.textContent = isLgWebOsTv ? "Use the TV keyboard or the remote number keys." : "";
        pinEntryModal.classList.add("open");
        pinEntryModal.setAttribute("aria-hidden", "false");

        // LG webOS opens its system virtual keyboard only after a real input
        // receives focus. Do this inside the same user action and retry once
        // for older TV browser engines.
        try { pinEntryInput.focus(); } catch (error) {}
        window.setTimeout(() => {
            try { pinEntryInput.focus(); } catch (error) {}
        }, isLgWebOsTv ? 60 : 40);
    }

    function closePinEntryModal() {
        if (!pinEntryModal) return;
        pinEntryModal.classList.remove("open");
        pinEntryModal.setAttribute("aria-hidden", "true");
    }

    function submitPinEntryModal() {
        if (!pinEntryInput) return false;
        const value = String(pinEntryInput.value || "").replace(/\D/g, "").slice(0, 4);
        pinEntryInput.value = value;
        if (value.length !== 4) {
            if (pinEntryHint) pinEntryHint.textContent = "Enter 4 digits";
            return false;
        }
        closePinEntryModal();
        enteredPin = value;
        renderPin();
        return checkPin();
    }

    function triggerPinHaptic() {
        let nativeTriggered = false;
        try {
            if (window.AndroidNotifications && typeof window.AndroidNotifications.vibratePinKey === "function") {
                window.AndroidNotifications.vibratePinKey();
                nativeTriggered = true;
            }
        } catch (error) {}
        if (!nativeTriggered) {
            try { if (navigator.vibrate) navigator.vibrate(28); } catch (error) {}
        }
    }

    // Use the browser's native 5-way focus for LG/webOS.
    // This is intentional: old LG browsers are much more reliable when the
    // actual <button> receives focus and Enter/OK triggers its native click.
    // We only keep track of which real button is focused; arrows themselves
    // are left to the browser so they cannot fight with its spatial navigation.
    const pinCells = Array.from(pinKeypad.querySelectorAll("button.app-pin-key"));
    pinCells.forEach((button) => {
        if (button.classList.contains("spacer")) {
            button.setAttribute("tabindex", "-1");
            button.setAttribute("focusable", "false");
        } else {
            button.setAttribute("tabindex", "0");
            button.setAttribute("type", "button");
            button.setAttribute("focusable", "true");
        }
    });
    pinKeypad.setAttribute("tabindex", "-1");
    pinOverlay.setAttribute("tabindex", "-1");

    function syncNativePinFocus(button) {
        const index = pinCells.indexOf(button);
        if (index >= 0 && !button.classList.contains("spacer")) {
            focusedPinIndex = index;
            renderPinFocus();
        }
    }

    pinCells.forEach((button) => {
        button.addEventListener("focus", () => syncNativePinFocus(button), false);
        button.addEventListener("focusin", () => syncNativePinFocus(button), false);
        button.addEventListener("mousedown", () => syncNativePinFocus(button), false);
        button.addEventListener("pointerdown", () => syncNativePinFocus(button), false);
    });

    // R03: direct handler for old LG browsers. This avoids relying on
    // delegated DOM helpers that some legacy webOS engines do not implement.
    window.__financialPinPress = function(button) {
        if (!button || !button.getAttribute) return false;
        var now = Date.now();
        suppressNextPinClickUntil = now + 650;
        syncNativePinFocus(button);
        var inputOpen = button.getAttribute("data-pin-input-open") === "true";
        var deleteKey = button.getAttribute("data-pin-delete") === "true";
        var digit = button.getAttribute("data-pin");
        if (inputOpen) openPinEntryModal();
        else if (deleteKey) deleteDigit();
        else if (digit !== null && digit !== "") addDigit(digit);
        triggerPinHaptic();
        return false;
    };

    window.__financialPinCancel = function() {
        closePinEntryModal();
        try { pinKeypad.focus(); } catch (error) {}
        return false;
    };

    window.__financialPinSubmit = function() {
        submitPinEntryModal();
        return false;
    };

    pinKeypad.addEventListener("click", event => {
        let button = event.target;
        while (button && button !== pinKeypad && !(button.classList && button.classList.contains("app-pin-key"))) {
            button = button.parentNode;
        }
        if (!button || button.classList.contains("spacer")) return;
        // LG webOS can dispatch a synthetic click after the remote OK/Enter
        // key has already been handled by the explicit keyboard path below.
        // Consume that one duplicate click so one remote press = one digit.
        if (Date.now() < suppressNextPinClickUntil) {
            suppressNextPinClickUntil = 0;
            return;
        }
        syncNativePinFocus(button);
        if (button.dataset.pinInputOpen === "true") {
            openPinEntryModal();
        } else if (button.dataset.pinDelete === "true") deleteDigit();
        else if (button.dataset.pin !== undefined) addDigit(button.dataset.pin);
        triggerPinHaptic();
    }, false);

    // Some LG webOS WebViews move the remote cursor/focus to a <button>, but
    // do not reliably synthesize the button's click event for the OK/Enter
    // key. Handle OK/Enter directly on the focused PIN button as a fallback.
    // This is limited to the PIN keypad and does not affect the dashboard.
    let pinOkHandledAt = 0;

    function handlePinOkEvent(event) {
        if (pinOverlay.classList.contains("pin-unlocked")) return false;
        const key = String(event.key || "");
        const code = Number(event.keyCode || event.which || 0);
        const isOkEnter = key === "Enter" || key === "OK" || code === 13;
        if (!isOkEnter) return false;

        // LG webOS TV: do not depend on native focus/click at all.  The PIN
        // controller owns Arrow/OK while the overlay is visible.
        if (isLgWebOsTv) {
            event.preventDefault();
            event.stopPropagation();
            const now = Date.now();
            if (tvPinKeyType === "ok" && now - tvPinKeyAt < 650) return true;
            tvPinKeyAt = now;
            tvPinKeyType = "ok";
            if (pinEntryModal && pinEntryModal.classList.contains("open")) {
                submitPinEntryModal();
            } else {
                activateFocusedPinKey();
            }
            suppressNextPinClickUntil = now + 650;
            return true;
        }

        const active = document.activeElement;
        if (!active || !active.classList || !active.classList.contains("app-pin-key")) return false;
        if (active.classList.contains("spacer")) return false;

        const now = Date.now();
        if (now - tvPinKeyAt < 650 && tvPinKeyType === "ok") {
            event.preventDefault();
            event.stopPropagation();
            return true;
        }
        event.preventDefault();
        event.stopPropagation();
        syncNativePinFocus(active);
        tvPinKeyAt = now;
        tvPinKeyType = "ok";
        suppressNextPinClickUntil = now + 700;
        activateFocusedPinKey();
        return true;
    }

    window.addEventListener("keydown", event => {
        handlePinOkEvent(event);
    }, true);

    window.addEventListener("keyup", event => {
        handlePinOkEvent(event);
    }, true);

    // LG webOS TV gets a fully deterministic 5-way controller while the PIN
    // overlay is open. Mobile/desktop keep the existing native focus behavior.
    const pinGrid = [
        pinCells.findIndex(b => b.dataset.pin === "1"),
        pinCells.findIndex(b => b.dataset.pin === "2"),
        pinCells.findIndex(b => b.dataset.pin === "3"),
        pinCells.findIndex(b => b.dataset.pin === "4"),
        pinCells.findIndex(b => b.dataset.pin === "5"),
        pinCells.findIndex(b => b.dataset.pin === "6"),
        pinCells.findIndex(b => b.dataset.pin === "7"),
        pinCells.findIndex(b => b.dataset.pin === "8"),
        pinCells.findIndex(b => b.dataset.pin === "9"),
        pinCells.findIndex(b => b.dataset.pinInputOpen === "true"),
        pinCells.findIndex(b => b.dataset.pin === "0"),
        pinCells.findIndex(b => b.dataset.pinDelete === "true")
    ];

    function movePinFocusFallback(dx, dy) {
        const current = pinGrid.indexOf(focusedPinIndex);
        if (current < 0) return;
        const row = Math.floor(current / 3);
        const col = current % 3;
        let r = Math.max(0, Math.min(3, row + dy));
        let c = Math.max(0, Math.min(2, col + dx));
        let target = r * 3 + c;
        const index = pinGrid[target];
        const button = index != null && index >= 0 ? pinCells[index] : null;
        if (button) {
            focusedPinIndex = index;
            renderPinFocus();
            if (!isLgWebOsTv) {
                try { button.focus(); } catch (error) {}
            }
        }
    }

    function activateFocusedPinKey() {
        const button = pinCells[focusedPinIndex];
        if (!button || button.classList.contains("spacer")) return;
        syncNativePinFocus(button);
        if (button.dataset.pinInputOpen === "true") {
            openPinEntryModal();
            triggerPinHaptic();
            return;
        }
        if (enteredPin.length >= 4) {
            checkPin();
            triggerPinHaptic();
            return;
        }
        if (button.dataset.pinDelete === "true") deleteDigit();
        else if (button.dataset.pin !== undefined) addDigit(button.dataset.pin);
        triggerPinHaptic();
    }

    window.addEventListener("keydown", event => {
        if (!isLgWebOsTv || pinOverlay.classList.contains("pin-unlocked")) return;
        if (pinEntryModal && pinEntryModal.classList.contains("open")) return;
        const key = String(event.key || "");
        const code = Number(event.keyCode || event.which || 0);
        const digitMatch = key.match(/^[0-9]$/);
        let digit = digitMatch ? digitMatch[0] : null;
        if (digit == null && code >= 48 && code <= 57) digit = String(code - 48);
        if (digit == null && code >= 96 && code <= 105) digit = String(code - 96);
        // Older LG/webOS TV remote browsers expose numeric remote buttons
        // with keycodes 7..16 instead of 48..57. Keep this mapping for
        // compatibility with the original TV PIN flow.
        if (digit == null && code >= 7 && code <= 16) digit = String(code - 7);
        if (digit == null) return;
        event.preventDefault();
        event.stopPropagation();
        addDigit(digit);
        const target = pinCells.findIndex(button => button.dataset.pin === digit);
        if (target >= 0) {
            focusedPinIndex = target;
            renderPinFocus();
        }
        triggerPinHaptic();
    }, true);

    // Desktop/laptop physical keyboard: enter PIN digits directly without Enter.
    // Keep this limited to the PIN overlay so normal dashboard typing is untouched.
    window.addEventListener("keydown", event => {
        if (isLgWebOsTv || pinOverlay.classList.contains("pin-unlocked")) return;
        if (pinEntryModal && pinEntryModal.classList.contains("open")) return;
        const key = String(event.key || "");
        const code = Number(event.keyCode || event.which || 0);
        let digit = /^[0-9]$/.test(key) ? key : null;
        if (digit == null && code >= 48 && code <= 57) digit = String(code - 48);
        if (digit == null && code >= 96 && code <= 105) digit = String(code - 96);
        if (digit == null) return;
        event.preventDefault();
        event.stopPropagation();
        addDigit(digit);
        const target = pinCells.findIndex(button => button.dataset.pin === digit);
        if (target >= 0) {
            focusedPinIndex = target;
            renderPinFocus();
        }
    }, true);

    // Some older TV browsers expose remote numeric input through keypress
    // rather than keydown. Reuse the same decoder without double-entering a digit.
    let lastTvDigitAt = 0;
    let lastTvDigit = "";
    window.addEventListener("keypress", event => {
        if (!isLgWebOsTv || pinOverlay.classList.contains("pin-unlocked")) return;
        if (pinEntryModal && pinEntryModal.classList.contains("open")) return;
        const key = String(event.key || "");
        const code = Number(event.keyCode || event.which || 0);
        let digit = /^[0-9]$/.test(key) ? key : null;
        if (digit == null && code >= 48 && code <= 57) digit = String(code - 48);
        if (digit == null && code >= 7 && code <= 16) digit = String(code - 7);
        if (digit == null) return;
        const now = Date.now();
        if (lastTvDigit === digit && now - lastTvDigitAt < 120) return;
        lastTvDigit = digit;
        lastTvDigitAt = now;
        event.preventDefault();
        event.stopPropagation();
        addDigit(digit);
        triggerPinHaptic();
    }, true);

    window.addEventListener("keydown", event => {
        if (!isLgWebOsTv || pinOverlay.classList.contains("pin-unlocked")) return;
        const key = String(event.key || "");
        const code = Number(event.keyCode || event.which || 0);
        const isArrow = key === "ArrowUp" || key === "ArrowDown" || key === "ArrowLeft" || key === "ArrowRight"
            || code === 37 || code === 38 || code === 39 || code === 40;
        if (!isArrow) return;
        event.preventDefault();
        event.stopPropagation();
        let dx = 0, dy = 0;
        if (key === "ArrowUp" || code === 38) dy = -1;
        else if (key === "ArrowDown" || code === 40) dy = 1;
        else if (key === "ArrowLeft" || code === 37) dx = -1;
        else if (key === "ArrowRight" || code === 39) dx = 1;
        movePinFocusFallback(dx, dy);
    }, true);

    // Non-TV fallback: keep visual state synchronized with native spatial focus.
    window.addEventListener("keydown", event => {
        if (isLgWebOsTv || pinOverlay.classList.contains("pin-unlocked")) return;
        const key = String(event.key || "");
        const code = Number(event.keyCode || event.which || 0);
        const isArrow = key === "ArrowUp" || key === "ArrowDown" || key === "ArrowLeft" || key === "ArrowRight"
            || code === 37 || code === 38 || code === 39 || code === 40;
        if (!isArrow) return;
        window.setTimeout(() => {
            const active = document.activeElement;
            if (active && active.classList && active.classList.contains("app-pin-key")) syncNativePinFocus(active);
        }, 0);
    }, false);

    window.addEventListener("keydown", event => {
        if (pinOverlay.classList.contains("pin-unlocked")) return;
        const key = String(event.key || "");
        const code = Number(event.keyCode || event.which || 0);
        if (key === "Backspace" || key === "Delete" || code === 8 || code === 67) {
            event.preventDefault();
            event.stopPropagation();
            deleteDigit();
            triggerPinHaptic();
        }
    }, true);

    if (pinEntryCancel) pinEntryCancel.addEventListener("click", () => { closePinEntryModal(); });
    if (pinEntrySubmit) pinEntrySubmit.addEventListener("click", () => { submitPinEntryModal(); });
    if (pinEntryInput) {
        pinEntryInput.addEventListener("input", () => {
            const value = String(pinEntryInput.value || "").replace(/\D/g, "").slice(0, 4);
            pinEntryInput.value = value;
            if (value.length === 4) {
                window.setTimeout(() => submitPinEntryModal(), 60);
            }
        }, false);
        pinEntryInput.addEventListener("keydown", (event) => {
            const key = String(event.key || "");
            const code = Number(event.keyCode || event.which || 0);
            if (key === "Enter" || key === "OK" || code === 13) {
                event.preventDefault();
                event.stopPropagation();
                submitPinEntryModal();
            }
        }, true);
    }
    window.addEventListener("keydown", (event) => {
        if (!pinEntryModal || !pinEntryModal.classList.contains("open")) return;
        const key = String(event.key || "");
        const code = Number(event.keyCode || event.which || 0);
        if (key === "Enter" || key === "OK" || code === 13) {
            event.preventDefault();
            event.stopPropagation();
            submitPinEntryModal();
        } else if (key === "Escape" || code === 27) {
            event.preventDefault();
            event.stopPropagation();
            closePinEntryModal();
        }
    }, true);

    // Start on the first real key. This gives LG a genuine focus target and
    // therefore lets Arrow + OK/Enter use its native 5-way implementation.
    window.setTimeout(() => {
        if (!pinOverlay.classList.contains("pin-unlocked")) {
            const first = pinCells.find(button => !button.classList.contains("spacer"));
            if (first) {
                focusedPinIndex = pinCells.indexOf(first);
                renderPinFocus();
                if (!isLgWebOsTv) {
                    try { first.focus(); } catch (error) {}
                }
            }
        }
    }, 80);

    const unlockedForSession = isSessionUnlocked();
    const lockedByNative = nativeLocked();
    if (nativeAvailable() && !lockedByNative) {
        hidePin();
    } else if (!nativeAvailable() && unlockedForSession) {
        pinOverlay.classList.add("pin-unlocked");
        pinOverlay.setAttribute("aria-hidden", "true");
    } else {
        setNativeLockState(true);
        showPin();
    }
})();
/* =====================================================
   RF / KJ SIGNAL MATRIX MONITOR
===================================================== */
const RFKJ_WORKER_URL = "https://signalmatrix.rxw1.workers.dev/";
const RFKJ_FALLBACK_URL = "https://ropmwamrcvvuwrvajxdr.supabase.co/functions/v1/clever-handler/";
const RFKJ_REQUEST_TIMEOUT_MS = 5000;
let rfkjActiveApiIndex = 0; // 0 = primary worker, 1 = Supabase fallback

async function rfkjFetchWithFailover(query){
    const urls = [RFKJ_WORKER_URL, RFKJ_FALLBACK_URL];
    const makeUrl = base => base + (base.includes("?") ? "&" : "?") + query;
    const tryFetch = async base => {
        const response = await fetchWithTimeout(makeUrl(base), {
            method:"GET", cache:"no-store", headers:{"Accept":"application/json"}
        }, RFKJ_REQUEST_TIMEOUT_MS);
        if(!response.ok) throw new Error(`Signal Matrix HTTP ${response.status}`);
        let data = await response.json();
        if(data && typeof data === "object" && data.data && typeof data.data === "object") data = data.data;
        if(!data || typeof data !== "object" || Array.isArray(data)) throw new Error("Invalid Signal Matrix response");
        const hasAny = keys => keys.some(key => Object.prototype.hasOwnProperty.call(data,key));
        const hasRF = hasAny(["RF","rf","rangeFilter","range_filter"]);
        const hasKJ = hasAny(["KJ","kj","kijunsen"]);
        const hasST = hasAny(["ST","st","superTrend","supertrend"]);
        if(!hasRF || !hasKJ || !hasST) throw new Error("Signal Matrix response is missing RF/KJ/ST values");
        return data;
    };
    // Probe primary, then fallback exactly once. Repeating the primary in the
    // same cycle multiplied traffic without improving recovery probability.
    let primaryError = null;
    try {
        const data = await tryFetch(urls[0]);
        rfkjActiveApiIndex = 0;
        return data;
    } catch (error) {
        primaryError = error;
    }
    try {
        const data = await tryFetch(urls[1]);
        rfkjActiveApiIndex = 1;
        return data;
    } catch (fallbackError) {
        throw new Error(`Signal Matrix APIs unavailable (primary: ${primaryError?.message || "failed"}; fallback: ${fallbackError?.message || "failed"})`);
    }
}
const RFKJ_POLL_MS = 5000;
const RFKJ_LIVE_CACHE_KEY = "rfkjLiveSignalCache-v1";
const RFKJ_SIGNAL_CACHE_KEY = "rfkjSignalDisplayCache-v1";
const RFKJ_SLOT_COUNT = 3;
const RFKJ_TIMEFRAMES = ["1","3","5","15","30","60","4h","1D","1W"];
const RFKJ_STATE_KEY = "rfkjMatrixState-v4";
const rfkjInstances = Array.from({length:RFKJ_SLOT_COUNT},(_,i)=>({
    index:i+1,
    selectedTimeframes:["1"],
    running:false,
    timer:null,
    requestInProgress:false,
    cycleInProgress:false,
    cyclePricePromise:null,
    cycleDailyChangePromise:null,
    previousSignals:{},
    latestTimeframe:"1",
    latestPrice:null,
    latestPriceRaw:null,
    latestUpdated:null,
    latestDailyChangePercent:null,
    priceDirection:0,
    ultraRF:0,
    ultraST:0,
    ultraKJ:0,
    cachedLive:null
}));
function rfkjGet(id,index){return document.getElementById(`${id}${index}`);}
function rfkjInstance(index){return rfkjInstances[index-1];}
function rfkjNormalizeSignal(value){
    if(value&&typeof value==="object"){
        const nested=value.signal??value.value??value.score??value.direction??value.action??value.type;
        if(nested!==undefined&&nested!==value)return rfkjNormalizeSignal(nested);
    }
    const text=String(value??"").trim().toUpperCase();
    if(text==="BUY"||text==="BULL"||text==="BULLISH"||text==="LONG"||text==="1")return 1;
    if(text==="SELL"||text==="BEAR"||text==="BEARISH"||text==="SHORT"||text==="-1")return -1;
    const n=Number(value);
    return Number.isFinite(n)?(n>0?1:n<0?-1:0):0;
}
function rfkjFormatPrice(value, asset){
    // USD price is displayed without decimals in the Signal Matrix header and Ultra Compact.
    const assetKey = String(asset || "").trim().toUpperCase();
    const usdNumber = Number(String(value ?? "").trim().replace(/,/g, ""));
    if(assetKey === "USD") return Number.isFinite(usdNumber) && usdNumber > 0 ? Math.round(usdNumber).toLocaleString("en-US", {maximumFractionDigits:0}) : "-";
    // Signal Matrix / Ultra Compact: preserve decimal assets explicitly.
    // Keep the raw decimal string when available so a small price such as
    // DOGE 0.12345 or GRAM 0.015234 can never be collapsed to integer 0.
    const decimals = cryptoPriceDecimals(asset);
    const raw = String(value ?? "").trim().replace(/,/g, "");
    const number = Number(raw);
    if(!Number.isFinite(number) || number<=0)return "-";
    if(decimals===undefined){
        return number.toLocaleString("en-US",{
            minimumFractionDigits:number<1?4:2,
            maximumFractionDigits:number<1?6:2
        });
    }
    return number.toLocaleString("en-US",{
        minimumFractionDigits:decimals,
        maximumFractionDigits:decimals,
        useGrouping:true
    });
}
function rfkjTimeframeLabel(tf){return ({"1":"1M","3":"3M","5":"5M","15":"15M","30":"30M","60":"1H","4h":"4H","1D":"1D","1W":"1W"})[String(tf)]||String(tf);}
function rfkjApplySignal(el,value,valid=true){
    if(!el)return;
    const n=rfkjNormalizeSignal(value);
    /* Signal Matrix boxes must show the indicator name/percent only,
       never the raw numeric state (0 / 1 / -1). */
    Array.from(el.childNodes).forEach(node=>{
        if(node.nodeType===Node.TEXT_NODE)node.remove();
    });
    el.classList.remove("signal-plus","signal-minus","signal-zero","signal-invalid");
    if(!valid)el.classList.add("signal-invalid");
    else if(n>0)el.classList.add("signal-plus");
    else if(n<0)el.classList.add("signal-minus");
    else el.classList.add("signal-zero");
}
function rfkjNowText(){
    const d=new Date();
    const pad=n=>String(n).padStart(2,"0");
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
function rfkjFormatUpdated(value){
    const d=value?new Date(value):new Date();
    if(Number.isNaN(d.getTime()))return "";
    const pad=n=>String(n).padStart(2,"0");
    return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
function rfkjSetStatus(index,message,error=false){
    const el=rfkjGet("rfkjStatus",index);
    if(!el)return;
    el.textContent=message;
    el.style.color=error?"#fca5a5":"#cbd5e1";
}
function rfkjClearIssues(index){
    RFKJ_TIMEFRAMES.forEach(tf=>clearPageIssue(`rfkj:${index}:${tf}`));
}
function rfkjUpdateHeaderPrice(index){
    const el=document.getElementById(`rfkjHeaderPrice${index}`);
    const s=rfkjInstance(index);
    if(!el||!s)return;
    const coin=rfkjGet("rfkjCoin",index)?.value||"BTC";
    const priceEl=el.querySelector(".rfkj-header-price-value");
    const coinEl=el.querySelector(".rfkj-header-coin");
    const updatedEl=el.querySelector(".rfkj-header-updated");
    const dailyEl=el.querySelector(".rfkj-header-daily-change");
    if(coinEl)coinEl.textContent=coin;
    if(priceEl){
        const price=Number(s.latestPrice);
        const valid=Number.isFinite(price)&&price>0;
        priceEl.textContent=valid?rfkjFormatPrice(price, coin):"-";
        priceEl.className=`rfkj-header-price-value ${s.priceDirection>0?"price-up":s.priceDirection<0?"price-down":"price-flat"}`;
    }
    if(dailyEl){
        const daily=Number(s.latestDailyChangePercent);
        dailyEl.textContent=Number.isFinite(daily)?`${daily.toFixed(2)}%`:"--";
        dailyEl.classList.remove("daily-positive","daily-negative","daily-zero","daily-invalid");
        if(!Number.isFinite(daily))dailyEl.classList.add("daily-invalid");
        else if(daily>0)dailyEl.classList.add("daily-positive");
        else if(daily<0)dailyEl.classList.add("daily-negative");
        else dailyEl.classList.add("daily-zero");
    }
    if(updatedEl)updatedEl.textContent=s.latestUpdated?rfkjFormatUpdated(s.latestUpdated):"--:--:--";
}
function rfkjSetSelectionStatus(index, error=false){
    const el=rfkjGet("rfkjStatus",index);
    const s=rfkjInstance(index);
    if(!el||!s)return;
    rfkjUpdateHeaderPrice(index);
    const timeframes=s.selectedTimeframes.map(rfkjTimeframeLabel).join(", ")||"-";
    el.innerHTML=`<span class="rfkj-status-line rfkj-status-timeframe">${timeframes}</span>`;
    el.style.color=error?"#fca5a5":"#cbd5e1";
}
function rfkjFitSignalLabels(root=document){
    const codes=root.querySelectorAll?.('.rfkj-code')||[];
    codes.forEach(el=>{
        const value=el.closest('.rfkj-value');
        if(!value)return;
        const width=value.getBoundingClientRect().width;
        const compact=width>0&&width<92;
        const short=el.dataset.shortLabel||'';
        const full=el.dataset.fullLabel||el.textContent.trim();
        if(!el.dataset.fullLabel)el.dataset.fullLabel=full;
        if(!el.dataset.shortLabel){
            el.dataset.shortLabel=full==='Range Filter'?'RF':full==='Super Trend'?'ST':full==='Kijun 288'?'K288':full;
        }
        el.textContent=compact?el.dataset.shortLabel:el.dataset.fullLabel;
    });
}
let rfkjSignalLabelObserver;
function rfkjObserveSignalLabels(){
    if(typeof ResizeObserver==='undefined')return;
    if(!rfkjSignalLabelObserver)rfkjSignalLabelObserver=new ResizeObserver(entries=>{
        entries.forEach(entry=>rfkjFitSignalLabels(entry.target));
    });
    document.querySelectorAll('.rfkj-signals').forEach(el=>rfkjSignalLabelObserver.observe(el));
    rfkjFitSignalLabels(document);
}

function rfkjRenderSignalRows(index){
    const s=rfkjInstance(index);
    const host=document.getElementById(`rfkjSignalRows${index}`);
    if(!s||!host)return;
    host.innerHTML="";
    // Always display selected timeframes from lowest to highest, regardless of click order.
    const orderedTimeframes=s.selectedTimeframes.slice().sort((a,b)=>rfkjTimeframeRank(a)-rfkjTimeframeRank(b));
    s.selectedTimeframes=orderedTimeframes;
    orderedTimeframes.forEach(tf=>{
        const row=document.createElement("div");
        row.className="rfkj-signal-row";
        row.dataset.timeframe=tf;
        row.innerHTML=`
            <div class="rfkj-row-timeframe">${rfkjTimeframeLabel(tf)}</div>
            <div class="rfkj-signals">
                <div class="rfkj-signal"><div class="rfkj-value signal-zero" data-signal="RF"><span class="rfkj-code">Range Filter</span><span class="rfkj-percent" data-percent-for="RF">—</span></div></div>
                <div class="rfkj-signal"><div class="rfkj-value signal-zero" data-signal="ST"><span class="rfkj-code">Super Trend</span><span class="rfkj-percent" data-percent-for="ST">—</span></div></div>
                <div class="rfkj-signal"><div class="rfkj-value signal-zero" data-signal="KJ"><span class="rfkj-code">Kijun 288</span><span class="rfkj-percent" data-percent-for="KJ">—</span></div></div>
            </div>`;
        host.appendChild(row);
    });
    rfkjObserveSignalLabels();
    /* Restore cached Matrix values only while this Matrix is running.
       A stopped Matrix must remain inactive after refresh; its last values are
       not allowed to reappear until it is started again and fresh data arrives. */
    if(rfkjInstance(index)?.running)rfkjRestoreDisplayedSignals(index);
}
function rfkjSignalElement(index,timeframe,key){
    return document.querySelector(`#rfkjSignalRows${index} .rfkj-signal-row[data-timeframe="${CSS.escape(String(timeframe))}"] [data-signal="${key}"]`);
}
function rfkjFormatPercent(value){
    const n=Number(value);
    if(!Number.isFinite(n))return "—";
    return `${n>0?"+":""}${n.toFixed(2)}%`;
}
function rfkjSetSignalPercent(index,timeframe,key,value){
    const signal=rfkjSignalElement(index,timeframe,key);
    const el=signal?.querySelector(`[data-percent-for="${key}"]`);
    if(!el)return;
    el.classList.remove("percent-positive","percent-negative","percent-zero","percent-invalid");
    if(value===null||value===undefined||value===""){
        el.textContent="—";
        el.classList.add("percent-invalid");
        return;
    }
    el.textContent=rfkjFormatPercent(value);
    const n=Number(value);
    if(!Number.isFinite(n))el.classList.add("percent-invalid");
    else if(n>0)el.classList.add("percent-positive");
    else if(n<0)el.classList.add("percent-negative");
    else el.classList.add("percent-zero");
}
function rfkjTimeframeRank(tf){
    const order={"1":1,"3":3,"5":5,"15":15,"30":30,"60":60,"4h":240,"1D":1440,"1W":10080};
    return order[String(tf)]??Number.MAX_SAFE_INTEGER;
}
function rfkjLowestSelectedTimeframe(index){
    const s=rfkjInstance(index);
    if(!s||!s.selectedTimeframes?.length)return "1";
    return s.selectedTimeframes.slice().sort((a,b)=>rfkjTimeframeRank(a)-rfkjTimeframeRank(b))[0];
}
function rfkjReadLiveCache(){
    try{
        const raw=localStorage.getItem(RFKJ_LIVE_CACHE_KEY);
        const data=raw?JSON.parse(raw):{};
        return data&&typeof data==="object"?data:{};
    }catch(e){return {};}
}
function rfkjWriteLiveCache(index,payload){
    try{
        const data=rfkjReadLiveCache();
        data[String(index)]={...payload, savedAt:Date.now()};
        localStorage.setItem(RFKJ_LIVE_CACHE_KEY,JSON.stringify(data));
    }catch(e){}
}
function rfkjReadSignalDisplayCache(){
    try{
        const raw=localStorage.getItem(RFKJ_SIGNAL_CACHE_KEY);
        const data=raw?JSON.parse(raw):{};
        return data&&typeof data==="object"?data:{};
    }catch(e){return {};}
}
function rfkjWriteSignalDisplayCache(index,payload){
    try{
        const data=rfkjReadSignalDisplayCache();
        data[String(index)]={...payload,savedAt:Date.now()};
        localStorage.setItem(RFKJ_SIGNAL_CACHE_KEY,JSON.stringify(data));
    }catch(e){}
}
function rfkjGetSignalDisplayCache(index){
    const data=rfkjReadSignalDisplayCache();
    const cached=data[String(index)];
    return cached&&typeof cached==="object"?cached:null;
}
function rfkjSaveDisplayedSignals(index,timeframe,values){
    const coin=String(rfkjGet("rfkjCoin",index)?.value||"").trim().toUpperCase();
    if(!coin)return;
    const old=rfkjGetSignalDisplayCache(index);
    const signals=(old&&old.coin===coin&&old.signals&&typeof old.signals==="object")?old.signals:{};
    signals[String(timeframe)]={...values,savedAt:Date.now()};
    const state=rfkjInstance(index);
    rfkjWriteSignalDisplayCache(index,{
        coin,
        signals,
        latestPrice:state?.latestPrice??null,
        latestPriceRaw:state?.latestPriceRaw??null,
        latestUpdated:state?.latestUpdated??null,
        latestDailyChangePercent:state?.latestDailyChangePercent??null,
        priceDirection:state?.priceDirection??0
    });
}
function rfkjRestoreDisplayedSignals(index){
    const state=rfkjInstance(index);
    const coin=String(rfkjGet("rfkjCoin",index)?.value||"").trim().toUpperCase();
    const cached=rfkjGetSignalDisplayCache(index);
    if(!state||!cached||!coin||String(cached.coin||"").toUpperCase()!==coin)return;
    if(cached.signals&&typeof cached.signals==="object"){
        state.previousSignals={};
        Object.entries(cached.signals).forEach(([tf,v])=>{
            if(!RFKJ_TIMEFRAMES.includes(String(tf))||!v||typeof v!=="object")return;
            const signalTF=String(tf);
            rfkjApplySignal(rfkjSignalElement(index,signalTF,"RF"),v.RF, v.valid!==false);
            rfkjApplySignal(rfkjSignalElement(index,signalTF,"ST"),v.ST, v.valid!==false);
            rfkjApplySignal(rfkjSignalElement(index,signalTF,"KJ"),v.KJ, v.valid!==false);
            rfkjSetSignalPercent(index,signalTF,"RF",v.RFPercent);
            rfkjSetSignalPercent(index,signalTF,"ST",v.STPercent);
            rfkjSetSignalPercent(index,signalTF,"KJ",v.KJPercent);
            state.previousSignals[signalTF]={RF:rfkjNormalizeSignal(v.RF),ST:rfkjNormalizeSignal(v.ST),KJ:rfkjNormalizeSignal(v.KJ)};
        });
    }
    if(Number.isFinite(Number(cached.latestPrice))&&Number(cached.latestPrice)>0){
        state.latestPrice=Number(cached.latestPrice);
        state.latestPriceRaw=cached.latestPriceRaw??String(cached.latestPrice);
        state.latestUpdated=cached.latestUpdated||null;
        state.latestDailyChangePercent=Number.isFinite(Number(cached.latestDailyChangePercent))
            ?Number(cached.latestDailyChangePercent):null;
        state.priceDirection=Number(cached.priceDirection)||0;
        rfkjUpdateHeaderPrice(index);
        rfkjSetSelectionStatus(index);
    }
}
function rfkjGetLiveCache(index){
    const s=rfkjInstance(index);
    if(s?.cachedLive)return s.cachedLive;
    const data=rfkjReadLiveCache();
    const cached=data[String(index)];
    if(cached&&Number.isFinite(Number(cached.price))&&Number(cached.price)>0){
        if(s)s.cachedLive=cached;
        return cached;
    }
    return null;
}
function rfkjUpdateUltraMini(price,rf,kj,st,valid=false){
    const priceEl=document.getElementById("rfkjUltraPrice");
    const rfEl=document.getElementById("rfkjUltraRF");
    const stEl=document.getElementById("rfkjUltraST");
    const kjEl=document.getElementById("rfkjUltraKJ");
    if(!priceEl||!rfEl||!stEl||!kjEl)return;

    // rfkjUltraIndex is the ZERO-BASED position inside the candidate list,
    // not the real Matrix number. Always resolve the selected candidate first
    // and then use its actual `index` to read rfkjCoin1/rfkjCoin2/rfkjCoin3.
    const candidates=rfkjUltraCandidates();
    const candidateIndex=candidates.length?rfkjUltraIndex%candidates.length:0;
    const ultraState=candidates[candidateIndex] || null;
    const ultraMatrixIndex=ultraState?.index || 1;
    const ultraCoin=String(rfkjGet("rfkjCoin", ultraMatrixIndex)?.value || ultraState?.coin || "").trim().toUpperCase();

    let live={price,rf,kj,st};
    if(valid&&Number.isFinite(Number(price))&&Number(price)>0&&ultraState){
        ultraState.cachedLive={...live,savedAt:Date.now()};
        rfkjWriteLiveCache(ultraState.index,ultraState.cachedLive);
    }
    if(!valid&&ultraState){
        const cached=rfkjGetLiveCache(ultraState.index);
        if(cached){live=cached;valid=true;}
    }

    const safePrice=Number(live.price);
    const displayPrice=live.rawPrice??live.price??safePrice;
    // Never let a missing/incorrect asset fall back to BTC's integer formatter.
    // For a running Matrix, its real selected coin is mandatory here.
    priceEl.textContent=valid&&Number.isFinite(safePrice)&&safePrice>0
        ?rfkjFormatPrice(displayPrice, ultraCoin||"BTC")
        :"";
    const set=(el,value)=>{
        el.className="rfkj-ultra-arrow";
        const n=rfkjNormalizeSignal(value);
        el.textContent=n>0?"↑":n<0?"↓":"→";
        el.classList.add(n>0?"up":n<0?"down":"neutral");
    };
    set(rfEl,live.rf);set(stEl,live.st);set(kjEl,live.kj);
}
let rfkjUltraTimer=null;
let rfkjUltraIndex=0;
let rfkjUltraSignature="";
function rfkjUltraCandidates(){
    return rfkjInstances.filter(s=>{
        // Running is the priority flag. Never let a running Matrix fall through
        // to Crossover just because a legacy state is missing its selected-coin marker.
        return !!s.running;
    });
}
function rfkjApplyDailyTint(el,value){
    if(!el)return;
    const n=Number(value);
    el.style.background="";
    el.style.boxShadow="";
    el.classList.remove("daily-positive","daily-negative","daily-neutral");
    if(!Number.isFinite(n)||n===0)el.classList.add("daily-neutral");
    else if(n>0)el.classList.add("daily-positive");
    else el.classList.add("daily-negative");
}
// Crossover uses a continuous daily-change gradient: -3% red, 0% amber, +3% green.
// Signal Matrix and Global keep their existing color logic.
function rfkjApplyCrossoverDailyTint(el,value){
    if(!el)return;
    const n=Number(value);
    el.classList.remove("daily-positive","daily-negative","daily-neutral");
    if(!Number.isFinite(n)){
        el.classList.add("daily-neutral");
        el.style.background="";
        el.style.boxShadow="";
        return;
    }
    const v=Math.max(-3,Math.min(3,n));
    const red=[239,68,68], amber=[250,204,21], green=[34,197,94];
    const a=v<0?red:amber, b=v<0?amber:green, t=Math.abs(v)/3;
    const rgb=a.map((c,i)=>Math.round(c+(b[i]-c)*t));
    const color=`${rgb[0]},${rgb[1]},${rgb[2]}`;
    const strength=0.09+Math.min(1,Math.abs(v)/3)*0.13;
    el.style.background=`linear-gradient(135deg,rgba(${color},${strength}),rgba(${color},${strength*0.42}))`;
    el.style.boxShadow=`inset 0 0 0 1px rgba(${color},${0.10+Math.min(1,Math.abs(v)/3)*0.16})`;
}
function rfkjRenderUltraCompact(animate=true){
    const candidates=rfkjUltraCandidates();
    const signature=candidates.map(s=>s.index).join(",");
    if(signature!==rfkjUltraSignature){
        rfkjUltraSignature=signature;
        rfkjUltraIndex=0;
    }
    if(!candidates.length){
        // No Signal Matrix is running: do not resurrect Matrix #1's cached price
        // or arrows. Show the first running Crossover's live price instead.
        // This keeps the compact strip consistent with the native home widget.
        rfkjUltraIndex=0;
        const priceEl=document.getElementById("rfkjUltraPrice");
        const rfEl=document.getElementById("rfkjUltraRF");
        const stEl=document.getElementById("rfkjUltraST");
        const kjEl=document.getElementById("rfkjUltraKJ");
        let crossover=null;
        try{
            const saved=crossoverReadState();
            const list=Array.isArray(saved?.crossovers)?saved.crossovers:[];
            crossover=list.find(c=>c&&c.running&&String(c.activeCoin||c.coin||"").trim())||null;
        }catch(e){}
        if(crossover){
            const coin=String(crossover.activeCoin||crossover.coin||"").trim().toUpperCase();
            let price=Number(crossover.latestPrice);
            if(!Number.isFinite(price)||price<=0){
                const cached=typeof crossoverReadCachedPrice==="function"?crossoverReadCachedPrice(coin):null;
                price=Number(cached?.price);
            }
            if(priceEl){
                priceEl.textContent=Number.isFinite(price)&&price>0
                    ?crossoverPriceText(price,coin):"";
            }
            rfkjApplyCrossoverDailyTint(document.getElementById("rfkjUltraMini"), crossover.dailyChangePercent);
        }else if(priceEl){
            priceEl.textContent="";
            rfkjApplyDailyTint(document.getElementById("rfkjUltraMini"), null);
        }
        [rfEl,stEl,kjEl].forEach(el=>{
            if(!el)return;
            el.textContent="";
            el.className="rfkj-ultra-arrow";
        });
        return;
    }
    rfkjUltraIndex=rfkjUltraIndex%candidates.length;
    const s=candidates[rfkjUltraIndex];
    const valid=Number.isFinite(Number(s.latestPrice))&&Number(s.latestPrice)>0;
    const mini=document.getElementById("rfkjUltraMini");
    rfkjApplyDailyTint(mini,s.latestDailyChangePercent);
    if(animate&&mini){
        mini.classList.remove("rfkj-ultra-switch");
        void mini.offsetWidth;
        mini.classList.add("rfkj-ultra-switch");
    }
    rfkjUpdateUltraMini(s.latestPriceRaw ?? s.latestPrice,s.ultraRF,s.ultraKJ,s.ultraST,valid);
}
function rfkjStartUltraRotation(){
    if(rfkjUltraTimer)clearInterval(rfkjUltraTimer);
    rfkjUltraTimer=setInterval(()=>{
        const candidates=rfkjUltraCandidates();
        if(candidates.length<=1){rfkjUltraIndex=0;rfkjRenderUltraCompact(false);return;}
        rfkjUltraIndex=(rfkjUltraIndex+1)%candidates.length;
        rfkjRenderUltraCompact(true);
    },5000);
    rfkjRenderUltraCompact(false);
}
function rfkjReadSavedState(){
    try{
        const raw=localStorage.getItem(RFKJ_STATE_KEY);
        if(raw)return JSON.parse(raw);
    }catch(e){}
    if(window.AndroidNotifications&&typeof window.AndroidNotifications.getSignalMatrixStateJson==="function"){
        try{const raw=window.AndroidNotifications.getSignalMatrixStateJson();if(raw)return JSON.parse(raw);}catch(e){}
    }
    return null;
}
function rfkjSaveState(){
    const data={
        version:4,
        count:Number(document.getElementById("rfkjMatrixGrid")?.dataset.count||1),
        matrices:rfkjInstances.map(s=>({
            coin:rfkjGet("rfkjCoin",s.index)?.value||"",
            selectedCoin:rfkjGet("rfkjCoin",s.index)?.dataset.userSelected==="1",
            timeframes:s.selectedTimeframes.slice(),
            running:!!s.running
        }))
    };
    const raw=JSON.stringify(data);
    try{localStorage.setItem(RFKJ_STATE_KEY,raw);localStorage.setItem("rfkjMatrixState-v3",raw);}catch(e){}
    if(window.AndroidNotifications&&typeof window.AndroidNotifications.setSignalMatrixStateJson==="function"){
        try{window.AndroidNotifications.setSignalMatrixStateJson(raw);}catch(e){}
    }
}
async function rfkjSendNotification(index,title,message,type="RFKJ"){
    try{
        const coin=rfkjGet("rfkjCoin",index)?.value||"BTC";
        await sendDashboardNotification(title,message,getCryptoIconSrc(coin),type);
    }catch(e){console.warn("Signal Matrix notification failed",e);}
}
async function rfkjFetchOne(index,timeframe,notify=true){
    const state=rfkjInstance(index);
    if(!state||!state.running)return;
    const coin=rfkjGet("rfkjCoin",index)?.value||"BTC";
    try{
        const data=await rfkjFetchWithFailover(`coin=${encodeURIComponent(coin)}&timeframe=${encodeURIComponent(timeframe)}&t=${Date.now()}`);
        const rf=rfkjNormalizeSignal(data.RF??data.rf??data.rangeFilter??data.range_filter??0);
        const kj=rfkjNormalizeSignal(data.KJ??data.kj??data.kijunsen??0);
        const st=rfkjNormalizeSignal(data.ST??data.st??data.superTrend??data.supertrend??0);
        const rfPercent=data.RFPercent??data.rfPercent??data.RF_PERCENT??data.rf_percent;
        const kjPercent=data.KJPercent??data.kjPercent??data.KJ_PERCENT??data.kj_percent;
        const stPercent=data.STPercent??data.stPercent??data.ST_PERCENT??data.st_percent;
        state.latestTimeframe=timeframe;
        clearPageIssue(`rfkj:${index}:${timeframe}`);
        rfkjApplySignal(rfkjSignalElement(index,timeframe,"RF"),rf);
        rfkjApplySignal(rfkjSignalElement(index,timeframe,"ST"),st);
        rfkjApplySignal(rfkjSignalElement(index,timeframe,"KJ"),kj);
        rfkjSetSignalPercent(index,timeframe,"RF",rfPercent);
        rfkjSetSignalPercent(index,timeframe,"ST",stPercent);
        rfkjSetSignalPercent(index,timeframe,"KJ",kjPercent);
        // Register the freshly received Matrix data immediately. Do not wait
        // for the other selected timeframes to finish their requests.
        rfkjSaveDisplayedSignals(index,timeframe,{
            RF:rf, ST:st, KJ:kj,
            RFPercent:rfPercent, STPercent:stPercent, KJPercent:kjPercent,
            valid:true
        });
        // Persist successful worker data immediately after the blocks are updated.
        // The next timeframe request must never delay this registration.
        rfkjSaveState();
        syncNativeWidgetSnapshot();
        let dailyChangePercent=Number(data.dailyChangePercent);
        if(!Number.isFinite(dailyChangePercent)){
            try{
                if (!state.cycleDailyChangePromise) state.cycleDailyChangePromise = crossoverFetchDailyPercent(coin).catch(() => null);
                dailyChangePercent = await state.cycleDailyChangePromise;
            }catch(e){}
        }
        state.latestDailyChangePercent=Number.isFinite(Number(dailyChangePercent))?Number(dailyChangePercent):null;
        let price=Number(data.price);
        let priceRaw=(data.price===undefined||data.price===null)?null:String(data.price).trim();
        try{
            if (!state.cyclePricePromise) state.cyclePricePromise = getUnifiedCryptoPrice(coin).catch(error => { console.warn(`Unified crypto price failed for ${coin}:`, error); return null; });
            const unifiedPrice=await state.cyclePricePromise;
            if(unifiedPrice && Number.isFinite(Number(unifiedPrice.price))&&Number(unifiedPrice.price)>0){
                price=Number(unifiedPrice.price);
                priceRaw=String(unifiedPrice.price);
            }
        }catch(priceError){
            console.warn(`Unified crypto price failed for ${coin}, keeping Signal Matrix worker price:`,priceError);
        }
        const previousPrice=Number(state.latestPrice);
        state.priceDirection=(Number.isFinite(previousPrice)&&Number.isFinite(price)&&price>0)
            ? (price>previousPrice?1:price<previousPrice?-1:0)
            : 0;
        state.latestPriceRaw=priceRaw;
        state.latestPrice=Number.isFinite(price)&&price>0?price:null;
        state.latestUpdated=data.updated||new Date().toISOString();
        rfkjSaveDisplayedSignals(index,timeframe,{
            RF:rf, ST:st, KJ:kj,
            RFPercent:rfPercent, STPercent:stPercent, KJPercent:kjPercent,
            valid:true
        });
        /* Update cached header values after the final unified price is known. */
        try{
            const cachedDisplay=rfkjGetSignalDisplayCache(index);
            if(cachedDisplay&&cachedDisplay.coin===String(coin).trim().toUpperCase()){
                cachedDisplay.latestPrice=state.latestPrice;
                cachedDisplay.latestPriceRaw=state.latestPriceRaw;
                cachedDisplay.latestUpdated=state.latestUpdated;
                cachedDisplay.latestDailyChangePercent=state.latestDailyChangePercent;
                cachedDisplay.priceDirection=state.priceDirection;
                rfkjWriteSignalDisplayCache(index,cachedDisplay);
            }
        }catch(e){}
        rfkjSetSelectionStatus(index);
        rfkjUpdateHeaderPrice(index);
        /* Keep only successful values as the live cache. Never replace a good
           Signal Matrix number with a dash during a transient worker failure. */
        if(String(timeframe)===String(rfkjLowestSelectedTimeframe(index)) && Number.isFinite(price) && price>0){
            state.ultraRF=rf; state.ultraST=st; state.ultraKJ=kj;
            state.cachedLive={price,rawPrice:state.latestPriceRaw,rf,kj,st,savedAt:Date.now()};
            rfkjWriteLiveCache(index,state.cachedLive);
            rfkjRenderUltraCompact(false);
            syncNativeWidgetSnapshot();
        } else {
            syncNativeWidgetSnapshot();
        }
        const old=state.previousSignals[timeframe];
        if(notify&&old){
            const changes=[];
            if(old.RF!==rf)changes.push({name:"Range Filter",value:rf});
            if(old.ST!==st)changes.push({name:"Super Trend",value:st});
            if(old.KJ!==kj)changes.push({name:"Kijun 288",value:kj});
            for(const change of changes){
                const colorType=change.value>0?"RFKJ_WHITE":"RFKJ_BLUE";
                const colorText=change.value>0?"WHITE":"BLUE";
                await rfkjSendNotification(index,`🔔 ${coin} • ${rfkjTimeframeLabel(timeframe)} • #${index}`,`${change.name} → ${colorText}`,colorType);
            }
        }
        state.previousSignals[timeframe]={RF:rf,KJ:kj,ST:st};
    }catch(error){
        console.warn(`Signal Matrix ${index}/${timeframe} failed`,error);
        setPageIssue(`rfkj:${index}:${timeframe}`,`Worker unavailable • ${error.message||"request failed"}`,"error");
        if(timeframe===state.latestTimeframe)rfkjSetStatus(index,`Worker unavailable • ${error.message||"request failed"}`,true);
    }
}
async function rfkjFetchSignal(index){
    const state=rfkjInstance(index);
    if(!state||!state.running||state.cycleInProgress)return;
    state.cycleInProgress=true;
    state.cyclePricePromise=null;
    state.cycleDailyChangePromise=null;
    try {
        const timeframes=state.selectedTimeframes.slice();
        await Promise.all(timeframes.map(tf=>rfkjFetchOne(index,tf,true)));
        rfkjSaveState();
    } finally {
        state.cyclePricePromise=null;
        state.cycleDailyChangePromise=null;
        state.cycleInProgress=false;
    }
}
function rfkjScheduleNext(index){
    const state=rfkjInstance(index);
    if(!state||!state.running)return;
    state.timer=setTimeout(async()=>{
        try { await rfkjFetchSignal(index); }
        catch (error) { console.warn(`Signal Matrix ${index} cycle failed unexpectedly:`, error); }
        finally { rfkjScheduleNext(index); }
    },RFKJ_POLL_MS);
}


function rfkjStart(index,sendStart=true){
    const s=rfkjInstance(index);
    if(!s||s.running)return;
    const coinInput=rfkjGet("rfkjCoin",index);
    // Starting Matrix is an explicit request to use Matrix, even if an older
    // saved selection left the coin input blank. Match the existing BTC default.
    if(coinInput&&!String(coinInput.value||"").trim()){
        coinInput.value="BTC";
        coinInput.dataset.userSelected="1";
    }
    s.running=true;
    const b=rfkjGet("rfkjStartStop",index);
    if(b){b.textContent="■ Stop Signal";b.classList.add("running");}
    if(sendStart){
        const coin=rfkjGet("rfkjCoin",index)?.value||"BTC";
        rfkjSendNotification(index,`🔔 ${coin} • #${index}`,`Signal started ↑`);
    }
    // Persist the new running state and switch Ultra Compact before starting any
    // network request. A slow/failing worker must never leave the old Crossover
    // price visible while Matrix #1 is already running.
    rfkjSaveState();
    rfkjRenderUltraCompact(false);
    syncNativeWidgetSnapshot();
    void (async () => {
        try { await rfkjFetchSignal(index); }
        catch (error) { console.warn(`Signal Matrix ${index} initial cycle failed:`, error); }
        finally { rfkjScheduleNext(index); }
    })();
    rfkjSaveState();
    rfkjSyncCountLineStates();
}
function rfkjStop(index,sendStop=true){
    const s=rfkjInstance(index);
    if(!s)return;
    s.running=false;
    if(s.timer){clearTimeout(s.timer);s.timer=null;}
    const b=rfkjGet("rfkjStartStop",index);
    if(b){b.textContent="▶ Start Signal";b.classList.remove("running");}
    s.selectedTimeframes.forEach(tf=>{
        rfkjApplySignal(rfkjSignalElement(index,tf,"RF"),0,false);
        rfkjApplySignal(rfkjSignalElement(index,tf,"ST"),0,false);
        rfkjApplySignal(rfkjSignalElement(index,tf,"KJ"),0,false);
        // While stopped, hide the last percentage value and show only a neutral dash.
        rfkjSetSignalPercent(index,tf,"RF",null);
        rfkjSetSignalPercent(index,tf,"ST",null);
        rfkjSetSignalPercent(index,tf,"KJ",null);
    });
    s.previousSignals={};
    s.priceDirection=0;
    s.latestDailyChangePercent=null;
    rfkjClearIssues(index);
    rfkjSetStatus(index,"Signal stopped.");
    rfkjUpdateHeaderPrice(index);
    if(index===1)rfkjUpdateUltraMini(null,0,0,0,false);
    if(sendStop){
        const coin=rfkjGet("rfkjCoin",index)?.value||"BTC";
        rfkjSendNotification(index,`Signal Matrix #${index} • ${coin}`,`${coin} • Signal STOPPED`);
    }
    rfkjSaveState();
    rfkjSyncCountLineStates();
}

function rfkjSelectTimeframe(index,button){
    const s=rfkjInstance(index);
    const card=button.closest(".rfkj-instance");
    const multi=card?.dataset.timeframeMode==="multi-timeframe";
    const tf=button.dataset.rfkjTime||"1";
    if(!multi){
        card.querySelectorAll(".rfkj-timeframe").forEach(x=>x.classList.remove("active"));
        button.classList.add("active");
        s.selectedTimeframes=[tf];
    }else{
        if(button.classList.contains("active")){
            if(s.selectedTimeframes.length===1)return;
            button.classList.remove("active");
            s.selectedTimeframes=s.selectedTimeframes.filter(x=>x!==tf);
        }else{
            button.classList.add("active");
            s.selectedTimeframes.push(tf);
        }
    }
    // Keep the internal selection ordered as well, so saved state and every refresh use low → high.
    s.selectedTimeframes.sort((a,b)=>rfkjTimeframeRank(a)-rfkjTimeframeRank(b));
    s.previousSignals={};
    rfkjRenderSignalRows(index);
    rfkjSetSelectionStatus(index);
    if(s.running)rfkjFetchSignal(index);
    rfkjSaveState();
}
function rfkjInitInstance(index){
    const s=rfkjInstance(index);
    const coin=rfkjGet("rfkjCoin",index);
    const btn=rfkjGet("rfkjStartStop",index);
    if(!s||!coin||!btn)return;
    document.querySelectorAll(`.rfkj-instance[data-rfkj-index="${index}"] .rfkj-timeframe`).forEach(b=>b.addEventListener("click",()=>rfkjSelectTimeframe(index,b)));
    coin.addEventListener("change",()=>{
        if(index===1){
            if(coin.value)coin.dataset.userSelected="1";
            else delete coin.dataset.userSelected;
        }
        s.previousSignals={};
        rfkjClearIssues(index);
        rfkjRenderSignalRows(index);
        s.latestPrice=null;
        s.latestUpdated=null;
        s.latestDailyChangePercent=null;
        s.priceDirection=0;
        rfkjSetSelectionStatus(index);
        rfkjUpdateHeaderPrice(index);
        rfkjRenderUltraCompact(false);
        if(s.running)rfkjFetchSignal(index);
        rfkjSaveState();
    });
    btn.addEventListener("click",()=>s.running?rfkjStop(index):rfkjStart(index));
}
function rfkjSyncCountLineStates(){
    const grid=document.getElementById("rfkjMatrixGrid");
    const count=Math.max(1,Math.min(RFKJ_SLOT_COUNT,Number(grid?.dataset.count||1)));
    document.querySelectorAll(".rfkj-slot-btn").forEach(btn=>{
        const index=Number(btn.dataset.rfkjCount), state=rfkjInstance(index);
        btn.classList.toggle("status-running",!!state?.running);
        btn.classList.toggle("status-stopped",!state?.running);
        btn.classList.toggle("active",index<=count);
    });
}
function rfkjSetVisibleCount(count,restore=false){
    const n=Math.max(1,Math.min(4,Number(count)||1));
    const grid=document.getElementById("rfkjMatrixGrid");
    if(!grid)return;
    grid.dataset.count=String(n);
    document.querySelectorAll(".rfkj-instance").forEach(card=>{
        const i=Number(card.dataset.rfkjIndex);
        card.style.display=i<=n?"":"none";
    });
    document.querySelectorAll(".rfkj-slot-btn").forEach(btn=>btn.classList.toggle("active",Number(btn.dataset.rfkjCount)<=n));
    if(!restore)rfkjSaveState();
    rfkjSyncCountLineStates();
}
function rfkjRestoreState(){
    const saved=rfkjReadSavedState();
    const liveCache=rfkjReadLiveCache();
    Object.keys(liveCache).forEach(key=>{
        const s=rfkjInstance(Number(key));
        const cached=liveCache[key];
        if(s&&cached&&Number.isFinite(Number(cached.price))&&Number(cached.price)>0)s.cachedLive=cached;
    });
    if(!saved)return false;
    const count=Math.max(1,Math.min(3,Number(saved.count)||1));
    rfkjSetVisibleCount(count,true);
    (saved.matrices||[]).slice(0,3).forEach((m,i)=>{
        const idx=i+1,s=rfkjInstance(idx),coin=rfkjGet("rfkjCoin",idx),card=document.querySelector(`.rfkj-instance[data-rfkj-index="${idx}"]`);
        if(!s||!coin||!card)return;
        // A persisted coin is a valid selection for every Matrix slot, including BTC.
        // The running flag controls whether it is displayed; do not accidentally
        // hide a running first Matrix just because an older state lacks selectedCoin.
        const isSelected = m.selectedCoin===true || !!String(m.coin||"").trim() || !!m.running;
        coin.value=isSelected?(m.coin||"BTC"):"";
        if(isSelected)coin.dataset.userSelected="1";
        else delete coin.dataset.userSelected;
        const multi=card.dataset.timeframeMode==="multi-timeframe";
        let tfs=Array.isArray(m.timeframes)&&m.timeframes.length?m.timeframes.map(String):["1"];
        if(!multi)tfs=[tfs[0]||"1"];
        s.selectedTimeframes=tfs.filter(x=>RFKJ_TIMEFRAMES.includes(x));
        if(!s.selectedTimeframes.length)s.selectedTimeframes=["1"];
        card.querySelectorAll(".rfkj-timeframe").forEach(b=>b.classList.toggle("active",s.selectedTimeframes.includes(b.dataset.rfkjTime)));
        rfkjRenderSignalRows(idx);
        /* Only a Matrix saved in RUNNING state may restore its cached signal values.
           STOPPED state must come back with inactive/empty Signal Boxes. */
        if(m.running)rfkjRestoreDisplayedSignals(idx);
        rfkjSetSelectionStatus(idx);
        const custom=card.querySelector(".rfkj-compact-coin-picker");
        if(custom&&typeof custom._syncVisual==="function")custom._syncVisual();
        if(m.running)rfkjStart(idx,false);
    });
    return true;
}
function initRFKJSignalMonitor(){
    for(let i=1;i<=RFKJ_SLOT_COUNT;i++){rfkjInitInstance(i);rfkjRenderSignalRows(i);}

    document.querySelectorAll(".rfkj-slot-btn").forEach(btn=>btn.addEventListener("click",()=>rfkjSetVisibleCount(btn.dataset.rfkjCount)));
    const restored=rfkjRestoreState();
    if(!restored){
        rfkjSetVisibleCount(1,true);
        for(let i=1;i<=RFKJ_SLOT_COUNT;i++)rfkjSetSelectionStatus(i);
    }
}
initRFKJSignalMonitor();

/* =====================================================
   CROSSOVER MONITOR
   Uses the existing Signal Matrix worker for live USD price.
===================================================== */
const CROSSOVER_WORKER_URL = "https://signalmatrix.rxw1.workers.dev/";
/* Cloudflare Crossover monitor: independent 15-second server-side checking. */
const CROSSOVER_EMAIL_WORKER_URL = "https://crossoverworker.rxw1.workers.dev/";
/* The email Worker is intentionally NOT read during startup. It is contacted only when M is toggled. */
const CROSSOVER_POLL_MS = 5000;
const CROSSOVER_COUNT = 4;
const CROSSOVER_STATE_KEY = "crossoverState-v2";
const CROSSOVER_SELECTION_KEY_PREFIX = "crossoverSelectedCoin-";
const CROSSOVER_LAST_CROSS_KEY_PREFIX = "crossoverLastCross-";
function crossoverSaveLastCross(index, timestamp, direction){
    try{
        const key=`${CROSSOVER_LAST_CROSS_KEY_PREFIX}${index}`;
        if(timestamp) localStorage.setItem(key,JSON.stringify({timestamp,direction:Number(direction)||0}));
        else localStorage.removeItem(key);
    }catch(e){}
}
function crossoverReadLastCross(index){
    try{
        const raw=localStorage.getItem(`${CROSSOVER_LAST_CROSS_KEY_PREFIX}${index}`);
        if(!raw)return null;
        const saved=JSON.parse(raw);
        return saved&&saved.timestamp&&Number.isFinite(new Date(saved.timestamp).getTime())?saved:null;
    }catch(e){return null;}
}
const CROSSOVER_COINS = ["BTC","BNB","ETH","DOGE","ADA","SOL","GRAM","AVAX","POL","TRX","USD"];
const CROSSOVER_COMMODITY_ASSETS = {
    GOLD:{label:"GOLD", apiSymbol:"XAUUSD", iconClass:"gold", iconText:"Au"},
    OIL:{label:"OIL", apiSymbol:"USOIL", iconClass:"oil", iconText:"WTI"}
};
function crossoverIsSpecialAsset(value){
    const v=String(value||"").toUpperCase();
    return v==="GOLD"||v==="OIL"||v.startsWith("TSE|");
}
function crossoverIsValidAsset(value){
    const v=String(value||"").trim().toUpperCase();
    return CROSSOVER_COINS.includes(v)||crossoverIsSpecialAsset(v);
}
function crossoverSelectionKey(index){
    return `${CROSSOVER_SELECTION_KEY_PREFIX}${index}`;
}
function crossoverReadPersistedSelection(index){
    try{
        const value=String(localStorage.getItem(crossoverSelectionKey(index))||"").trim().toUpperCase();
        return crossoverIsValidAsset(value)?value:"";
    }catch(e){return "";}
}
function crossoverPersistSelection(index,value){
    const v=String(value||"").trim().toUpperCase();
    if(!crossoverIsValidAsset(v))return false;
    try{
        localStorage.setItem(crossoverSelectionKey(index),v);
        return true;
    }catch(e){return false;}
}
function crossoverAssetSymbol(value){
    const v=String(value||"").toUpperCase();
    if(v==="GOLD")return "Gold";
    if(v==="OIL")return "Crude Oil";
    if(v.startsWith("TSE|"))return crossoverTseSymbol(v)||"TSE";
    return v;
}
function crossoverTseSymbol(value){
    const v=String(value||"").toUpperCase();
    if(!v.startsWith("TSE|"))return "";
    const symbol=v.slice(4);
    const slotMatch=symbol.match(/^SLOT([1-4])$/);
    if(slotMatch){
        const instrument=globalPulseTsetmcGetSlots()[Number(slotMatch[1])-1];
        return String(instrument?.symbol||"");
    }
    return symbol;
}
function crossoverAssetIconHTML(value){
    const v=String(value||"").toUpperCase();
    if(v==="USD")return '<span class="crossover-usd-icon">$</span>';
    if(CROSSOVER_COMMODITY_ASSETS[v]){
        const a=CROSSOVER_COMMODITY_ASSETS[v];
        return `<span class="crossover-special-icon ${a.iconClass}">${a.iconText}</span>`;
    }
    if(v.startsWith("TSE|")){
        const symbol=crossoverTseSymbol(v);
        const text=String(symbol||"TSE").slice(0,2);
        return `<span class="crossover-special-icon tse">${text}</span>`;
    }
    return `<img src="${getCryptoIconSrc(v)}" alt="${v}">`;
}
function crossoverAssetLabel(value){
    const v=String(value||"").toUpperCase();
    if(v==="GOLD")return "GOLD";
    if(v==="OIL")return "OIL";
    if(v.startsWith("TSE|"))return crossoverTseSymbol(v)||"TSE";
    return v;
}
function crossoverNotificationAssetLabel(value){
    const v=String(value||"").toUpperCase();
    if(v.startsWith("TSE|")){
        const symbol=crossoverTseSymbol(v);
        return symbol ? `${symbol} / IR` : "TSE / IR";
    }
    return v;
}
function crossoverIsTseAsset(value){
    return String(value||"").toUpperCase().startsWith("TSE|");
}
function crossoverPriceText(value,asset){
    const n=Number(value);
    if(!Number.isFinite(n)||n<=0)return "-";
    if(crossoverIsTseAsset(asset))return Math.round(n).toLocaleString("en-US");
    return crossoverFormatPrice(n, asset);
}
function crossoverAssetOptions(){
    /*
     * IMPORTANT: the selected asset of every Crossover slot is persisted
     * independently.  Build the option list from that persisted state first,
     * then add live/discovered assets.  This means GOLD, OIL and TSE assets
     * are treated exactly like crypto assets during restore; they are never
     * lost just because their live source/cache is temporarily unavailable.
     */
    const out=[];
    const add=value=>{
        let v=String(value||"").trim().toUpperCase();
        if(!v)return;
        const slotRef=v.match(/^TSE\|SLOT([1-4])$/);
        if(slotRef&&Number(slotRef[1])>globalPulseTsetmcGetSlots().length)return;
        if(v.startsWith("TSE|")&&!/^TSE\|SLOT[1-4]$/.test(v)){
            const symbol=v.slice(4);
            const slots=globalPulseTsetmcGetSlots();
            const slotIndex=slots.findIndex(item=>item&&String(item.symbol||"").trim().toUpperCase()===symbol);
            if(slotIndex>=0)v=`TSE|SLOT${slotIndex+1}`;
        }
        if(CROSSOVER_COINS.includes(v)||crossoverIsSpecialAsset(v))out.push(v);
    };

    const matrixCoinSelect=document.querySelector(".rfkj-instance-coin");
    if(matrixCoinSelect){
        Array.from(matrixCoinSelect.options).forEach(o=>add(o.value));
    }else{
        CROSSOVER_COINS.forEach(add);
    }

    ["USD","GOLD","OIL"].forEach(add);

    /* Persisted slot selections are ALWAYS added, including TSE symbols. */
    for(let i=1;i<=CROSSOVER_COUNT;i++){
        try{
            const saved=String(localStorage.getItem(crossoverSelectionKey(i))||"").trim().toUpperCase();
            add(saved);
        }catch(e){}
    }

    /* Also recover selections from the saved crossover object itself. */
    try{
        const raw=localStorage.getItem(CROSSOVER_STATE_KEY);
        const state=raw?JSON.parse(raw):null;
        (state?.crossovers||[]).slice(0,CROSSOVER_COUNT).forEach(m=>{
            add(m?.activeCoin);
            add(m?.selectedCoin);
            add(m?.coin);
        });
    }catch(e){}

    /* One Crossover TSE asset is exposed for every active GLOBAL slot; its label follows the slot configuration. */
    try{
        globalPulseTsetmcGetSlots().forEach((item,index)=>add(`TSE|SLOT${index+1}`));
    }catch(e){}

    return [...new Set(out)];
}
async function crossoverFetchSpecialPrice(asset){
    const v=String(asset||"").toUpperCase();
    if(v==="GOLD"||v==="OIL"){
        const symbol=CROSSOVER_COMMODITY_ASSETS[v].apiSymbol;
        try {
            const data=await fetchGlobalPulseJson(GLOBAL_PULSE_COMMODITY_BATCH_URL,payload=>Number(payload?.[symbol]?.mid??payload?.symbols?.[symbol]?.mid)>0);
            const row=data?.[symbol]||data?.symbols?.[symbol];
            const price=Number(row?.mid);
            if(Number.isFinite(price)&&price>0)return {price,updated:new Date().toISOString()};
        } catch (error) { console.warn(`${v} aggregate price unavailable; trying OHLC fallback`, error); }
        const historyUrl=v==="GOLD"?GLOBAL_PULSE_GOLD_OHLC_URL:GLOBAL_PULSE_OIL_OHLC_URL;
        const history=await fetchGlobalPulseJson(historyUrl,payload=>globalPulseCommodityBars(payload).length>0);
        const bars=globalPulseCommodityBars(history);
        const price=Number(bars[bars.length-1]?.close);
        if(!Number.isFinite(price)||price<=0)throw new Error(`${v} price unavailable`);
        return {price,updated:new Date().toISOString()};
    }
    if(v.startsWith("TSE|")){
        const symbol=crossoverTseSymbol(v);
        const instrument=globalPulseTsetmcFindInstrument(symbol);
        if(!instrument)throw new Error(symbol?`TSETMC ${symbol} is not configured in GLOBAL`:"Select a TSETMC symbol in GLOBAL first");
        const quote=await globalPulseTsetmcFetchQuote(instrument);
        return {price:quote.lastPrice,dailyChangePercent:quote.lastPricePercent,updated:new Date(quote.updatedAt||Date.now()).toISOString()};
    }
    return null;
}
function crossoverNotifyAssetIcon(value){
    const v=String(value||"").toUpperCase();
    if(v==="GOLD")return "./icon-192.png";
    if(v==="OIL")return "./icon-192.png";
    if(v.startsWith("TSE|"))return "./icon-192.png";
    return getCryptoIconSrc(v);
}
const crossoverInstances = Array.from({length:CROSSOVER_COUNT},(_,i)=>({
    index:i+1, coin:"BTC", activeCoin:null, target:null, activeTarget:null, running:false, timer:null,
    notificationEnabled:false, emailEnabled:false, percentageAlertTimes:Object.create(null),
    requestInProgress:false, wasOffline:false, previousPrice:null, latestPrice:null,
    latestUpdated:null, lastCrossAt:null, lastCrossDirection:0, priceDirection:0, dailyChangePercent:null
}));
function crossoverGet(id,index){return document.getElementById(`${id}${index}`);}
function crossoverInstance(index){return crossoverInstances[index-1];}
function crossoverClientId(){
    /* Crossover state is shared between devices through the default remote profile.
       The latest device that opens the page reads/writes this same profile. */
    return "default";
}
function crossoverNotificationPrefsKey(){return "crossoverNotificationPrefs-v1";}
function crossoverReadNotificationPrefs(){
    try{
        const raw=localStorage.getItem(crossoverNotificationPrefsKey());
        const data=raw?JSON.parse(raw):{};
        return data&&typeof data==="object"?data:{};
    }catch(e){return {};}
}
function crossoverSaveNotificationPrefs(){
    try{
        const data={};
        crossoverInstances.forEach(s=>{data[String(s.index)]=!!s.notificationEnabled;});
        localStorage.setItem(crossoverNotificationPrefsKey(),JSON.stringify(data));
    }catch(e){}
}
function crossoverLoadNotificationPrefs(){
    const data=crossoverReadNotificationPrefs();
    crossoverInstances.forEach(s=>{s.notificationEnabled=data[String(s.index)]===true;});
}
function crossoverIsCryptoEmailAsset(value){
    const v=String(value||"").trim().toUpperCase();
    /* The Crossover Email Worker supports crypto assets only. USD, GOLD, OIL
       and TSE symbols must never be registered with that Worker. */
    return ["BTC","BNB","ETH","DOGE","ADA","SOL","GRAM","AVAX","POL","TRX"].includes(v);
}
function crossoverMailPrefsKey(){return "crossoverEmailPrefs-v2";}
function crossoverReadMailPrefs(){
    try{
        const raw=localStorage.getItem(crossoverMailPrefsKey());
        const data=raw?JSON.parse(raw):{};
        return data&&typeof data==="object"?data:{};
    }catch(e){return {};}
}
function crossoverSaveMailPrefs(){
    try{
        const data={version:3,slots:{}};
        crossoverInstances.forEach(s=>{
            data.slots[String(s.index)]={
                enabled:!!s.emailEnabled,
                running:!!s.running,
                coin:String(s.activeCoin||s.coin||"").toUpperCase(),
                target:Number(s.activeTarget)
            };
        });
        localStorage.setItem(crossoverMailPrefsKey(),JSON.stringify(data));
    }catch(e){}
}
function crossoverLoadMailPrefs(){
    const data=crossoverReadMailPrefs();
    const slots=(data&&data.slots&&typeof data.slots==="object")?data.slots:data;
    crossoverInstances.forEach(s=>{
        const saved=slots?.[String(s.index)];
        s.emailEnabled= saved&&typeof saved==="object"
            ? saved.enabled===true
            : saved===true;
    });
}
function crossoverLoadLocalAlertHistory(){
    try{
        const raw=localStorage.getItem(CROSSOVER_STATE_KEY);
        const data=raw?JSON.parse(raw):null;
        (data?.crossovers||[]).slice(0,CROSSOVER_COUNT).forEach((m,i)=>{
            const times=m?.percentageAlertTimes;
            if(times&&typeof times==="object")crossoverInstance(i+1).percentageAlertTimes={...times};
        });
    }catch(e){}
}
function crossoverUpdateNotificationButton(index){
    const s=crossoverInstance(index),btn=crossoverGet("crossoverNotify",index);
    if(!s||!btn)return;
    const enabled=!!s.notificationEnabled;
    btn.disabled=!s.running;
    btn.classList.toggle("enabled",enabled&&s.running);
    btn.textContent=enabled?"🔔":"🔕";
    btn.title=s.running?(enabled?"Percentage notifications are on":"Percentage notifications are off"):"Start Crossover first";
    btn.setAttribute("aria-pressed",enabled?"true":"false");
}
function crossoverUpdateMailButton(index){
    const s=crossoverInstance(index),btn=crossoverGet("crossoverMail",index);
    if(!s||!btn)return;
    const enabled=!!s.emailEnabled;
    const configured=Number.isFinite(Number(s.activeTarget))&&Number(s.activeTarget)>0;
    const cryptoAsset=crossoverIsCryptoEmailAsset(s.activeCoin||s.coin);
    const available=!!s.running&&configured&&cryptoAsset;
    btn.disabled=!available;
    btn.classList.toggle("enabled",enabled&&available);
    btn.textContent="M";
    btn.title=available?(enabled?"Email alerts on":"Email alerts off"):"Start Crossover first";
    btn.setAttribute("aria-pressed",enabled?"true":"false");
    btn.setAttribute("aria-label",enabled?"Email alerts on":"Email alerts off");
}
let crossoverEmailSyncInProgress = null;
async function crossoverSyncEmailWorker(){
    if (crossoverEmailSyncInProgress) return crossoverEmailSyncInProgress;
    crossoverEmailSyncInProgress = crossoverSyncEmailWorkerInternal();
    try { return await crossoverEmailSyncInProgress; }
    finally { crossoverEmailSyncInProgress = null; }
}
async function crossoverSyncEmailWorkerInternal(){
    /*
     * The Email Worker is a CRYPTO-only remote monitor.  UI Crossover slots
     * may also monitor USD/GOLD/OIL/TSE, but those assets are deliberately
     * excluded here because this Worker has no corresponding crypto feed.
     *
     * The payload contract remains: clientId, active, count, items.
     */
    /* Email monitoring is independent of how many Crossover cards are
       currently visible.  All four slots must be sent to the Worker. */
    const count=CROSSOVER_COUNT;
    const items=crossoverInstances.map(s=>({
        index:s.index,
        coin:String(s.running?(s.activeCoin||s.coin):s.coin||"BTC").toUpperCase(),
        target:Number(s.running?s.activeTarget:s.target),
        running:!!s.emailEnabled&&!!s.running
    })).filter(x=>x.running&&crossoverIsCryptoEmailAsset(x.coin)&&Number.isFinite(x.target)&&x.target>0);
    const active=items.length>0;
    const payload={clientId:crossoverClientId(),active,count,items};
    try{
        const response=await fetchWithTimeout(CROSSOVER_EMAIL_WORKER_URL,{
            method:"POST",
            cache:"no-store",
            headers:{"Content-Type":"application/json","Accept":"application/json"},
            body:JSON.stringify(payload),
            keepalive:true
        },8000);
        const text=await response.text().catch(()=>"");
        if(!response.ok)throw new Error(`Email Worker HTTP ${response.status}${text?` • ${text.slice(0,160)}`:""}`);
        clearPageIssue("crossover-email-worker");
        console.log("Crossover Email Worker command sent",payload);
        return true;
    }catch(e){
        setPageIssue("crossover-email-worker",e?.message||"Email Worker unavailable","warn");
        console.warn("Crossover Email Worker command failed:",e,payload);
        return false;
    }
}
async function crossoverToggleEmail(index){
    const s=crossoverInstance(index),btn=crossoverGet("crossoverMail",index);
    if(!s||!btn)return;
    if(!s.running)return;
    if(!crossoverIsCryptoEmailAsset(s.activeCoin||s.coin)){
        s.emailEnabled=false;
        crossoverUpdateMailButton(index);
        crossoverSaveMailPrefs();
        crossoverSaveState();
        return;
    }
    const target=Number(s.activeTarget);
    if(!Number.isFinite(target)||target<=0)return;
    const previous=!!s.emailEnabled;
    s.emailEnabled=!previous;
    /* Persist the user's explicit M choice BEFORE the network call.
       A transient Worker/network failure must never erase the local switch.
       Startup will retry the Worker registration for any restored ON slot. */
    crossoverSaveMailPrefs();
    crossoverSaveState();
    crossoverUpdateMailButton(index);
    /* Only this explicit M action talks to the Worker. */
    const ok=await crossoverSyncEmailWorker();
    if(!ok){
        console.warn("Crossover Email Worker sync failed; local M state retained for retry.");
    }
    const status=crossoverGet("crossoverStatus",index);
    if(status)status.textContent=s.emailEnabled?"Email ON":"Email OFF";
}
function crossoverPercentageLevels(oldPrice,newPrice,target){
    const oldPct=((oldPrice-target)/target)*100;
    const newPct=((newPrice-target)/target)*100;
    if(!Number.isFinite(oldPct)||!Number.isFinite(newPct)||oldPct===newPct)return [];
    const levels=[];
    if(newPct>oldPct){
        const from=Math.ceil(oldPct),to=Math.floor(newPct);
        for(let level=from;level<=to;level++)if(level!==0)levels.push({level,direction:1});
    }else{
        const from=Math.floor(oldPct),to=Math.ceil(newPct);
        for(let level=from;level>=to;level--)if(level!==0)levels.push({level,direction:-1});
    }
    return levels;
}
async function crossoverSendPercentageAlert(index,level,price,target,direction){
    const s=crossoverInstance(index);
    if(!s||!s.running||!s.notificationEnabled)return false;
    const now=Date.now(),key=String(level);
    // Every genuine threshold crossing is an independent event.
    s.percentageAlertTimes[key]=now;
    const coin=s.activeCoin||s.coin;
    const pctText=`${level>0?"+":""}${level.toFixed(0)}%`;
    const isUp=direction>0;
    const assetLabel=crossoverNotificationAssetLabel(coin);
    const title=`${isUp?"🟢 ↑":"🔴 ↓"} ${assetLabel} • Crossover #${index}`;
    const relation=isUp?"crossed up":"crossed down";
    const message=`${assetLabel} • ${pctText} ${relation} target • Price ${crossoverPriceText(price,coin)} • Target ${crossoverPriceText(target,coin)}`;
    try{await sendDashboardNotification(title,message,crossoverNotifyAssetIcon(coin),"CROSSOVER");return true;}catch(e){console.warn("Crossover percentage notification failed",e);return false;}
}
async function crossoverToggleNotifications(index){
    const s=crossoverInstance(index);
    if(!s||!s.running)return;
    s.notificationEnabled=!s.notificationEnabled;
    crossoverSaveNotificationPrefs();
    crossoverUpdateNotificationButton(index);
    crossoverSaveState();
    if(s.notificationEnabled){
        const coin=s.activeCoin||s.coin,target=Number(s.activeTarget);
        const assetLabel=crossoverNotificationAssetLabel(coin);
        const title=`🔔 Crossover #${index} • ${assetLabel}`;
        const message=`Percentage notifications activated for ${assetLabel} • Target ${crossoverPriceText(target,coin)}`;
        try{await sendDashboardNotification(title,message,crossoverNotifyAssetIcon(coin),"CROSSOVER");}catch(e){console.warn("Crossover notification activation failed",e);}
    }
}
function crossoverFormatPrice(value, asset){
    const key=String(asset||"").trim().toUpperCase();
    if(key==="USD") return formatGlobalPulseNumber(value,0);
    if(key==="GOLD") return formatGlobalPulseNumber(value,0);
    if(key==="OIL") return formatGlobalPulseNumber(value,2);
    if(key.startsWith("TSE|")) return formatGlobalPulseNumber(value,0);
    return formatCryptoAssetPrice(value, key, false);
}
function crossoverNowText(date=new Date()){
    const d=date instanceof Date?date:new Date(date),pad=n=>String(n).padStart(2,"0");
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
/* Short label designed for the narrow status pill beside the email icon. */
function crossoverLastCrossLabel(timestamp,direction=0){
    if(!timestamp)return "";
    const d=new Date(timestamp);
    if(!Number.isFinite(d.getTime()))return "";
    const pad=n=>String(n).padStart(2,"0");
    const arrow=direction>0?"▲":direction<0?"▼":"◆";
    return `${arrow} ${pad(d.getMonth()+1)}/${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
function crossoverSaveState(){
    const data={
        version:1,
        count:Number(document.getElementById("crossoverGrid")?.dataset.count||1),
        liveAverage:Number((document.getElementById("overallLive")?.textContent||"").replace(/,/g,""))||null,
        crossovers:crossoverInstances.map(s=>({
            selectedCoin:s.coin,coin:s.coin,activeCoin:s.activeCoin,target:s.target,activeTarget:s.activeTarget,running:!!s.running,previousPrice:s.previousPrice,
            latestPrice:s.latestPrice,latestUpdated:s.latestUpdated,dailyChangePercent:s.dailyChangePercent,
            lastCrossAt:s.lastCrossAt,lastCrossDirection:s.lastCrossDirection,
            priceDirection:s.priceDirection||0,
            notificationEnabled:!!s.notificationEnabled,
            emailEnabled:!!s.emailEnabled,
            percentageAlertTimes:s.percentageAlertTimes||{}
        }))
    };
    const raw=JSON.stringify(data);
    try{
        localStorage.setItem(CROSSOVER_STATE_KEY,raw);
        crossoverInstances.forEach((item,i)=>{
            const selected=String(item.running?(item.activeCoin||item.coin):item.coin||"").toUpperCase();
            if(selected && crossoverIsValidAsset(selected)){
                crossoverPersistSelection(i+1,selected);
            }
            localStorage.setItem(`crossoverRunning-${i+1}`,item.running?"1":"0");
            if(Number.isFinite(Number(item.activeTarget))&&Number(item.activeTarget)>0){
                localStorage.setItem(`crossoverActiveTarget-${i+1}`,String(item.activeTarget));
            }
        });
    }catch(e){}
    try{
        if(window.AndroidNotifications&&typeof window.AndroidNotifications.getSignalMatrixStateJson==="function"){
            const matrixRaw=window.AndroidNotifications.getSignalMatrixStateJson();
            const matrix=matrixRaw?JSON.parse(matrixRaw):{count:1,matrices:[]};
            matrix.crossovers=data.crossovers;
            matrix.crossoverCount=data.count;
            if(typeof window.AndroidNotifications.setSignalMatrixStateJson==="function"){
                window.AndroidNotifications.setSignalMatrixStateJson(JSON.stringify(matrix));
            }
        }
    }catch(e){console.warn("Crossover native state save failed",e);}
}
function crossoverReadState(){
    /*
     * UI state is LOCAL-FIRST. The selector/Start state saved by this WebView
     * is authoritative for what must be shown after a refresh. The native
     * state can be older (for example it may still contain BTC), so it must
     * never overwrite a newer local selection such as POL or GOLD.
     *
     * Native state is used only as a fallback when there is no local Crossover
     * state at all. This also keeps the fast-startup path independent of the
     * remote Crossover Worker.
     */
    try{
        const raw=localStorage.getItem(CROSSOVER_STATE_KEY);
        if(raw){
            const local=JSON.parse(raw);
            if(local&&Array.isArray(local.crossovers))return local;
        }
    }catch(e){console.warn("Crossover local state read failed",e);}
    try{
        if(window.AndroidNotifications&&typeof window.AndroidNotifications.getSignalMatrixStateJson==="function"){
            const raw=window.AndroidNotifications.getSignalMatrixStateJson();
            const matrix=raw?JSON.parse(raw):null;
            if(matrix&&Array.isArray(matrix.crossovers)){
                return {
                    version:1,
                    count:Number(matrix.crossoverCount||matrix.count||1),
                    crossovers:matrix.crossovers
                };
            }
        }
    }catch(e){console.warn("Crossover native state read failed",e);}
    return null;
}
function crossoverBuffettGuidance(percent){
    if(percent===null||percent===undefined||percent===""||!Number.isFinite(Number(percent)))return {text:"Waiting for daily change…",tone:"hold",title:"Waiting for a valid daily percentage change."};
    const p=Number(percent);
    if(p <= -25)return {text:"Stop buying; serious review",tone:"buy",title:"Stop adding to the position and reassess the investment thesis carefully."};
    if(p <= -15)return {text:"Increase position by 10%",tone:"buy",title:"Only consider adding after confirming the analysis and investment thesis."};
    if(p <= -10)return {text:"Reassess; no automatic buying",tone:"hold",title:"Review the situation; do not buy automatically."};
    if(p <= -5)return {text:"No change",tone:"hold",title:"Avoid acting on this price move alone."};
    if(p < 20)return {text:"Hold",tone:"hold",title:"Hold; avoid reacting to ordinary price movements."};
    if(p < 30)return {text:"Sell 10% of initial position",tone:"sell",title:"Custom portfolio rule, not a fixed Warren Buffett rule."};
    if(p < 40)return {text:"Sell 15% of initial position",tone:"sell",title:"Custom portfolio rule, not a fixed Warren Buffett rule."};
    if(p < 75)return {text:"Sell 20% of initial position",tone:"sell",title:"Custom portfolio rule, not a fixed Warren Buffett rule."};
    return {text:"Evaluate selling the remainder",tone:"sell",title:"Review the investment and decide whether to sell the remaining position."};
}
function crossoverGuidePercentFromTarget(index){
    const s=crossoverInstance(index);
    if(!s)return null;
    const asset=String((s.running?(s.activeCoin||s.coin):s.coin)||"").toUpperCase();
    const target=Number(s.running?s.activeTarget:s.target);
    let price=Number(s.latestPrice);
    if((!Number.isFinite(price)||price<=0) && asset){
        const cached=crossoverReadCachedPrice(asset);
        if(cached)price=Number(cached.price);
    }
    // The guide is attached to Crossover's own target: compare the live asset
    // price with the target entered by the user. Example: 85,000 vs 60,000 = +41.67%.
    if(Number.isFinite(price)&&price>0&&Number.isFinite(target)&&target>0){
        const pct=((price-target)/target)*100;
        return Number.isFinite(pct)?pct:null;
    }
    return null;
}
async function crossoverRefreshBuffettGuide(index){
    const s=crossoverInstance(index);
    if(!s)return;
    const asset=String((s.running?(s.activeCoin||s.coin):s.coin)||"").toUpperCase();
    // Prefer Crossover's actual live-price-versus-target percentage. This is
    // available immediately when both the target and the current/cached price exist.
    const targetPct=crossoverGuidePercentFromTarget(index);
    if(targetPct!==null){
        crossoverUpdateBuffettGuide(index,targetPct);
        return;
    }
    // Otherwise reuse a valid daily-change value already held for this asset.
    if(s.dailyChangePercent!==null && s.dailyChangePercent!==undefined &&
       s.dailyChangePercent!=="" && Number.isFinite(Number(s.dailyChangePercent))){
        crossoverUpdateBuffettGuide(index,Number(s.dailyChangePercent));
    }
    if(!asset||asset==="USD")return;
    try{
        const pct=await crossoverFetchDailyPercent(asset);
        if(pct!==null&&pct!==undefined&&pct!==""&&Number.isFinite(Number(pct))){
            s.dailyChangePercent=Number(pct);
            // If target/current price became available during the request, use that
            // instead of applying a daily percentage to the wrong Crossover context.
            const latestTargetPct=crossoverGuidePercentFromTarget(index);
            crossoverUpdateBuffettGuide(index,latestTargetPct!==null?latestTargetPct:s.dailyChangePercent);
            crossoverSaveState();
        }else if(s.dailyChangePercent===null||s.dailyChangePercent===undefined||s.dailyChangePercent===""){
            const el=document.getElementById(`crossoverBuffettGuide${index}`);
            if(el){el.textContent="Daily change unavailable";el.classList.remove("buy","hold","sell");el.classList.add("hold");el.title="No valid daily percentage or target-relative percentage is available yet.";}
        }
    }catch(e){
        const el=document.getElementById(`crossoverBuffettGuide${index}`);
        if(el && (s.dailyChangePercent===null||s.dailyChangePercent===undefined||s.dailyChangePercent==="")){
            el.textContent="Daily change unavailable";el.classList.remove("buy","hold","sell");el.classList.add("hold");el.title="No valid daily percentage or target-relative percentage is available yet.";
        }
    }
}
function crossoverUpdateBuffettGuide(index,percent){
    const el=document.getElementById(`crossoverBuffettGuide${index}`);
    if(!el)return;
    const guidance=crossoverBuffettGuidance(percent);
    el.textContent=guidance.text;
    el.classList.remove("buy","hold","sell");
    el.classList.add(guidance.tone);
    el.title=guidance.title;
}
function crossoverUpdateUI(index){
    const s=crossoverInstance(index);
    if(!s)return;
    const live=crossoverGet("crossoverLive",index);
    const priceEl=live?.querySelector(".crossover-live-price");
    const coinEl=live?.querySelector(".crossover-live-coin");
    const distance=crossoverGet("crossoverDistance",index);
    const status=crossoverGet("crossoverStatus",index);
    const effectiveCoin=s.running?(s.activeCoin||s.coin):s.coin;
    const targetGuidePercent=crossoverGuidePercentFromTarget(index);
    crossoverUpdateBuffettGuide(index,targetGuidePercent!==null?targetGuidePercent:s.dailyChangePercent);
    const isUSD=effectiveCoin==="USD";
    const assetLabel=crossoverAssetLabel(effectiveCoin);
    const coinSelect=crossoverGet("crossoverCoin",index);
    const hasUserSelection=coinSelect?.dataset.userSelected==="1";
    if(live) live.style.display=hasUserSelection?"flex":"none";
    const liveIcon=live?.querySelector(".crossover-live-icon");
    const liveSymbol=live?.querySelector(".crossover-live-symbol");
    if(coinEl){
        if(liveIcon){
            liveIcon.innerHTML=crossoverAssetIconHTML(effectiveCoin);
        }
        if(liveSymbol)liveSymbol.textContent=assetLabel;
    }
    const price=Number(s.latestPrice),target=Number(s.running?s.activeTarget:s.target);
    if(priceEl){
        priceEl.textContent=Number.isFinite(price)&&price>0?crossoverPriceText(price,effectiveCoin):"-";
        priceEl.classList.remove("price-up","price-down");
        if(s.priceDirection>0)priceEl.classList.add("price-up");
        else if(s.priceDirection<0)priceEl.classList.add("price-down");
    }
    if(distance){
        distance.classList.remove("above","below","invalid");
        if(s.running&&Number.isFinite(price)&&price>0&&Number.isFinite(target)&&target>0){
            const pct=((price-target)/target)*100;
            distance.textContent=`${pct>=0?"+":""}${pct.toFixed(2)}% • ${price>=target?"Above":"Below"} target • Target: ${assetLabel} ${crossoverPriceText(target,effectiveCoin)}`;
            distance.classList.add(price>=target?"above":"below");
        }else{
            distance.textContent=`Current value ${Number.isFinite(price)?crossoverPriceText(price,effectiveCoin):"—"} • Target: ${assetLabel} ${Number.isFinite(target)?crossoverPriceText(target,effectiveCoin):"—"}`;
            distance.classList.add("invalid");
        }
    }
    if(status){
        if(!s.running){
            if(Number.isFinite(target)&&target>0)status.textContent="Ready";
        }else if(s.lastCrossAt){
            status.textContent=crossoverLastCrossLabel(s.lastCrossAt,s.lastCrossDirection);
            status.title=`Last crossover: ${crossoverNowText(s.lastCrossAt)}`;
        }else{
            status.textContent="Monitoring";
            status.removeAttribute("title");
        }
    }
    crossoverUpdateNotificationButton(index);
    crossoverUpdateMailButton(index);
}
// Keep the Buffett guidance live even when a Crossover card is not running.
if(!window.__crossoverBuffettGuideRefreshStarted){
    window.__crossoverBuffettGuideRefreshStarted=true;
    const refreshAllCrossoverGuides=()=>{for(let i=1;i<=4;i++)crossoverRefreshBuffettGuide(i);};
    setTimeout(refreshAllCrossoverGuides,1200);
    setInterval(refreshAllCrossoverGuides,30000);
}
// Crossover alerts are not rate-limited. Each actual crossing is delivered.
async function crossoverSendAlert(index,direction,price,target){
    const s=crossoverInstance(index);
    const arrow=direction>0?"↑":"↓";
    const color=direction>0?"🟢":"🔴";
    const effectiveCoin=s.running?(s.activeCoin||s.coin):s.coin;
    const assetLabel=crossoverNotificationAssetLabel(effectiveCoin);
    const title=`${color} ${arrow} ${assetLabel} • Crossover #${index}`;
    const time=crossoverNowText();
    const message=`Crossover ${arrow} • ${crossoverPriceText(price,effectiveCoin)} • Target ${crossoverPriceText(target,effectiveCoin)} • ${time}`;
    try{await sendDashboardNotification(title,message,crossoverNotifyAssetIcon(effectiveCoin),"CROSSOVER");}catch(e){console.warn("Crossover notification failed",e);}
    /* Crossover email is sent exclusively by CROSSOVER_EMAIL_WORKER_URL. */
}
function crossoverPriceCacheKey(asset){
    const value=String(asset||"").trim().toUpperCase();
    const resolved=value.startsWith("TSE|")?crossoverTseSymbol(value)||"UNSET":value;
    return `crossoverPriceCache-v2:${value}:${resolved}`;
}
function crossoverReadCachedPrice(asset){
    try{
        const raw=localStorage.getItem(crossoverPriceCacheKey(asset));
        const saved=raw?JSON.parse(raw):null;
        const price=Number(saved?.price);
        if(!Number.isFinite(price)||price<=0)return null;
        return {price,updated:saved.updated||null};
    }catch(e){return null;}
}
function crossoverWriteCachedPrice(asset,price,updated){
    const value=Number(price);
    if(!Number.isFinite(value)||value<=0)return;
    try{
        localStorage.setItem(crossoverPriceCacheKey(asset),JSON.stringify({price:value,updated:updated||new Date().toISOString()}));
    }catch(e){}
}
const crossoverDailyPercentCache = new Map();
const crossoverDailyPercentInFlight = new Map();
async function crossoverFetchDailyPercent(asset){
    const v=String(asset||"").trim().toUpperCase();
    if(!v||v==="USD")return null;
    if(v==="GOLD"||v==="OIL"||v.startsWith("TSE|")){
        try{
            const item=globalPulseCompactAssetPrice(v==="GOLD"?"gold":v==="OIL"?"oil":"tse",v.startsWith("TSE|")?crossoverTseSymbol(v):"");
            const raw=item?.dailyChangePercent;
            if(raw!==null&&raw!==undefined&&String(raw).trim()!==""&&Number.isFinite(Number(raw)))return Number(raw);
        }catch(e){}
        return null;
    }
    const cached=crossoverDailyPercentCache.get(v);
    if(cached&&Date.now()-cached.at<60000)return cached.value;
    if(crossoverDailyPercentInFlight.has(v))return crossoverDailyPercentInFlight.get(v);
    const task=(async()=>{
        const compact=v.replace(/[-_\\/]/g,"");
        const symbol=typeof cryptoSpotSymbol==="function"?cryptoSpotSymbol(v):(compact.endsWith("USDT")?compact:compact+"USDT");
        const mexcUrl=`https://api.mexc.com/api/v3/ticker/24hr?symbol=${encodeURIComponent(symbol)}`;
        const binanceUrl=`https://api.binance.com/api/v3/ticker/24hr?symbol=${encodeURIComponent(symbol)}`;
        const urls=[mexcUrl,`https://proxy.cors.dev/${mexcUrl}`,binanceUrl,`https://proxy.cors.dev/${binanceUrl}`];
        for(const url of urls){
            try{
                const response=await fetchWithTimeout(url,{method:"GET",headers:{Accept:"application/json"},cache:"no-store"},4500);
                if(!response?.ok)continue;
                const payload=await response.json();
                const raw=payload?.priceChangePercent;
                if(raw!==null&&raw!==undefined&&String(raw).trim()!==""&&Number.isFinite(Number(raw))){
                    const value=Number(raw);
                    crossoverDailyPercentCache.set(v,{value,at:Date.now()});
                    return value;
                }
            }catch(e){}
        }
        return null;
    })();
    crossoverDailyPercentInFlight.set(v,task);
    try{return await task;}finally{crossoverDailyPercentInFlight.delete(v);}
}


async function crossoverFetchOne(index,notify=true){
    const s=crossoverInstance(index);
    if(!s||!s.running||s.requestInProgress)return;
    const target=Number(s.activeTarget);
    if(!Number.isFinite(target)||target<=0){
        const status=crossoverGet("crossoverStatus",index);
        if(status)status.textContent="Enter a valid target price";
        return;
    }
    s.requestInProgress=true;
    const effectiveCoin=s.activeCoin||s.coin;
    /* Cache-first paint: after a manual page refresh, show the last successful
       Crossover quote immediately while the live request is being refreshed. */
    if(!Number.isFinite(Number(s.latestPrice))||Number(s.latestPrice)<=0){
        const cached=crossoverReadCachedPrice(effectiveCoin);
        if(cached){
            s.latestPrice=cached.price;
            s.latestUpdated=cached.updated||s.latestUpdated;
            crossoverUpdateUI(index);
        }
    }
    try{
        let price;
        let data={};
        if(effectiveCoin==="USD") {
            const liveEl=document.getElementById("overallLive");
            price=Number(String(liveEl?.textContent||"").replace(/,/g,""));
            if(!Number.isFinite(price)||price<=0){
                try{const raw=localStorage.getItem(CROSSOVER_STATE_KEY);const saved=raw?JSON.parse(raw):null;price=Number(saved?.liveAverage);}catch(e){}
            }
            if(!Number.isFinite(price)||price<=0)throw new Error("Live Average unavailable");
            data={updated:new Date().toISOString()};
        } else if(crossoverIsSpecialAsset(effectiveCoin)) {
            const special=await crossoverFetchSpecialPrice(effectiveCoin);
            data=special||{};
            price=Number(special?.price);
        } else {
            const unifiedPrice=await getUnifiedCryptoPrice(effectiveCoin);
            data={updated:new Date(unifiedPrice.time).toISOString(),price:unifiedPrice.price,source:unifiedPrice.source};
            price=Number(unifiedPrice.price);
        }
        if(!Number.isFinite(price)||price<=0)throw new Error("Invalid current price");
        clearPageIssue(`crossover:${index}`);
        const old=Number(s.previousPrice);
        const suppressAlertsAfterOffline=!!s.wasOffline;
        s.wasOffline=false;
        s.priceDirection=(Number.isFinite(old)&&Number.isFinite(price)&&price>0)
            ? (price>old?1:price<old?-1:0)
            : 0;
        s.latestPrice=price;
        s.latestUpdated=data.updated||new Date().toISOString();
        const suppliedDailyPercent=data?.dailyChangePercent;
        const dailyChangePercent=suppliedDailyPercent!==null&&suppliedDailyPercent!==undefined&&Number.isFinite(Number(suppliedDailyPercent))
            ?Number(suppliedDailyPercent):await crossoverFetchDailyPercent(effectiveCoin);
        if(Number.isFinite(dailyChangePercent))s.dailyChangePercent=dailyChangePercent;
        crossoverWriteCachedPrice(effectiveCoin,price,s.latestUpdated);
        // Persist the latest Crossover daily percentage before rendering Ultra Compact.
        // In Crossover-only mode the compact strip reads crossoverReadState() from
        // localStorage, so without this save it could keep a stale/missing percentage.
        crossoverSaveState();
        syncNativeWidgetSnapshot();
        rfkjRenderUltraCompact(false);
        if(notify&&!suppressAlertsAfterOffline&&Number.isFinite(old)&&old!==price){
            const crossedUp=old<target&&price>=target;
            const crossedDown=old>target&&price<=target;
            if(crossedUp||crossedDown){
                const direction=crossedUp?1:-1;
                s.lastCrossAt=new Date().toISOString();
                s.lastCrossDirection=direction;
                crossoverSaveLastCross(index,s.lastCrossAt,direction);
                const status=crossoverGet("crossoverStatus",index);
                if(status){
                    status.textContent=crossoverLastCrossLabel(s.lastCrossAt,direction);
                    status.title=`Last crossover: ${crossoverNowText(s.lastCrossAt)}`;
                }
                if(s.notificationEnabled){
                    await crossoverSendAlert(index,direction,price,target);
                }
            }
            if(s.notificationEnabled){
                const levels=crossoverPercentageLevels(old,price,target);
                for(const item of levels)await crossoverSendPercentageAlert(index,item.level,price,target,item.direction);
            }
        }
        s.previousPrice=price;
        crossoverUpdateUI(index);
        crossoverSaveState();
    }catch(error){
        s.wasOffline=true;
        if(!Number.isFinite(Number(s.latestPrice))||Number(s.latestPrice)<=0){
            const cached=crossoverReadCachedPrice(effectiveCoin);
            if(cached){s.latestPrice=cached.price;s.latestUpdated=cached.updated||s.latestUpdated;crossoverUpdateUI(index);}
        }
        setPageIssue(`crossover:${index}`,`Price APIs unavailable • keeping last price`,"error");
        console.warn(`Crossover ${index} failed`,error);
        const status=crossoverGet("crossoverStatus",index);
        if(status)status.textContent=`Worker unavailable • ${error.message||"request failed"}`;
    }finally{s.requestInProgress=false;}
}
function crossoverStart(index,sendStart=true,restoreEmail=false){
    const s=crossoverInstance(index);
    const target=Number(s.target);
    if(!s||s.running)return;
    if(!Number.isFinite(target)||target<=0){
        const input=crossoverGet("crossoverTarget",index);
        if(input){input.focus();input.reportValidity?.();}
        const status=crossoverGet("crossoverStatus",index);
        if(status)status.textContent="Enter a valid target price";
        return;
    }
    s.coin=String(s.coin||"BTC").toUpperCase();
    s.activeCoin=s.coin;
    s.activeTarget=target;
    s.running=true;
    /* A normal fresh Start clears Email so it is an explicit M permission.
       During startup restore, preserve the saved Email state for this running
       crypto Crossover; it is restored separately and re-synced with the Worker. */
    if(!restoreEmail)s.emailEnabled=false;
    crossoverPersistSelection(index,s.activeCoin);
    /* Keep the last saved price so the first new quote can be compared to it. */
    if(!Number.isFinite(Number(s.previousPrice)))s.previousPrice=null;
    s.priceDirection=0;
    /* A fresh Start begins a new monitoring session. During state restoration,
       preserve the cached last-cross timestamp so it survives app refreshes. */
    if(!restoreEmail){
        s.lastCrossAt=null;
        s.lastCrossDirection=0;
        crossoverSaveLastCross(index,null,0);
    }
    const b=crossoverGet("crossoverStartStop",index);
    if(b){b.textContent="■ Stop Crossover";b.classList.add("running");}
    const status=crossoverGet("crossoverStatus",index);
    if(status)status.textContent="Monitoring";
    crossoverUpdateNotificationButton(index);
    if(sendStart){
        const price=Number(s.latestPrice);
        crossoverSendStartNotification(index,price,target);
    }
    crossoverFetchOne(index,false);
    s.timer=setInterval(()=>crossoverFetchOne(index,true),CROSSOVER_POLL_MS);
    crossoverSaveState();
    crossoverSyncCountLineStates();
}
async function crossoverSendStartNotification(index,price,target){
    const s=crossoverInstance(index);
    const effectiveCoin=s.activeCoin||s.coin;
    const assetLabel=crossoverNotificationAssetLabel(effectiveCoin);
    const title=`🔔 ${assetLabel} • Crossover #${index}`;
    const message=`Crossover started • Target ${crossoverPriceText(target,effectiveCoin)}`;
    try{await sendDashboardNotification(title,message,crossoverNotifyAssetIcon(effectiveCoin),"CROSSOVER");}catch(e){}
}
function crossoverStop(index,sendStop=true){
    const s=crossoverInstance(index);
    if(!s)return;
    const hadEmail=!!s.emailEnabled;
    s.running=false;
    s.emailEnabled=false;
    const stoppedCoin=s.activeCoin||s.coin;
    s.activeCoin=null;
    s.activeTarget=null;
    if(s.coin!==stoppedCoin){
        s.previousPrice=null;s.latestPrice=null;s.latestUpdated=null;
        s.lastCrossAt=null;s.lastCrossDirection=0;s.priceDirection=0;
    }
    if(s.timer){clearTimeout(s.timer);s.timer=null;}
    const b=crossoverGet("crossoverStartStop",index);
    if(b){b.textContent="▶ Start Crossover";b.classList.remove("running");}
    /* Stop clears the visible and cached crossover marker. The next Start
       starts clean and records a new timestamp only after a real crossing. */
    s.lastCrossAt=null;
    s.lastCrossDirection=0;
    crossoverSaveLastCross(index,null,0);
    const status=crossoverGet("crossoverStatus",index);
    if(status){status.textContent="";status.removeAttribute("title");}
    crossoverUpdateNotificationButton(index);
    clearPageIssue(`crossover:${index}`);
    crossoverUpdateUI(index);
    crossoverSaveState();
    crossoverSyncCountLineStates();
    if(hadEmail){
        crossoverSaveMailPrefs();
        crossoverSyncEmailWorker().catch(()=>{});
    }
    if(sendStop){
        const stoppedLabel=crossoverNotificationAssetLabel(stoppedCoin);
        const title=`Crossover #${index} • ${stoppedLabel}`;
        sendDashboardNotification(title,`${stoppedLabel} • Crossover STOPPED`,crossoverNotifyAssetIcon(stoppedCoin),"CROSSOVER").catch(()=>{});
    }
}
function crossoverSyncCountLineStates(){
    const grid=document.getElementById("crossoverGrid");
    const count=Math.max(1,Math.min(CROSSOVER_COUNT,Number(grid?.dataset.count||1)));
    document.querySelectorAll(".crossover-slot-btn").forEach(btn=>{
        const index=Number(btn.dataset.crossoverCount), state=crossoverInstance(index);
        btn.classList.toggle("status-running",!!state?.running);
        btn.classList.toggle("status-stopped",!state?.running);
        btn.classList.toggle("active",index<=count);
    });
}
function crossoverSetVisibleCount(count,restore=false){
    const n=Math.max(1,Math.min(4,Number(count)||1));
    const grid=document.getElementById("crossoverGrid");
    if(!grid)return;
    grid.dataset.count=String(n);
    /* Keep the Crossover grid layout tied directly to the selected count.
       This avoids relying on an unset CSS variable and guarantees one row on desktop. */
    grid.style.setProperty("--crossover-count", String(n));
    document.querySelectorAll(".crossover-card").forEach(card=>{
        const i=Number(card.dataset.crossoverIndex);card.style.display=i<=n?"":"none";
    });
    document.querySelectorAll(".crossover-slot-btn").forEach(btn=>btn.classList.toggle("active",Number(btn.dataset.crossoverCount)<=n));
    if(!restore){crossoverSaveState();}
    crossoverSyncCountLineStates();
}
function crossoverApplyState(saved){
    if(!saved)return false;
    crossoverSetVisibleCount(saved.count,true);
    (saved.crossovers||[]).slice(0,4).forEach((m,i)=>{
        const idx=i+1,s=crossoverInstance(idx);
        if(!s)return;
        const localSelected=crossoverReadPersistedSelection(idx);
        let savedCoin=String(m.selectedCoin||m.activeCoin||m.coin||"").toUpperCase();
        /* Per-slot local selection is the strongest source of truth. */
        if(localSelected && (CROSSOVER_COINS.includes(localSelected)||crossoverIsSpecialAsset(localSelected))){
            savedCoin=localSelected;
        }
        s.coin=crossoverIsValidAsset(savedCoin)?savedCoin:(localSelected||"BTC");
        const savedActive=String(m.activeCoin||"").toUpperCase();
        const validActive=CROSSOVER_COINS.includes(savedActive)||crossoverIsSpecialAsset(savedActive);
        s.activeCoin=m.running?(validActive?savedActive:s.coin):null;
        /* A per-slot saved selection is authoritative even when an older
           native/local activeCoin still says BTC. */
        if(m.running && localSelected && crossoverIsValidAsset(localSelected)){
            s.coin=localSelected;
            s.activeCoin=localSelected;
        }
        s.target=Number.isFinite(Number(m.target))?Number(m.target):null;
        s.activeTarget=Number.isFinite(Number(m.activeTarget))?Number(m.activeTarget):(m.running&&Number.isFinite(Number(m.target))?Number(m.target):null);
        // If this crossover was running, restore the exact target that was registered
        // with the worker. Ignore any pending Target Price edit made before refresh.
        if(m.running&&Number.isFinite(Number(s.activeTarget))&&Number(s.activeTarget)>0){
            s.target=Number(s.activeTarget);
        }
        s.previousPrice=Number.isFinite(Number(m.previousPrice))?Number(m.previousPrice):null;
        s.latestPrice=Number.isFinite(Number(m.latestPrice))?Number(m.latestPrice):null;
        s.latestUpdated=m.latestUpdated||null;
        // Restore the last valid daily percentage so the guidance is available
        // immediately after a page/WebView refresh, before the next API response.
        const savedDaily=m.dailyChangePercent;
        s.dailyChangePercent=(savedDaily!==null && savedDaily!==undefined &&
            savedDaily!=="" && Number.isFinite(Number(savedDaily)))
            ? Number(savedDaily) : null;
        const savedLastCross=crossoverReadLastCross(idx);
        s.lastCrossAt=m.lastCrossAt||savedLastCross?.timestamp||null;
        s.lastCrossDirection=Number(m.lastCrossDirection)||Number(savedLastCross?.direction)||0;
        if(s.lastCrossAt)crossoverSaveLastCross(idx,s.lastCrossAt,s.lastCrossDirection);
        s.priceDirection=Number(m.priceDirection)||0;
        s.notificationEnabled=!!m.notificationEnabled;
        /* The dedicated Email preference is authoritative.  Do not let an
           older crossoverState snapshot overwrite a user's saved M choice. */
        const mailPrefs=crossoverReadMailPrefs();
        const mailSlots=(mailPrefs&&mailPrefs.slots&&typeof mailPrefs.slots==="object")?mailPrefs.slots:mailPrefs;
        const savedMail=mailSlots?.[String(idx)];
        const savedMailEnabled=savedMail&&typeof savedMail==="object"
            ? savedMail.enabled===true
            : savedMail===true;
        s.emailEnabled=!!m.running&&savedMailEnabled&&crossoverIsCryptoEmailAsset(s.activeCoin||s.coin);
        s.percentageAlertTimes=(m.percentageAlertTimes&&typeof m.percentageAlertTimes==="object")?m.percentageAlertTimes:Object.create(null);
        const coin=crossoverGet("crossoverCoin",idx),input=crossoverGet("crossoverTarget",idx);
        if(coin){coin.value=s.coin; coin.dataset.userSelected="1";}
        if(input)input.value=Number.isFinite(s.target)&&s.target>0?s.target.toLocaleString("en-US",{maximumFractionDigits:8}):"";
        crossoverUpdateUI(idx);
        if(m.running){
            /* On refresh, the selector must show the same coin that is actually
               registered as active. Any pending selector edit is not restored
               while the crossover is running; it can be changed after refresh
               and becomes active only after Stop → Start. */
            const activeCoin=s.activeCoin||s.coin;
            s.coin=activeCoin;
            if(coin){
                coin.value=activeCoin;
                coin.dataset.userSelected="1";
                /* The visible custom picker listens to change; without this,
                   its label can remain on BTC after a refresh. */
                coin.dispatchEvent(new Event("change",{bubbles:true}));
            }
            const savedEmailEnabled=!!s.emailEnabled;
            crossoverStart(idx,false,true);
            s.coin=activeCoin;
            s.emailEnabled=savedEmailEnabled&&crossoverIsCryptoEmailAsset(s.activeCoin||s.coin);
            crossoverUpdateMailButton(idx);
            crossoverSaveMailPrefs();
            crossoverSaveState();
            crossoverUpdateUI(idx);
        }
    });
    return true;
}
function crossoverInit(){
    crossoverLoadNotificationPrefs();
    for(let i=1;i<=CROSSOVER_COUNT;i++){
        const s=crossoverInstance(i),coin=crossoverGet("crossoverCoin",i),input=crossoverGet("crossoverTarget",i),btn=crossoverGet("crossoverStartStop",i),notifyBtn=crossoverGet("crossoverNotify",i),mailBtn=crossoverGet("crossoverMail",i);
        if(!s||!coin||!input||!btn)continue;
        coin.addEventListener("change",()=>{
            const selected=String(coin.value||"").toUpperCase();
            /* A special asset (GOLD/OIL/TSE) can be restored before the
               custom picker rebuilds the native <select>. In that short
               window the browser may expose an empty value. Never convert
               that temporary empty value into BTC. */
            if(!crossoverIsValidAsset(selected))return;
            s.coin=selected;
            coin.dataset.userSelected="1";
            crossoverPersistSelection(i,s.coin);
            /* While running, the selector is only a pending edit. The active
               coin remains registered exactly like activeTarget. */
            if(!s.running){
                s.previousPrice=null;s.latestPrice=null;s.latestUpdated=null;
                // The previous asset's percentage must not be shown for the new selection.
                s.dailyChangePercent=null;
                s.priceDirection=0;
                /* Do not clear lastCrossAt here: during page restoration the
                   active selector dispatches change before running state is
                   reattached. Last-cross history is cleared only by Stop or
                   by an intentional fresh Start. */
            }
            clearPageIssue(`crossover:${i}`);
            crossoverUpdateUI(i);crossoverSaveState();
            if(!s.running)crossoverRefreshBuffettGuide(i);
        });
        input.addEventListener("input",()=>{
            // Keep a decimal point while the user is typing (e.g. 125.5).
            // The previous Number(...).toLocaleString(...) converted "125."
            // back to "125", which made decimal entry feel impossible.
            let raw=String(input.value||"").replace(/,/g,"").replace(/[^0-9.]/g,"");
            const dot=raw.indexOf(".");
            if(dot>=0){
                raw=raw.slice(0,dot+1)+raw.slice(dot+1).replace(/\\./g,"");
                const decimals=raw.slice(dot+1).slice(0,8);
                raw=raw.slice(0,dot+1)+decimals;
            }
            const clean=raw;
            const n=Number(clean);
            s.target=Number.isFinite(n)&&n>0?n:null;

            // Preserve the exact text while it is being edited so a trailing
            // decimal point is not removed. Format only the integer part.
            if(clean){
                const hasDot=clean.includes(".");
                const [integerPart,fractionPart=""]=clean.split(".");
                const formattedInteger=integerPart
                    ? Number(integerPart).toLocaleString("en-US")
                    : "0";
                input.value=hasDot
                    ? `${formattedInteger}.${fractionPart}`
                    : formattedInteger;
            }else{
                input.value="";
            }

            s.previousPrice=null;s.priceDirection=0;
            /* Editing the pending target must not erase the persisted last-cross
               marker; only Stop or a fresh Start starts a clean history. */
            crossoverUpdateUI(i);crossoverSaveState();
            /* While running, editing Target Price changes only the pending input.
               The worker keeps the target captured at Start until Stop is pressed. */
        });
        input.addEventListener("blur",()=>{
            const n=Number(String(input.value||"").replace(/,/g,""));
            if(Number.isFinite(n)&&n>0) input.value=n.toLocaleString("en-US",{maximumFractionDigits:8});
        });
        btn.addEventListener("click",()=>s.running?crossoverStop(i):crossoverStart(i));
        if(notifyBtn)notifyBtn.addEventListener("click",()=>crossoverToggleNotifications(i));
        if(mailBtn)mailBtn.addEventListener("click",()=>crossoverToggleEmail(i));
        crossoverUpdateUI(i);
        crossoverUpdateMailButton(i);
    }
    document.querySelectorAll(".crossover-slot-btn").forEach(btn=>btn.addEventListener("click",()=>crossoverSetVisibleCount(btn.dataset.crossoverCount)));

    /*
     * FAST STARTUP:
     * Crossover UI is restored ONLY from the last locally saved state.
     * The email Worker is deliberately not queried here. This makes the
     * number of cards, selected assets, targets and local running state
     * available immediately, even with no network connection.
     */
    crossoverLoadMailPrefs();
    for(let i=1;i<=CROSSOVER_COUNT;i++){
        try{
            const saved=crossoverReadPersistedSelection(i);
            const select=crossoverGet("crossoverCoin",i);
            if(saved && select){
                select.value=saved;
                select.dataset.userSelected="1";
                crossoverInstance(i).coin=saved;
            }
        }catch(e){}
    }
    const restored=crossoverApplyState(crossoverReadState());
    if(!restored){
        crossoverSetVisibleCount(1,true);
        for(let i=1;i<=4;i++){
            const s=crossoverInstance(i),coin=crossoverGet("crossoverCoin",i);
            const savedCoin=crossoverReadPersistedSelection(i);
            if(savedCoin){
                s.coin=savedCoin;
                if(coin){coin.value=savedCoin;coin.dataset.userSelected="1";}
            }
            crossoverUpdateUI(i);
        }
        crossoverSaveState();
    }
    /*
     * Restore the email switch exactly as it was for running crypto slots.
     * Non-crypto slots are always forced OFF.  Then re-register the restored
     * crypto email monitors with the Worker so a refresh/reopen does not lose
     * the server-side email command.
     */
    let emailStateChanged=false;
    for(let i=1;i<=CROSSOVER_COUNT;i++){
        const s=crossoverInstance(i);
        if(s.emailEnabled&&!s.running){
            s.emailEnabled=false;
            emailStateChanged=true;
        }
        if(s.emailEnabled&&!crossoverIsCryptoEmailAsset(s.activeCoin||s.coin)){
            s.emailEnabled=false;
            emailStateChanged=true;
        }
        crossoverUpdateNotificationButton(i);
        crossoverUpdateMailButton(i);
    }
    if(emailStateChanged){
        crossoverSaveMailPrefs();
        crossoverSaveState();
    }
    crossoverSyncCountLineStates();
    /* Re-sync restored email-enabled crypto slots after startup. */
    if(crossoverInstances.some(s=>s.running&&s.emailEnabled&&crossoverIsCryptoEmailAsset(s.activeCoin||s.coin))){
        crossoverSyncEmailWorker().catch(()=>{});
    }
}
crossoverInit();
rfkjStartUltraRotation();

/* =====================================================
   CRYPTO PORTFOLIO AUTO REFRESH
   R05: the live prices refresh on their own dedicated loop.
   This intentionally bypasses the portfolio-load cooldown so an old
   browser/WebView cannot leave the visible prices frozen until a manual
   page refresh.
===================================================== */
const CRYPTO_PORTFOLIO_LIVE_REFRESH_MS = 15000;
let cryptoPortfolioLiveRefreshInProgress = false;

function getCurrentCryptoPortfolioHoldings() {
    try {
        const cachedHoldings = loadCryptoHoldingsCache();
        if (
            cachedHoldings &&
            typeof cachedHoldings === "object" &&
            Object.keys(cachedHoldings).length > 0
        ) {
            return cachedHoldings;
        }
    } catch (error) {
        console.warn("Crypto Portfolio holdings cache read failed:", error);
    }

    try {
        const cachedDashboardData = loadCachedDashboardData();
        const holdings = cachedDashboardData?.cryptoHoldings;
        if (
            holdings &&
            typeof holdings === "object" &&
            Object.keys(holdings).length > 0
        ) {
            return holdings;
        }
    } catch (error) {
        console.warn("Crypto Portfolio dashboard cache read failed:", error);
    }

    return null;
}

async function refreshCryptoPortfolioPricesLive() {
    if (cryptoPortfolioLiveRefreshInProgress) {
        return;
    }

    const holdings = getCurrentCryptoPortfolioHoldings();
    if (
        !holdings ||
        typeof holdings !== "object" ||
        Object.keys(holdings).length === 0
    ) {
        return;
    }

    cryptoPortfolioLiveRefreshInProgress = true;
    try {
        const existingMarketData =
            getCachedCryptoPortfolioMarketData(holdings);
        const marketData = { ...existingMarketData };
        const coins = Object.keys(holdings);

        await Promise.all(
            coins.map(async coin => {
                const symbol =
                    holdings[coin] && holdings[coin].symbol
                        ? holdings[coin].symbol
                        : coin + "_USDT";
                try {
                    const liveData = await getMexcCryptoData(symbol);
                    if (
                        liveData &&
                        Number.isFinite(Number(liveData.price)) &&
                        Number(liveData.price) > 0
                    ) {
                        marketData[coin] = liveData;
                        saveCryptoPortfolioCache(symbol, liveData);
                        clearPageIssue(`mexc:${coin}`);
                    }
                } catch (error) {
                    const cached = loadCryptoPortfolioCache(symbol);
                    if (cached) {
                        marketData[coin] = {
                            ...cached,
                            offline: true
                        };
                        setPageIssue(
                            `mexc:${coin}`,
                            `${coin} price cached`,
                            "cache"
                        );
                    }
                    console.warn(
                        "Crypto Portfolio live price refresh failed:",
                        coin,
                        error
                    );
                }
            })
        );

        renderCryptoPortfolio(holdings, marketData);
        const liveCount = Object.values(marketData).filter(
            item => item && !item.offline
        ).length;
        const cachedCount = Object.values(marketData).filter(
            item => item && item.offline
        ).length;
        if (liveCount > 0) {
            clearPageIssue("mexc-api");
        }
        if (cachedCount > 0) {
            setPageIssue(
                "mexc-cache",
                `${cachedCount} crypto prices cached`,
                "cache"
            );
        } else {
            clearPageIssue("mexc-cache");
        }
    } finally {
        cryptoPortfolioLiveRefreshInProgress = false;
    }
}

function scheduleCryptoPortfolioLiveRefresh(delay = CRYPTO_PORTFOLIO_LIVE_REFRESH_MS) {
    if (cryptoPortfolioRefreshTimer) {
        clearTimeout(cryptoPortfolioRefreshTimer);
    }

    cryptoPortfolioRefreshTimer = setTimeout(async () => {
        try {
            await refreshCryptoPortfolioPricesLive();
        } catch (error) {
            console.warn("Crypto Portfolio scheduled live refresh failed:", error);
        } finally {
            scheduleCryptoPortfolioLiveRefresh();
        }
    }, delay);
}

function startCryptoPortfolioAutoRefresh() {
    scheduleCryptoPortfolioLiveRefresh(5000);

    // Older TV/WebView engines may pause timers while focus/visibility changes.
    // These events force a fresh quote as soon as the document becomes active.
    const refreshOnResume = () => {
        refreshCryptoPortfolioPricesLive().catch(error => {
            console.warn("Crypto Portfolio resume refresh failed:", error);
        });
        scheduleCryptoPortfolioLiveRefresh(CRYPTO_PORTFOLIO_LIVE_REFRESH_MS);
    };

    window.removeEventListener("focus", refreshOnResume);
    window.addEventListener("focus", refreshOnResume);
    window.removeEventListener("pageshow", refreshOnResume);
    window.addEventListener("pageshow", refreshOnResume);
    window.removeEventListener("online", refreshOnResume);
    window.addEventListener("online", refreshOnResume);

    document.removeEventListener("visibilitychange", refreshOnResume);
    document.addEventListener("visibilitychange", () => {
        if (document.visibilityState === "visible") {
            refreshOnResume();
        }
    });
}

/* =====================================================
   INIT
===================================================== */
/*
   IMPORTANT FIX:
   The old version did:
       loadDashboardData();
       startGlobalPulse();
       startAutoRefresh();
   without awaiting loadDashboardData().
   This version waits for the async dashboard
   initialization so errors are caught correctly.
   Financial calculations are NOT changed.
*/
async function init() {
    try {
        /*
           FAST BOOT:
           1) Paint the last successful snapshot immediately.
           2) Start every network refresh without blocking first paint.
           3) Never expose the old "Loading..." gate again.
        */
        // TOP PRIORITY: paint cached dollar values and start both live price
        // requests before GlobalPulse, crypto, dashboard and other APIs.
        const topDollarBoot = Promise.allSettled([
            loadUsdTxt(),
            loadYekusd()
        ]);

        startGlobalPulse();

        const cachedBoot = loadDashboardData({ cacheOnly: true })
            .catch(error => {
                console.warn("Cached dashboard boot unavailable:", error);
                return null;
            });

        void Promise.allSettled([
            prepareCryptoIconCache(),
            loadCryptoPortfolio(),
            loadCryptoSignals()
        ]);
        startCryptoPortfolioAutoRefresh();

        await cachedBoot;
        // These requests run independently; this await only observes their
        // completion and does not delay the cached dashboard first paint.
        void topDollarBoot;

        /*
           Refresh live data only after the cached UI is already visible.
           This pass may take several seconds, but it no longer controls
           the initial page paint.
        */
        void loadDashboardData()
            .catch(error => {
                console.error("Dashboard live refresh error:", error);
            })
            .finally(() => {
                startAutoRefresh();
            });
    }
    catch (error) {
        console.error(
            "Dashboard initialization error:",
            error
        );
        const loading =
            $("loading");
        if (loading) {
            loading.style.display =
                "none";
        }
        const errorBox =
            $("errorBox");
        if (errorBox) {
            errorBox.innerHTML =
                `<div class="error">
                    ${error.message || "Unable to load dashboard."}
                </div>`;
        }
    }
}
/* PIN protection is handled by the HTML overlay above. */
init();
/* =====================================================
   SERVICE WORKER
===================================================== */
if (
    "serviceWorker" in navigator
) {
    window.addEventListener(
        "load",
        () => {
            navigator.serviceWorker
                .register(
                    "./sw.js",
                    {
                        updateViaCache:
                            "none"
                    }
                )
                .then(
                    async registration => {
                        console.log(
                            "Financial Dashboard PWA Ready"
                        );
                        await registration.update();
                        /*
                           Best-effort background check. Browser/OS decides
                           when periodic sync is actually allowed to run.
                        */
                        if (
                            "periodicSync" in registration &&
                            registration.periodicSync &&
                            typeof registration.periodicSync.register === "function"
                        ) {
                            try {
                                await registration.periodicSync.register(
                                    "financial-dashboard-background-check",
                                    { minInterval: 60 * 1000 }
                                );
                            }
                            catch (error) {
                                console.warn(
                                    "Periodic background check unavailable:",
                                    error
                                );
                            }
                        }
                    }
                )
                .catch(
                    error => {
                        console.error(
                            "Service Worker Error:",
                            error
                        );
                    }
                );
        }
    );
}



(function initCompactCryptoSelectors(){
    document.querySelectorAll(".rfkj-instance-coin").forEach(select=>{
        if(!select||select.dataset.compactPickerReady==="1")return;
        select.dataset.compactPickerReady="1";
        const picker=document.createElement("div");
        picker.className="rfkj-compact-coin-picker";
        const trigger=document.createElement("button");
        trigger.type="button";
        trigger.className="rfkj-compact-coin-trigger";
        trigger.setAttribute("aria-haspopup","listbox");
        trigger.setAttribute("aria-expanded","false");
        const current=document.createElement("span");
        current.className="rfkj-compact-coin-current";
        const currentName=document.createElement("span");
        currentName.className="rfkj-compact-coin-current-name";
        const chevron=document.createElement("span");
        chevron.className="rfkj-compact-coin-chevron";
        chevron.textContent="▼";
        current.appendChild(currentName);
        trigger.appendChild(current);
        trigger.appendChild(chevron);
        const menu=document.createElement("div");
        menu.className="rfkj-compact-coin-menu";
        menu.setAttribute("role","listbox");
        function syncVisual(){
            currentName.textContent=select.value||"BTC";
            menu.querySelectorAll(".rfkj-compact-coin-option").forEach(option=>{
                const active=option.dataset.value===select.value;
                option.classList.toggle("active",active);
                option.setAttribute("aria-selected",active?"true":"false");
            });
        }
        Array.from(select.options).forEach(option=>{
            const item=document.createElement("button");
            item.type="button";
            item.className="rfkj-compact-coin-option";
            item.dataset.value=option.value;
            item.textContent=option.textContent;
            item.setAttribute("role","option");
            item.addEventListener("click",()=>{
                if(select.value!==option.value){
                    select.value=option.value;
                    select.dispatchEvent(new Event("change",{bubbles:true}));
                }
                syncVisual();
                picker.classList.remove("open");
                trigger.classList.remove("open");
                trigger.setAttribute("aria-expanded","false");
            });
            menu.appendChild(item);
        });
        function closePicker(){
            picker.classList.remove("open");
            trigger.classList.remove("open");
            trigger.setAttribute("aria-expanded","false");
        }
        trigger.addEventListener("click",event=>{
            event.stopPropagation();
            const open=picker.classList.toggle("open");
            trigger.classList.toggle("open",open);
            trigger.setAttribute("aria-expanded",open?"true":"false");
        });
        select.addEventListener("change",syncVisual);
        document.addEventListener("click",event=>{if(!picker.contains(event.target))closePicker();});
        picker.appendChild(trigger);
        picker.appendChild(menu);
        select.parentNode.insertBefore(picker,select);
        /* The native select is the data source only; it is never a second visible picker. */
        select.style.position="absolute";
        select.style.width="1px";
        select.style.height="1px";
        select.style.opacity="0";
        select.style.pointerEvents="none";
        select.style.clipPath="inset(50%)";
        picker._syncVisual=syncVisual;
        syncVisual();
    });
})();



(function initCrossoverSelectors(){
    function setup(select){
        if(!select||select.dataset.compactPickerReady==="1")return;
        select.dataset.compactPickerReady="1";
        const picker=document.createElement("div");picker.className="crossover-coin-picker";
        const trigger=document.createElement("button");trigger.type="button";trigger.className="crossover-coin-trigger";trigger.setAttribute("aria-haspopup","listbox");trigger.setAttribute("aria-expanded","false");
        const current=document.createElement("span");current.className="crossover-coin-current";
        const icon=document.createElement("span");icon.className="crossover-coin-current-icon";
        const name=document.createElement("span");name.className="crossover-coin-current-name";
        const chev=document.createElement("span");chev.className="crossover-coin-chevron";chev.textContent="▼";
        current.append(icon,name);trigger.append(current,chev);
        const menu=document.createElement("div");menu.className="crossover-coin-menu crossover-coin-menu-portal";menu.setAttribute("role","listbox");
        const selectorIndex=(select.id.match(/(\d+)$/)||[])[1]||"1";
        let hasUserSelection=select.dataset.userSelected==="1";
        let preservedValue=String(select.value||"").toUpperCase();
        if(!preservedValue)preservedValue=crossoverReadPersistedSelection(selectorIndex);
        /* A persisted selection is still a real selection after refresh.
           Do not hide its icon/name merely because the custom picker itself
           was recreated after the page reload. */
        if(preservedValue && (CROSSOVER_COINS.includes(preservedValue)||crossoverIsSpecialAsset(preservedValue))){
            hasUserSelection=true;
        }
        function rebuild(){
            const options=crossoverAssetOptions();
            const state=crossoverInstances[Number(selectorIndex)-1];
            let desired=String(preservedValue||"").toUpperCase();
            if(!desired){
                desired=String(state?.running?(state.activeCoin||state.coin):state?.coin||"").toUpperCase();
            }
            if(!desired)desired=crossoverReadPersistedSelection(selectorIndex);
            select.innerHTML="";
            options.forEach(v=>{const o=document.createElement("option");o.value=v;o.textContent=crossoverAssetLabel(v);select.appendChild(o);});
            /* Never manufacture BTC when a persisted asset exists. */
            if(desired && options.includes(desired)){
                select.value=desired;
                preservedValue=desired;
                hasUserSelection=true;
            }else if(!select.value){
                select.value=options[0]||"BTC";
            }
            preservedValue=String(select.value||desired||"").toUpperCase();
            menu.innerHTML="";
            options.forEach(v=>{
                const item=document.createElement("button");item.type="button";item.className="crossover-coin-option";item.dataset.value=v;
                item.innerHTML=crossoverAssetIconHTML(v)+`<span>${crossoverAssetLabel(v)}</span>`;item.setAttribute("role","option");
                item.addEventListener("click",()=>{
                    select.value=v;select.dataset.userSelected="1";hasUserSelection=true;preservedValue=v;
                    picker.classList.add("has-user-selection");
                    crossoverPersistSelection(selectorIndex,v);
                    select.dispatchEvent(new Event("change",{bubbles:true}));sync();close();
                });
                menu.appendChild(item);
            });
            sync();
        }
        function sync(){
            let v=String(select.value||"").toUpperCase();
            const state=crossoverInstances[Number(selectorIndex)-1];
            /* While a Crossover is running, the registered active asset is the
               authoritative value shown in the selector. This also survives
               remote status refreshes and picker recreation. */
            const active=String(state?.running?(state.activeCoin||state.coin):state?.coin||"").toUpperCase();
            const persisted=crossoverReadPersistedSelection(selectorIndex);
            const authoritative=(state?.running?active:(persisted||active));
            if(authoritative && (CROSSOVER_COINS.includes(authoritative)||crossoverIsSpecialAsset(authoritative))){
                v=authoritative;
                if(select.value!==authoritative)select.value=authoritative;
            }
            const shouldShow=hasUserSelection || !!(v && crossoverIsValidAsset(v) && (active===v || persisted));
            if(shouldShow){
                icon.innerHTML=crossoverAssetIconHTML(v);
                name.textContent=crossoverAssetLabel(v);
                picker.classList.add("has-user-selection");
            }else{
                icon.innerHTML="";
                name.textContent="—";
                picker.classList.remove("has-user-selection");
            }
            menu.querySelectorAll(".crossover-coin-option").forEach(o=>{const a=shouldShow&&o.dataset.value===v;o.classList.toggle("active",a);o.setAttribute("aria-selected",a?"true":"false");});
        }
        document.body.appendChild(menu);
        function positionMenu(){
            if(!menu.classList.contains("is-open"))return;
            const r=trigger.getBoundingClientRect(),card=trigger.closest(".crossover-card"),cr=card?card.getBoundingClientRect():r;
            const vw=document.documentElement.clientWidth||window.innerWidth,vh=document.documentElement.clientHeight||window.innerHeight,margin=10,available=Math.max(180,vw-margin*2),w=Math.min(Math.max(180,cr.width),available);
            const compact=r.width<230;picker.classList.toggle("is-compact",compact);menu.classList.toggle("is-compact",compact);
            menu.style.width=w+"px";menu.style.setProperty("--crossover-selector-width",w+"px");menu.style.setProperty("--crossover-card-width",w+"px");menu.style.maxWidth=available+"px";
            menu.style.left=Math.max(margin,Math.min(vw-w-margin,cr.left))+"px";menu.style.right="auto";menu.style.top="0px";menu.style.maxHeight=Math.max(120,vh-20)+"px";
            const mh=menu.offsetHeight;let top=r.bottom+6;if(top+mh>vh-margin&&r.top-mh-6>=margin)top=r.top-mh-6;else if(top+mh>vh-margin)top=Math.max(margin,vh-mh-margin);menu.style.top=top+"px";
        }
        function close(){menu.classList.remove("is-open");picker.classList.remove("open");trigger.classList.remove("open");trigger.setAttribute("aria-expanded","false");}
        select.addEventListener("change",()=>{
            preservedValue=String(select.value||"").toUpperCase();
            crossoverPersistSelection(selectorIndex,preservedValue);
            sync();
        });
        trigger.addEventListener("click",e=>{e.stopPropagation();const open=!menu.classList.contains("is-open");if(open){document.querySelectorAll(".crossover-coin-menu.is-open").forEach(m=>m.classList.remove("is-open"));document.querySelectorAll(".crossover-coin-picker.open").forEach(p=>p.classList.remove("open"));menu.classList.add("is-open");picker.classList.add("open");trigger.classList.add("open");trigger.setAttribute("aria-expanded","true");requestAnimationFrame(positionMenu);}else close();});
        window.addEventListener("resize",positionMenu,{passive:true});window.addEventListener("scroll",positionMenu,{passive:true,capture:true});document.addEventListener("click",e=>{if(e.target!==menu&&!menu.contains(e.target)&&e.target!==trigger&&!trigger.contains(e.target))close();});
        picker.appendChild(trigger);select.parentNode.insertBefore(picker,select);select.style.position="absolute";select.style.width="1px";select.style.height="1px";select.style.opacity="0";select.style.pointerEvents="none";select.style.clipPath="inset(50%)";
        select._rebuildCrossoverPicker=rebuild;rebuild();
    }
    document.querySelectorAll(".crossover-coin").forEach(setup);
    window.refreshCrossoverAssetPickers=function(){document.querySelectorAll(".crossover-coin").forEach(s=>s._rebuildCrossoverPicker&&s._rebuildCrossoverPicker());};
    window.addEventListener("crossover-assets-updated",()=>window.refreshCrossoverAssetPickers());
})();



/* LG webOS TV only: keep the hidden native selects out of remote focus
   and make the numeric target fields reliably editable with the TV remote.
   Desktop / large-monitor browsers are intentionally untouched. */
(function initLgTvInputCompat(){
    const ua = String(navigator.userAgent || "");
    const isLgWebOS = /Web0S|WebOS|webOS|LG Browser|SMART-TV/i.test(ua) ||
        typeof window.webOS !== "undefined" || typeof window.PalmSystem !== "undefined";
    if(!isLgWebOS) return;

    document.documentElement.classList.add("lg-webos-tv");

    // The real selects are data sources; the visible custom pickers handle TV focus.
    document.querySelectorAll(".rfkj-instance-coin, .crossover-coin").forEach(select=>{
        select.setAttribute("tabindex","-1");
    });

    // Use the TV-friendly numeric input control only on LG webOS.
    document.querySelectorAll(".crossover-target").forEach(input=>{
        try{ input.type="number"; }catch(e){}
        input.setAttribute("inputmode","decimal");
        input.setAttribute("step","any");
        input.setAttribute("tabindex","0");

        input.addEventListener("keydown", function(e){
            let key = String(e.key || "");
            const persian="۰۱۲۳۴۵۶۷۸۹", arabic="٠١٢٣٤٥٦٧٨٩";
            if(persian.includes(key)) key=String(persian.indexOf(key));
            else if(arabic.includes(key)) key=String(arabic.indexOf(key));

            const allowed = /^[0-9]$/.test(key);
            if(allowed || key === "." || key === "," || key === "-" || key === "Backspace" || key === "Delete") {
                if(allowed || key === "." || key === "," || key === "-") {
                    e.preventDefault();
                    const ch = key === "," ? "." : key;
                    const start = input.selectionStart ?? input.value.length;
                    const end = input.selectionEnd ?? input.value.length;
                    let value = input.value;
                    if(ch === "." && value.includes(".")) return;
                    if(ch === "-" && start !== 0) return;
                    if(ch === "-" && value.includes("-")) return;
                    input.value = value.slice(0,start) + ch + value.slice(end);
                    const pos = start + ch.length;
                    try{ input.setSelectionRange(pos,pos); }catch(_){}
                    input.dispatchEvent(new Event("input",{bubbles:true}));
                    return;
                }
                if(key === "Backspace" || key === "Delete") {
                    // Let the browser perform deletion; webOS handles this reliably once
                    // the native select is removed from the TV focus order.
                    return;
                }
            }
        });
    });

    // Make the visible TV controls explicitly focusable.
    document.querySelectorAll(".rfkj-slot-btn, .crossover-slot-btn, .rfkj-compact-coin-trigger, .crossover-coin-trigger, .crossover-start-stop, .crossover-notify")
        .forEach(el=>el.setAttribute("tabindex","0"));
})();
