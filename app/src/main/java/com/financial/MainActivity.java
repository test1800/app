package com.financial;

import android.Manifest;
import android.app.Activity;
import android.content.Context;
import android.content.ContentValues;
import android.content.ContentResolver;
import android.net.Uri;
import android.provider.MediaStore;
import android.os.Environment;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.net.NetworkRequest;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.os.VibrationEffect;
import android.os.VibrationAttributes;
import android.os.Vibrator;
import android.os.VibratorManager;
import android.webkit.JavascriptInterface;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.webkit.WebChromeClient;
import android.webkit.ValueCallback;
import android.graphics.Rect;
import android.view.WindowInsets;
import android.view.HapticFeedbackConstants;
import android.widget.FrameLayout;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

public class MainActivity extends Activity {

    private static volatile WebView activeWebView;
    private static volatile boolean activityResumed = false;
    private static volatile Context applicationContext;
    private ValueCallback<Uri[]> pendingFileChooserCallback;
    private static final int GLOBAL_BACKUP_FILE_REQUEST = 4202;

    private static final String PREFS = "financial_native_state";
    private static final String KEY_MONITOR_ENABLED = "monitor_enabled";
    private static final String KEY_APP_LOCKED = "app_locked";

    private static final String LOCAL_INDEX_URL =
            "file:///android_asset/index.html";

    private static final int TSETMC_HTTP_TIMEOUT_MS = 7000;
    private static final int EXTERNAL_API_HTTP_TIMEOUT_MS = 15000;
    private static final ExecutorService MARKET_API_EXECUTOR =
            Executors.newCachedThreadPool();
    private static final String TSETMC_WORKER_URL = "https://tsetmc.rxw1.workers.dev/";
    private static final String TSETMC_FALLBACK_URL = "https://ropmwamrcvvuwrvajxdr.supabase.co/functions/v1/dynamic-service/";
    private static final String TSETMC_ACTIVE_API_PREF = "tsetmc_active_api";

    private static String httpGet(String urlString) throws Exception {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(urlString);
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(TSETMC_HTTP_TIMEOUT_MS);
            connection.setReadTimeout(TSETMC_HTTP_TIMEOUT_MS);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("User-Agent", "FinancialDashboard/1.0 (Android)");
            connection.setUseCaches(false);

            int code = connection.getResponseCode();
            InputStream stream = code >= 200 && code < 300
                    ? connection.getInputStream()
                    : connection.getErrorStream();
            if (stream == null) throw new IllegalStateException("HTTP " + code);

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                StringBuilder out = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) out.append(line);
                if (code < 200 || code >= 300) throw new IllegalStateException("HTTP " + code);
                return out.toString();
            }
        } finally {
            if (connection != null) connection.disconnect();
        }
    }

    private static String getTsetmcWithFailover(Context context, String suffix) {
        String safeSuffix = (suffix == null || suffix.isEmpty())
                ? String.valueOf(System.currentTimeMillis()) : suffix;
        int active = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                .getInt(TSETMC_ACTIVE_API_PREF, 0);
        int[] order = {active, active == 0 ? 1 : 0};
        Exception firstError = null;
        String[] bases = {TSETMC_WORKER_URL, TSETMC_FALLBACK_URL};
        for (int index : order) {
            String target = bases[index] + "?t=" + safeSuffix + "&fresh=" + safeSuffix;
            try {
                String body = httpGet(target);
                org.json.JSONObject data = new org.json.JSONObject(body);
                org.json.JSONArray results = data.optJSONArray("results");
                if (!data.optBoolean("ok", false) || results == null || results.length() == 0) {
                    throw new IllegalStateException("TSETMC source returned no valid results");
                }
                if (index != active) {
                    context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit()
                            .putInt(TSETMC_ACTIVE_API_PREF, index).apply();
                }
                return body;
            } catch (Exception e) {
                if (firstError == null) firstError = e;
            }
        }
        return "";
    }

    public static class AndroidNotificationBridge {
        private final Context context;

        AndroidNotificationBridge(Context context) {
            this.context = context.getApplicationContext();
        }

        /** Save a text backup directly to the user's Downloads folder from WebView. */
        @JavascriptInterface
        public String saveTextFile(String fileName, String content) {
            if (fileName == null || content == null) return "ERROR: Missing file name or content";
            boolean allowedName = "FinancialChartHistory.json".equals(fileName)
                    || "CoinChartHistory.json".equals(fileName)
                    || (fileName.startsWith("FinancialRecord-") && fileName.endsWith(".json")
                    && fileName.matches("FinancialRecord-[A-Za-z0-9._-]+\\.json"))
                    || (fileName.startsWith("financial-dashboard-cache-") && fileName.endsWith(".json")
                    && fileName.matches("[A-Za-z0-9._-]+"));
            if (!allowedName) return "ERROR: Unsupported file name";
            try {
                byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                    ContentResolver resolver = context.getContentResolver();
                    String relativePath = Environment.DIRECTORY_DOWNLOADS;
                    try {
                        resolver.delete(MediaStore.Downloads.EXTERNAL_CONTENT_URI,
                                MediaStore.MediaColumns.DISPLAY_NAME + "=? AND " + MediaStore.MediaColumns.RELATIVE_PATH + "=?",
                                new String[]{fileName, relativePath + "/"});
                    } catch (Exception ignored) {}
                    ContentValues values = new ContentValues();
                    values.put(MediaStore.MediaColumns.DISPLAY_NAME, fileName);
                    values.put(MediaStore.MediaColumns.MIME_TYPE, "application/json");
                    values.put(MediaStore.MediaColumns.RELATIVE_PATH, relativePath);
                    values.put(MediaStore.MediaColumns.IS_PENDING, 1);
                    Uri uri = resolver.insert(MediaStore.Downloads.EXTERNAL_CONTENT_URI, values);
                    if (uri == null) return "ERROR: Could not create file in Downloads";
                    try (OutputStream out = resolver.openOutputStream(uri, "w")) {
                        if (out == null) throw new IllegalStateException("Could not open output stream");
                        out.write(bytes);
                        out.flush();
                    }
                    ContentValues ready = new ContentValues();
                    ready.put(MediaStore.MediaColumns.IS_PENDING, 0);
                    resolver.update(uri, ready, null, null);
                    return "OK:" + bytes.length;
                }
                File dir = context.getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS);
                if (dir == null) return "ERROR: Downloads folder is unavailable";
                if (!dir.exists() && !dir.mkdirs()) return "ERROR: Could not create Downloads folder";
                File file = new File(dir, fileName);
                try (FileOutputStream out = new FileOutputStream(file, false)) {
                    out.write(bytes);
                    out.flush();
                }
                return "OK:" + file.getAbsolutePath();
            } catch (Exception e) {
                return "ERROR: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            }
        }

        @JavascriptInterface
        public void showNotification(String type, String title, String message) {
            // WebView is allowed to publish notifications only while the Activity
            // is actually resumed AND the app PIN is unlocked. During pause/stop
            // the native engine is the sole owner. This closes the race window
            // where WebView could publish while the native checker was taking over.
            if (!MainActivity.isActivityResumed() || MainActivity.isAppLocked(context)) {
                return;
            }

            NotificationHelper.show(
                    context,
                    type,
                    title,
                    message,
                    false
            );
        }

        /**
         * Called by index.html whenever the app PIN overlay becomes locked/unlocked.
         * This state is intentionally independent from Android's device Keyguard.
         */
        @JavascriptInterface
        public void setAppLocked(boolean locked) {
            MainActivity.setAppLocked(locked);
        }

        @JavascriptInterface
        public boolean isAppLocked() {
            return MainActivity.isAppLocked(context);
        }

        @JavascriptInterface
        public void vibratePinKey() {
            // First use Android's View haptic path. Samsung devices (including
            // Galaxy A05) can handle this path even when direct WebView vibration
            // is ignored. The existing Vibrator path remains as a fallback.
            WebView view = MainActivity.activeWebView;
            if (view != null) {
                try {
                    view.post(() -> {
                        try {
                            view.performHapticFeedback(HapticFeedbackConstants.KEYBOARD_TAP);
                        } catch (Throwable ignored) {}
                    });
                } catch (Throwable ignored) {}
            }
            vibratePinFallback(context);
        }

        private static void vibratePinFallback(Context context) {
            try {
                final Vibrator vibrator;
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                    VibratorManager manager = (VibratorManager) context.getSystemService(Context.VIBRATOR_MANAGER_SERVICE);
                    vibrator = manager != null ? manager.getDefaultVibrator() : null;
                } else {
                    vibrator = (Vibrator) context.getSystemService(Context.VIBRATOR_SERVICE);
                }
                if (vibrator == null || !vibrator.hasVibrator()) return;
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    VibrationEffect effect = VibrationEffect.createOneShot(45, 220);
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                        VibrationAttributes attributes = new VibrationAttributes.Builder()
                                .setUsage(VibrationAttributes.USAGE_TOUCH)
                                .build();
                        vibrator.vibrate(effect, attributes);
                    } else {
                        vibrator.vibrate(effect);
                    }
                } else {
                    vibrator.vibrate(35);
                }
            } catch (Throwable ignored) {
                // Haptic feedback is optional and must never affect PIN input.
            }
        }


        /**
         * Persists the Signal Matrix monitoring state so the native checker can
         * continue monitoring the selected coin/timeframe after the WebView
         * is closed, minimized, or the task is swiped away.
         */
        @JavascriptInterface
        public void setSignalMatrixState(
                boolean enabled,
                String coin,
                String timeframe
        ) {
            MainActivity.setSignalMatrixState(context, enabled, coin, timeframe);
        }

        /**
         * Pushes the values already displayed by the WebView to the home-screen
         * widget immediately. This avoids waiting for the widget's independent
         * network refresh cycle.
         */
        @JavascriptInterface
        public void syncWidgetSnapshot(String json) {
            if (json == null || json.trim().isEmpty()) return;
            try {
                new org.json.JSONObject(json);
            } catch (Throwable ignored) {
                return;
            }
            try {
                context.getSharedPreferences("ultra_widget_live", Context.MODE_PRIVATE)
                        .edit().putString("snapshot", json).apply();
                UltraCompactWidget.updateFromNativeSnapshot(context, json);
            } catch (Throwable ignored) {
                // Widget sync is best-effort and must never affect the dashboard.
            }
        }

        @JavascriptInterface
        public String getSignalMatrixState() {
            return MainActivity.getSignalMatrixState(context);
        }

        @JavascriptInterface
        public void setSignalMatrixStateJson(String json) {
            MainActivity.setSignalMatrixStateJson(context, json);
        }

        @JavascriptInterface
        public String getSignalMatrixStateJson() {
            return MainActivity.getSignalMatrixStateJson(context);
        }

        @JavascriptInterface
        public String getBackgroundSignalStateJson() {
            return MainActivity.getBackgroundSignalStateJson(context);
        }

        /**
         * Persists the selected GLOBAL TSETMC instruments for native background
         * notifications and native Crossover monitoring when the WebView is stopped.
         */
        @JavascriptInterface
        public void setTsetmcWatchlistJson(String json) {
            if (json == null || json.trim().isEmpty()) return;
            try {
                org.json.JSONArray list = new org.json.JSONArray(json);
                if (list.length() > 4) return;
                for (int i = 0; i < list.length(); i++) {
                    org.json.JSONObject item = list.optJSONObject(i);
                    if (item == null) return;
                    String symbol = item.optString("symbol", "").trim();
                    String insCode = item.optString("insCode", "").trim();
                    if (symbol.isEmpty() || !insCode.matches("\\d+")) return;
                }
                context.getSharedPreferences("native_notification_engine", Context.MODE_PRIVATE)
                        .edit()
                        .putString("tsetmc_watchlist_json", list.toString())
                        .putBoolean("tsetmc_watchlist_configured", true)
                        .apply();
            } catch (Exception ignored) {
                // Invalid configuration must never interrupt the dashboard UI.
            }
        }

        @JavascriptInterface
        public String getTsetmcData(String cacheBust) {
            String suffix = (cacheBust == null || cacheBust.isEmpty())
                    ? String.valueOf(System.currentTimeMillis())
                    : cacheBust;
            return MainActivity.getTsetmcWithFailover(context, suffix);
        }

        /**
         * Fetches public market JSON through Android's native network stack.
         *
         * WebView fetch() is subject to CORS. Exir and MEXC are public REST
         * APIs, but their browser CORS behaviour is not guaranteed. The native
         * path avoids that browser restriction and is used only for a small
         * allow-list of public market-data hosts.
         */
        /**
         * Asynchronous live Exir price fetch.
         * The WebView is notified through a callback after the native worker
         * finishes. This avoids blocking the JavaScript bridge while Android
         * performs network I/O.
         */
        @JavascriptInterface
        public void fetchExirPriceAsync(String callbackId, String requestedUrl) {
            final String baseUrl = sanitizeExirUrl(requestedUrl);
            final WebView view = MainActivity.activeWebView;
            if (view == null) {
                return;
            }

            MARKET_API_EXECUTOR.execute(() -> {
                String price = fetchExirPriceOnWorker(baseUrl);
                try {
                    final String safeCallbackId = org.json.JSONObject.quote(
                            callbackId == null ? "" : callbackId
                    );
                    final String safePrice = org.json.JSONObject.quote(
                            price == null ? "" : price
                    );
                    view.post(() -> {
                        try {
                            view.evaluateJavascript(
                                    "window.__nativeExirPriceResult(" +
                                            safeCallbackId + "," +
                                            safePrice + ");",
                                    null
                            );
                        } catch (Throwable ignored) {
                        }
                    });
                } catch (Throwable ignored) {
                }
            });
        }

        private String sanitizeExirUrl(String requestedUrl) {
            final String fallback =
                    "https://api.exir.io/v2/orderbook?symbol=usdt-irt";
            if (requestedUrl == null || requestedUrl.trim().isEmpty()) return fallback;
            try {
                URL url = new URL(requestedUrl.trim());
                String host = url.getHost().toLowerCase(java.util.Locale.ROOT);
                String path = url.getPath();
                String query = url.getQuery();
                boolean supportedPath = "/v2/orderbook".equals(path) || "/v2/ticker".equals(path);
                if (!"https".equalsIgnoreCase(url.getProtocol())
                        || !"api.exir.io".equals(host)
                        || !supportedPath
                        || query == null
                        || !query.toLowerCase(java.util.Locale.ROOT).contains("symbol=usdt-irt")) {
                    return fallback;
                }
                return requestedUrl.trim();
            } catch (Exception ignored) {
                return fallback;
            }
        }

        private String fetchExirPriceOnWorker(String baseUrl) {
            // IMPORTANT: Exir does NOT accept arbitrary cache-busting query
            // parameters on /v2/orderbook.  The documented URL must be used
            // exactly as-is; e.g. adding &_=timestamp returns HTTP 404.
            final String directUrl = baseUrl;
            final String encodedTarget;
            try {
                encodedTarget = URLEncoder.encode(directUrl, "UTF-8");
            } catch (Exception ignored) {
                return "";
            }

            // EXIR can reject direct Android/ISP requests even though the public
            // endpoint itself is valid. Try direct first, then the same public
            // proxy chain used by the Web/PWA version.
            String[] urls = new String[]{
                    directUrl,
                    // Same proxy sequence as the dashboard.
                    "https://proxy.cors.dev/" + directUrl,
                    "https://api.cors.syrins.tech/?url=" + encodedTarget,
                    "https://api.allorigins.win/raw?url=" + encodedTarget
            };

            String userAgent =
                    "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0 Mobile Safari/537.36";

            for (String target : urls) {
                    HttpURLConnection connection = null;
                    try {
                        URL url = new URL(target);
                        connection = (HttpURLConnection) url.openConnection();
                        connection.setConnectTimeout(5000);
                        connection.setReadTimeout(5000);
                        connection.setRequestMethod("GET");
                        connection.setInstanceFollowRedirects(true);
                        connection.setUseCaches(false);
                        connection.setRequestProperty("Accept", "application/json, text/plain, */*");
                        connection.setRequestProperty("Accept-Encoding", "identity");
                        connection.setRequestProperty("Cache-Control", "no-cache, no-store, max-age=0");
                        connection.setRequestProperty("Pragma", "no-cache");
                        connection.setRequestProperty("User-Agent", userAgent);
                        connection.setRequestProperty("Connection", "close");

                        int code = connection.getResponseCode();
                        if (code < 200 || code >= 300) continue;

                        try (BufferedReader reader = new BufferedReader(
                                new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                            StringBuilder body = new StringBuilder();
                            String line;
                            while ((line = reader.readLine()) != null) body.append(line);

                            org.json.JSONObject root = new org.json.JSONObject(body.toString());
                            if (directUrl.contains("/v2/ticker")) {
                                double tickerPrice = extractExirTickerPrice(root);
                                if (Double.isFinite(tickerPrice) && tickerPrice > 0) return Double.toString(tickerPrice);
                                continue;
                            }
                            org.json.JSONObject pair = root.optJSONObject("usdt-irt");
                            if (pair == null) pair = root.optJSONObject("USDT-IRT");
                            org.json.JSONArray asks = pair == null ? null : pair.optJSONArray("asks");
                            if (asks == null || asks.length() == 0) continue;

                            double lowestAsk = Double.POSITIVE_INFINITY;
                            for (int i = 0; i < asks.length(); i++) {
                                Object item = asks.opt(i);
                                double price = Double.NaN;
                                if (item instanceof org.json.JSONArray) {
                                    org.json.JSONArray level = (org.json.JSONArray) item;
                                    if (level.length() > 0) {
                                        Object raw = level.opt(0);
                                        try { price = Double.parseDouble(String.valueOf(raw)); } catch (Exception ignored) {}
                                    }
                                } else if (item instanceof org.json.JSONObject) {
                                    org.json.JSONObject row = (org.json.JSONObject) item;
                                    price = row.optDouble("price", row.optDouble("rate", row.optDouble("value", Double.NaN)));
                                }
                                if (Double.isFinite(price) && price > 0 && price < lowestAsk) {
                                    lowestAsk = price;
                                }
                            }
                            if (Double.isFinite(lowestAsk) && lowestAsk > 0) {
                                return Double.toString(lowestAsk);
                            }
                        }
                    } catch (Throwable ignored) {
                        // Try the next URL/profile.
                    } finally {
                        if (connection != null) connection.disconnect();
                    }
            }
            return "";
        }
        private double extractExirTickerPrice(org.json.JSONObject root) {
            org.json.JSONObject data = root.optJSONObject("data");
            org.json.JSONObject result = root.optJSONObject("result");
            org.json.JSONObject dataResult = data == null ? null : data.optJSONObject("result");
            org.json.JSONObject[] candidates = new org.json.JSONObject[]{root, data, result, dataResult, root.optJSONObject("ticker")};
            String[] pairKeys = new String[]{"usdt-irt", "USDT-IRT", "USDT_IRT", "usdt_irt"};
            String[] keys = new String[]{"last", "close", "price", "lastPrice"};
            for (org.json.JSONObject candidate : candidates) {
                if (candidate == null) continue;
                org.json.JSONObject pair = candidate;
                for (String pairKey : pairKeys) { if (candidate.optJSONObject(pairKey) != null) { pair = candidate.optJSONObject(pairKey); break; } }
                for (String key : keys) {
                    Object raw = pair.opt(key);
                    if (raw == null || raw == org.json.JSONObject.NULL) continue;
                    try {
                        double value = Double.parseDouble(String.valueOf(raw).replace(",", "").trim());
                        if (Double.isFinite(value) && value > 0) return value;
                    } catch (Exception ignored) {}
                }
            }
            return Double.NaN;
        }

        @JavascriptInterface
        public String fetchExirPrice(String ignoredUrlString) {
            Future<String> future = MARKET_API_EXECUTOR.submit(() ->
                    fetchExirPriceOnWorker(
                            "https://api.exir.io/v2/orderbook?symbol=usdt-irt"
                    )
            );
            try {
                return future.get(25000L, TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
                future.cancel(true);
                return "";
            }
        }

        @JavascriptInterface
        public String fetchExternalMarketJson(String urlString) {
            if (!isAllowedMarketApi(urlString)) {
                return "";
            }

            /*
             * IMPORTANT:
             * JavascriptInterface methods are invoked from the WebView/UI side.
             * Performing HttpURLConnection directly here can trigger
             * NetworkOnMainThreadException on Android. The previous version did
             * exactly that, so the bridge silently returned an empty string and
             * Exir looked permanently unavailable even though its endpoint was
             * healthy. Run the actual network operation on a worker thread and
             * wait only for its result.
             */
            Future<String> future = MARKET_API_EXECUTOR.submit(() ->
                    fetchExternalMarketJsonOnWorker(urlString)
            );
            try {
                return future.get(
                        EXTERNAL_API_HTTP_TIMEOUT_MS + 1000L,
                        TimeUnit.MILLISECONDS
                );
            } catch (TimeoutException timeout) {
                future.cancel(true);
                return "";
            } catch (Exception ignored) {
                return "";
            }
        }

        private String fetchExternalMarketJsonOnWorker(String urlString) {
            HttpURLConnection connection = null;
            try {
                URL url = new URL(urlString.trim());
                connection = (HttpURLConnection) url.openConnection();
                connection.setConnectTimeout(EXTERNAL_API_HTTP_TIMEOUT_MS);
                connection.setReadTimeout(EXTERNAL_API_HTTP_TIMEOUT_MS);
                connection.setRequestMethod("GET");
                connection.setRequestProperty("Accept", "application/json,text/plain,*/*");
                connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/120.0 Mobile Safari/537.36");
                if ("cdn.tsetmc.com".equalsIgnoreCase(url.getHost()) || "cdn10.tsetmc.com".equalsIgnoreCase(url.getHost())) {
                    connection.setRequestProperty("Referer", "https://www.tsetmc.com/");
                    connection.setRequestProperty("Origin", "https://www.tsetmc.com");
                    connection.setRequestProperty("Accept-Language", "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7");
                }
                connection.setRequestProperty("Cache-Control", "no-cache");
                connection.setUseCaches(false);

                int code = connection.getResponseCode();
                InputStream stream = code >= 200 && code < 300
                        ? connection.getInputStream()
                        : connection.getErrorStream();
                if (stream == null || code < 200 || code >= 300) {
                    return "";
                }

                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                    StringBuilder out = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        out.append(line);
                    }
                    return out.toString();
                }
            } catch (Exception ignored) {
                return "";
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }

        private boolean isAllowedMarketApi(String urlString) {
            if (urlString == null || urlString.trim().isEmpty()) return false;
            try {
                URL url = new URL(urlString.trim());
                if (!"https".equalsIgnoreCase(url.getProtocol())) return false;
                String host = url.getHost().toLowerCase(java.util.Locale.ROOT);
                return "cdn.tsetmc.com".equals(host)
                        || "cdn10.tsetmc.com".equals(host)
                        || "api.exir.io".equals(host)
                        || "api.mexc.com".equals(host)
                        || "api.coingecko.com".equals(host)
                        || "api.nestex.one".equals(host)
                        || "api.bitpin.org".equals(host)
                        || "api.alternative.me".equals(host);
            } catch (Exception ignored) {
                return false;
            }
        }

        @JavascriptInterface
        public void setGlobalNightMode(boolean enabled) {
            context.getSharedPreferences("global_pulse_settings", Context.MODE_PRIVATE)
                    .edit().putBoolean("night_mode", enabled).apply();
        }

        @JavascriptInterface
        public void setGlobalEconomicState(String country, String title, long timestamp) {
            context.getSharedPreferences("global_pulse_settings", Context.MODE_PRIVATE)
                    .edit().putString("economic_country", country == null ? "" : country)
                    .putString("economic_title", title == null ? "Economic Event" : title)
                    .putLong("economic_timestamp", timestamp).apply();
        }

        @JavascriptInterface
        public void setGlobalCompactAsset(String type, String symbol) {
            context.getSharedPreferences("global_pulse_settings", Context.MODE_PRIVATE)
                    .edit().putString("compact_asset_type", type == null ? "" : type)
                    .putString("compact_asset_symbol", symbol == null ? "" : symbol)
                    .apply();
            UltraCompactWidget.updateAll(context);
        }
    }

    private WebView webView;

    private ConnectivityManager connectivityManager;
    private ConnectivityManager.NetworkCallback networkCallback;

    private final Handler handler =
            new Handler(Looper.getMainLooper());

    public static boolean isActivityResumed() {
        return activityResumed;
    }

    public static boolean isAppLocked(Context context) {
        return context.getApplicationContext()
                .getSharedPreferences(PREFS, MODE_PRIVATE)
                .getBoolean(KEY_APP_LOCKED, true);
    }

    public static void setAppLocked(boolean locked) {
        Context app = applicationContext;
        if (app == null) return;
        app.getSharedPreferences(PREFS, MODE_PRIVATE)
                .edit()
                .putBoolean(KEY_APP_LOCKED, locked)
                .apply();
    }

    public static String getBackgroundSignalStateJson(Context context) {
        android.content.SharedPreferences sp =
                context.getApplicationContext().getSharedPreferences(
                        "background_notifications", Context.MODE_PRIVATE);
        org.json.JSONObject object = new org.json.JSONObject();
        try {
            String[] coins = {"BTC","BNB","ETH","DOGE","ADA","SOL","GRAM","AVAX","POL","TRX"};
            for (String coin : coins) {
                String key = "signal_" + coin;
                if (sp.contains(key)) object.put(coin, sp.getInt(key, 0));
            }
            return object.toString();
        } catch (Exception ignored) {
            return "{}";
        }
    }

    public static void setSignalMatrixState(
            Context context,
            boolean enabled,
            String coin,
            String timeframe
    ) {
        if (context == null) return;

        String safeCoin = coin == null ? "" : coin.trim().toUpperCase(java.util.Locale.ROOT);
        String safeTimeframe = timeframe == null ? "1" : timeframe.trim();

        context.getApplicationContext()
                .getSharedPreferences(PREFS, MODE_PRIVATE)
                .edit()
                .putBoolean("signal_matrix_enabled", enabled)
                .putString("signal_matrix_coin", safeCoin)
                .putString("signal_matrix_timeframe", safeTimeframe)
                .apply();
    }

    public static void setSignalMatrixStateJson(Context context, String json) {
        if (context == null) return;
        String safe = json == null ? "" : json.trim();
        if (safe.isEmpty()) return;
        try {
            org.json.JSONObject object = new org.json.JSONObject(safe);
            int count = Math.max(1, Math.min(3, object.optInt("count", 1)));
            object.put("count", count);
            context.getApplicationContext().getSharedPreferences(PREFS, MODE_PRIVATE)
                    .edit()
                    .putString("signal_matrix_config", object.toString())
                    .putBoolean("signal_matrix_enabled", true)
                    .putString("signal_matrix_state_version", "4")
                    .apply();
            // Refresh the home-screen widget immediately after Matrix #1 changes.
            UltraCompactWidget.updateAll(context);
        } catch (Exception ignored) {
        }
    }

    public static String getSignalMatrixStateJson(Context context) {
        if (context == null) return "";
        return context.getApplicationContext().getSharedPreferences(PREFS, MODE_PRIVATE)
                .getString("signal_matrix_config", "");
    }

    /**
     * Returns the last persisted Signal Matrix selection/state to the WebView.
     * This is read-only: reopening the app never resets the native state.
     */
    public static String getSignalMatrixState(Context context) {
        if (context == null) return "{}";

        android.content.SharedPreferences sp =
                context.getApplicationContext()
                        .getSharedPreferences(PREFS, MODE_PRIVATE);

        org.json.JSONObject state = new org.json.JSONObject();
        try {
            state.put("enabled", sp.getBoolean("signal_matrix_enabled", false));
            state.put("coin", sp.getString("signal_matrix_coin", ""));
            state.put("timeframe", sp.getString("signal_matrix_timeframe", "1"));
        } catch (Exception ignored) {
        }
        return state.toString();
    }

    @Override
    protected void onResume() {
        super.onResume();
        activityResumed = true;

        // Keep the optional home-screen Ultra Compact widget in sync with
        // the same Signal Matrix selection used by the dashboard.
        UltraCompactWidget.updateAll(this);
    }

    @Override
    protected void onPause() {
        activityResumed = false;
        super.onPause();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        applicationContext = getApplicationContext();

        // A newly-created Activity starts behind the PIN overlay. The HTML page
        // will switch this flag to false only after the correct PIN is entered.
        getSharedPreferences(PREFS, MODE_PRIVATE)
                .edit()
                .putBoolean(KEY_APP_LOCKED, true)
                .apply();

        // Keep the native/background notification system alive independently
        // from the PIN UI. The dashboard WebView is now loaded immediately,
        // with the PIN rendered as an HTML overlay inside index.html.
        BackgroundCheckScheduler.schedule(this);
        NotificationHelper.createChannels(this);

        getSharedPreferences(PREFS, MODE_PRIVATE)
                .edit()
                .putBoolean(KEY_MONITOR_ENABLED, true)
                .apply();

        requestNotificationPermissionIfNeeded();
        startNetworkMonitorService();
        setupNetworkManager();

        // When a numeric Target Price field receives focus, make the Android
        // keyboard resize the WebView instead of covering the focused field.
        // This is especially important for the packaged APK/WebView; Chrome
        // handles this case more aggressively on its own.
        getWindow().setSoftInputMode(
                android.view.WindowManager.LayoutParams.SOFT_INPUT_ADJUST_RESIZE
        );

        setupWebView();
    }


    /**
     * Refreshes the dashboard even when the Activity is currently in the
     * background. Android may throttle JavaScript timers/intervals in a
     * background WebView; the foreground service calls this method so the
     * existing dashboard notification logic is still evaluated.
     */
    public static void requestDashboardBackgroundRefresh() {
        // WebView is the notification engine only while the Activity is actually
        // visible and the app PIN is unlocked. When the Activity is minimized,
        // stopped/closed, or PIN-locked, NetworkMonitorService uses the native
        // notification engine instead.
        final Context app = applicationContext;
        if (app == null) return;
        if (!activityResumed || isAppLocked(app)) return;

        final WebView view = activeWebView;
        if (view == null) return;

        view.post(() -> {
            try {
                view.evaluateJavascript(
                        "(async function(){ try { if (typeof loadDashboardData === 'function') await loadDashboardData(); if (typeof loadCryptoSignals === 'function') await loadCryptoSignals(); } catch(e) { console.warn('Background dashboard refresh failed', e); } })();",
                        null
                );
            } catch (Exception ignored) {
            }
        });
    }

    private void setupWebView() {
        webView = new WebView(this);
        activeWebView = webView;

        // Keep the WebView viewport above the system navigation area without
        // adding internal bottom padding to the HTML document. This prevents
        // the Error Box from being pushed away from the Crypto Portfolio while
        // still keeping the page visible above the navigation bar on Android 15+.
        FrameLayout root = new FrameLayout(this);
        root.setLayoutParams(new FrameLayout.LayoutParams(
                FrameLayout.LayoutParams.MATCH_PARENT,
                FrameLayout.LayoutParams.MATCH_PARENT
        ));
        FrameLayout.LayoutParams webParams = new FrameLayout.LayoutParams(
                FrameLayout.LayoutParams.MATCH_PARENT,
                FrameLayout.LayoutParams.MATCH_PARENT
        );
        root.addView(webView, webParams);
        setContentView(root);

        webView.setOnApplyWindowInsetsListener((view, insets) -> {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
                android.graphics.Insets bars =
                        insets.getInsets(WindowInsets.Type.navigationBars());
                android.graphics.Insets ime =
                        insets.getInsets(WindowInsets.Type.ime());

                FrameLayout.LayoutParams lp =
                        (FrameLayout.LayoutParams) view.getLayoutParams();
                // Reserve the larger of the navigation-bar and IME insets.
                // This causes the WebView viewport to shrink above the keyboard
                // so a focused Target Price input remains visible.
                int bottom = Math.max(0, Math.max(bars.bottom, ime.bottom));
                if (lp.bottomMargin != bottom) {
                    lp.bottomMargin = bottom;
                    view.setLayoutParams(lp);
                }
            } else {
                Rect systemWindow = new Rect();
                view.getWindowVisibleDisplayFrame(systemWindow);
                FrameLayout.LayoutParams lp =
                        (FrameLayout.LayoutParams) view.getLayoutParams();
                int bottom = Math.max(0, view.getRootView().getHeight() - systemWindow.bottom);
                if (lp.bottomMargin != bottom) {
                    lp.bottomMargin = bottom;
                    view.setLayoutParams(lp);
                }
            }
            return insets;
        });
        webView.requestApplyInsets();

        WebSettings settings = webView.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        settings.setAllowFileAccess(true);
        settings.setAllowContentAccess(true);
        settings.setCacheMode(WebSettings.LOAD_DEFAULT);

        webView.addJavascriptInterface(
                new AndroidNotificationBridge(this),
                "AndroidNotifications"
        );

        webView.setWebViewClient(new WebViewClient());
        webView.setWebChromeClient(new WebChromeClient() {
            @Override
            public boolean onShowFileChooser(WebView view, ValueCallback<Uri[]> filePathCallback,
                    FileChooserParams fileChooserParams) {
                if (pendingFileChooserCallback != null) {
                    pendingFileChooserCallback.onReceiveValue(null);
                }
                pendingFileChooserCallback = filePathCallback;
                Intent intent = new Intent(Intent.ACTION_OPEN_DOCUMENT);
                intent.addCategory(Intent.CATEGORY_OPENABLE);
                intent.setType("*/*");
                intent.putExtra(Intent.EXTRA_MIME_TYPES, new String[]{
                        "application/json", "text/json", "application/octet-stream", "*/*"
                });
                intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION);
                try {
                    startActivityForResult(intent, GLOBAL_BACKUP_FILE_REQUEST);
                    return true;
                } catch (Exception e) {
                    pendingFileChooserCallback = null;
                    filePathCallback.onReceiveValue(null);
                    return false;
                }
            }
        });
        webView.loadUrl(LOCAL_INDEX_URL);

        handler.postDelayed(
                this::updateWebViewNetworkStatus,
                1000
        );
    }

    private void requestNotificationPermissionIfNeeded() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU
                && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS)
                != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(
                    new String[]{Manifest.permission.POST_NOTIFICATIONS},
                    1201
            );
        }
    }

    private void startNetworkMonitorService() {
        Intent serviceIntent =
                new Intent(this, NetworkMonitorService.class);

        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                startForegroundService(serviceIntent);
            } else {
                startService(serviceIntent);
            }
        } catch (Exception ignored) {
        }
    }

    private void setupNetworkManager() {
        connectivityManager =
                (ConnectivityManager) getSystemService(
                        Context.CONNECTIVITY_SERVICE
                );

        registerNetworkCallback();
    }

    private boolean isInternetAvailable() {
        if (connectivityManager == null) {
            return false;
        }

        Network network = connectivityManager.getActiveNetwork();
        if (network == null) {
            return false;
        }

        NetworkCapabilities capabilities =
                connectivityManager.getNetworkCapabilities(network);

        if (capabilities == null) {
            return false;
        }

        return capabilities.hasCapability(
                NetworkCapabilities.NET_CAPABILITY_INTERNET
        ) && capabilities.hasCapability(
                NetworkCapabilities.NET_CAPABILITY_VALIDATED
        );
    }

    private void updateWebViewNetworkStatus() {
        if (webView == null) {
            return;
        }

        boolean online = isInternetAvailable();

        String javascript =
                "window.dispatchEvent(" +
                "new CustomEvent('androidNetworkStatus', {" +
                "detail: {online: " + online + "}" +
                "})" +
                ");";

        webView.evaluateJavascript(javascript, null);
    }

    private void registerNetworkCallback() {
        if (connectivityManager == null) {
            return;
        }

        NetworkRequest request =
                new NetworkRequest.Builder()
                        .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                        .build();

        networkCallback = new ConnectivityManager.NetworkCallback() {
            @Override
            public void onAvailable(Network network) {
                handler.post(MainActivity.this::updateWebViewNetworkStatus);
            }

            @Override
            public void onLost(Network network) {
                handler.post(MainActivity.this::updateWebViewNetworkStatus);
            }

            @Override
            public void onCapabilitiesChanged(
                    Network network,
                    NetworkCapabilities capabilities
            ) {
                handler.post(MainActivity.this::updateWebViewNetworkStatus);
            }
        };

        try {
            connectivityManager.registerNetworkCallback(
                    request,
                    networkCallback
            );
        } catch (Exception ignored) {
            networkCallback = null;
        }
    }


    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode == GLOBAL_BACKUP_FILE_REQUEST) {
            if (pendingFileChooserCallback != null) {
                Uri[] results = null;
                if (resultCode == RESULT_OK && data != null && data.getData() != null) {
                    results = new Uri[]{data.getData()};
                }
                pendingFileChooserCallback.onReceiveValue(results);
                pendingFileChooserCallback = null;
            }
        }
    }

    @Override
    public void onBackPressed() {
        if (webView != null && webView.canGoBack()) {
            webView.goBack();
        } else {
            super.onBackPressed();
        }
    }

    @Override
    protected void onDestroy() {
        if (connectivityManager != null && networkCallback != null) {
            try {
                connectivityManager.unregisterNetworkCallback(networkCallback);
            } catch (Exception ignored) {
            }
        }

        networkCallback = null;
        handler.removeCallbacksAndMessages(null);

        if (pendingFileChooserCallback != null) {
            pendingFileChooserCallback.onReceiveValue(null);
            pendingFileChooserCallback = null;
        }

        if (webView != null) {
            activeWebView = null;
            webView.stopLoading();
            webView.loadUrl("about:blank");
            webView.destroy();
            webView = null;
        }

        super.onDestroy();
    }
}
