package com.financial;

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.appwidget.AppWidgetManager;
import android.appwidget.AppWidgetProvider;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.os.Build;
import android.os.SystemClock;
import android.os.Handler;
import android.os.Looper;
import android.text.Html;
import android.widget.RemoteViews;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UltraCompactWidget extends AppWidgetProvider {

    private static final String ACTION_WIDGET_UPDATE =
            "com.financial.ACTION_ULTRA_COMPACT_UPDATE";
    private static final String ACTION_WIDGET_ROTATE =
            "com.financial.ACTION_ULTRA_COMPACT_ROTATE";
    private static final String ACTION_WIDGET_SELECT_GLOBAL =
            "com.financial.ACTION_ULTRA_COMPACT_SELECT_GLOBAL";
    private static final String EXTRA_GLOBAL_TYPE = "global_type";
    private static final String EXTRA_GLOBAL_SYMBOL = "global_symbol";
    private static final String GOLD_URL = "https://biquote.io/api/latest?symbols=XAUUSD";
    private static final String TSE_URL = "https://tsetmc.rxw1.workers.dev/";
    private static final String TSE_FALLBACK_URL = "https://ropmwamrcvvuwrvajxdr.supabase.co/functions/v1/dynamic-service/";
    private static final String TSE_ACTIVE_API_PREF = "tsetmc_active_api";
    private static final String GLOBAL_PREFS = "global_pulse_settings";

    private static final long WIDGET_REFRESH_MS = 10 * 1000L;
    private static final long WIDGET_ROTATE_MS = 5 * 1000L;

    private static final String SIGNAL_URL =
            "https://signalmatrix.rxw1.workers.dev/";
    private static final String SIGNAL_FALLBACK_URL =
            "https://ropmwamrcvvuwrvajxdr.supabase.co/functions/v1/clever-handler/";
    private static volatile int signalMatrixActiveApiIndex = 0; // 0=worker, 1=Supabase

    private static final String DATA_URL =
            "https://test1800.github.io/data.json";

    // Keep the native widget on the same source order as index.html.
    private static final String[] USD_URLS = new String[]{
            "https://usdd.rxw.workers.dev/",
            "https://proxy.cors.dev/https://usdd.rxw.workers.dev/",
            "https://yekusd-rateusd.rxw.workers.dev/",
            "https://proxy.cors.dev/https://yekusd-rateusd.rxw.workers.dev/"
    };
    private static final String[] YEKUSD_URLS = new String[]{
            "https://bahausd.rxw.workers.dev/",
            "https://proxy.cors.dev/https://bahausd.rxw.workers.dev/"
    };
    private static final String NOBITEX_ORDERBOOK_API =
            "https://apiv2.nobitex.ir/v3/orderbook/USDTIRT";
    private static final String EXIR_WORKER_PRICE_API =
            "https://tether.fridnika.workers.dev/";
    private static final String EXIR_TICKER_API =
            "https://api.exir.io/v2/ticker?symbol=usdt-irt";
    private static final String EXIR_ORDERBOOK_API =
            "https://api.exir.io/v2/orderbook?symbol=usdt-irt";
    private static final String BITPIN_ORDERBOOK_API =
            "https://api.bitpin.org/api/v1/mth/orderbook/USDT_IRT/";

    private static final String CRYPTO_URL =
            "https://coin.rxw.workers.dev/";

    private static final ExecutorService EXECUTOR =
            Executors.newCachedThreadPool();

    private static final String PREFS = "ultra_widget_cache";
    private static final String KEY_DATA = "last_good_data";
    // Signal Matrix has its own live channel so the periodic widget network
    // refresh can never overwrite a newer value pushed by the dashboard.
    private static final String LIVE_SIGNAL_PREFS = "ultra_widget_live_signal";
    private static final String KEY_LIVE_SIGNAL_HTML = "signal_html";
    private static final String KEY_LIVE_SIGNAL_AT = "signal_at";
    private static final String CROSSOVER_WIDGET_PREFS = "widget_crossover_cache";
    private static final String KEY_CROSSOVER_HTML = "crossover_html";
    private static final String KEY_CROSSOVER_SIGNAL = "crossover_signal";
    private static final String DAILY_TINT_PREFS = "ultra_widget_daily_tint";
    private static final String KEY_SIGNAL_DAILY = "signal_daily";
    private static final String KEY_CROSSOVER_DAILY = "crossover_daily";
    private static final String KEY_GLOBAL_DAILY = "global_daily";

    @Override
    public void onEnabled(Context context) {
        super.onEnabled(context);
        scheduleRefresh(context);
        scheduleRotation(context);
        updateAll(context);
    }

    @Override
    public void onDisabled(Context context) {
        cancelRefresh(context);
        cancelRotation(context);
        super.onDisabled(context);
    }

    @Override
    public void onReceive(Context context, Intent intent) {
        super.onReceive(context, intent);
        if (ACTION_WIDGET_UPDATE.equals(intent.getAction())) {
            updateAll(context);
            // Schedule the next 10-second data refresh after every alarm delivery.
            scheduleRefresh(context);
        } else if (ACTION_WIDGET_ROTATE.equals(intent.getAction())) {
            rotateAll(context);
            scheduleRotation(context);
        } else if (ACTION_WIDGET_SELECT_GLOBAL.equals(intent.getAction())) {
            String type = intent.getStringExtra(EXTRA_GLOBAL_TYPE);
            String symbol = intent.getStringExtra(EXTRA_GLOBAL_SYMBOL);
            selectGlobalAsset(context, type, symbol);
            updateAll(context);
        }
    }

    @Override
    public void onUpdate(
            Context context,
            AppWidgetManager manager,
            int[] appWidgetIds
    ) {
        updateAll(context);
        scheduleRefresh(context);
        scheduleRotation(context);
    }

    /**
     * Immediate, event-driven widget update from values that the main WebView
     * has just rendered. No network request is made here.
     */
    public static void updateFromNativeSnapshot(Context context, String json) {
        final Context app = context.getApplicationContext();
        EXECUTOR.execute(() -> {
            try {
                WidgetData data = readCachedData(app);
                JSONObject o = new JSONObject(json);
                data.usd = o.optString("usd", data.usd);
                data.yekusd = o.optString("yekusd", data.yekusd);
                String incomingCrypto = formatWidgetCrypto(o.optString("crypto", "").trim());
                // Never erase a valid cached Crypto total with a transient empty/- snapshot.
                if (!incomingCrypto.isEmpty() && !"-".equals(incomingCrypto)) {
                    data.crypto = incomingCrypto;
                }
                String oldNobitex = data.nobitex, oldExir = data.exir, oldBitpin = data.bitpin;
                String incomingNobitex = o.optString("nobitex", "").trim();
                String incomingExir = o.optString("exir", "").trim();
                String incomingBitpin = o.optString("bitpin", "").trim();
                if (isUsablePriceText(incomingNobitex)) data.nobitex = incomingNobitex;
                if (isUsablePriceText(incomingExir)) data.exir = incomingExir;
                if (isUsablePriceText(incomingBitpin)) data.bitpin = incomingBitpin;
                int nd = o.optInt("nobitexDirection", 0);
                int ed = o.optInt("exirDirection", 0);
                int bd = o.optInt("bitpinDirection", 0);
                data.nobitexDirection = snapshotPriceDirection(oldNobitex, data.nobitex, data.nobitexDirection, nd);
                data.exirDirection = snapshotPriceDirection(oldExir, data.exir, data.exirDirection, ed);
                data.bitpinDirection = snapshotPriceDirection(oldBitpin, data.bitpin, data.bitpinDirection, bd);
                data.updated = o.optString("updated", data.updated);
                String incomingSignal = o.optString("signalHtml", "").trim();
                long incomingSignalAt = o.optLong("signalUpdatedAt", System.currentTimeMillis());
                // Never overwrite the last good Signal Matrix value with an empty
                // snapshot while the WebView is between two worker responses.
                // Also reject an older queued snapshot: WebView callbacks can
                // arrive faster than the native executor consumes them.
                if (!incomingSignal.isEmpty() && !"-".equals(incomingSignal)) {
                    long storedAt = readLiveSignalAt(app);
                    if (incomingSignalAt >= storedAt) {
                        saveLiveSignalHtml(app, incomingSignal, incomingSignalAt);
                        data.signalHtml = incomingSignal;
                    }
                }
                // Always prefer the newest live Signal Matrix value.
                String liveSignal = readLiveSignalHtml(app);
                if (!liveSignal.isEmpty() && !"-".equals(liveSignal)) {
                    data.signalHtml = liveSignal;
                }
                try {
                    android.content.SharedPreferences tintPrefs = app.getSharedPreferences(DAILY_TINT_PREFS, Context.MODE_PRIVATE);
                    android.content.SharedPreferences.Editor tintEdit = tintPrefs.edit();
                    if (o.has("signalDailyChangePercent")) {
                        tintEdit.putString(KEY_SIGNAL_DAILY, o.isNull("signalDailyChangePercent") ? "" : o.optString("signalDailyChangePercent", ""));
                    }
                    if (o.has("crossoverDailyChangePercent")) {
                        tintEdit.putString(KEY_CROSSOVER_DAILY, o.isNull("crossoverDailyChangePercent") ? "" : o.optString("crossoverDailyChangePercent", ""));
                    }
                    if (o.has("globalAssetDailyChangePercent")) {
                        tintEdit.putString(KEY_GLOBAL_DAILY, o.isNull("globalAssetDailyChangePercent") ? "" : o.optString("globalAssetDailyChangePercent", ""));
                    }
                    tintEdit.apply();
                } catch (Throwable ignored) {}
                if (o.has("globalAssetPrice")) data.globalAssetPrice = o.optString("globalAssetPrice", data.globalAssetPrice);
                if (o.has("globalAssetType")) data.globalAssetType = o.optString("globalAssetType", data.globalAssetType);
                if (o.has("globalAssetSymbol")) data.globalAssetSymbol = o.optString("globalAssetSymbol", data.globalAssetSymbol);
                data.globalAssetFlashDirection = o.optInt("globalAssetFlashDirection", 0);
                writeCachedData(app, data);
                AppWidgetManager manager = AppWidgetManager.getInstance(app);
                ComponentName component = new ComponentName(app, UltraCompactWidget.class);
                int[] ids = manager.getAppWidgetIds(component);
                for (int id : ids) render(app, manager, id, data);
            } catch (Throwable ignored) {
                // Never let a widget sync failure interrupt the main dashboard.
            }
        });
    }

    private static void saveLiveSignalHtml(Context context, String signalHtml, long signalAt) {
        if (signalHtml == null || signalHtml.trim().isEmpty() || "-".equals(signalHtml.trim())) return;
        try {
            context.getApplicationContext()
                    .getSharedPreferences(LIVE_SIGNAL_PREFS, Context.MODE_PRIVATE)
                    .edit()
                    .putString(KEY_LIVE_SIGNAL_HTML, signalHtml)
                    .putLong(KEY_LIVE_SIGNAL_AT, signalAt)
                    .apply();
        } catch (Throwable ignored) {}
    }

    private static long readLiveSignalAt(Context context) {
        try {
            return context.getApplicationContext()
                    .getSharedPreferences(LIVE_SIGNAL_PREFS, Context.MODE_PRIVATE)
                    .getLong(KEY_LIVE_SIGNAL_AT, 0L);
        } catch (Throwable ignored) {
            return 0L;
        }
    }

    private static String readLiveSignalHtml(Context context) {
        try {
            return context.getApplicationContext()
                    .getSharedPreferences(LIVE_SIGNAL_PREFS, Context.MODE_PRIVATE)
                    .getString(KEY_LIVE_SIGNAL_HTML, "");
        } catch (Throwable ignored) {
            return "";
        }
    }

    private static void mergeLiveSignalHtml(Context context, WidgetData data) {
        if (data == null) return;
        // A native background fetch has priority over the last WebView snapshot.
        // The old implementation always preferred the WebView cache here, which
        // made Signal Matrix appear frozen as soon as the app was minimized.
        String current = data.signalHtml == null ? "" : data.signalHtml.trim();
        if (!current.isEmpty() && !"-".equals(current)) return;
        String live = readLiveSignalHtml(context);
        if (!live.isEmpty() && !"-".equals(live)) data.signalHtml = live;
    }

    /**
     * Fast Signal Matrix-only refresh used by the background monitor.
     * It deliberately does NOT run loadData(), because that method also polls
     * USD/Crypto/other sources. Signal Matrix can therefore be refreshed much
     * faster without delaying the widget or hammering unrelated APIs.
     */
    public static void updateSignalMatrixOnly(Context context) {
        final Context app = context.getApplicationContext();
        EXECUTOR.execute(() -> {
            try {
                WidgetData data = readCachedData(app);
                String coin = "";
                String timeframe = "1";
                boolean running = false;

                String config = MainActivity.getSignalMatrixStateJson(app);
                if (config != null && !config.trim().isEmpty()) {
                    JSONObject root = new JSONObject(config);
                    JSONArray matrices = root.optJSONArray("matrices");
                    JSONObject m = matrices == null ? null : matrices.optJSONObject(0);
                    if (m != null) {
                        coin = m.optString("coin", "").trim().toUpperCase(Locale.ROOT);
                        running = m.optBoolean("running", false);
                        JSONArray tfs = m.optJSONArray("timeframes");
                        if (tfs != null && tfs.length() > 0) {
                            String best = null;
                            int bestRank = Integer.MAX_VALUE;
                            for (int i = 0; i < tfs.length(); i++) {
                                String candidate = tfs.optString(i, "1");
                                int rank = timeframeRank(candidate);
                                if (rank < bestRank) { bestRank = rank; best = candidate; }
                            }
                            if (best != null && !best.isEmpty()) timeframe = best;
                        }
                    }
                }

                if (coin.isEmpty()) {
                    android.content.SharedPreferences legacy = app.getSharedPreferences(
                            "financial_native_state", Context.MODE_PRIVATE);
                    if (legacy.getBoolean("signal_matrix_enabled", false)) {
                        coin = legacy.getString("signal_matrix_coin", "")
                                .trim().toUpperCase(Locale.ROOT);
                        timeframe = legacy.getString("signal_matrix_timeframe", "1").trim();
                        running = !coin.isEmpty();
                    }
                }

                if (!running || coin.isEmpty()) return;

                String query = "?coin=" + URLEncoder.encode(coin, "UTF-8")
                        + "&timeframe=" + URLEncoder.encode(timeframe, "UTF-8")
                        + "&t=" + System.currentTimeMillis();
                JSONObject signal = new JSONObject(httpGetSignalMatrixWithFailover(query));

                WidgetData.MatrixData m1 = data.matrices[0];
                m1.coin = coin;
                m1.timeframe = timeframe;
                m1.selected = true;
                m1.running = true;
                m1.rf = normalize(signal.opt("RF"));
                m1.st = normalize(signal.opt("ST"));
                m1.kj = normalize(signal.opt("KJ"));

                double price = signal.optDouble("price", Double.NaN);
                try {
                    CryptoPriceProvider.PriceData unified = CryptoPriceProvider.get(app, coin);
                    if (Double.isFinite(unified.price) && unified.price > 0) {
                        price = unified.price;
                    }
                } catch (Exception priceError) {
                    // Keep the worker price as an in-memory last-resort value.
                }
                if (!Double.isNaN(price) && price > 0) {
                    m1.signalPrice = formatCryptoPrice(price, coin);
                    data.signalPrice = m1.signalPrice;
                }
                data.coin = coin;
                data.timeframe = timeframe;
                data.hasSelectedCoin = true;
                data.signalHtml = signalHtml(m1, data.tseRows);

                long now = System.currentTimeMillis();
                saveLiveSignalHtml(app, data.signalHtml, now);
                writeCachedData(app, data);

                AppWidgetManager manager = AppWidgetManager.getInstance(app);
                ComponentName component = new ComponentName(app, UltraCompactWidget.class);
                int[] ids = manager.getAppWidgetIds(component);
                for (int id : ids) render(app, manager, id, data);
            } catch (Throwable ignored) {
                // Keep the last valid Signal Matrix value on any transient error.
            }
        });
    }

    /** Refresh only the rendered widget from its latest persisted Crossover state. */
    public static void refreshCrossoverDisplay(Context context) {
        if (context == null) return;
        final Context app = context.getApplicationContext();
        EXECUTOR.execute(() -> {
            try {
                WidgetData data = readCachedData(app);
                mergeLiveSignalHtml(app, data);
                AppWidgetManager manager = AppWidgetManager.getInstance(app);
                ComponentName component = new ComponentName(app, UltraCompactWidget.class);
                int[] ids = manager.getAppWidgetIds(component);
                for (int id : ids) render(app, manager, id, data);
            } catch (Throwable ignored) {
                // Widget refresh must never interfere with background monitoring.
            }
        });
    }

    public static void updateAll(Context context) {
        final Context app = context.getApplicationContext();

        // Cache-first: paint the last valid widget values immediately after a
        // refresh/reopen. The network refresh below then replaces them with the
        // newest values as soon as the workers respond.
        try {
            WidgetData cached = readCachedData(app);
            mergeLiveSignalHtml(app, cached);
            AppWidgetManager manager = AppWidgetManager.getInstance(app);
            ComponentName component =
                    new ComponentName(app, UltraCompactWidget.class);
            int[] ids = manager.getAppWidgetIds(component);
            for (int id : ids) {
                render(app, manager, id, cached);
            }
        } catch (Throwable ignored) {
            // Continue to the fresh network update.
        }

        EXECUTOR.execute(() -> {
            WidgetData data = loadData(app);
            mergeLiveSignalHtml(app, data);
            AppWidgetManager manager = AppWidgetManager.getInstance(app);
            ComponentName component =
                    new ComponentName(app, UltraCompactWidget.class);
            int[] ids = manager.getAppWidgetIds(component);

            for (int id : ids) {
                render(app, manager, id, data);
            }
        });
    }

    private static void rotateAll(Context context) {
        final Context app = context.getApplicationContext();
        EXECUTOR.execute(() -> {
            WidgetData data = readCachedData(app);
            mergeLiveSignalHtml(app, data);
            AppWidgetManager manager = AppWidgetManager.getInstance(app);
            ComponentName component = new ComponentName(app, UltraCompactWidget.class);
            int[] ids = manager.getAppWidgetIds(component);
            for (int id : ids) render(app, manager, id, data);
        });
    }

    private static double readDailyPercent(Context context, String key) {
        try {
            String value = context.getApplicationContext().getSharedPreferences(DAILY_TINT_PREFS, Context.MODE_PRIVATE).getString(key, "");
            if (value == null || value.trim().isEmpty()) return Double.NaN;
            double parsed = Double.parseDouble(value.trim());
            return Double.isFinite(parsed) ? parsed : Double.NaN;
        } catch (Throwable ignored) { return Double.NaN; }
    }

    private static void setDailyTint(RemoteViews views, int viewId, double dailyPercent) {
        // Widget numbers stay transparent: only a rounded, color-coded outline remains.
        int background = !Double.isFinite(dailyPercent) || dailyPercent == 0.0
                ? com.financial.R.drawable.widget_number_neutral
                : dailyPercent > 0
                    ? com.financial.R.drawable.widget_number_positive
                    : com.financial.R.drawable.widget_number_negative;
        try { views.setInt(viewId, "setBackgroundResource", background); } catch (Throwable ignored) {}
    }

    private static void setCrossoverDailyTint(RemoteViews views, int viewId, double dailyPercent) {
        // Preserve the red -> amber -> green -3% ... +3% logic without filling the number's background.
        int background;
        if (!Double.isFinite(dailyPercent)) {
            background = com.financial.R.drawable.widget_number_neutral;
        } else {
            double v = Math.max(-3.0, Math.min(3.0, dailyPercent));
            if (v <= -2.0) background = com.financial.R.drawable.widget_number_red;
            else if (v <= -1.0) background = com.financial.R.drawable.widget_number_orange_red;
            else if (v < 0.0) background = com.financial.R.drawable.widget_number_amber;
            else if (v == 0.0) background = com.financial.R.drawable.widget_number_neutral;
            else if (v < 1.0) background = com.financial.R.drawable.widget_number_yellow_green;
            else if (v < 2.0) background = com.financial.R.drawable.widget_number_lime;
            else background = com.financial.R.drawable.widget_number_green;
        }
        try { views.setInt(viewId, "setBackgroundResource", background); } catch (Throwable ignored) {}
    }

    private static void render(
            Context context,
            AppWidgetManager manager,
            int widgetId,
            WidgetData data
    ) {
        RemoteViews views = new RemoteViews(
                context.getPackageName(),
                com.financial.R.layout.ultra_compact_widget
        );
        WidgetData.MatrixData matrix = data.currentMatrix();

        views.setTextViewText(R.id.widgetUsd, safe(data.usd));
        views.setTextViewText(R.id.widgetYekUsd, safe(data.yekusd));
        // Crypto Total is a widget-only value: always normalize it to an integer
        // with thousands separators, regardless of which source updated the cache.
        views.setTextViewText(R.id.widgetCrypto, safe(formatWidgetCrypto(data.crypto)));
        // B03: distinct bright-white tones for each number family; no gray.
        views.setTextColor(R.id.widgetUsd, 0xFFFFFFFF);       // USD: pure white
        views.setTextColor(R.id.widgetYekUsd, 0xFFE5E7EB);    // YEK/USD: soft white
        views.setTextColor(R.id.widgetCrypto, 0xFFE0F2FE);    // Crypto: cool white
        views.setTextColor(R.id.widgetSignal, 0xFFF1F5FF);    // Signal Matrix: cool/lavender white

                CrossoverWidgetData crossoverWidget = readCrossoverWidgetData(context, data.tseRows);
        // The widget priority is fixed to Signal Matrix slot #1, not the rotating
        // currentMatrix() result. Otherwise Matrix #2/#3 could hide Crossover even
        // while the first Matrix is stopped, and cached arrows from an old Matrix
        // could remain visible.
        WidgetData.MatrixData firstMatrix = (data.matrices != null && data.matrices.length > 0)
                ? data.matrices[0] : null;
        // State can change in the WebView before the next background/network
        // refresh updates WidgetData. Resolve Matrix #1's running state from the
        // newest persisted configuration on every render, so an old Crossover
        // value cannot win a race immediately after Matrix #1 is started.
        refreshFirstMatrixDisplayState(context, firstMatrix);
        // Running + a configured coin is authoritative for Matrix #1. Some older
        // persisted snapshots omit selectedCoin even though Matrix #1 is running;
        // requiring selected here would incorrectly leave Crossover on screen.
        // Matrix running state always wins over Crossover. Do not require a
        // selected marker or non-empty cached coin: older states may omit them.
        boolean firstMatrixActive = firstMatrix != null && firstMatrix.running;
        String liveSignalHtml;
        if (firstMatrixActive) {
            // When Matrix #1 is active, always render its latest values and arrows.
            liveSignalHtml = signalHtml(firstMatrix, data.tseRows);
        } else if (crossoverWidget.signalHtml != null && !crossoverWidget.signalHtml.trim().isEmpty()) {
            // If Matrix #1 is inactive, use the first active Crossover's coin and
            // price only. readCrossoverWidgetData intentionally adds no arrows.
            liveSignalHtml = crossoverWidget.signalHtml;
        } else {
            // Do not leak a stale Signal Matrix value when neither source is active.
            liveSignalHtml = "-";
        }
        views.setTextViewText(
                R.id.widgetSignal,
                Html.fromHtml(liveSignalHtml == null ? "" : liveSignalHtml, Html.FROM_HTML_MODE_LEGACY)
        );
        // Tint the displayed number using the asset's daily percentage, never its last-tick direction.
        boolean showingMatrix = firstMatrixActive;
        double signalDaily = readDailyPercent(context, showingMatrix ? KEY_SIGNAL_DAILY : KEY_CROSSOVER_DAILY);
        if (showingMatrix) setDailyTint(views, R.id.widgetSignal, signalDaily);
        else setCrossoverDailyTint(views, R.id.widgetSignal, signalDaily);

        views.setTextViewText(R.id.widgetNobitex, safe(data.nobitex));
        views.setTextViewText(R.id.widgetExir, safe(data.exir));
        views.setTextViewText(R.id.widgetBitpin, safe(data.bitpin));
        setPriceArrow(views, R.id.widgetNobitexArrow, data.nobitexDirection);
        setPriceArrow(views, R.id.widgetExirArrow, data.exirDirection);
        setPriceArrow(views, R.id.widgetBitpinArrow, data.bitpinDirection);

        views.setTextViewText(
                R.id.widgetUpdated,
                data.updated == null ? "● Ultra Compact" : "● " + data.updated
        );

        // Crossover percentages are cached locally so a transient state read can never
        // make the widget blink empty. All active Crossovers are shown side-by-side.
        if (crossoverWidget.visible) {
            views.setTextViewText(
                    R.id.widgetCrossoverPercent,
                    Html.fromHtml(crossoverWidget.html, Html.FROM_HTML_MODE_LEGACY)
            );
            views.setViewVisibility(R.id.widgetCrossoverPercent, android.view.View.VISIBLE);
        } else {
            views.setViewVisibility(R.id.widgetCrossoverPercent, android.view.View.GONE);
        }

        Intent open = new Intent(context, MainActivity.class);
        open.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK |
                Intent.FLAG_ACTIVITY_CLEAR_TOP);
        PendingIntent pending;
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            pending = PendingIntent.getActivity(
                    context,
                    widgetId,
                    open,
                    PendingIntent.FLAG_UPDATE_CURRENT |
                            PendingIntent.FLAG_IMMUTABLE
            );
        } else {
            pending = PendingIntent.getActivity(
                    context,
                    widgetId,
                    open,
                    PendingIntent.FLAG_UPDATE_CURRENT
            );
        }
        // Make the entire widget open the main application.
        views.setOnClickPendingIntent(R.id.widgetRoot, pending);

        setGlobalSelectorClick(context, views, widgetId, "gold", "");
        setGlobalSelectorClick(context, views, widgetId, "oil", "");
        setGlobalSelectorClick(context, views, widgetId, "tse", "");
        boolean hasAsset = data.globalAssetType != null && !data.globalAssetType.isEmpty();
        views.setViewVisibility(R.id.widgetGlobalAssetPrice, hasAsset ? android.view.View.VISIBLE : android.view.View.GONE);
        if (!hasAsset) {
            views.setTextColor(R.id.widgetGlobalAssetPrice, 0xFFF8FAFC);
            setDailyTint(views, R.id.widgetGlobalAssetPrice, Double.NaN);
        }
        if (hasAsset) {
            views.setTextViewText(R.id.widgetGlobalAssetPrice, safe(data.globalAssetPrice));
            views.setTextColor(R.id.widgetGlobalAssetPrice, 0xFFF8FAFC);
            setDailyTint(views, R.id.widgetGlobalAssetPrice, readDailyPercent(context, KEY_GLOBAL_DAILY));
            views.setTextViewTextSize(R.id.widgetNobitex, android.util.TypedValue.COMPLEX_UNIT_SP, 19f);
            views.setTextViewTextSize(R.id.widgetExir, android.util.TypedValue.COMPLEX_UNIT_SP, 19f);
            views.setTextViewTextSize(R.id.widgetBitpin, android.util.TypedValue.COMPLEX_UNIT_SP, 19f);
        } else {
            views.setTextViewTextSize(R.id.widgetNobitex, android.util.TypedValue.COMPLEX_UNIT_SP, 24f);
            views.setTextViewTextSize(R.id.widgetExir, android.util.TypedValue.COMPLEX_UNIT_SP, 24f);
            views.setTextViewTextSize(R.id.widgetBitpin, android.util.TypedValue.COMPLEX_UNIT_SP, 24f);
        }
        views.setTextColor(R.id.widgetGlobalGoldButton, "gold".equals(data.globalAssetType) ? 0xFF34D399 : 0xFF64748B);
        views.setTextColor(R.id.widgetGlobalOilButton, "oil".equals(data.globalAssetType) ? 0xFF34D399 : 0xFF64748B);
        views.setTextColor(R.id.widgetGlobalTseButton, "tse".equals(data.globalAssetType) ? 0xFF34D399 : 0xFF64748B);

        manager.updateAppWidget(widgetId, views);
        if (hasAsset && data.globalAssetFlashDirection != 0) {
            new Handler(Looper.getMainLooper()).postDelayed(() -> {
                views.setTextColor(R.id.widgetGlobalAssetPrice, 0xFFF8FAFC);
                manager.updateAppWidget(widgetId, views);
            }, 650L);
        }
    }

    private static void setGlobalSelectorClick(Context context, RemoteViews views, int widgetId, String type, String symbol) {
        Intent intent = new Intent(context, UltraCompactWidget.class);
        intent.setAction(ACTION_WIDGET_SELECT_GLOBAL);
        intent.putExtra(EXTRA_GLOBAL_TYPE, type);
        if (symbol != null) intent.putExtra(EXTRA_GLOBAL_SYMBOL, symbol);
        int requestCode = 3000 + type.hashCode() + widgetId;
        int flags = PendingIntent.FLAG_UPDATE_CURRENT;
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) flags |= PendingIntent.FLAG_IMMUTABLE;
        PendingIntent pi = PendingIntent.getBroadcast(context, requestCode, intent, flags);
        int id = "gold".equals(type) ? R.id.widgetGlobalGoldButton : "oil".equals(type) ? R.id.widgetGlobalOilButton : R.id.widgetGlobalTseButton;
        views.setOnClickPendingIntent(id, pi);
    }

    private static void selectGlobalAsset(Context context, String type, String symbol) {
        if (type == null) type = "";
        android.content.SharedPreferences sp = context.getSharedPreferences(GLOBAL_PREFS, Context.MODE_PRIVATE);
        String currentType = sp.getString("compact_asset_type", "");
        String currentSymbol = sp.getString("compact_asset_symbol", "");
        if (type.equals(currentType) && (!"tse".equals(type) || (symbol != null && !symbol.trim().isEmpty() && symbol.trim().equals(currentSymbol)))) {
            sp.edit().putString("compact_asset_type", "").putString("compact_asset_symbol", "").apply();
            return;
        }
        if ("tse".equals(type) && (symbol == null || symbol.trim().isEmpty())) {
            WidgetData cached = readCachedData(context);
            if (!cached.tseSymbols.isEmpty()) {
                String current = sp.getString("compact_asset_symbol", "");
                int next = 0;
                for (int i = 0; i < cached.tseSymbols.size(); i++) if (cached.tseSymbols.get(i).equals(current)) { next = (i + 1) % cached.tseSymbols.size(); break; }
                symbol = cached.tseSymbols.get(next);
            }
        }
        sp.edit().putString("compact_asset_type", type).putString("compact_asset_symbol", symbol == null ? "" : symbol.trim()).apply();
    }

    private static void refreshFirstMatrixDisplayState(Context context, WidgetData.MatrixData firstMatrix) {
        if (context == null || firstMatrix == null) return;
        try {
            String config = MainActivity.getSignalMatrixStateJson(context);
            if (config == null || config.trim().isEmpty()) return;
            JSONObject root = new JSONObject(config);
            JSONArray matrices = root.optJSONArray("matrices");
            JSONObject matrix = matrices == null ? null : matrices.optJSONObject(0);
            if (matrix == null) return;

            String configuredCoin = matrix.optString("coin", "").trim().toUpperCase(Locale.ROOT);
            boolean running = matrix.optBoolean("running", false);
            boolean selected = matrix.optBoolean("selectedCoin", false);
            if (running && configuredCoin.isEmpty()) {
                configuredCoin = firstMatrix.coin == null || firstMatrix.coin.trim().isEmpty()
                        || "-".equals(firstMatrix.coin.trim()) ? "BTC" : firstMatrix.coin.trim().toUpperCase(Locale.ROOT);
            }

            // If the coin changed since the last widget cache, never pair the new
            // coin label with the previous coin's saved price/arrows.
            if (!configuredCoin.isEmpty()
                    && firstMatrix.coin != null
                    && !configuredCoin.equalsIgnoreCase(firstMatrix.coin.trim())) {
                firstMatrix.signalPrice = "-";
                firstMatrix.rf = 0;
                firstMatrix.st = 0;
                firstMatrix.kj = 0;
            }
            if (!configuredCoin.isEmpty()) firstMatrix.coin = configuredCoin;
            firstMatrix.running = running;
            firstMatrix.selected = selected || running || !configuredCoin.isEmpty();

            JSONArray timeframes = matrix.optJSONArray("timeframes");
            if (timeframes != null && timeframes.length() > 0) {
                String best = null;
                int bestRank = Integer.MAX_VALUE;
                for (int i = 0; i < timeframes.length(); i++) {
                    String candidate = timeframes.optString(i, "1");
                    int rank = timeframeRank(candidate);
                    if (rank < bestRank) { bestRank = rank; best = candidate; }
                }
                if (best != null && !best.isEmpty()) firstMatrix.timeframe = best;
            }
        } catch (Throwable ignored) {
            // A malformed/transient state read must not break widget rendering.
        }
    }

    private static String signalHtml(WidgetData.MatrixData d, List<WidgetData.TseRow> tseRows) {
        if (d == null || !d.selected || !d.running) {
            return "-";
        }
        return escape(widgetDisplayCoin(d.coin, tseRows)) + " " +
                escape(safe(d.signalPrice)) + " " +
                coloredArrow(d.rf) +
                coloredArrow(d.st) +
                coloredArrow(d.kj);
    }

    // Resolve the internal TSE|SLOTn identifier only for the Ultra Compact
    // widget's visible label. The stored coin/index remains unchanged.
    private static String widgetDisplayCoin(String coin, List<WidgetData.TseRow> tseRows) {
        String value = coin == null ? "" : coin.trim();
        java.util.regex.Matcher matcher = java.util.regex.Pattern
                .compile("(?i)^TSE\\|SLOT([1-4])$")
                .matcher(value);
        if (!matcher.matches()) return value;
        int slot = Integer.parseInt(matcher.group(1)) - 1;
        if (tseRows != null && slot >= 0 && slot < tseRows.size()) {
            String symbol = tseRows.get(slot).symbol;
            if (symbol != null && !symbol.trim().isEmpty()) return symbol.trim();
        }
        return "TSE";
    }

    private static String coloredArrow(int value) {
        String color;
        String symbol;
        if (value > 0) {
            color = "#FFFFFF";
            symbol = "↑";
        } else if (value < 0) {
            color = "#38BDF8";
            symbol = "↓";
        } else {
            color = "#64748B";
            symbol = "→";
        }
        return "<font color=\"" + color + "\">" + symbol + "</font>";
    }

    private static String arrow(int value) {
        return value > 0 ? "↑" : value < 0 ? "↓" : "→";
    }

    private static void setPriceArrow(RemoteViews views, int viewId, int direction) {
        String symbol;
        int color;
        if (direction > 0) {
            symbol = "↑";
            color = 0xFF34D399;
        } else if (direction < 0) {
            symbol = "↓";
            color = 0xFFF87171;
        } else {
            symbol = "→";
            color = 0xFF94A3B8;
        }
        views.setTextViewText(viewId, symbol);
        views.setTextColor(viewId, color);
        views.setTextViewTextSize(viewId, android.util.TypedValue.COMPLEX_UNIT_SP, 16f);
    }

    private static String priceHtml(String price, int direction) {
        String color;
        String symbol;
        if (direction > 0) {
            color = "#34D399";
            symbol = "↑";
        } else if (direction < 0) {
            color = "#F87171";
            symbol = "↓";
        } else {
            color = "#94A3B8";
            symbol = "→";
        }
        return "<b>" + escape(safe(price)) + "</b> " +
                "<font color=\"" + color + "\">" + symbol + "</font>";
    }

    private static int priceDirection(String previous, String current) {
        try {
            if (previous == null || current == null) return 0;
            double p = Double.parseDouble(previous.replace(",", "").trim());
            double c = Double.parseDouble(current.replace(",", "").trim());
            return c > p ? 1 : c < p ? -1 : 0;
        } catch (Exception ignored) {
            return 0;
        }
    }

    private static int snapshotPriceDirection(String previous, String current, int previousDirection, int incomingDirection) {
        if (incomingDirection != 0) return incomingDirection;
        int changedDirection = priceDirection(previous, current);
        // A dashboard snapshot can arrive between price ticks. Do not clear a
        // valid last movement arrow just because the formatted number is unchanged.
        return changedDirection != 0 ? changedDirection : previousDirection;
    }

    private static boolean isUsablePriceText(String value) {
        if (value == null) return false;
        String normalized = value.replace(",", "").trim();
        try { double number = Double.parseDouble(normalized); return Double.isFinite(number) && number > 0; }
        catch (Exception ignored) { return false; }
    }

    // Shared positive-price guard for API results. Reject NaN, infinities and
    // zero/negative values before updating the widget's cached prices.
    private static boolean isPositive(double value) {
        return !Double.isNaN(value) && !Double.isInfinite(value) && value > 0.0;
    }

    private static String safe(String value) {
        return value == null || value.trim().isEmpty() ? "-" : value;
    }

    private static String escape(String value) {
        if (value == null) return "";
        return value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }

    private static WidgetData loadData(Context context) {
        WidgetData out = readCachedData(context);
        // Keep the previously displayed exchange prices so the second-row arrows
        // can show the same direction logic as the dashboard.
        String previousNobitex = out.nobitex;
        String previousExir = out.exir;
        String previousBitpin = out.bitpin;
        String previousGlobalType = out.globalAssetType;
        String previousGlobalSymbol = out.globalAssetSymbol;
        String previousGlobalPrice = out.globalAssetPrice;
        boolean freshAny = false;

        String config =
                MainActivity.getSignalMatrixStateJson(context);
        try {
            if (config != null && !config.trim().isEmpty()) {
                JSONObject root = new JSONObject(config);
                JSONArray matrices = root.optJSONArray("matrices");
                if (matrices != null && matrices.length() > 0) {
                    for (int mi = 0; mi < Math.min(3, matrices.length()); mi++) {
                        JSONObject mm = matrices.optJSONObject(mi);
                        if (mm == null) continue;
                        WidgetData.MatrixData md = out.matrices[mi];
                        String matrixCoin = mm.optString("coin", "").trim().toUpperCase(Locale.ROOT);
                        boolean running = mm.optBoolean("running", false);
                        boolean selectedFlag = mm.optBoolean("selectedCoin", false);
                        // Treat a real saved coin as selected for Matrix #2/#3, and
                        // also recover Matrix #1 when an older app state has the
                        // coin saved but its selectedCoin flag is missing.
                        md.selected = selectedFlag || !matrixCoin.isEmpty() && (mi > 0 || running);
                        md.running = running;
                        md.coin = matrixCoin;
                        JSONArray mtfs = mm.optJSONArray("timeframes");
                        if (mtfs != null && mtfs.length() > 0) {
                            String best = null; int bestRank = Integer.MAX_VALUE;
                            for (int ti = 0; ti < mtfs.length(); ti++) {
                                String candidate = mtfs.optString(ti, "1");
                                int rank = timeframeRank(candidate);
                                if (rank < bestRank) { bestRank = rank; best = candidate; }
                            }
                            if (best != null && !best.isEmpty()) md.timeframe = best;
                        }
                    }
                    JSONObject matrix = matrices.optJSONObject(0);
                    if (matrix != null) {
                        boolean selected = matrix.optBoolean("selectedCoin", false);
                        boolean running = matrix.optBoolean("running", false);
                        String savedCoin = matrix.optString("coin", "")
                                .trim().toUpperCase(Locale.ROOT);
                        // Never invent BTC for a fresh/unselected Matrix #1.
                        // The widget starts showing the matrix coin only after the
                        // user has actually selected a coin in Matrix #1.
                        if ((selected || running) && !savedCoin.isEmpty()) {
                            out.coin = savedCoin;
                            out.hasSelectedCoin = true;
                            out.matrices[0].selected = true;
                        }
                        JSONArray tfs = matrix.optJSONArray("timeframes");
                        if (tfs != null && tfs.length() > 0) {
                            String best = null;
                            int bestRank = Integer.MAX_VALUE;
                            for (int i = 0; i < tfs.length(); i++) {
                                String candidate = tfs.optString(i, "1");
                                int rank = timeframeRank(candidate);
                                if (rank < bestRank) {
                                    bestRank = rank;
                                    best = candidate;
                                }
                            }
                            if (best != null && !best.isEmpty()) {
                                out.timeframe = best;
                            }
                        }
                    }
                }
            }
        } catch (Exception ignored) {
        }

        // Compatibility fallback for states saved by older Signal Matrix versions.
        // This keeps the home-screen widget alive even when the new JSON state was
        // not written yet after an app update.
        if (!out.hasSelectedCoin) {
            android.content.SharedPreferences legacy =
                    context.getApplicationContext().getSharedPreferences("financial_native_state", Context.MODE_PRIVATE);
            boolean legacyEnabled = legacy.getBoolean("signal_matrix_enabled", false);
            String legacyCoin = legacy.getString("signal_matrix_coin", "").trim().toUpperCase(Locale.ROOT);
            String legacyTimeframe = legacy.getString("signal_matrix_timeframe", "1").trim();
            if (legacyEnabled && !legacyCoin.isEmpty()) {
                out.coin = legacyCoin;
                out.hasSelectedCoin = true;
                out.matrices[0].selected = true;
                out.matrices[0].running = true;
                if (!legacyTimeframe.isEmpty()) out.timeframe = legacyTimeframe;
            }
        }
        if (!out.hasSelectedCoin) {
            // No selected Matrix #1 coin yet: leave the widget neutral instead of
            // displaying BTC by default.
            out.coin = "-";
            out.signalPrice = "-";
        }
        if (out.timeframe == null || out.timeframe.isEmpty()) out.timeframe = "1";

        if (out.hasSelectedCoin && out.matrices[0].running) {
            try {
                String query = "?coin=" + URLEncoder.encode(out.coin, "UTF-8")
                        + "&timeframe=" + URLEncoder.encode(out.timeframe, "UTF-8")
                        + "&t=" + System.currentTimeMillis();
                JSONObject signal = new JSONObject(httpGetSignalMatrixWithFailover(query));
            out.rf = normalize(signal.opt("RF"));
            out.st = normalize(signal.opt("ST"));
            out.kj = normalize(signal.opt("KJ"));
            freshAny = true;

            double p = signal.optDouble("price", Double.NaN);
            if (!Double.isNaN(p) && p > 0) {
                out.signalPrice = formatCryptoPrice(p, out.coin);
            }
            WidgetData.MatrixData m1 = out.matrices[0];
            m1.coin = out.coin; m1.timeframe = out.timeframe; m1.selected = out.hasSelectedCoin;
            m1.signalPrice = out.signalPrice; m1.rf = out.rf; m1.st = out.st; m1.kj = out.kj;
            // IMPORTANT: the home-screen Widget renders signalHtml before the
            // matrix fallback. Keep it synchronized with this native fetch so
            // background updates are visible even when WebView is closed.
            out.signalHtml = signalHtml(m1, out.tseRows);
            saveLiveSignalHtml(context, out.signalHtml, System.currentTimeMillis());
        } catch (Exception ignored) {
        }
        }

        // Fetch the other running/selected matrices in parallel so a 10-second
        // data refresh does not leave Matrix #2/#3 waiting behind another request.
        final List<java.util.concurrent.Callable<Boolean>> matrixTasks = new ArrayList<>();
        for (int mi = 1; mi < 3; mi++) {
            final WidgetData.MatrixData md = out.matrices[mi];
            if (!md.selected || !md.running || md.coin.isEmpty()) continue;

            // The widget must always show the signal values of the LOWEST
            // selected timeframe for each matrix. The dashboard can monitor
            // several timeframes, so never let a later/higher timeframe
            // overwrite the values intended for the widget.
            final String lowestTimeframe = md.timeframe;
            matrixTasks.add(() -> {
                try {
                    String query = "?coin=" + URLEncoder.encode(md.coin, "UTF-8")
                            + "&timeframe=" + URLEncoder.encode(lowestTimeframe, "UTF-8")
                            + "&t=" + System.currentTimeMillis();
                    JSONObject signal = new JSONObject(httpGetSignalMatrixWithFailover(query));
                    md.rf = normalize(signal.opt("RF"));
                    md.st = normalize(signal.opt("ST"));
                    md.kj = normalize(signal.opt("KJ"));
                    double p = signal.optDouble("price", Double.NaN);
                    try {
                        CryptoPriceProvider.PriceData unified = CryptoPriceProvider.get(context, md.coin);
                        if (Double.isFinite(unified.price) && unified.price > 0) {
                            p = unified.price;
                        }
                    } catch (Exception priceError) {
                        // Keep the worker price as an in-memory last-resort value.
                    }
                    if (!Double.isNaN(p) && p > 0) md.signalPrice = formatCryptoPrice(p, md.coin);
                    // Matrix #2/#3 are also cached so rotation never exposes a
                    // stale WebView snapshot when the app is not running.
                    return true;
                } catch (Exception ignored) {
                    return false;
                }
            });
        }
        if (!matrixTasks.isEmpty()) {
            try {
                for (java.util.concurrent.Future<Boolean> result : EXECUTOR.invokeAll(matrixTasks)) {
                    try { if (Boolean.TRUE.equals(result.get())) freshAny = true; } catch (Exception ignored) {}
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Match index.html exactly: USD uses USDD first, then YEKUSD; the
        // secondary dollar quote uses BahaUSD. Keep last-good values on failure.
        try {
            String v = fetchFirstValidPlainPrice(USD_URLS);
            if (v != null) { out.usd = v; freshAny = true; }
        } catch (Exception ignored) {}
        try {
            String v = fetchFirstValidPlainPrice(YEKUSD_URLS);
            if (v != null) { out.yekusd = v; freshAny = true; }
        } catch (Exception ignored) {}
        try {
            String v = httpGetWithFallback(CRYPTO_URL).trim();
            if (!v.isEmpty() && !"-".equals(v)) {
                try {
                    double cryptoValue = Double.parseDouble(v.replace(",", "").trim());
                    if (!Double.isNaN(cryptoValue) && !Double.isInfinite(cryptoValue)) {
                        // Widget Crypto is always shown as a whole number with thousands separators.
                        out.crypto = String.format(Locale.US, "%,.0f", cryptoValue);
                        freshAny = true;
                    }
                } catch (Exception ignored) {
                    // Keep the last valid cached Crypto number rather than flashing text/-.
                }
            }
        } catch (Exception ignored) {}

        // Tether prices use the same fixed endpoints and Toman conversion as
        // index.html. data.json is not authoritative for these API URLs.
        try {
            double v = fetchBestAskPrice(NOBITEX_ORDERBOOK_API, 10.0, null);
            if (isPositive(v)) { out.nobitex = formatInteger(v); freshAny = true; }
        } catch (Exception ignored) {}
        try {
            double v = fetchExirWidgetPrice();
            if (isPositive(v)) { out.exir = formatInteger(v); freshAny = true; }
        } catch (Exception ignored) {}
        try {
            double v = fetchBestAskPrice(BITPIN_ORDERBOOK_API, 1.0, null);
            if (isPositive(v)) { out.bitpin = formatInteger(v); freshAny = true; }
        } catch (Exception ignored) {}

        // GLOBAL asset values use the same public sources as the dashboard.
        try {
            JSONObject batch = new JSONObject(httpGetWithFallback("https://biquote.io/api/latest?symbols=XAUUSD&symbols=USOIL"));
            JSONObject xau = batch.optJSONObject("XAUUSD");
            JSONObject oil = batch.optJSONObject("USOIL");
            if (xau != null) {
                double v=xau.optDouble("mid", Double.NaN);
                if (!Double.isNaN(v)) out.globalGoldPrice = formatPrice(v);
                double pct=xau.optDouble("dayDiffPercent", Double.NaN);
                if (!Double.isNaN(pct)) out.globalGoldDirection = pct > 0 ? 1 : pct < 0 ? -1 : 0;
            }
            if (oil != null) {
                double v=oil.optDouble("mid", Double.NaN);
                if (!Double.isNaN(v)) out.globalOilPrice = formatPrice(v);
                double pct=oil.optDouble("dayDiffPercent", Double.NaN);
                if (!Double.isNaN(pct)) out.globalOilDirection = pct > 0 ? 1 : pct < 0 ? -1 : 0;
            }
            if (!out.globalGoldPrice.equals("-")) freshAny = true;
            if (!out.globalOilPrice.equals("-")) freshAny = true;
        } catch (Exception ignored) {}
        try {
            JSONObject tse = new JSONObject(httpGetTsetmcWithFailover(context, System.currentTimeMillis()));
            JSONArray results = tse.optJSONArray("results");
            if (results != null) {
                out.tseRows.clear(); out.tseSymbols.clear();
                for (int i=0;i<results.length();i++) {
                    JSONObject row=results.optJSONObject(i); if(row==null)continue;
                    String sym=row.optString("symbol","").trim(); if(sym.isEmpty())continue;
                    String price= row.has("lastPrice") ? formatInteger(row.optDouble("lastPrice",Double.NaN)) : "-";
                    double pct=row.optDouble("lastPricePercent",Double.NaN);
                    int direction=Double.isNaN(pct)?0:(pct>0?1:pct<0?-1:0);
                    out.tseRows.add(new WidgetData.TseRow(sym,price,direction)); out.tseSymbols.add(sym);
                }
                if(!out.tseRows.isEmpty()) freshAny=true;
            }
        } catch (Exception ignored) {}
        android.content.SharedPreferences gps = context.getSharedPreferences(GLOBAL_PREFS, Context.MODE_PRIVATE);
        out.globalAssetType = gps.getString("compact_asset_type", "");
        out.globalAssetSymbol = gps.getString("compact_asset_symbol", "");
        if ("gold".equals(out.globalAssetType)) { out.globalAssetPrice = out.globalGoldPrice; out.globalAssetDirection = out.globalGoldDirection; }
        else if ("oil".equals(out.globalAssetType)) { out.globalAssetPrice = out.globalOilPrice; out.globalAssetDirection = out.globalOilDirection; }
        else if ("tse".equals(out.globalAssetType)) {
            for (WidgetData.TseRow row : out.tseRows) if (row.symbol.equals(out.globalAssetSymbol)) { out.globalAssetPrice = row.price; out.globalAssetDirection = row.direction; break; }
            if (out.globalAssetPrice == null || out.globalAssetPrice.equals("-")) {
                if (!out.tseRows.isEmpty()) { out.globalAssetSymbol = out.tseRows.get(0).symbol; out.globalAssetPrice = out.tseRows.get(0).price; out.globalAssetDirection = out.tseRows.get(0).direction; }
            }
        }
        if (out.globalAssetPrice == null) out.globalAssetPrice = "-";
        out.globalAssetFlashDirection = 0;
        if (previousGlobalType != null && previousGlobalType.equals(out.globalAssetType)
                && (out.globalAssetType == null || !"tse".equals(out.globalAssetType)
                    || String.valueOf(previousGlobalSymbol).equals(String.valueOf(out.globalAssetSymbol)))
                && previousGlobalPrice != null && !previousGlobalPrice.equals("-")
                && out.globalAssetPrice != null && !out.globalAssetPrice.equals("-")
                && !previousGlobalPrice.equals(out.globalAssetPrice)) {
            try {
                double oldPrice = Double.parseDouble(previousGlobalPrice.replace(",", "").trim());
                double newPrice = Double.parseDouble(out.globalAssetPrice.replace(",", "").trim());
                out.globalAssetFlashDirection = newPrice > oldPrice ? 1 : newPrice < oldPrice ? -1 : 0;
            } catch (Exception ignored) {}
        }

        // Direction is calculated against the last successfully cached value.
        out.nobitexDirection = priceDirection(previousNobitex, out.nobitex);
        out.exirDirection = priceDirection(previousExir, out.exir);
        out.bitpinDirection = priceDirection(previousBitpin, out.bitpin);

        if (out.signalPrice == null || out.signalPrice.isEmpty()) {
            out.signalPrice = safe(out.crypto);
        }

        if (freshAny) {
            if (out.rotationStartMs <= 0L) out.rotationStartMs = System.currentTimeMillis();
            out.updated = new java.text.SimpleDateFormat(
                    "HH:mm:ss",
                    Locale.US
            ).format(new java.util.Date());
            writeCachedData(context, out);
        }

        return out;
    }

    private static void writeCachedData(Context context, WidgetData d) {
        try {
            JSONObject o = new JSONObject();
            o.put("coin", d.coin);
            o.put("timeframe", d.timeframe);
            o.put("hasSelectedCoin", d.hasSelectedCoin);
            o.put("signalPrice", d.signalPrice);
            o.put("signalHtml", d.signalHtml);
            o.put("usd", d.usd);
            o.put("yekusd", d.yekusd);
            o.put("crypto", d.crypto);
            o.put("nobitex", d.nobitex);
            o.put("exir", d.exir);
            o.put("bitpin", d.bitpin);
            o.put("nobitexDirection", d.nobitexDirection);
            o.put("exirDirection", d.exirDirection);
            o.put("bitpinDirection", d.bitpinDirection);
            o.put("updated", d.updated);
            o.put("rf", d.rf);
            o.put("st", d.st);
            o.put("kj", d.kj);
            o.put("rotationStartMs", d.rotationStartMs);
            o.put("globalGoldPrice", d.globalGoldPrice);
            o.put("globalOilPrice", d.globalOilPrice);
            o.put("globalGoldDirection", d.globalGoldDirection);
            o.put("globalOilDirection", d.globalOilDirection);
            o.put("globalAssetDirection", d.globalAssetDirection);
            o.put("globalAssetType", d.globalAssetType);
            o.put("globalAssetSymbol", d.globalAssetSymbol);
            o.put("globalAssetPrice", d.globalAssetPrice);
            JSONArray ts = new JSONArray();
            for (WidgetData.TseRow row : d.tseRows) { JSONObject tr = new JSONObject(); tr.put("symbol", row.symbol); tr.put("price", row.price); tr.put("direction", row.direction); ts.put(tr); }
            o.put("tseRows", ts);
            JSONArray ms = new JSONArray();
            for (WidgetData.MatrixData m : d.matrices) {
                JSONObject mo = new JSONObject();
                mo.put("coin", m.coin); mo.put("timeframe", m.timeframe);
                mo.put("selected", m.selected); mo.put("running", m.running);
                mo.put("signalPrice", m.signalPrice); mo.put("rf", m.rf);
                mo.put("st", m.st); mo.put("kj", m.kj);
                ms.put(mo);
            }
            o.put("matrices", ms);
            context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                    .edit().putString(KEY_DATA, o.toString()).apply();
        } catch (Exception ignored) {}
    }

    private static WidgetData readCachedData(Context context) {
        WidgetData d = new WidgetData();
        try {
            String raw = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                    .getString(KEY_DATA, null);
            if (raw == null || raw.trim().isEmpty()) return d;
            JSONObject o = new JSONObject(raw);
            d.coin = o.optString("coin", "-");
            d.timeframe = o.optString("timeframe", "1");
            d.hasSelectedCoin = o.optBoolean("hasSelectedCoin", false);
            d.signalPrice = o.optString("signalPrice", "-");
            d.signalHtml = o.optString("signalHtml", "");
            d.usd = o.optString("usd", "-");
            d.yekusd = o.optString("yekusd", "-");
            d.crypto = formatWidgetCrypto(o.optString("crypto", "-"));
            d.nobitex = o.optString("nobitex", "-");
            d.exir = o.optString("exir", "-");
            d.bitpin = o.optString("bitpin", "-");
            d.nobitexDirection = o.optInt("nobitexDirection", 0);
            d.exirDirection = o.optInt("exirDirection", 0);
            d.bitpinDirection = o.optInt("bitpinDirection", 0);
            d.updated = o.optString("updated", null);
            d.rf = o.optInt("rf", 0);
            d.st = o.optInt("st", 0);
            d.kj = o.optInt("kj", 0);
            d.rotationStartMs = o.optLong("rotationStartMs", 0L);
            d.globalGoldPrice = o.optString("globalGoldPrice", "-");
            d.globalOilPrice = o.optString("globalOilPrice", "-");
            d.globalGoldDirection = o.optInt("globalGoldDirection", 0);
            d.globalOilDirection = o.optInt("globalOilDirection", 0);
            d.globalAssetDirection = o.optInt("globalAssetDirection", 0);
            d.globalAssetType = o.optString("globalAssetType", "");
            d.globalAssetSymbol = o.optString("globalAssetSymbol", "");
            d.globalAssetPrice = o.optString("globalAssetPrice", "-");
            JSONArray ts = o.optJSONArray("tseRows");
            if (ts != null) for (int i=0;i<ts.length();i++){ JSONObject tr=ts.optJSONObject(i); if(tr!=null){ String sym=tr.optString("symbol",""); String pr=tr.optString("price","-"); if(!sym.isEmpty()){ int dir=tr.optInt("direction",0); d.tseRows.add(new WidgetData.TseRow(sym,pr,dir)); d.tseSymbols.add(sym); } } }
            JSONArray ms = o.optJSONArray("matrices");
            if (ms != null) {
                for (int i = 0; i < Math.min(3, ms.length()); i++) {
                    JSONObject mo = ms.optJSONObject(i);
                    if (mo == null) continue;
                    WidgetData.MatrixData m = d.matrices[i];
                    m.coin = mo.optString("coin", "-"); m.timeframe = mo.optString("timeframe", "1");
                    m.selected = mo.optBoolean("selected", false); m.running = mo.optBoolean("running", false);
                    m.signalPrice = mo.optString("signalPrice", "-"); m.rf = mo.optInt("rf", 0);
                    m.st = mo.optInt("st", 0); m.kj = mo.optInt("kj", 0);
                }
            }
        } catch (Exception ignored) {}
        return d;
    }

    private static CrossoverWidgetData readCrossoverWidgetData(Context context, List<WidgetData.TseRow> tseRows) {
        CrossoverWidgetData out = new CrossoverWidgetData();
        try {
            String raw = MainActivity.getSignalMatrixStateJson(context);
            if (raw != null && !raw.trim().isEmpty()) {
                JSONObject root = new JSONObject(raw);
                JSONArray crossovers = root.optJSONArray("crossovers");
                if (crossovers != null) {
                    StringBuilder html = new StringBuilder();
                    int shown = 0;
                    boolean hasActiveCrossover = false;
                    boolean firstRunningCrossoverHandled = false;
                    for (int i = 0; i < crossovers.length(); i++) {
                        JSONObject c = crossovers.optJSONObject(i);
                        if (c == null || !c.optBoolean("running", false)) continue;
                        hasActiveCrossover = true;

                        // The widget's signal slot represents the FIRST running Crossover,
                        // not the first one that happens to have a price in this refresh.
                        // Otherwise an empty/stale price in slot #1 silently makes slot #2
                        // appear, or leaves the signal slot blank when Matrix is stopped.
                        if (!firstRunningCrossoverHandled) {
                            firstRunningCrossoverHandled = true;
                            String coin = c.optString("activeCoin", c.optString("coin", "")).trim();
                            double currentPrice = c.optDouble("latestPrice", Double.NaN);
                            if (!Double.isFinite(currentPrice) || currentPrice <= 0) {
                                String rawPrice = c.optString("latestPriceRaw", "").replace(",", "").trim();
                                try { currentPrice = Double.parseDouble(rawPrice); }
                                catch (Exception ignored) { currentPrice = Double.NaN; }
                            }
                            if (!coin.isEmpty()) {
                                String signalPrefsKey = coin.toUpperCase(Locale.ROOT) + "|";
                                if (Double.isFinite(currentPrice) && currentPrice > 0) {
                                    out.signalHtml = escape(widgetDisplayCoin(coin, tseRows)) + " " + escape(formatCryptoPrice(currentPrice, coin));
                                    try {
                                        context.getApplicationContext().getSharedPreferences(CROSSOVER_WIDGET_PREFS, Context.MODE_PRIVATE)
                                                .edit().putString(KEY_CROSSOVER_SIGNAL, signalPrefsKey + out.signalHtml).apply();
                                    } catch (Throwable ignored) {}
                                } else {
                                    try {
                                        String cachedSignal = context.getApplicationContext().getSharedPreferences(CROSSOVER_WIDGET_PREFS, Context.MODE_PRIVATE)
                                                .getString(KEY_CROSSOVER_SIGNAL, "");
                                        if (cachedSignal != null && cachedSignal.startsWith(signalPrefsKey)) {
                                            String cachedHtml = cachedSignal.substring(signalPrefsKey.length());
                                             // Cached Crossover HTML may contain the internal TSE slot
                                             // identifier from an earlier refresh. Replace only its
                                             // leading visible label; keep the cache key and slot index intact.
                                             String oldLabel = escape(coin);
                                             String displayLabel = escape(widgetDisplayCoin(coin, tseRows));
                                             if (cachedHtml.startsWith(oldLabel)) {
                                                 cachedHtml = displayLabel + cachedHtml.substring(oldLabel.length());
                                             }
                                             out.signalHtml = cachedHtml;
                                        }
                                    } catch (Throwable ignored) {}
                                }
                            }
                        }

                        double target = c.optDouble("activeTarget", Double.NaN);
                        if (!Double.isFinite(target) || target <= 0) {
                            target = c.optDouble("target", Double.NaN);
                        }
                        double price = c.optDouble("latestPrice", Double.NaN);
                        if (!Double.isFinite(price) || price <= 0 || !Double.isFinite(target) || target <= 0) continue;

                        double pct = ((price - target) / target) * 100.0;
                        String textValue = (pct >= 0 ? "+" : "") + String.format(Locale.US, "%.2f%%", pct);
                        if (shown > 0) html.append("&nbsp;&nbsp;&nbsp;");
                        html.append("<font color=\"")
                                .append(pct >= 0 ? "#34D399" : "#F87171")
                                .append("\">")
                                .append(escape(textValue))
                                .append("</font>");
                        shown++;
                    }

                    if (shown > 0) {
                        out.html = html.toString();
                        out.visible = true;
                        try {
                            context.getApplicationContext()
                                    .getSharedPreferences(CROSSOVER_WIDGET_PREFS, Context.MODE_PRIVATE)
                                    .edit()
                                    .putString(KEY_CROSSOVER_HTML, out.html)
                                    .apply();
                        } catch (Throwable ignored) {}
                        return out;
                    }

                    // A valid state with no running Crossovers means the user
                    // intentionally stopped them: clear the old widget value.
                    if (!hasActiveCrossover) {
                        try {
                            context.getApplicationContext()
                                    .getSharedPreferences(CROSSOVER_WIDGET_PREFS, Context.MODE_PRIVATE)
                                    .edit()
                                    .remove(KEY_CROSSOVER_HTML)
                                    .apply();
                        } catch (Throwable ignored) {}
                        return out;
                    }
                }
            }
        } catch (Throwable ignored) {
        }

        // State may briefly be unavailable during app/Service refresh. Keep the last
        // good Crossover display rather than making the widget blink/disappear.
        try {
            String cached = context.getApplicationContext()
                    .getSharedPreferences(CROSSOVER_WIDGET_PREFS, Context.MODE_PRIVATE)
                    .getString(KEY_CROSSOVER_HTML, "");
            if (cached != null && !cached.trim().isEmpty()) {
                out.html = cached;
                out.visible = true;
            }
        } catch (Throwable ignored) {}
        return out;
    }

    private static class CrossoverWidgetData {
        boolean visible = false;
        String html = "";
        String signalHtml = "";
    }

    /**
     * Signal Matrix recovery policy: primary worker, Supabase fallback, then
     * one final primary retry. This keeps the widget aligned with WebView and
     * quickly returns to the primary source as soon as it recovers.
     */
    private static String httpGetSignalMatrixWithFailover(String query) throws Exception {
        String[] bases = {SIGNAL_URL, SIGNAL_FALLBACK_URL};
        Exception primaryError;
        try {
            String body = validateSignalMatrixPayload(httpGet(bases[0] + query));
            signalMatrixActiveApiIndex = 0;
            return body;
        } catch (Exception error) {
            primaryError = error;
        }
        Exception fallbackError;
        try {
            String body = validateSignalMatrixPayload(httpGet(bases[1] + query));
            signalMatrixActiveApiIndex = 1;
            return body;
        } catch (Exception error) {
            fallbackError = error;
        }
        try {
            String body = validateSignalMatrixPayload(httpGet(bases[0] + query));
            signalMatrixActiveApiIndex = 0;
            return body;
        } catch (Exception retryError) {
            throw new Exception("Signal Matrix APIs unavailable; primary: "
                    + retryError.getMessage() + "; fallback: " + fallbackError.getMessage(), primaryError);
        }
    }

    private static String validateSignalMatrixPayload(String body) throws Exception {
        JSONObject data = new JSONObject(body);
        JSONObject nested = data.optJSONObject("data");
        if (nested != null) data = nested;
        if (!(data.has("RF") || data.has("rf") || data.has("rangeFilter") || data.has("range_filter")
                || data.has("KJ") || data.has("kj") || data.has("kijunsen")
                || data.has("ST") || data.has("st") || data.has("superTrend") || data.has("supertrend"))) {
            throw new Exception("Signal Matrix response contains no signal values");
        }
        return data.toString();
    }

    private static String httpGetTsetmcWithFailover(Context context, long stamp) throws Exception {
        int active = context.getSharedPreferences(GLOBAL_PREFS, Context.MODE_PRIVATE)
                .getInt(TSE_ACTIVE_API_PREF, 0);
        int[] order = {active, active == 0 ? 1 : 0};
        String[] bases = {TSE_URL, TSE_FALLBACK_URL};
        Exception firstError = null;
        for (int index : order) {
            try {
                String body = httpGet(bases[index] + "?t=" + stamp + "&fresh=" + stamp);
                JSONObject data = new JSONObject(body);
                JSONArray results = data.optJSONArray("results");
                if (!data.optBoolean("ok", false) || results == null || results.length() == 0) {
                    throw new Exception("TSETMC source returned no valid results");
                }
                if (index != active) {
                    context.getSharedPreferences(GLOBAL_PREFS, Context.MODE_PRIVATE).edit()
                            .putInt(TSE_ACTIVE_API_PREF, index).apply();
                }
                return body;
            } catch (Exception e) {
                if (firstError == null) firstError = e;
            }
        }
        throw firstError == null ? new Exception("TSETMC sources unavailable") : firstError;
    }

    private static String fetchFirstValidPlainPrice(String[] urls) throws Exception {
        Exception last = null;
        for (String url : urls) {
            try {
                String value = httpGetWithFallback(url).trim().replace(",", "");
                if (value.matches("(?:\\d+\\.?\\d*|\\.\\d+)")) {
                    double number = Double.parseDouble(value);
                    if (Double.isFinite(number) && number > 0) return value;
                }
                last = new Exception("Invalid price response from " + url);
            } catch (Exception e) { last = e; }
        }
        throw last == null ? new Exception("No valid price source") : last;
    }

    private static double fetchBestAskPrice(String url, double divisor, String pairKey) throws Exception {
        JSONObject root = new JSONObject(httpGetWithFallback(url));
        JSONObject pair = pairKey == null ? root : root.optJSONObject(pairKey);
        JSONArray asks = pair == null ? root.optJSONArray("asks") : pair.optJSONArray("asks");
        if (asks == null && pair != null) asks = pair.optJSONArray("ask");
        if (asks == null || asks.length() == 0) throw new Exception("Missing asks: " + url);
        double best = Double.POSITIVE_INFINITY;
        for (int i = 0; i < asks.length(); i++) {
            Object item = asks.opt(i);
            double price = Double.NaN;
            if (item instanceof JSONArray) price = ((JSONArray) item).optDouble(0, Double.NaN);
            else if (item instanceof JSONObject) {
                JSONObject row = (JSONObject) item;
                price = row.optDouble("price", row.optDouble("rate", row.optDouble("value", Double.NaN)));
            } else {
                try { price = Double.parseDouble(String.valueOf(item)); } catch (Exception ignored) {}
            }
            if (Double.isFinite(price) && price > 0 && price < best) best = price;
        }
        if (!Double.isFinite(best)) throw new Exception("No valid ask price: " + url);
        double result = best / (divisor > 0 ? divisor : 1.0);
        if (!Double.isFinite(result) || result <= 0) throw new Exception("Invalid normalized ask price");
        return result;
    }

    private static double fetchExirWidgetPrice() throws Exception {
        // Index's primary source returns a plain numeric price. Retry it first
        // on every widget cycle so the widget automatically recovers to it.
        try {
            String raw = httpGetWithFallback(EXIR_WORKER_PRICE_API).trim().replace(",", "");
            if (raw.matches("(?:\\d+\\.?\\d*|\\.\\d+)")) {
                double value = Double.parseDouble(raw);
                if (Double.isFinite(value) && value > 0) return value;
            }
        } catch (Exception ignored) {}
        // Same fallback order as the dashboard: ticker, then lowest orderbook ask.
        try {
            JSONObject ticker = new JSONObject(httpGetWithFallback(EXIR_TICKER_API));
            double price = findExirTickerPrice(ticker);
            if (Double.isFinite(price) && price > 0) return price;
        } catch (Exception ignored) {}
        return fetchBestAskPrice(EXIR_ORDERBOOK_API, 1.0, "usdt-irt");
    }

    private static double findExirTickerPrice(JSONObject root) {
        JSONObject data = root.optJSONObject("data");
        JSONObject result = root.optJSONObject("result");
        JSONObject dataResult = data == null ? null : data.optJSONObject("result");
        JSONObject[] candidates = new JSONObject[]{root, data, result, dataResult, root.optJSONObject("ticker")};
        String[] pairKeys = new String[]{"usdt-irt", "USDT-IRT", "USDT_IRT", "usdt_irt"};
        String[] keys = new String[]{"last", "close", "price", "lastPrice"};
        for (JSONObject candidate : candidates) {
            if (candidate == null) continue;
            JSONObject pair = candidate;
            for (String pairKey : pairKeys) { if (candidate.optJSONObject(pairKey) != null) { pair = candidate.optJSONObject(pairKey); break; } }
            for (String key : keys) {
                Object raw = pair.opt(key);
                if (raw == null || raw == JSONObject.NULL) continue;
                try {
                    double value = Double.parseDouble(String.valueOf(raw).replace(",", "").trim());
                    if (Double.isFinite(value) && value > 0) return value;
                } catch (Exception ignored) {}
            }
        }
        return Double.NaN;
    }

    private static String httpGetWithFallback(String url) throws Exception {
        List<String> urls = new ArrayList<>();
        urls.add(url);

        if (!url.contains("proxy.cors.dev/") && !url.contains("api.cors.syrins.tech")) {
            urls.add("https://proxy.cors.dev/" + url);
        }
        if (!url.contains("api.cors.syrins.tech")) {
            urls.add("https://api.cors.syrins.tech/?url=" + URLEncoder.encode(url, "UTF-8"));
        }

        Exception last = null;
        for (String candidate : urls) {
            try {
                return httpGet(candidate);
            } catch (Exception e) {
                last = e;
            }
        }
        throw last == null ? new Exception("HTTP failed") : last;
    }

    private static String httpGet(String urlString) throws Exception {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(urlString);
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(7000);
            connection.setReadTimeout(7000);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json,text/plain,*/*");
            connection.setUseCaches(false);

            int code = connection.getResponseCode();
            if (code < 200 || code >= 300) {
                throw new Exception("HTTP " + code);
            }

            InputStream stream = connection.getInputStream();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(stream)
            );
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
            reader.close();
            return result.toString();
        } finally {
            if (connection != null) connection.disconnect();
        }
    }

    private static int timeframeRank(String tf) {
        if (tf == null) return Integer.MAX_VALUE;
        switch (tf) {
            case "1": return 1;
            case "3": return 3;
            case "5": return 5;
            case "15": return 15;
            case "30": return 30;
            case "60": return 60;
            case "4h": return 240;
            case "1D": return 1440;
            case "1W": return 10080;
            default:
                try {
                    return Integer.parseInt(tf);
                } catch (Exception ignored) {
                    return Integer.MAX_VALUE;
                }
        }
    }

    private static int normalize(Object value) {
        if (value == null) return 0;
        try {
            if (value instanceof Number) {
                double n = ((Number) value).doubleValue();
                return n > 0 ? 1 : n < 0 ? -1 : 0;
            }
            String s = String.valueOf(value).trim().toLowerCase(Locale.ROOT);
            if (s.contains("buy") || s.contains("bull") ||
                    s.equals("up") || s.equals("1") || s.equals("+1")) return 1;
            if (s.contains("sell") || s.contains("bear") ||
                    s.equals("down") || s.equals("-1")) return -1;
        } catch (Exception ignored) {
        }
        return 0;
    }

    private static int cryptoPriceDecimals(String asset) {
        if (asset == null) return -1;
        switch (asset.trim().toUpperCase(Locale.ROOT)) {
            case "BTC": return 0;
            case "BNB": return 2;
            case "ETH": return 1;
            case "DOGE": return 5;
            case "ADA": return 4;
            case "SOL": return 2;
            case "POL": return 5;
            case "AVAX": return 3;
            case "GRAM": return 3;
            case "TRX": return 5;
            default: return -1;
        }
    }

    private static String formatCryptoPrice(double value, String asset) {
        if (Double.isNaN(value) || Double.isInfinite(value) || value <= 0) return "-";
        // TSETMC instruments use whole-number prices everywhere in the native
        // Signal Matrix / Ultra Compact widget, matching the WebView formatter.
        if (asset != null && asset.trim().toUpperCase(Locale.ROOT).startsWith("TSE|")) {
            return formatInteger(value);
        }
        int decimals = cryptoPriceDecimals(asset);
        if (decimals < 0) return formatPrice(value);
        String pattern = "#,##0." + "0".repeat(decimals);
        return new DecimalFormat(pattern, DecimalFormatSymbols.getInstance(Locale.US)).format(value);
    }

    private static String formatPrice(double value) {
        // Keep the widget price formatting identical to Signal Matrix:
        // >=1000 -> exactly 2 decimals, >=1 -> exactly 3 decimals,
        // below 1 -> 4 to 6 decimals.
        DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(Locale.US);
        final String pattern;
        if (value >= 1000) {
            pattern = "#,##0.00";
        } else if (value >= 1) {
            pattern = "#,##0.000";
        } else {
            pattern = "0.0000##";
        }
        return new DecimalFormat(pattern, symbols).format(value);
    }

    private static String formatWidgetCrypto(String raw) {
        if (raw == null) return "-";
        String value = raw.trim();
        if (value.isEmpty() || "-".equals(value)) return "-";
        try {
            String normalized = value.replace(",", "").replace("$", "").trim();
            double number = Double.parseDouble(normalized);
            if (Double.isNaN(number) || Double.isInfinite(number)) return "-";
            return String.format(Locale.US, "%,.0f", number);
        } catch (Exception ignored) {
            return value;
        }
    }

    private static String formatInteger(double value) {
        return String.format(Locale.US, "%,.0f", value);
    }

    private static void scheduleRefresh(Context context) {
        AlarmManager alarm =
                (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
        if (alarm == null) return;

        Intent intent = new Intent(context, UltraCompactWidget.class);
        intent.setAction(ACTION_WIDGET_UPDATE);

        int flags = PendingIntent.FLAG_UPDATE_CURRENT;
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            flags |= PendingIntent.FLAG_IMMUTABLE;
        }

        PendingIntent pending =
                PendingIntent.getBroadcast(context, 1907, intent, flags);

        long triggerAt = SystemClock.elapsedRealtime() + WIDGET_REFRESH_MS;

        // Android AppWidget's updatePeriodMillis cannot provide 10-second updates.
        // Use a self-rescheduling exact alarm instead. This keeps the widget refresh
        // interval at approximately 10 seconds even though the provider itself may
        // be recreated by Android.
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                alarm.setExactAndAllowWhileIdle(
                        AlarmManager.ELAPSED_REALTIME_WAKEUP,
                        triggerAt,
                        pending
                );
            } else {
                alarm.setExact(
                        AlarmManager.ELAPSED_REALTIME_WAKEUP,
                        triggerAt,
                        pending
                );
            }
        } catch (SecurityException ignored) {
            // If exact alarms are restricted by the device, fall back to an
            // inexact one-shot alarm rather than disabling widget refresh.
            alarm.set(
                    AlarmManager.ELAPSED_REALTIME_WAKEUP,
                    triggerAt,
                    pending
            );
        }
    }

    private static void scheduleRotation(Context context) {
        AlarmManager alarm = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
        if (alarm == null) return;
        Intent intent = new Intent(context, UltraCompactWidget.class);
        intent.setAction(ACTION_WIDGET_ROTATE);
        int flags = PendingIntent.FLAG_UPDATE_CURRENT;
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) flags |= PendingIntent.FLAG_IMMUTABLE;
        PendingIntent pending = PendingIntent.getBroadcast(context, 1908, intent, flags);
        // Keep rotation locked to one fixed 5-second timeline. Do not schedule
        // the next rotation as 5 seconds after the alarm was delivered because
        // Android may deliver alarms late; that would make one matrix visible for
        // 2 seconds and another for 7+ seconds.
        long nowWall = System.currentTimeMillis();
        WidgetData cached = readCachedData(context);
        long startWall = cached.rotationStartMs;
        long triggerAt;
        if (startWall > 0L) {
            long elapsed = Math.max(0L, nowWall - startWall);
            long nextBoundary = startWall + ((elapsed / WIDGET_ROTATE_MS) + 1L) * WIDGET_ROTATE_MS;
            long delay = Math.max(250L, nextBoundary - nowWall);
            triggerAt = SystemClock.elapsedRealtime() + delay;
        } else {
            triggerAt = SystemClock.elapsedRealtime() + WIDGET_ROTATE_MS;
        }
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) alarm.setExactAndAllowWhileIdle(AlarmManager.ELAPSED_REALTIME_WAKEUP, triggerAt, pending);
            else alarm.setExact(AlarmManager.ELAPSED_REALTIME_WAKEUP, triggerAt, pending);
        } catch (SecurityException ignored) {
            alarm.set(AlarmManager.ELAPSED_REALTIME_WAKEUP, triggerAt, pending);
        }
    }

    private static void cancelRotation(Context context) {
        AlarmManager alarm = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
        if (alarm == null) return;
        Intent intent = new Intent(context, UltraCompactWidget.class);
        intent.setAction(ACTION_WIDGET_ROTATE);
        int flags = PendingIntent.FLAG_UPDATE_CURRENT;
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) flags |= PendingIntent.FLAG_IMMUTABLE;
        PendingIntent pending = PendingIntent.getBroadcast(context, 1908, intent, flags);
        alarm.cancel(pending);
    }

    private static void cancelRefresh(Context context) {
        AlarmManager alarm =
                (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
        if (alarm == null) return;

        Intent intent = new Intent(context, UltraCompactWidget.class);
        intent.setAction(ACTION_WIDGET_UPDATE);

        int flags = PendingIntent.FLAG_UPDATE_CURRENT;
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            flags |= PendingIntent.FLAG_IMMUTABLE;
        }

        PendingIntent pending =
                PendingIntent.getBroadcast(context, 1907, intent, flags);

        alarm.cancel(pending);
    }

    private static class WidgetData {
        String coin = "-";
        String timeframe = "1";
        boolean hasSelectedCoin = false;
        String signalPrice = "-";
        String signalHtml = "";
        String usd = "-";
        String yekusd = "-";
        String crypto = "-";
        String nobitex = "-";
        String exir = "-";
        String bitpin = "-";
        String updated = null;
        int rf = 0;
        int st = 0;
        int kj = 0;
        int nobitexDirection = 0;
        int exirDirection = 0;
        int bitpinDirection = 0;
        MatrixData[] matrices = new MatrixData[]{new MatrixData(), new MatrixData(), new MatrixData()};
        long rotationStartMs = 0L;
        String globalGoldPrice = "-";
        String globalOilPrice = "-";
        int globalGoldDirection = 0;
        int globalOilDirection = 0;
        String globalAssetType = "";
        String globalAssetSymbol = "";
        String globalAssetPrice = "-";
        int globalAssetDirection = 0;
        int globalAssetFlashDirection = 0;
        List<TseRow> tseRows = new ArrayList<>();
        List<String> tseSymbols = new ArrayList<>();

        MatrixData currentMatrix() {
            List<MatrixData> active = new ArrayList<>();
            for (MatrixData m : matrices) {
                if (m != null && m.selected && m.running) active.add(m);
            }
            if (active.isEmpty()) {
                MatrixData fallback = matrices.length > 0 ? matrices[0] : null;
                return fallback != null && fallback.selected && fallback.running ? fallback : null;
            }
            long start = rotationStartMs > 0 ? rotationStartMs : System.currentTimeMillis();
            int slot = (int)Math.max(0L, (System.currentTimeMillis() - start) / WIDGET_ROTATE_MS);
            return active.get(slot % active.size());
        }

        static class TseRow {
            final String symbol;
            final String price;
            final int direction;
            TseRow(String symbol, String price, int direction){ this.symbol=symbol; this.price=price; this.direction=direction; }
        }

        static class MatrixData {
            String coin = "-";
            String timeframe = "1";
            boolean selected = false;
            boolean running = false;
            String signalPrice = "-";
            int rf = 0;
            int st = 0;
            int kj = 0;
        }
    }
}
