package com.financial;

import android.app.KeyguardManager;
import android.content.Context;
import android.content.SharedPreferences;
import android.net.Uri;
import android.os.Build;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Native notification engine used while Android's keyguard is active.
 * It intentionally does not depend on WebView/JavaScript being executable.
 */
public final class NativeNotificationChecker {

    private static final String DATA_URL = "https://test1800.github.io/data.json";
    private static final String SIGNAL_URL = "https://coinsignaltable.rxw.workers.dev/";
    private static final String SIGNAL_MATRIX_WORKER_URL = "https://signalmatrix.rxw1.workers.dev/";
    private static final String SIGNAL_MATRIX_FALLBACK_URL = "https://ropmwamrcvvuwrvajxdr.supabase.co/functions/v1/clever-handler/";
    private static volatile int signalMatrixActiveApiIndex = 0; // 0=worker, 1=Supabase
    private static final String FEAR_GREED_API = "https://api.alternative.me/fng/?limit=1";
    private static final String TSE_TSETMC_WORKER_URL = "https://tsetmc.rxw1.workers.dev/";
    private static final String TSE_TSETMC_FALLBACK_URL = "https://ropmwamrcvvuwrvajxdr.supabase.co/functions/v1/dynamic-service/";
    private static final String TSE_TSETMC_ACTIVE_API_PREF = "tsetmc_active_api";
    private static final String TSE_WATCHLIST_PREF = "tsetmc_watchlist_json";
    private static final String TSE_CDN_API_BASE = "https://cdn.tsetmc.com/api/";
    private static final String USD_CHART_HISTORY_URL = "https://financial-chart-history.rahdatair.workers.dev/history";
    private static final String COIN_CHART_HISTORY_URL = "https://coin-chart-history.info-intoken.workers.dev/history";
    private static final String COMMODITY_BATCH_URL = "https://biquote.io/api/latest?symbols=XAUUSD&symbols=USOIL";
    private static final long TSE_TSETMC_REFRESH_MS = 60_000L;
    private static final int TSE_TSETMC_START_MIN = 8 * 60 + 45;
    private static final int TSE_TSETMC_END_MIN = 18 * 60;

    private static final String PREFS = "native_notification_engine";
    private static final String SIGNAL_PREFS = "background_notifications";
    private static final String SIGNAL_MATRIX_PREFS = "financial_native_state";

    private static final String[] SIGNAL_COINS = {
            "BTC", "BNB", "ETH", "DOGE", "ADA", "SOL", "POL", "GRAM", "TRX", "AVAX"
    };

    private static final String[] CARD_IDS = {
            "overallMainPrice", "sixMainPrice", "lastMainPrice"
    };

    private static final long HTTP_TIMEOUT_MS = 8000L;
    private static final long NOTIFICATION_COOLDOWN_MS = 5L * 60L * 1000L;
    private static final long APP_ERROR_REPEAT_MS = 5L * 60L * 1000L;
    private static final int SIGN_CONFIRMATIONS_REQUIRED = 5;
    private static final Map<String, ChartAssetCandleCache> CHART_ASSET_CANDLE_CACHE = new HashMap<>();
    private static final long CHART_ASSET_CANDLE_CACHE_TTL_MS = 30_000L;

    private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "financial-native-notifications");
        t.setDaemon(true);
        return t;
    });

    /*
     * Crossover has a separate background lane. It must never be skipped just
     * because the general notification checker is busy with another API call.
     */
    private static final ExecutorService CROSSOVER_EXECUTOR = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "financial-crossover-notifications");
        t.setDaemon(true);
        return t;
    });

    // Time-sensitive alerts must not wait behind the general dashboard/API queue.
    private static final ExecutorService SIGNAL_MATRIX_EXECUTOR = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "financial-signal-matrix-notifications");
        t.setDaemon(true);
        return t;
    });
    private static final ExecutorService CRYPTO_SIGNAL_EXECUTOR = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "financial-crypto-signal-notifications");
        t.setDaemon(true);
        return t;
    });
    private static final ExecutorService SCHEDULE_EXECUTOR = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "financial-schedule-notifications");
        t.setDaemon(true);
        return t;
    });

    private static volatile boolean running = false;
    private static volatile boolean crossoverRunning = false;
    private static volatile boolean signalMatrixRunning = false;
    private static volatile boolean cryptoSignalRunning = false;
    private static volatile boolean scheduleRunning = false;

    private NativeNotificationChecker() {}

    public static void checkIfLocked(Context context) {
        if (running) return;

        Context app = context.getApplicationContext();

        // This checker is needed for two independent lock states:
        // 1) the PIN overlay inside the app, and
        // 2) Android's real device Keyguard.
        // The previous implementation only accepted (2), which is why the
        // notification engine stopped on the app's own PIN screen.
        boolean appLocked = MainActivity.isAppLocked(app);
        boolean deviceLocked = isKeyguardLocked(app);
        boolean activityStopped = !MainActivity.isActivityResumed();

        // Native notifications are required whenever the dashboard WebView is not
        // the active execution path: PIN lock, Android lock screen, minimized, or
        // closed/stopped Activity. This is the important distinction that was
        // missing before: a stopped Activity does not imply Android Keyguard is on.
        // Keep index.html as the only dashboard notification source while the
        // Activity/WebView is alive (including the app PIN overlay). Native
        // dashboard notifications are used only when the Activity is stopped
        // or Android's real lock screen is active, preventing duplicate alerts.
        // The native owner must also take over while the app's own PIN overlay
        // is active. This keeps Signal Matrix monitoring alive without WebView.
        if (!deviceLocked && !activityStopped && !appLocked) return;

        running = true;
        EXECUTOR.execute(() -> {
            try {
                // Signal Matrix, Crypto Signal, and Crossover have dedicated
                // background lanes so slow dashboard APIs cannot make them miss
                // a short-lived market transition.
                checkGlobalPulse(app);
                checkTsetmcPulse(app);
                checkFearGreed(app);
                checkDashboard(app);
            } catch (Throwable ignored) {
                // Background notification work must never be allowed to kill the service.
            } finally {
                running = false;
            }
        });
    }

    /**
     * Fast background owner for Crossover only.
     * It runs only when the Activity/WebView is not the active notification owner.
     */
    public static void checkCrossoversIfBackground(Context context) {
        /*
         * Crossover is an important alert path and is deliberately independent
         * from the general background checker. The old shared `running` flag
         * meant a Signal/Global/TSE request could make a Crossover tick return
         * without checking at all.
         */
        if (crossoverRunning) return;
        Context app = context.getApplicationContext();
        boolean appLocked = MainActivity.isAppLocked(app);
        boolean deviceLocked = isKeyguardLocked(app);
        boolean activityStopped = !MainActivity.isActivityResumed();
        if (!deviceLocked && !activityStopped && !appLocked) return;

        crossoverRunning = true;
        CROSSOVER_EXECUTOR.execute(() -> {
            try {
                checkCrossovers(app);
            } catch (Throwable ignored) {
            } finally {
                crossoverRunning = false;
            }
        });
    }

    /**
     * Fast Signal Matrix background lane. It is independent from the long
     * general notification cycle and therefore cannot be blocked by Gold/TSE/
     * Fear&Greed/economic API requests.
     */
    public static void checkSignalMatrixIfBackground(Context context) {
        if (signalMatrixRunning) return;
        Context app = context.getApplicationContext();
        if (!isBackgroundNotificationOwner(app)) return;

        signalMatrixRunning = true;
        SIGNAL_MATRIX_EXECUTOR.execute(() -> {
            try {
                checkSignalMatrix(app);
            } catch (Throwable ignored) {
            } finally {
                signalMatrixRunning = false;
            }
        });
    }

    /**
     * Fast Crypto Signal background lane. It is kept separate from Signal Matrix
     * and from the general dashboard checker so one failing/slow API cannot
     * suppress another notification category.
     */
    public static void checkCryptoSignalsIfBackground(Context context) {
        if (cryptoSignalRunning) return;
        Context app = context.getApplicationContext();
        if (!isBackgroundNotificationOwner(app)) return;

        cryptoSignalRunning = true;
        CRYPTO_SIGNAL_EXECUTOR.execute(() -> {
            try {
                checkSignals(app);
            } catch (Throwable ignored) {
            } finally {
                cryptoSignalRunning = false;
            }
        });
    }

    /**
     * Fast clock-based notification lane. Economic exact-time and market-open/
     * close reminders do not need to wait behind network requests.
     */
    public static void checkScheduleIfBackground(Context context) {
        if (scheduleRunning) return;
        Context app = context.getApplicationContext();
        if (!isBackgroundNotificationOwner(app)) return;

        scheduleRunning = true;
        SCHEDULE_EXECUTOR.execute(() -> {
            try {
                checkIranMarketSchedule(app);
                checkGlobalTimeSchedule(app);
            } catch (Throwable ignored) {
            } finally {
                scheduleRunning = false;
            }
        });
    }

    private static boolean isBackgroundNotificationOwner(Context context) {
        boolean appLocked = MainActivity.isAppLocked(context);
        boolean deviceLocked = isKeyguardLocked(context);
        boolean activityStopped = !MainActivity.isActivityResumed();
        return deviceLocked || activityStopped || appLocked;
    }

    private static boolean isKeyguardLocked(Context context) {
        KeyguardManager km = (KeyguardManager) context.getSystemService(Context.KEYGUARD_SERVICE);
        if (km == null) return false;
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
            return km.isKeyguardLocked();
        }
        return false;
    }

    private static void checkSignals(Context context) {
        try {
            JSONObject data = new JSONObject(httpGet(SIGNAL_URL));
            SharedPreferences sp = context.getSharedPreferences(SIGNAL_PREFS, Context.MODE_PRIVATE);
            SharedPreferences.Editor editor = sp.edit();

            List<String> changedCoins = new ArrayList<>();
            Map<String, Integer> changedValues = new HashMap<>();

            for (String coin : SIGNAL_COINS) {
                int value = signalValue(data.opt(coin));
                String key = "signal_" + coin;

                if (!sp.contains(key)) {
                    editor.putInt(key, value);
                    continue;
                }

                int old = sp.getInt(key, 0);
                if (old != value) {
                    /*
                       Only a real BUY/SELL result can notify. Neutral/invalid
                       worker transitions are kept as state but never alerted.
                    */
                    if (value == 1 || value == -1) {
                        changedCoins.add(coin);
                        changedValues.put(coin, value);
                    }
                    editor.putInt(key, value);
                }
            }

            /*
               When two or more coins change in the same worker response, treat
               the response as a likely transient worker glitch and suppress the
               entire Crypto Signal notification batch. This applies equally to
               background/closed/minimized operation.
            */
            if (changedCoins.size() == 1) {
                String coin = changedCoins.get(0);
                int value = changedValues.get(coin);
                String type = value > 0 ? "BUY" : "SELL";
                String icon = value > 0 ? "🟢" : "🔴";

                NotificationHelper.show(
                        context,
                        type,
                        icon + " " + coin + " Crypto Signal",
                        icon + " " + coin + " → " + type,
                        false
                );
            }

            editor.apply();
        } catch (Exception ignored) {
        }
    }

    /**
     * Background owner for Signal Matrix.
     *
     * It is called only when the WebView is no longer the active notification
     * owner, so it cannot race with index.html. The selected coin/timeframe
     * configuration is persisted by the WebView bridge.
     */
    private static void checkSignalMatrix(Context context) {
        try {
            SharedPreferences state = context.getSharedPreferences(SIGNAL_MATRIX_PREFS, Context.MODE_PRIVATE);
            String raw = state.getString("signal_matrix_config", "");
            if (raw == null || raw.trim().isEmpty()) {
                if (!state.getBoolean("signal_matrix_enabled", false)) return;
                raw = legacySingleMatrixConfig(state);
            }
            JSONObject config = new JSONObject(raw);
            int count = Math.max(1, Math.min(4, config.optInt("count", 1)));
            JSONArray matrices = config.optJSONArray("matrices");
            if (matrices == null) return;
            for (int i = 0; i < Math.min(count, matrices.length()); i++) {
                JSONObject matrix = matrices.optJSONObject(i);
                if (matrix == null || !matrix.optBoolean("running", false)) continue;
                String coin = matrix.optString("coin", "BTC").trim().toUpperCase(Locale.ROOT);
                if (coin.isEmpty()) coin = "BTC";
                JSONArray timeframes = matrix.optJSONArray("timeframes");
                if (timeframes == null || timeframes.length() == 0) timeframes = new JSONArray().put("1");
                for (int t = 0; t < timeframes.length(); t++) {
                    String timeframe = timeframes.optString(t, "1").trim();
                    if (!isAllowedTimeframe(timeframe)) continue;
                    if ("4H".equals(timeframe)) timeframe = "4h";
                    checkSignalMatrixPair(context, state, i + 1, coin, timeframe);
                }
            }
        } catch (Exception ignored) {
        }
    }


    /**
     * Background owner for Crossover. It uses the same Signal Matrix price
     * endpoint, so no additional worker is required. The WebView persists the
     * three crossover configurations inside signal_matrix_config.
     */
    private static void checkCrossovers(Context context) {
        SharedPreferences state = context.getSharedPreferences(SIGNAL_MATRIX_PREFS, Context.MODE_PRIVATE);
        try {
            String raw = state.getString("signal_matrix_config", "");
            if (raw == null || raw.trim().isEmpty()) return;

            JSONObject config = new JSONObject(raw);
            JSONArray crossovers = config.optJSONArray("crossovers");
            if (crossovers == null || crossovers.length() == 0) return;

            Map<String, Double> priceCache = new HashMap<>();

            for (int i = 0; i < Math.min(4, crossovers.length()); i++) {
                final int index = i + 1;
                try {
                    JSONObject crossover = crossovers.optJSONObject(i);
                    if (crossover == null || !crossover.optBoolean("running", false)) continue;
                    // Keep the widget's live percentage fresh even when alerts are disabled.
                    // The notificationEnabled flag controls alerts only, not price tracking.
                    boolean notificationsEnabled = crossover.optBoolean("notificationEnabled", false);

                    String coin = crossover.optString("activeCoin", "").trim().toUpperCase(Locale.ROOT);
                    if (coin.isEmpty()) coin = crossover.optString("coin", "BTC").trim().toUpperCase(Locale.ROOT);
                    if (coin.isEmpty()) coin = "BTC";

                    double target = numberOr(crossover.opt("activeTarget"), Double.NaN);
                    if (!isPositive(target)) target = numberOr(crossover.opt("target"), Double.NaN);
                    if (!isPositive(target)) continue;

                    double price;
                    String priceKey = "price:" + coin;
                    Double cachedPrice = priceCache.get(priceKey);
                    if (cachedPrice != null && isPositive(cachedPrice)) {
                        price = cachedPrice;
                    } else {
                        if ("USD".equals(coin)) {
                            price = numberOr(config.opt("liveAverage"), Double.NaN);
                        } else if (isCrossoverSpecialAsset(coin)) {
                            price = fetchCrossoverSpecialPrice(context, coin);
                        } else {
                            CryptoPriceProvider.PriceData unified = CryptoPriceProvider.get(context, coin);
                            price = unified.price;
                        }
                        if (!isPositive(price)) continue;
                        priceCache.put(priceKey, price);
                    }

                    String prefix = "crossover_" + index + "_" + coin + "_" + Double.toString(target);
                    String initializedKey = prefix + "_initialized";
                    String storedPriceKey = prefix + "_price";
                    String offlineKey = "crossover_" + index + "_offline";

                    /*
                     * A reconnect can make the first fresh quote jump from the
                     * last pre-outage price to the current price. Never interpret
                     * that recovery jump as dozens of crossed percentage levels.
                     */
                    if (state.getBoolean(offlineKey, false)) {
                        state.edit()
                                .putBoolean(offlineKey, false)
                                .putBoolean(initializedKey, true)
                                .putString(storedPriceKey, Double.toString(price))
                                .apply();
                        continue;
                    }

                    if (!state.getBoolean(initializedKey, false)) {
                        state.edit()
                                .putBoolean(initializedKey, true)
                                .putString(storedPriceKey, Double.toString(price))
                                .apply();
                        continue;
                    }

                    double oldPrice = numberOr(state.getString(storedPriceKey, ""), Double.NaN);
                    if (notificationsEnabled && isPositive(oldPrice) && oldPrice != price) {
                        SharedPreferences.Editor editor = state.edit();
                        boolean crossedUp = oldPrice < target && price >= target;
                        boolean crossedDown = oldPrice > target && price <= target;
                        if (crossedUp || crossedDown) {
                            long now = System.currentTimeMillis();
                            String crossKey = prefix + "_cross_at";
                            long lastCross = state.getLong(crossKey, 0L);
                            String assetLabel = crossoverDisplayName(context, coin);
                            String arrowTitle = (crossedUp ? "🟢 ↑ " : "🔴 ↓ ")
                                    + assetLabel + " • Crossover #" + index;
                            String crossMessage =
                                    assetLabel
                                    + " • Crossover " + (crossedUp ? "UP ↑" : "DOWN ↓")
                                    + " • Price " + formatCrossoverNotificationPrice(price, coin)
                                    + " • Target " + formatCrossoverNotificationPrice(target, coin);
                            // Every real crossing is delivered. Crossover has no
                            // five-minute notification cooldown.
                            NotificationHelper.show(
                                    context, "CROSSOVER", arrowTitle, crossMessage, false
                            );
                            editor.putLong(crossKey, now);
                        }

                        double oldPct = ((oldPrice - target) / target) * 100.0;
                        double newPct = ((price - target) / target) * 100.0;
                        if (Double.isFinite(oldPct) && Double.isFinite(newPct) && oldPct != newPct) {
                            long now = System.currentTimeMillis();

                            if (newPct > oldPct) {
                                int from = (int) Math.ceil(oldPct);
                                int to = (int) Math.floor(newPct);
                                for (int level = from; level <= to; level++) {
                                    if (level == 0) continue;
                                    maybeSendCrossoverPercentageNotification(
                                            context, state, editor, index, coin, target, price, level, 1, now, prefix
                                    );
                                }
                            } else {
                                int from = (int) Math.floor(oldPct);
                                int to = (int) Math.ceil(newPct);
                                for (int level = from; level >= to; level--) {
                                    if (level == 0) continue;
                                    maybeSendCrossoverPercentageNotification(
                                            context, state, editor, index, coin, target, price, level, -1, now, prefix
                                    );
                                }
                            }
                            editor.apply();
                        }
                    }

                    state.edit().putString(storedPriceKey, Double.toString(price)).apply();
                    persistCrossoverRuntimeState(state, index, price, prefix);
                    // Push the newly persisted price to the home-screen widget immediately.
                    UltraCompactWidget.refreshCrossoverDisplay(context);
                } catch (Throwable crossoverError) {
                    state.edit()
                            .putBoolean("crossover_" + index + "_offline", true)
                            .apply();
                    android.util.Log.w(
                            "NativeNotificationChecker",
                            "Crossover #" + index + " background check failed",
                            crossoverError
                    );
                }
            }
        } catch (Throwable ignored) {
        }
    }

    /**
     * Mirror native Crossover runtime state into the shared JSON used by the
     * WebView. This prevents an alert already delivered while the app was
     * closed from being replayed when the Activity is opened again.
     */
    private static void persistCrossoverRuntimeState(
            SharedPreferences state,
            int index,
            double latestPrice,
            String prefix
    ) {
        try {
            String raw = state.getString("signal_matrix_config", "");
            if (raw == null || raw.trim().isEmpty()) return;
            JSONObject config = new JSONObject(raw);
            JSONArray crossovers = config.optJSONArray("crossovers");
            if (crossovers == null || index < 1 || index > crossovers.length()) return;

            JSONObject crossover = crossovers.optJSONObject(index - 1);
            if (crossover == null) return;

            crossover.put("previousPrice", latestPrice);
            crossover.put("latestPrice", latestPrice);
            crossover.put("nativeRuntimeUpdatedAt", System.currentTimeMillis());

            JSONObject alertTimes = new JSONObject();
            // Crossover levels are percentage integers; cover the complete
            // practical range without imposing a notification cooldown.
            for (int level = -1000; level <= 1000; level++) {
                if (level == 0) continue;
                long at = state.getLong(prefix + "_pct_" + level, 0L);
                if (at > 0L) alertTimes.put(String.valueOf(level), at);
            }
            crossover.put("percentageAlertTimes", alertTimes);
            config.put("crossovers", crossovers);
            config.put("crossoverNativeRuntimeUpdatedAt", System.currentTimeMillis());

            state.edit().putString("signal_matrix_config", config.toString()).apply();
        } catch (Throwable ignored) {
        }
    }

    private static void maybeSendCrossoverPercentageNotification(
            Context context,
            SharedPreferences state,
            SharedPreferences.Editor editor,
            int index,
            String coin,
            double target,
            double price,
            int level,
            int direction,
            long now,
            String prefix
    ) {
        String levelKey = prefix + "_pct_" + level;
        // Every genuine percentage-level crossing is an independent event.
        // Do not suppress the same level merely because it crossed recently.
        String pctText = (level > 0 ? "+" : "") + level + "%";
        boolean isUp = direction > 0;
        String relation = isUp ? "crossed up" : "crossed down";
        String assetLabel = crossoverDisplayName(context, coin);
        String title = (isUp ? "🟢 ↑ " : "🔴 ↓ ")
                + assetLabel + " • Crossover #" + index;
        String message = assetLabel
                + " • " + pctText + " " + relation + " target"
                + " • Price " + formatCrossoverNotificationPrice(price, coin)
                + " • Target " + formatCrossoverNotificationPrice(target, coin);

        NotificationHelper.show(context, "CROSSOVER", title, message, false);
        editor.putLong(levelKey, now);
    }

    private static boolean isCrossoverSpecialAsset(String asset) {
        if (asset == null) return false;
        String value = asset.trim().toUpperCase(Locale.ROOT);
        return "GOLD".equals(value) || "OIL".equals(value) || value.startsWith("TSE|");
    }

    private static String crossoverDisplayName(Context context, String asset) {
        if (asset == null || asset.trim().isEmpty()) return "Crossover";
        String value = asset.trim().toUpperCase(Locale.ROOT);
        if ("GOLD".equals(value)) return "GOLD";
        if ("OIL".equals(value)) return "OIL";
        if (value.startsWith("TSE|")) {
            String symbol = value.substring(4);
            if (symbol.matches("SLOT\\d+")) {
                JSONObject instrument = findConfiguredTsetmcInstrument(context, symbol);
                symbol = instrument == null ? "" : instrument.optString("symbol", "").trim();
            }
            return symbol.isEmpty() ? "TSE / IR" : symbol + " / IR";
        }
        if ("USD".equals(value)) return "USD";
        return value + "/USD";
    }

    private static String formatCrossoverNotificationPrice(double value, String asset) {
        if (!isPositive(value)) return "-";
        String key = asset == null ? "" : asset.trim().toUpperCase(Locale.ROOT);
        if ("USD".equals(key)) return String.format(Locale.US, "%,.0f", value);
        if ("GOLD".equals(key)) return String.format(Locale.US, "%,.0f", value);
        if ("OIL".equals(key)) return String.format(Locale.US, "%,.2f", value);
        if (key.startsWith("TSE|")) return String.format(Locale.US, "%,.0f", value);
        int decimals = cryptoPriceDecimals(key);
        if (decimals >= 0) return String.format(Locale.US, "%,." + decimals + "f", value);
        return formatNotificationPrice(value);
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

    private static String httpGetTsetmcWithFailover(Context context, long stamp) throws Exception {
        int active = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                .getInt(TSE_TSETMC_ACTIVE_API_PREF, 0);
        int[] order = {active, active == 0 ? 1 : 0};
        String[] bases = {TSE_TSETMC_WORKER_URL, TSE_TSETMC_FALLBACK_URL};
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
                    context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit()
                            .putInt(TSE_TSETMC_ACTIVE_API_PREF, index).apply();
                }
                return body;
            } catch (Exception e) {
                if (firstError == null) firstError = e;
            }
        }
        throw firstError == null ? new Exception("TSETMC sources unavailable") : firstError;
    }

    private static JSONArray readConfiguredTsetmcWatchlist(Context context) {
        try {
            String raw = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                    .getString(TSE_WATCHLIST_PREF, "[]");
            JSONArray array = new JSONArray(raw);
            return array.length() > 4 ? new JSONArray() : array;
        } catch (Exception ignored) {
            return new JSONArray();
        }
    }

    private static JSONObject fetchTsetmcClosingQuote(String insCode) throws Exception {
        String code = insCode == null ? "" : insCode.trim();
        if (!code.matches("\\d+")) throw new IllegalArgumentException("Invalid TSETMC instrument code");
        JSONObject root = new JSONObject(httpGet(TSE_CDN_API_BASE + "ClosingPrice/GetClosingPriceInfo/" + code));
        JSONObject info = root.optJSONObject("closingPriceInfo");
        if (info == null) throw new IllegalStateException("TSETMC closingPriceInfo is missing");
        double last = numberOr(info.opt("pDrCotVal"), Double.NaN);
        double close = numberOr(info.opt("pClosing"), Double.NaN);
        double yesterday = numberOr(info.opt("priceYesterday"), Double.NaN);
        double price = isPositive(last) ? last : close;
        if (!isPositive(price)) throw new IllegalStateException("TSETMC price is unavailable");
        double pct = numberOr(info.opt("priceChangePercent"), Double.NaN);
        if (isPositive(yesterday)) {
            pct = ((price - yesterday) / yesterday) * 100.0;
        }
        double closePct = numberOr(info.opt("priceChangePercent"), pct);
        if (isPositive(yesterday) && isPositive(close)) {
            closePct = ((close - yesterday) / yesterday) * 100.0;
        }
        JSONObject result = new JSONObject();
        result.put("lastPrice", price);
        result.put("lastPricePercent", pct);
        result.put("closingPrice", isPositive(close) ? close : price);
        result.put("closingPricePercent", closePct);
        return result;
    }

    private static JSONObject findConfiguredTsetmcInstrument(Context context, String symbol) {
        JSONArray list = readConfiguredTsetmcWatchlist(context);
        String wanted = symbol == null ? "" : symbol.trim();
        int slot = -1;
        if (wanted.matches("SLOT\\d+")) {
            try { slot = Integer.parseInt(wanted.substring(4)); } catch (Exception ignored) { slot = -1; }
        }
        for (int i = 0; i < list.length(); i++) {
            JSONObject item = list.optJSONObject(i);
            if (item == null) continue;
            if (slot > 0 && item.optInt("slot", -1) == slot) return item;
            if (slot < 0 && wanted.equalsIgnoreCase(item.optString("symbol", "").trim())) return item;
        }
        return null;
    }

    private static void processTsetmcPulseRow(
            Context context,
            SharedPreferences.Editor editor,
            String symbol,
            double current,
            double price,
            long now
    ) {
        if (symbol == null || symbol.trim().isEmpty() || !Double.isFinite(current)) return;
        String safeSymbol = symbol.trim();
        String previousKey = "tsetmc_" + safeSymbol + "_previous_percent";
        String alertPrefix = "tsetmc_" + safeSymbol + "_alert_";
        if (!context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).contains(previousKey)) {
            editor.putString(previousKey, Double.toString(current));
            return;
        }
        double previous = numberOr(context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString(previousKey, ""), Double.NaN);
        if (!Double.isFinite(previous) || previous == current) {
            editor.putString(previousKey, Double.toString(current));
            return;
        }
        for (int level : GLOBAL_PULSE_ALERT_LEVELS) {
            boolean crossed = (previous < level && current >= level) || (previous > level && current <= level);
            if (!crossed) continue;
            String key = alertPrefix + level;
            long lastAlert = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getLong(key, 0L);
            if (now - lastAlert < NOTIFICATION_COOLDOWN_MS) continue;
            String pctText = (level > 0 ? "+" : "") + level + "%";
            String title = (level > 0 ? "🟢 ↑ " : "🔴 ↓ ") + "GLOBAL PULSE";
            String message = "GLOBAL PULSE • " + safeSymbol + " • " + pctText
                    + (isPositive(price) ? " • Price " + formatNotificationPrice(price) : "");
            NotificationHelper.show(context, "GLOBAL_PULSE", title, message, false);
            editor.putLong(key, now);
        }
        editor.putString(previousKey, Double.toString(current));
    }

    private static double fetchCrossoverSpecialPrice(Context context, String asset) throws Exception {
        String value = asset.trim().toUpperCase(Locale.ROOT);
        if ("GOLD".equals(value) || "OIL".equals(value)) {
            JSONObject data = new JSONObject(httpGet(COMMODITY_BATCH_URL));
            String key = "GOLD".equals(value) ? "XAUUSD" : "USOIL";
            JSONObject row = data.optJSONObject(key);
            if (row == null) {
                JSONObject symbols = data.optJSONObject("symbols");
                row = symbols == null ? null : symbols.optJSONObject(key);
            }
            return row == null ? Double.NaN : numberOr(row.opt("mid"), Double.NaN);
        }
        if (value.startsWith("TSE|")) {
            String symbol = value.substring(4).trim();
            JSONObject instrument = findConfiguredTsetmcInstrument(context, symbol);
            if (instrument != null) {
                JSONObject quote = fetchTsetmcClosingQuote(instrument.optString("insCode", ""));
                return numberOr(quote.opt("lastPrice"), Double.NaN);
            }
            // Compatibility fallback is only used for installations that have not opened
            // the new GLOBAL TSETMC selector yet. Once configured, GLOBAL is authoritative.
            if (context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                    .getBoolean("tsetmc_watchlist_configured", false)) return Double.NaN;
            JSONObject data = new JSONObject(httpGetTsetmcWithFailover(context, System.currentTimeMillis()));
            JSONArray results = data.optJSONArray("results");
            if (results != null) {
                for (int i = 0; i < results.length(); i++) {
                    JSONObject row = results.optJSONObject(i);
                    if (row != null && symbol.equalsIgnoreCase(row.optString("symbol", "").trim())) {
                        return numberOr(row.opt("lastPrice"), Double.NaN);
                    }
                }
            }
        }
        return Double.NaN;
    }

    private static String formatNotificationPrice(double value) {
        if (!isPositive(value)) return "-";
        if (value >= 1000) return String.format(Locale.US, "%,.2f", value);
        if (value >= 1) return String.format(Locale.US, "%.3f", value);
        return String.format(Locale.US, "%.6f", value);
    }

    private static String crossoverDateTime(long millis) {
        java.text.SimpleDateFormat format =
                new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
        return format.format(new java.util.Date(millis));
    }


    private static String legacySingleMatrixConfig(SharedPreferences state) {
        try {
            JSONObject object = new JSONObject();
            object.put("count", 1);
            JSONArray matrices = new JSONArray();
            JSONObject matrix = new JSONObject();
            matrix.put("coin", state.getString("signal_matrix_coin", "BTC"));
            matrix.put("timeframes", new JSONArray().put(state.getString("signal_matrix_timeframe", "1")));
            matrix.put("running", state.getBoolean("signal_matrix_enabled", false));
            matrices.put(matrix);
            object.put("matrices", matrices);
            return object.toString();
        } catch (Exception e) {
            return "{\"count\":1,\"matrices\":[]}";
        }
    }

    private static boolean isAllowedTimeframe(String timeframe) {
        return "1".equals(timeframe) || "3".equals(timeframe) || "5".equals(timeframe)
                || "15".equals(timeframe) || "30".equals(timeframe) || "60".equals(timeframe)
                || "4h".equalsIgnoreCase(timeframe) || "1D".equals(timeframe) || "1W".equals(timeframe);
    }


    private static final class TseCandle {
        String time;
        double open, high, low, close;
        TseCandle(String time, double open, double high, double low, double close) {
            this.time=time; this.open=open; this.high=high; this.low=low; this.close=close;
        }
    }

    private static final class ChartAssetCandleCache {
        final long savedAt;
        final List<TseCandle> candles;
        ChartAssetCandleCache(long savedAt, List<TseCandle> candles) {
            this.savedAt = savedAt;
            this.candles = new ArrayList<>(candles);
        }
    }

    private static JSONObject findSignalMatrixTseInstrument(Context context, String coin) {
        if (coin == null || !coin.toUpperCase(Locale.ROOT).startsWith("TSE|")) return null;
        String key = coin.substring(4).trim().toUpperCase(Locale.ROOT);
        JSONArray list = readConfiguredTsetmcWatchlist(context);
        for (int i=0;i<list.length();i++) {
            JSONObject item=list.optJSONObject(i);
            if(item==null) continue;
            int slot=item.optInt("slot", i+1);
            String symbol=item.optString("symbol","").trim();
            if (key.equals("SLOT"+slot) || key.equals(symbol.toUpperCase(Locale.ROOT))) {
                if(item.optString("insCode","").matches("\\d+")) return item;
            }
        }
        return null;
    }

    private static JSONArray tseRows(JSONObject payload) {
        JSONArray rows=payload.optJSONArray("closingPriceChartData");
        if(rows==null) rows=payload.optJSONArray("closingPriceDaily");
        if(rows==null) rows=payload.optJSONArray("closingPriceHistory");
        if(rows==null) rows=payload.optJSONArray("chartData");
        if(rows==null) rows=payload.optJSONArray("data");
        if(rows==null) rows=payload.optJSONArray("result");
        if(rows==null) {
            JSONObject nested=payload.optJSONObject("data");
            if(nested!=null) {
                rows=nested.optJSONArray("closingPriceChartData");
                if(rows==null) rows=nested.optJSONArray("data");
                if(rows==null) rows=nested.optJSONArray("result");
            }
        }
        return rows==null?new JSONArray():rows;
    }

    private static double tseField(JSONObject row, String... keys) {
        for(String key:keys) {
            Object value=row.opt(key);
            double n=numberOr(value, Double.NaN);
            if(Double.isFinite(n) && n>0) return n;
        }
        return Double.NaN;
    }

    private static String tseDate(Object raw) {
        String s=String.valueOf(raw==null?"":raw).replaceAll("[^0-9]","");
        if(s.length()>=8) return s.substring(0,8);
        return "";
    }

    private static List<TseCandle> normalizeTseCandles(JSONArray rows) {
        java.util.TreeMap<String,TseCandle> map=new java.util.TreeMap<>();
        for(int i=0;i<rows.length();i++) {
            JSONObject r=rows.optJSONObject(i); if(r==null) continue;
            Object rawDate=r.has("dEven")?r.opt("dEven"):(r.has("DEven")?r.opt("DEven"):r.opt("date"));
            Object rawTime=r.has("time")?r.opt("time"):(r.has("timestamp")?r.opt("timestamp"):rawDate);
            String stamp=String.valueOf(rawTime==null?"":rawTime).trim();
            String digits=stamp.replaceAll("[^0-9]","");
            if(digits.length()>=14) stamp=digits.substring(0,8)+"T"+digits.substring(8,14);
            else if(digits.length()==8) stamp=digits;
            else if(digits.length()==13 || digits.length()==10) stamp=digits;
            else {
                String date=tseDate(rawDate);
                if(date.isEmpty()) continue;
                String clock=String.valueOf(r.opt("hEven")).replaceAll("[^0-9]","");
                while(clock.length()<6) clock="0"+clock;
                stamp=date+"T"+clock.substring(clock.length()-6);
            }
            double close=tseField(r,"close","c","pClose","pc","pClosing","pDrCotVal","pl");
            if(!isPositive(close)) continue;
            double open=tseField(r,"open","o","pOpen","pf","priceFirst");
            double high=tseField(r,"high","h","pHigh","pMax","priceMax");
            double low=tseField(r,"low","l","pLow","pMin","priceMin");
            if(!isPositive(open)) open=close;
            if(!isPositive(high)) high=Math.max(open,close);
            if(!isPositive(low)) low=Math.min(open,close);
            map.put(stamp,new TseCandle(stamp,open,Math.max(high,Math.max(open,close)),
                    Math.min(low,Math.min(open,close)),close));
        }
        return new ArrayList<>(map.values());
    }

    private static List<TseCandle> tseAggregate(List<TseCandle> source, int minutes) {
        if(minutes==240) return source; // TSE's 4H is one complete trading session.
        java.util.TreeMap<String,TseCandle> grouped=new java.util.TreeMap<>();
        for(TseCandle c:source) {
            String digits=c.time.replaceAll("[^0-9]","");
            if(digits.length()<12) continue;
            String date=digits.substring(0,8);
            int hour=Integer.parseInt(digits.substring(8,10));
            int minute=Integer.parseInt(digits.substring(10,12));
            int total=hour*60+minute;
            int bucket=(total/minutes)*minutes;
            String key=date+String.format(Locale.US,"T%02d%02d",bucket/60,bucket%60);
            TseCandle old=grouped.get(key);
            if(old==null) grouped.put(key,new TseCandle(key,c.open,c.high,c.low,c.close));
            else { old.high=Math.max(old.high,c.high); old.low=Math.min(old.low,c.low); old.close=c.close; }
        }
        return new ArrayList<>(grouped.values());
    }

    private static List<TseCandle> tseAggregateWeekly(List<TseCandle> source) {
        java.util.TreeMap<String,TseCandle> grouped=new java.util.TreeMap<>();
        java.util.Calendar cal=java.util.Calendar.getInstance(TimeZone.getTimeZone("UTC"),Locale.US);
        for(TseCandle c:source) {
            String digits=c.time.replaceAll("[^0-9]","");
            if(digits.length()<8) continue;
            int y=Integer.parseInt(digits.substring(0,4)), m=Integer.parseInt(digits.substring(4,6))-1,
                    d=Integer.parseInt(digits.substring(6,8));
            cal.clear(); cal.set(y,m,d);
            int back=(cal.get(java.util.Calendar.DAY_OF_WEEK)+1)%7;
            cal.add(java.util.Calendar.DAY_OF_MONTH,-back);
            String key=String.format(Locale.US,"%04d%02d%02d",cal.get(java.util.Calendar.YEAR),
                    cal.get(java.util.Calendar.MONTH)+1,cal.get(java.util.Calendar.DAY_OF_MONTH));
            TseCandle old=grouped.get(key);
            if(old==null) grouped.put(key,new TseCandle(key,c.open,c.high,c.low,c.close));
            else {old.high=Math.max(old.high,c.high);old.low=Math.min(old.low,c.low);old.close=c.close;}
        }
        return new ArrayList<>(grouped.values());
    }

    private static List<TseCandle> fetchTseCandles(String code, String timeframe) throws Exception {
        String tf=timeframe;
        String base=TSE_CDN_API_BASE+"ClosingPrice/";
        if("30".equals(tf)) {
            return normalizeTseCandles(tseRows(new JSONObject(httpGet(base+"GetChartData/"+code+"/30m"))));
        }
        if("1D".equals(tf)||"1W".equals(tf)||"4h".equalsIgnoreCase(tf)) {
            List<TseCandle> daily=normalizeTseCandles(tseRows(new JSONObject(httpGet(base+"GetClosingPriceDailyList/"+code+"/500"))));
            if("1W".equals(tf)) return tseAggregateWeekly(daily);
            return daily;
        }
        if("60".equals(tf)) {
            List<TseCandle> base30=fetchTseCandles(code,"30");
            return tseAggregate(base30,60);
        }
        if("4h".equalsIgnoreCase(tf)) return fetchTseCandles(code,"1D");

        int minutes;
        try { minutes=Integer.parseInt(tf); } catch(Exception e) { throw new Exception("Unsupported TSE timeframe "+tf); }
        if(minutes!=1 && minutes!=3 && minutes!=5 && minutes!=15) throw new Exception("Unsupported TSE timeframe "+tf);

        JSONArray dailyRows=tseRows(new JSONObject(httpGet(base+"GetClosingPriceDailyList/"+code+"/500")));
        java.util.TreeSet<String> dates=new java.util.TreeSet<>(Collections.reverseOrder());
        for(int i=0;i<dailyRows.length();i++) {
            JSONObject row=dailyRows.optJSONObject(i);
            if(row!=null) { String date=tseDate(row.opt("dEven")); if(date.isEmpty()) date=tseDate(row.opt("DEven")); if(!date.isEmpty()) dates.add(date); }
        }
        int sessions=minutes==1?3:minutes==3?6:minutes==5?9:27;
        List<TseCandle> all=new ArrayList<>(); int used=0;
        for(String date:dates) {
            if(used++>=sessions) break;
            try {
                JSONArray history=tseRows(new JSONObject(httpGet(base+"GetClosingPriceHistory/"+code+"/"+date)));
                java.util.TreeMap<String,TseCandle> byMinute=new java.util.TreeMap<>();
                for(int i=0;i<history.length();i++) {
                    JSONObject row=history.optJSONObject(i); if(row==null) continue;
                    String clock=String.valueOf(row.has("hEven")?row.opt("hEven"):row.opt("HEven")).replaceAll("[^0-9]","");
                    while(clock.length()<6) clock="0"+clock;
                    clock=clock.substring(clock.length()-6);
                    int hour=Integer.parseInt(clock.substring(0,2)), minute=Integer.parseInt(clock.substring(2,4));
                    if(hour<9 || hour>13) continue;
                    double price=tseField(row,"pDrCotVal","last","pl","pClosing","close");
                    if(!isPositive(price)) continue;
                    String key=date+"T"+clock.substring(0,4);
                    TseCandle old=byMinute.get(key);
                    if(old==null) byMinute.put(key,new TseCandle(key,price,price,price,price));
                    else {old.high=Math.max(old.high,price);old.low=Math.min(old.low,price);old.close=price;}
                }
                all.addAll(byMinute.values());
            } catch(Exception ignored) { }
        }
        Collections.sort(all,Comparator.comparing(c->c.time));
        return tseAggregate(all,minutes);
    }

    private static JSONArray findChartHistoryRows(Object value) {
        if (value instanceof JSONArray) return (JSONArray) value;
        if (!(value instanceof JSONObject)) return new JSONArray();
        JSONObject object = (JSONObject) value;
        String[] keys = {"history", "records", "points", "data", "result"};
        for (String key : keys) {
            Object nested = object.opt(key);
            if (nested instanceof JSONArray) return (JSONArray) nested;
            if (nested instanceof JSONObject) {
                JSONArray found = findChartHistoryRows(nested);
                if (found.length() > 0) return found;
            }
        }
        return new JSONArray();
    }

    private static long chartTimestampMillis(Object raw) {
        if (raw == null || raw == JSONObject.NULL) return -1L;
        String value = String.valueOf(raw).trim();
        try {
            long time = (long) Double.parseDouble(value);
            if (time > 0L && time < 100000000000L) time *= 1000L;
            return time > 0L ? time : -1L;
        } catch (Exception ignored) { }
        String[] patterns = {"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss"};
        for (String pattern : patterns) {
            try {
                SimpleDateFormat format = new SimpleDateFormat(pattern, Locale.US);
                format.setLenient(false);
                if (pattern.endsWith("'Z'")) format.setTimeZone(TimeZone.getTimeZone("UTC"));
                Date parsed = format.parse(value);
                if (parsed != null) return parsed.getTime();
            } catch (Exception ignored) { }
        }
        return -1L;
    }

    private static List<TseCandle> fetchChartAssetCandles(String asset, String timeframe) throws Exception {
        String cacheKey = asset + ":" + timeframe;
        long now = System.currentTimeMillis();
        synchronized (CHART_ASSET_CANDLE_CACHE) {
            ChartAssetCandleCache cached = CHART_ASSET_CANDLE_CACHE.get(cacheKey);
            if (cached != null && now - cached.savedAt < CHART_ASSET_CANDLE_CACHE_TTL_MS) {
                return new ArrayList<>(cached.candles);
            }
        }
        String url = "USD".equals(asset) ? USD_CHART_HISTORY_URL : COIN_CHART_HISTORY_URL;
        String body = httpGet(url + (url.contains("?") ? "&" : "?") + "_matrix=" + System.currentTimeMillis());
        Object root = new JSONTokener(body).nextValue();
        JSONArray rows = findChartHistoryRows(root);
        if (rows.length() == 0) throw new Exception(asset + " chart history returned no records");

        long intervalMinutes;
        if ("1".equals(timeframe)) intervalMinutes = 1L;
        else if ("3".equals(timeframe)) intervalMinutes = 3L;
        else if ("5".equals(timeframe)) intervalMinutes = 5L;
        else if ("15".equals(timeframe)) intervalMinutes = 15L;
        else if ("30".equals(timeframe)) intervalMinutes = 30L;
        else if ("60".equals(timeframe)) intervalMinutes = 60L;
        else if ("4h".equalsIgnoreCase(timeframe)) intervalMinutes = 240L;
        else if ("1D".equals(timeframe)) intervalMinutes = 1440L;
        else if ("1W".equals(timeframe)) intervalMinutes = 10080L;
        else throw new Exception("Unsupported " + asset + " timeframe " + timeframe);

        long span = intervalMinutes * 60L * 1000L;
        java.util.TreeMap<Long, TseCandle> grouped = new java.util.TreeMap<>();
        for (int i = 0; i < rows.length(); i++) {
            Object row = rows.opt(i);
            Object rawTime = null, rawValue = null;
            if (row instanceof JSONArray) {
                JSONArray pair = (JSONArray) row;
                if (pair.length() >= 2) { rawTime = pair.opt(0); rawValue = pair.opt(1); }
            } else if (row instanceof JSONObject) {
                JSONObject point = (JSONObject) row;
                rawTime = point.has("t") ? point.opt("t") : point.has("timestamp") ? point.opt("timestamp") : point.has("time") ? point.opt("time") : point.has("date") ? point.opt("date") : point.has("createdAt") ? point.opt("createdAt") : point.opt("created_at");
                rawValue = point.has("v") ? point.opt("v") : point.has("value") ? point.opt("value") : point.has("price") ? point.opt("price") : point.has("average") ? point.opt("average") : point.has("avg") ? point.opt("avg") : point.has("averagePrice") ? point.opt("averagePrice") : point.has("average_price") ? point.opt("average_price") : point.has("priceAverage") ? point.opt("priceAverage") : point.opt("price_average");
            }
            long timestamp = chartTimestampMillis(rawTime);
            double price = numberOr(rawValue, Double.NaN);
            if (timestamp <= 0L || !isPositive(price)) continue;
            long bucket = (timestamp / span) * span;
            TseCandle candle = grouped.get(bucket);
            if (candle == null) grouped.put(bucket, new TseCandle(Long.toString(bucket), price, price, price, price));
            else { candle.high = Math.max(candle.high, price); candle.low = Math.min(candle.low, price); candle.close = price; }
        }
        List<TseCandle> result = new ArrayList<>(grouped.values());
        if (result.size() < 2) throw new Exception(asset + " " + timeframe + " history has insufficient candles");
        synchronized (CHART_ASSET_CANDLE_CACHE) {
            CHART_ASSET_CANDLE_CACHE.put(cacheKey, new ChartAssetCandleCache(now, result));
        }
        return result;
    }

    private static void checkChartAssetSignalMatrixPair(Context context, SharedPreferences sp, int matrixIndex,
                                                          String asset, String timeframe) throws Exception {
        List<TseCandle> candles = fetchChartAssetCandles(asset, timeframe);
        int[] signals = calculateTseSignals(candles);
        String prefix = "matrix_" + matrixIndex + "_" + asset + "_" + timeframe + "_";
        String rfKey = prefix + "rf", kjKey = prefix + "kj", stKey = prefix + "st";
        String initializedKey = prefix + "initialized";
        if (!sp.getBoolean(initializedKey, false)) {
            sp.edit().putBoolean(initializedKey, true).putInt(rfKey, signals[0])
                    .putInt(kjKey, signals[1]).putInt(stKey, signals[2]).apply();
            return;
        }
        int oldRF = sp.getInt(rfKey, signals[0]);
        int oldKJ = sp.getInt(kjKey, signals[1]);
        int oldST = sp.getInt(stKey, signals[2]);
        String title = "🔔 " + asset + " • " + timeframeLabel(timeframe) + " • #" + matrixIndex;
        if (oldRF != signals[0]) notifyMatrixTransition(context, sp, rfKey, "Range Filter", signals[0], title);
        if (oldST != signals[2]) notifyMatrixTransition(context, sp, stKey, "Super Trend", signals[2], title);
        if (oldKJ != signals[1]) notifyMatrixTransition(context, sp, kjKey, "Kijun 288", signals[1], title);
        sp.edit().putInt(rfKey, signals[0]).putInt(kjKey, signals[1]).putInt(stKey, signals[2]).apply();
    }

    private static int[] calculateTseSignals(List<TseCandle> candles) {
        if(candles==null||candles.isEmpty()) throw new IllegalStateException("No TSE candles");
        int n=candles.size();
        double[] src=new double[n], changes=new double[n], avg=new double[n], smooth=new double[n], filt=new double[n];
        int[] up=new int[n], down=new int[n], cond=new int[n];
        for(int i=0;i<n;i++) src[i]=(candles.get(i).high+candles.get(i).low)/2.0;
        for(int i=1;i<n;i++) changes[i]=Math.abs(src[i]-src[i-1]);
        double alpha=2.0/(21+1), current=changes[0]; avg[0]=current;
        for(int i=1;i<n;i++){current=alpha*changes[i]+(1-alpha)*current;avg[i]=current;}
        alpha=2.0/(41+1); current=avg[0]; smooth[0]=current*5;
        for(int i=1;i<n;i++){current=alpha*avg[i]+(1-alpha)*current;smooth[i]=current*5;}
        filt[0]=src[0];
        for(int i=1;i<n;i++){
            double prev=filt[i-1];
            filt[i]=src[i]>prev?(src[i]-smooth[i]<prev?prev:src[i]-smooth[i]):
                    (src[i]+smooth[i]>prev?prev:src[i]+smooth[i]);
            up[i]=filt[i]>filt[i-1]?up[i-1]+1:filt[i]<filt[i-1]?0:up[i-1];
            down[i]=filt[i]<filt[i-1]?down[i-1]+1:filt[i]>filt[i-1]?0:down[i-1];
            boolean longCond=src[i]>filt[i]&&src[i]!=src[i-1]&&up[i]>0;
            boolean shortCond=src[i]<filt[i]&&src[i]!=src[i-1]&&down[i]>0;
            cond[i]=longCond?1:shortCond?-1:cond[i-1];
        }
        int rf=cond[n-1];
        TseCandle last=candles.get(n-1);
        // Match FREE-R09 Kijun 288: use the latest 288 candles INCLUDING
        // the current candle, just like rollingMid(288) in the chart.
        int start=Math.max(0,n-288);
        double highest=Double.NEGATIVE_INFINITY,lowest=Double.POSITIVE_INFINITY;
        for(int i=start;i<n;i++){highest=Math.max(highest,candles.get(i).high);lowest=Math.min(lowest,candles.get(i).low);}
        if(!Double.isFinite(highest)||!Double.isFinite(lowest)){highest=last.high;lowest=last.low;}
        double kijun=(highest+lowest)/2.0;
        int kj=last.close>kijun?1:last.close<kijun?-1:0;

        double[] close=new double[n],high=new double[n],low=new double[n],delta1=new double[n],deltaQ=new double[n],tr=new double[n],atr=new double[n];
        for(int i=0;i<n;i++){
            TseCandle c=candles.get(i);close[i]=c.close;high[i]=c.high;low[i]=c.low;
            delta1[i]=i==0?Double.NaN:close[i]-close[i-1];
            deltaQ[i]=i<15?Double.NaN:close[i]-close[i-15];
            tr[i]=i==0?high[i]-low[i]:Math.max(high[i]-low[i],Math.max(Math.abs(high[i]-close[i-1]),Math.abs(low[i]-close[i-1])));
            atr[i]=Double.NaN;
        }
        if(n>=14){double seed=0;for(int i=0;i<14;i++)seed+=tr[i];atr[13]=seed/14.0;for(int i=14;i<n;i++)atr[i]=atr[i-1]+(tr[i]-atr[i-1])/14.0;}
        double kf=Double.NaN,upBand=Double.NaN,dnBand=Double.NaN;int trend=1;
        for(int i=0;i<n;i++){
            double var1=tseVariance(delta1,i,80),varq=tseVariance(deltaQ,i,80),h=0.5;
            if(Double.isFinite(var1)&&Double.isFinite(varq)){
                double raw=Math.log(varq/Math.max(var1,1e-10))/(2.0*Math.log(15));
                h=Math.max(0,Math.min(raw,1));if(!Double.isFinite(h))h=0.5;
            }
            double gain=Math.max(Math.min(0.10*(0.5+h),0.99),0.01);
            kf=Double.isFinite(kf)?kf+gain*(close[i]-kf):close[i];
            double mult=2.0+4.0*(1-h),band=Double.isFinite(atr[i])?atr[i]*mult:0;
            int prevTrend=trend;
            double prevUp=Double.isFinite(upBand)?upBand:close[i],prevDn=Double.isFinite(dnBand)?dnBand:close[i];
            upBand=prevTrend==1?Math.max(kf-band,prevUp):kf-band;
            dnBand=prevTrend==-1?Math.min(kf+band,prevDn):kf+band;
            trend=kf>prevDn?1:kf<prevUp?-1:prevTrend;
        }
        return new int[]{rf,kj,trend};
    }

    private static double tseVariance(double[] values,int end,int length) {
        int start=end-length+1;if(start<0)return Double.NaN;
        double sum=0;for(int i=start;i<=end;i++){if(!Double.isFinite(values[i]))return Double.NaN;sum+=values[i];}
        double mean=sum/length,sq=0;for(int i=start;i<=end;i++){double d=values[i]-mean;sq+=d*d;}
        return sq/length;
    }

    private static void checkTseSignalMatrixPair(Context context, SharedPreferences sp, int matrixIndex,
                                                  String coin, String timeframe) throws Exception {
        JSONObject instrument=findSignalMatrixTseInstrument(context,coin);
        if(instrument==null) throw new Exception("TSE Matrix symbol is not present in native watchlist");
        String symbol=instrument.optString("symbol",coin);
        String code=instrument.optString("insCode","");
        List<TseCandle> candles=fetchTseCandles(code,timeframe);
        if(candles.isEmpty()) throw new Exception("TSE candle history is empty");
        JSONObject quote=fetchTsetmcClosingQuote(code);
        double live=numberOr(quote.opt("lastPrice"),Double.NaN);
        if(isPositive(live)) {
            TseCandle last=candles.get(candles.size()-1);
            double ratio=live/last.close;
            if(Double.isFinite(ratio)&&(ratio<0.5||ratio>2.0||(ratio<0.9||ratio>1.1))) {
                for(TseCandle c:candles){c.open*=ratio;c.high*=ratio;c.low*=ratio;c.close*=ratio;}
            }
        }
        int[] signals=calculateTseSignals(candles);
        String prefix="matrix_"+matrixIndex+"_"+coin+"_"+timeframe+"_";
        String[] keys={prefix+"rf",prefix+"kj",prefix+"st"};
        String initializedKey=prefix+"initialized";
        if(!sp.getBoolean(initializedKey,false)) {
            sp.edit().putBoolean(initializedKey,true).putInt(keys[0],signals[0])
                    .putInt(keys[1],signals[1]).putInt(keys[2],signals[2]).apply();
            return;
        }
        int[] old={sp.getInt(keys[0],signals[0]),sp.getInt(keys[1],signals[1]),sp.getInt(keys[2],signals[2])};
        String title="🔔 "+symbol+" • "+timeframeLabel(timeframe)+" • #"+matrixIndex;
        if(old[0]!=signals[0]) notifyMatrixTransition(context,sp,keys[0],"Range Filter",signals[0],title);
        if(old[2]!=signals[2]) notifyMatrixTransition(context,sp,keys[2],"Super Trend",signals[2],title);
        if(old[1]!=signals[1]) notifyMatrixTransition(context,sp,keys[1],"Kijun 288",signals[1],title);
        sp.edit().putInt(keys[0],signals[0]).putInt(keys[1],signals[1]).putInt(keys[2],signals[2]).apply();
    }

    private static void checkSignalMatrixPair(Context context, SharedPreferences sp, int matrixIndex, String coin, String timeframe) {
        try {
            if ("USD".equalsIgnoreCase(coin) || "COIN".equalsIgnoreCase(coin)) {
                checkChartAssetSignalMatrixPair(context, sp, matrixIndex, coin.toUpperCase(Locale.ROOT), timeframe);
                return;
            }
            if (coin != null && coin.toUpperCase(Locale.ROOT).startsWith("TSE|")) {
                checkTseSignalMatrixPair(context, sp, matrixIndex, coin, timeframe);
                return;
            }
            String query = "?coin=" + Uri.encode(coin)
                    + "&timeframe=" + Uri.encode(timeframe)
                    + "&t=" + System.currentTimeMillis();
            JSONObject data = new JSONObject(httpGetSignalMatrixWithFailover(query));
            int rf = signalValue(data.has("RF") ? data.opt("RF") : data.opt("rf"));
            int kj = signalValue(data.has("KJ") ? data.opt("KJ") : data.opt("kj"));
            int st = signalValue(data.has("ST") ? data.opt("ST") : data.opt("st"));
            String prefix = "matrix_" + matrixIndex + "_" + coin + "_" + timeframe + "_";
            String rfKey = prefix + "rf", kjKey = prefix + "kj", stKey = prefix + "st", initializedKey = prefix + "initialized";
            if (!sp.getBoolean(initializedKey, false)) {
                sp.edit().putBoolean(initializedKey, true).putInt(rfKey, rf).putInt(kjKey, kj).putInt(stKey, st).apply();
                return;
            }
            int oldRF = sp.getInt(rfKey, rf), oldKJ = sp.getInt(kjKey, kj), oldST = sp.getInt(stKey, st);
            if (oldRF == rf && oldKJ == kj && oldST == st) return;
            List<String> changes = new ArrayList<>();
            if (oldRF != rf) changes.add("Range Filter");
            if (oldST != st) changes.add("Super Trend");
            if (oldKJ != kj) changes.add("Kijun 288");
            String title = "🔔 " + coin + " • " + timeframeLabel(timeframe) + " • #" + matrixIndex;

            // Each color transition has its own Android notification channel so
            // Android can play the requested sound even when the app is closed.
            if (oldRF != rf) notifyMatrixTransition(context, sp, prefix + "rf", "Range Filter", rf, title);
            if (oldST != st) notifyMatrixTransition(context, sp, prefix + "st", "Super Trend", st, title);
            if (oldKJ != kj) notifyMatrixTransition(context, sp, prefix + "kj", "Kijun 288", kj, title);

            sp.edit().putInt(rfKey, rf).putInt(kjKey, kj).putInt(stKey, st).apply();
        } catch (Exception ignored) {
        }
    }

    private static void notifyMatrixTransition(
            Context context,
            SharedPreferences sp,
            String stateKey,
            String signalName,
            int newValue,
            String title
    ) {
        try {
            String color = newValue > 0 ? "WHITE" : newValue < 0 ? "BLUE" : "NEUTRAL";
            if ("NEUTRAL".equals(color)) return;
            String alertKey = stateKey + "_color_alert";
            String signature = color;
            String previous = sp.getString(alertKey, "");
            long previousAt = sp.getLong(alertKey + "_at", 0L);
            long now = System.currentTimeMillis();
            if (signature.equals(previous) && now - previousAt < NOTIFICATION_COOLDOWN_MS) return;
            String type = "WHITE".equals(color) ? "RFKJ_WHITE" : "RFKJ_BLUE";
            NotificationHelper.show(context, type, title, signalName + " → " + color, false);
            sp.edit().putString(alertKey, signature).putLong(alertKey + "_at", now).apply();
        } catch (Exception ignored) {
        }
    }

    private static String timeframeLabel(String timeframe) {
        if ("1".equals(timeframe)) return "1M";
        if ("3".equals(timeframe)) return "3M";
        if ("5".equals(timeframe)) return "5M";
        if ("15".equals(timeframe)) return "15M";
        if ("30".equals(timeframe)) return "30M";
        if ("60".equals(timeframe)) return "1H";
        if ("4h".equalsIgnoreCase(timeframe)) return "4H";
        return timeframe;
    }

    // A failed background poll must not create a false signal notification.
    private static String signalMatrixDirectionArrow(int value) {
        if (value > 0) return "↑ ⬜";
        if (value < 0) return "↓ 🟦";
        return "→";
    }

    private static String joinChanges(List<String> changes) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < changes.size(); i++) {
            if (i > 0) out.append(" • ");
            out.append(changes.get(i));
        }
        return out.toString();
    }

    private static final String GLOBAL_PULSE_COMMODITY_BATCH_URL =
            "https://biquote.io/api/latest?symbols=XAUUSD&symbols=USOIL";
    private static final String GLOBAL_PULSE_GOLD_OHLC_URL =
            "https://biquote.io/api/XAUUSD/ohlc?interval=1d&limit=3";
    private static final String GLOBAL_PULSE_OIL_OHLC_URL =
            "https://biquote.io/api/USOIL/ohlc?interval=1d&limit=3";
    private static final int[] GLOBAL_PULSE_ALERT_LEVELS =
            new int[]{-10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    /**
     * Background owner for Global Pulse. It monitors Gold's 24H percentage
     * change and alerts only when one of the requested thresholds is crossed.
     * State is persisted so reopening/closing the app does not replay alerts.
     */
    private static boolean isTsetmcLiveWindow() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tehran"), Locale.US);
        int day = cal.get(Calendar.DAY_OF_WEEK);
        // Java: Sunday=1, Monday=2, ... Saturday=7. Active: Sat-Wed.
        boolean activeDay = day == Calendar.SATURDAY || day == Calendar.SUNDAY
                || day == Calendar.MONDAY || day == Calendar.TUESDAY || day == Calendar.WEDNESDAY;
        if (!activeDay) return false;
        int minute = cal.get(Calendar.HOUR_OF_DAY) * 60 + cal.get(Calendar.MINUTE);
        return minute >= TSE_TSETMC_START_MIN && minute < TSE_TSETMC_END_MIN;
    }

    private static void checkTsetmcPulse(Context context) {
        SharedPreferences sp = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
        if (!isTsetmcLiveWindow()) return;
        long now = System.currentTimeMillis();
        long lastFetch = sp.getLong("tsetmc_last_fetch", 0L);
        if (now - lastFetch < TSE_TSETMC_REFRESH_MS - 2_000L) return;
        try {
            JSONArray configured = readConfiguredTsetmcWatchlist(context);
            SharedPreferences.Editor editor = sp.edit().putLong("tsetmc_last_fetch", now);
            if (sp.getBoolean("tsetmc_watchlist_configured", false)) {
                for (int i = 0; i < configured.length(); i++) {
                    JSONObject instrument = configured.optJSONObject(i);
                    if (instrument == null) continue;
                    String symbol = instrument.optString("symbol", "").trim();
                    String code = instrument.optString("insCode", "").trim();
                    if (symbol.isEmpty() || !code.matches("\\d+")) continue;
                    try {
                        JSONObject quote = fetchTsetmcClosingQuote(code);
                        processTsetmcPulseRow(context, editor, symbol,
                                numberOr(quote.opt("lastPricePercent"), Double.NaN),
                                numberOr(quote.opt("lastPrice"), Double.NaN), now);
                    } catch (Exception ignored) {
                        // A failed symbol does not block alerts for the other selected slots.
                    }
                }
            } else {
                // Preserve legacy Global Pulse behavior until the user configures GLOBAL slots.
                JSONObject data = new JSONObject(httpGetTsetmcWithFailover(context, now));
                if (!data.optBoolean("ok", false)) return;
                JSONArray results = data.optJSONArray("results");
                if (results == null || results.length() == 0) return;
                for (int i = 0; i < results.length(); i++) {
                    JSONObject row = results.optJSONObject(i);
                    if (row == null || !row.optBoolean("ok", true)) continue;
                    processTsetmcPulseRow(context, editor,
                            row.optString("symbol", ""),
                            row.optDouble("lastPricePercent", Double.NaN),
                            row.optDouble("lastPrice", Double.NaN), now);
                }
            }
            editor.apply();
        } catch (Throwable ignored) {
        }
    }

    private static void checkGlobalPulse(Context context) {
        try {
            JSONObject batch = new JSONObject(httpGet(GLOBAL_PULSE_COMMODITY_BATCH_URL + "&t=" + System.currentTimeMillis()));
            double goldPrice = biquotePrice(batch, "XAUUSD");
            double oilPrice = biquotePrice(batch, "USOIL");
            // Match the dashboard exactly: use Biquote's dayDiffPercent field
            // from the same latest-price response used by GLOBAL.
            double goldChange = biquoteDayDiffPercent(batch, "XAUUSD");
            double oilChange = biquoteDayDiffPercent(batch, "USOIL");

            evaluateGlobalPulseCommodity(context, "gold", "Gold", goldPrice, goldChange);
            evaluateGlobalPulseCommodity(context, "oil", "Oil", oilPrice, oilChange);
        } catch (Throwable ignored) {
        }
    }

    private static void evaluateGlobalPulseCommodity(Context context, String commodityKey, String label,
                                                     double currentPrice, double previousClose) {
        if (!isPositive(currentPrice) || !isPositive(previousClose)) return;
        double current = ((currentPrice - previousClose) / previousClose) * 100.0;
        if (!Double.isFinite(current)) return;

        SharedPreferences sp = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
        final String previousKey = "global_pulse_" + commodityKey + "_previous_percent";
        final String alertPrefix = "global_pulse_" + commodityKey + "_alert_";
        if (!sp.contains(previousKey)) {
            sp.edit().putString(previousKey, Double.toString(current)).apply();
            return;
        }
        double previous = numberOr(sp.getString(previousKey, ""), Double.NaN);
        if (!Double.isFinite(previous) || previous == current) {
            sp.edit().putString(previousKey, Double.toString(current)).apply();
            return;
        }

        SharedPreferences.Editor editor = sp.edit();
        long now = System.currentTimeMillis();
        for (int level : GLOBAL_PULSE_ALERT_LEVELS) {
            boolean crossed = (previous < level && current >= level)
                    || (previous > level && current <= level);
            if (!crossed) continue;
            String key = alertPrefix + level;
            long lastAlert = sp.getLong(key, 0L);
            if (now - lastAlert < NOTIFICATION_COOLDOWN_MS) continue;

            String pctText = (level > 0 ? "+" : "") + level + "%";
            String title = (level > 0 ? "🟢 ↑ " : "🔴 ↓ ") + "GLOBAL PULSE";
            String message = "GLOBAL PULSE • " + label + " • " + pctText
                    + " • Price " + formatNotificationPrice(currentPrice);
            NotificationHelper.show(context, "GLOBAL_PULSE", title, message, false);
            editor.putLong(key, now);
        }
        editor.putString(previousKey, Double.toString(current)).apply();
    }


    private static double biquoteDayDiffPercent(JSONObject data, String symbol) {
        try {
            JSONObject direct = data.optJSONObject(symbol);
            if (direct != null) {
                double value = numberOr(direct.opt("dayDiffPercent"), Double.NaN);
                if (Double.isFinite(value)) return value;
            }
            JSONObject symbols = data.optJSONObject("symbols");
            if (symbols != null) {
                JSONObject row = symbols.optJSONObject(symbol);
                if (row != null) {
                    double value = numberOr(row.opt("dayDiffPercent"), Double.NaN);
                    if (Double.isFinite(value)) return value;
                }
            }
        } catch (Exception ignored) {}
        return Double.NaN;
    }

    private static double biquotePrice(JSONObject data, String symbol) {
        try {
            JSONObject direct = data.optJSONObject(symbol);
            if (direct != null) {
                double mid = numberOr(direct.opt("mid"), Double.NaN);
                if (isPositive(mid)) return mid;
                double price = numberOr(direct.opt("price"), Double.NaN);
                if (isPositive(price)) return price;
            }
            JSONArray symbols = data.optJSONArray("data");
            if (symbols != null) {
                for (int i = 0; i < symbols.length(); i++) {
                    JSONObject item = symbols.optJSONObject(i);
                    if (item == null) continue;
                    if (symbol.equalsIgnoreCase(item.optString("symbol", ""))) {
                        double mid = numberOr(item.opt("mid"), Double.NaN);
                        if (isPositive(mid)) return mid;
                    }
                }
            }
        } catch (Exception ignored) {}
        return Double.NaN;
    }

    private static double biquotePreviousDailyClose(JSONObject data) {
        try {
            JSONArray bars = data.optJSONArray("bars");
            if (bars == null) bars = data.optJSONArray("data");
            if (bars == null) bars = data.optJSONArray("candles");
            if (bars == null) bars = data.optJSONArray("ohlc");
            if (bars == null || bars.length() < 2) return Double.NaN;

            List<JSONObject> valid = new ArrayList<>();
            for (int i = 0; i < bars.length(); i++) {
                JSONObject bar = bars.optJSONObject(i);
                if (bar == null) continue;
                double close = numberOr(bar.opt("close"), Double.NaN);
                if (!isPositive(close)) close = numberOr(bar.opt("c"), Double.NaN);
                if (isPositive(close)) valid.add(bar);
            }
            if (valid.size() < 2) return Double.NaN;

            // Biquote returns the most recent candle last. If it is still open,
            // use the candle immediately before it as the previous completed close.
            JSONObject latest = valid.get(valid.size() - 1);
            boolean latestOpen = latest.optBoolean("isOpen", false);
            int previousIndex = valid.size() - (latestOpen ? 2 : 1);
            if (previousIndex < 0) return Double.NaN;
            JSONObject previous = valid.get(previousIndex);
            double close = numberOr(previous.opt("close"), Double.NaN);
            if (!isPositive(close)) close = numberOr(previous.opt("c"), Double.NaN);
            return isPositive(close) ? close : Double.NaN;
        } catch (Exception ignored) {
            return Double.NaN;
        }
    }

    /**
     * Iran market timing alerts. Runs only from the native/background owner, so
     * the WebView clock and this checker cannot produce duplicate notifications.
     * Tehran market: Saturday through Wednesday, 08:45–12:30 local time.
     * Alerts are emitted once, approximately 5 minutes and 1 minute before open,
     * plus once 5 minutes before close. Timing alerts use their own notification category.
     */
    /** Global session/economic timing alerts. Uses only the device clock plus the
     * last economic event saved by the WebView, so timing alerts continue while
     * the app is minimized, closed, or temporarily offline. */
    private static void checkGlobalTimeSchedule(Context context) {
        try {
            SharedPreferences sp = context.getSharedPreferences("global_pulse_settings", Context.MODE_PRIVATE);
            Calendar localNow = Calendar.getInstance();
            if (sp.getBoolean("night_mode", false)) {
                int h = localNow.get(Calendar.HOUR_OF_DAY);
                if (h >= 22 || h < 8) return;
            }

            // These three sessions use their own IANA time zones, so the 5-minute
            // alert remains correct across daylight-saving changes.
            checkGlobalSession(context, sp, "SYDNEY", "Australia/Sydney", 7, 0, 16, 0);
            checkGlobalSession(context, sp, "LONDON", "Europe/London", 8, 0, 17, 0);
            checkGlobalSession(context, sp, "NEW YORK", "America/New_York", 8, 0, 17, 0);

            long eventAt = sp.getLong("economic_timestamp", 0L);
            if (eventAt > 0L) {
                long diff = eventAt - System.currentTimeMillis();
                if (diff >= 0L && diff <= 5L * 60L * 1000L) {
                    String key = "economic_" + eventAt;
                    if (!key.equals(sp.getString("economic_notice", ""))) {
                        String country = sp.getString("economic_country", "");
                        String title = sp.getString("economic_title", "Economic Event");
                        NotificationHelper.show(context, "GLOBAL_TIME",
                                "📅 ECONOMIC" + (country.isEmpty() ? "" : " · " + country),
                                title + " starts in 5 minutes", false);
                        sp.edit().putString("economic_notice", key).apply();
                    }
                }
                // R01: a second, distinct notification fires at the actual release time.
                // Keep the existing 5-minute reminder above unchanged.
                if (diff <= 30000L && diff >= -30000L) {
                    String exactKey = "economic_exact_" + eventAt;
                    if (!exactKey.equals(sp.getString("economic_exact_notice", ""))) {
                        String country = sp.getString("economic_country", "");
                        String title = sp.getString("economic_title", "Economic Event");
                        NotificationHelper.show(context, "GLOBAL_TIME",
                                "🔔 ECONOMIC" + (country.isEmpty() ? "" : " · " + country),
                                title + " is now live", false);
                        sp.edit().putString("economic_exact_notice", exactKey).apply();
                    }
                }
            }
        } catch (Throwable ignored) {}
    }

    private static void checkGlobalSession(Context context, SharedPreferences sp,
                                           String name, String tzId,
                                           int openHour, int openMinute,
                                           int closeHour, int closeMinute) {
        Calendar now = Calendar.getInstance(TimeZone.getTimeZone(tzId), Locale.US);
        int day = now.get(Calendar.DAY_OF_WEEK);
        if (day == Calendar.SATURDAY || day == Calendar.SUNDAY) return;

        int minute = now.get(Calendar.HOUR_OF_DAY) * 60 + now.get(Calendar.MINUTE);
        int open = openHour * 60 + openMinute;
        int close = closeHour * 60 + closeMinute;
        String dayKey = now.get(Calendar.YEAR) + "-" + now.get(Calendar.DAY_OF_YEAR);
        String prefKey = "session_notice_" + name.replace(" ", "_");

        // Opening alert: exactly once during the 5-minute window.
        int openDelta = open - minute;
        if (openDelta >= 0 && openDelta <= 5) {
            String key = name + "_" + dayKey + "_open5";
            if (!key.equals(sp.getString(prefKey, ""))) {
                NotificationHelper.show(context, "GLOBAL_TIME",
                        "🌐 " + name + " OPEN",
                        name + " market opens in 5 minutes", false);
                sp.edit().putString(prefKey, key).apply();
            }
            return;
        }

        // Closing alert: exactly once during the 5-minute window.
        // This is native/background-safe, so it also works when the WebView is
        // closed or the device is temporarily offline.
        int closeDelta = close - minute;
        if (closeDelta >= 0 && closeDelta <= 5) {
            String key = name + "_" + dayKey + "_close5";
            if (!key.equals(sp.getString(prefKey, ""))) {
                NotificationHelper.show(context, "GLOBAL_TIME",
                        "🌐 " + name + " CLOSE",
                        name + " market closes in 5 minutes", false);
                sp.edit().putString(prefKey, key).apply();
            }
        }
    }

    private static void checkIranMarketSchedule(Context context) {
        try {
            // The Iran-specific 5-minute and 1-minute alerts must obey the same
            // Global Pulse Night Mode switch as London/New York/Sydney/Economic.
            SharedPreferences globalSettings = context.getSharedPreferences("global_pulse_settings", Context.MODE_PRIVATE);
            if (globalSettings.getBoolean("night_mode", false)) {
                Calendar localNow = Calendar.getInstance();
                int h = localNow.get(Calendar.HOUR_OF_DAY);
                if (h >= 22 || h < 8) return;
            }

            Calendar now = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tehran"), Locale.US);
            int day = now.get(Calendar.DAY_OF_WEEK);
            boolean activeDay = day == Calendar.SUNDAY
                    || day == Calendar.MONDAY
                    || day == Calendar.TUESDAY
                    || day == Calendar.WEDNESDAY
                    || day == Calendar.SATURDAY;
            if (!activeDay) return;

            int minuteOfDay = now.get(Calendar.HOUR_OF_DAY) * 60 + now.get(Calendar.MINUTE);
            SharedPreferences sp = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
            String dayKey = now.get(Calendar.YEAR) + "-" + now.get(Calendar.DAY_OF_YEAR);

            final int openMinute = 8 * 60 + 45;
            // Background checks are normally around every 60 seconds. Use short
            // tolerance windows so the two opening alerts are not missed.
            if (minuteOfDay >= (openMinute - 5) && minuteOfDay < (openMinute - 4)) {
                String key = dayKey + "_open5";
                if (!key.equals(sp.getString("iran_market_notice_5m", ""))) {
                    NotificationHelper.show(
                            context,
                            "GLOBAL_TIME",
                            "🇮🇷 ↑ IRAN MARKET",
                            "Iran market opens in 5 minutes • 08:45 Tehran",
                            false
                    );
                    sp.edit().putString("iran_market_notice_5m", key).apply();
                }
            }

            if (minuteOfDay >= (openMinute - 1) && minuteOfDay < openMinute) {
                String key = dayKey + "_open1";
                if (!key.equals(sp.getString("iran_market_notice_1m", ""))) {
                    NotificationHelper.show(
                            context,
                            "GLOBAL_TIME",
                            "🇮🇷 ↑ IRAN MARKET",
                            "Iran market opens in 1 minute • 08:45 Tehran",
                            false
                    );
                    sp.edit().putString("iran_market_notice_1m", key).apply();
                }
            }

            final int closeMinute = 12 * 60 + 30;
            if (minuteOfDay >= (closeMinute - 5) && minuteOfDay < (closeMinute - 4)) {
                String key = dayKey + "_close5";
                if (!key.equals(sp.getString("iran_market_notice_close5", ""))) {
                    NotificationHelper.show(
                            context,
                            "GLOBAL_TIME",
                            "🇮🇷 ↓ IRAN MARKET",
                            "Iran market closes in 5 minutes • 12:30 Tehran",
                            false
                    );
                    sp.edit().putString("iran_market_notice_close5", key).apply();
                }
            }
        } catch (Throwable ignored) {
        }
    }

    private static void checkFearGreed(Context context) {
        try {
            JSONObject response = new JSONObject(httpGet(FEAR_GREED_API + "&t=" + System.currentTimeMillis()));
            JSONArray data = response.optJSONArray("data");
            if (data == null || data.length() == 0) return;

            JSONObject item = data.optJSONObject(0);
            if (item == null) return;

            double value = numberOr(item.opt("value"), Double.NaN);
            if (!Double.isFinite(value) || value < 0.0 || value > 100.0) return;
            value = Math.max(0.0, Math.min(100.0, value));

            String band = fearGreedBand(value);
            String icon = fearGreedIcon(band);
            String label = fearGreedLabel(band);

            SharedPreferences sp = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
            String previousBand = sp.getString("fear_greed_band", "");
            if (previousBand.isEmpty()) {
                sp.edit()
                        .putString("fear_greed_band", band)
                        .putFloat("fear_greed_value", (float) value)
                        .apply();
                return;
            }

            if (!previousBand.equals(band)) {
                String previousIcon = fearGreedIcon(previousBand);
                String previousLabel = fearGreedLabel(previousBand);
                String title = icon + " Fear & Greed";
                String message = previousIcon + " " + previousLabel
                        + " → " + icon + " " + label
                        + " • " + Math.round(value);

                NotificationHelper.show(context, "FEAR_GREED", title, message, false);
            }

            sp.edit()
                    .putString("fear_greed_band", band)
                    .putFloat("fear_greed_value", (float) value)
                    .apply();
        } catch (Throwable ignored) {
            // Fear & Greed is an optional notification source; never affect other alerts.
        }
    }

    private static String fearGreedBand(double value) {
        if (value <= 24.0) return "extreme-fear";
        if (value <= 44.0) return "fear";
        if (value <= 55.0) return "neutral";
        if (value <= 75.0) return "greed";
        return "extreme-greed";
    }

    private static String fearGreedLabel(String band) {
        if ("extreme-fear".equals(band)) return "Extreme Fear";
        if ("fear".equals(band)) return "Fear";
        if ("neutral".equals(band)) return "Neutral";
        if ("greed".equals(band)) return "Greed";
        if ("extreme-greed".equals(band)) return "Extreme Greed";
        return "Unknown";
    }

    private static String fearGreedIcon(String band) {
        if ("extreme-fear".equals(band)) return "🔴";
        if ("fear".equals(band)) return "🟠";
        if ("neutral".equals(band)) return "⚪";
        if ("greed".equals(band)) return "🟢";
        if ("extreme-greed".equals(band)) return "🔥";
        return "";
    }

    private static void checkDashboard(Context context) {
        try {
            JSONObject data = new JSONObject(httpGet(DATA_URL + "?t=" + System.currentTimeMillis()));

            List<BankRatePeriod> bankRatePeriods = parseBankRatePeriods(data.optJSONArray("bankRatePeriods"));
            // Exchange endpoints and Rial/Toman conversion are owned by index.html,
            // not by data.json. Keep native background checks on the same sources.
            double divisor = 10.0;
            String priceApi = "https://apiv2.nobitex.ir/v3/orderbook/USDTIRT";
            String exirApi = "https://tether.fridnika.workers.dev/";
            String bitpinApi = "https://api.bitpin.org/api/v1/mth/orderbook/USDT_IRT/";

            List<Double> prices = new ArrayList<>();
            double nobitex = fetchNobitexPrice(priceApi, divisor);
            double exir = fetchExirPrice(exirApi);
            double bitpin = fetchBitpinPrice(bitpinApi);

            if (isPositive(nobitex)) prices.add(nobitex);
            if (isPositive(exir)) prices.add(exir);
            if (isPositive(bitpin)) prices.add(bitpin);

            double averagePrice;
            if (!prices.isEmpty()) {
                double sum = 0.0;
                for (double p : prices) sum += p;
                averagePrice = sum / prices.size();
                context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                        .edit()
                        .putString("last_average_price", Double.toString(averagePrice))
                        .apply();
            } else {
                String savedAverage = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                        .getString("last_average_price", "");
                averagePrice = numberOr(savedAverage, Double.NaN);
                if (!isPositive(averagePrice)) {
                    return;
                }
            }

            JSONArray transactionArray = data.optJSONArray("transactions");
            List<Transaction> transactions = parseTransactions(transactionArray);
            if (transactions.isEmpty()) return;

            Collections.sort(transactions, Comparator.comparingInt(t -> dateNumber(t.date)));

            String today = todayJalali();
            List<Transaction> sixTransactions = topSixHighestPricePurchases(transactions);
            List<Transaction> lastTransactions = getLastPeriod(transactions);

            double overallPercent = calculatePortfolio(transactions, averagePrice, today, bankRatePeriods).plBank;
            double sixPercent = calculatePortfolio(sixTransactions, averagePrice, today, bankRatePeriods).plBank;
            double lastPercent = calculatePortfolio(lastTransactions, averagePrice, today, bankRatePeriods).plBank;

            evaluateCard(context, "overallMainPrice", overallPercent, "Overall");
            evaluateCard(context, "sixMainPrice", sixPercent, "Top Purchases");
            evaluateCard(context, "lastMainPrice", lastPercent, "Last Period");
            clearApplicationError(context);
        } catch (Exception error) {
            notifyApplicationErrorIfNeeded(context, error);
        }
    }

    private static void notifyApplicationErrorIfNeeded(Context context, Exception error) {
        // Application-error notifications are intentionally suppressed while the
        // device has no validated internet connection. This rule applies only to
        // APP_ERROR; all other notification types keep their existing behavior.
        if (!isValidatedInternetAvailable(context)) {
            return;
        }

        SharedPreferences sp = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
        long now = System.currentTimeMillis();
        boolean active = sp.getBoolean("application_error_active", false);
        long lastNotified = sp.getLong("application_error_last_notification", 0L);
        if (!active || now - lastNotified >= APP_ERROR_REPEAT_MS) {
            NotificationHelper.show(
                    context,
                    "APP_ERROR",
                    "🔔 Application #1 error",
                    "Application #1 error",
                    false
            );
            sp.edit()
                    .putBoolean("application_error_active", true)
                    .putLong("application_error_last_notification", now)
                    .apply();
        } else if (!active) {
            sp.edit().putBoolean("application_error_active", true).apply();
        }
    }


    private static boolean isValidatedInternetAvailable(Context context) {
        try {
            android.net.ConnectivityManager cm =
                    (android.net.ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
            if (cm == null) return false;

            android.net.Network network = cm.getActiveNetwork();
            if (network == null) return false;

            android.net.NetworkCapabilities capabilities = cm.getNetworkCapabilities(network);
            if (capabilities == null) return false;

            return capabilities.hasCapability(
                    android.net.NetworkCapabilities.NET_CAPABILITY_INTERNET
            ) && capabilities.hasCapability(
                    android.net.NetworkCapabilities.NET_CAPABILITY_VALIDATED
            );
        } catch (Exception ignored) {
            return false;
        }
    }

    private static void clearApplicationError(Context context) {
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                .edit()
                .putBoolean("application_error_active", false)
                .remove("application_error_last_notification")
                .apply();
    }

    private static void evaluateCard(
            Context context,
            String cardId,
            double percent,
            String title
    ) {
        if (!Double.isFinite(percent)) return;

        SharedPreferences sp = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
        String prefix = "card_" + cardId + "_";

        boolean initialized = sp.getBoolean(prefix + "initialized", false);
        if (!initialized) {
            sp.edit()
                    .putBoolean(prefix + "initialized", true)
                    .putString(prefix + "color", colorFromPercent(percent))
                    .putFloat(prefix + "last_percent", (float) percent)
                    .putString(prefix + "candidate_color", "")
                    .putInt(prefix + "candidate_count", 0)
                    .apply();
            return;
        }

        String confirmedColor = sp.getString(prefix + "color", colorFromPercent(percent));
        String candidateColor = sp.getString(prefix + "candidate_color", "");
        int candidateCount = sp.getInt(prefix + "candidate_count", 0);
        double previousPercent = sp.getFloat(prefix + "last_percent", (float) percent);

        String currentColor = colorFromPercent(percent);
        String colorMessage = null;

        if ("neutral".equals(currentColor)) {
            candidateColor = "";
            candidateCount = 0;
        } else if (currentColor.equals(confirmedColor)) {
            candidateColor = "";
            candidateCount = 0;
        } else {
            if (!currentColor.equals(candidateColor)) {
                candidateColor = currentColor;
                candidateCount = 1;
            } else {
                candidateCount++;
            }

            if (candidateCount >= SIGN_CONFIRMATIONS_REQUIRED) {
                String previousColor = confirmedColor;
                confirmedColor = currentColor;
                candidateColor = "";
                candidateCount = 0;

                if ("red".equals(previousColor) && "green".equals(currentColor)) {
                    colorMessage = "Investment is better than bank return";
                } else if ("green".equals(previousColor) && "red".equals(currentColor)) {
                    colorMessage = "Bank return is better than investment";
                }
            }
        }

        /*
         * Threshold notifications are allowed ONLY for Overall.
         * Top Purchases and Last Period must never notify for numeric
         * threshold crossings (1, 2, 3, 4, 5, ...). Their color-change
         * notifications remain enabled.
         */
        double crossed = crossedThreshold(previousPercent, percent);
        String thresholdMessage = null;
        boolean thresholdCrossedDown = false;
        boolean thresholdCrossedUp = false;
        if ("overallMainPrice".equals(cardId) && crossed != 0.0) {
            // Thresholds are based on absolute distance from the bank line.
            // The icon must therefore follow movement through that absolute
            // threshold: farther from the bank line = UP (green), closer = DOWN (red).
            // This works correctly on both positive and negative percentages.
            double previousAbs = Math.abs(previousPercent);
            double currentAbs = Math.abs(percent);
            thresholdCrossedDown = currentAbs < previousAbs;
            thresholdCrossedUp = currentAbs > previousAbs;
            String thresholdKey = prefix + "threshold_" + thresholdKey(crossed);
            long lastTime = sp.getLong(thresholdKey, 0L);
            long now = System.currentTimeMillis();
            if (now - lastTime >= NOTIFICATION_COOLDOWN_MS) {
                sp.edit().putLong(thresholdKey, now).apply();
                String sign = percent >= 0 ? "+" : "-";
                thresholdMessage = title + ": Price " + sign + formatThreshold(crossed) + "% vs Bank";
            }
        }

        String message = colorMessage != null ? colorMessage : thresholdMessage;
        if (message != null) {
            String icon;
            if ("overallMainPrice".equals(cardId) && thresholdMessage != null && colorMessage == null) {
                // Overall threshold direction is explicit and independent of the
                // threshold sign: movement upward = green, downward = red.
                icon = thresholdCrossedUp ? "🟢" : thresholdCrossedDown ? "🔴" : "";
            } else {
                icon = notificationIcon(message);
            }
            String fullTitle = title;
            String fullMessage = icon.isEmpty() ? message : icon + " " + message;
            NotificationHelper.show(context, "USD", fullTitle, fullMessage, false);
        }

        sp.edit()
                .putBoolean(prefix + "initialized", true)
                .putString(prefix + "color", confirmedColor)
                .putString(prefix + "candidate_color", candidateColor)
                .putInt(prefix + "candidate_count", candidateCount)
                .putFloat(prefix + "last_percent", (float) percent)
                .apply();
    }

    private static String notificationIcon(String message) {
        if (message.contains("Investment is better than bank return")) return "🟢";
        if (message.contains("Bank return is better than investment")) return "🔴";
        if (message.contains("Price +")) return "🟢";
        if (message.contains("Price -")) return "🔴";
        return "";
    }

    private static String colorFromPercent(double percent) {
        if (percent == 0.0) return "neutral";
        return percent > 0.0 ? "green" : "red";
    }

    private static double crossedThreshold(double previous, double current) {
        if (!Double.isFinite(previous) || !Double.isFinite(current)) return 0.0;

        double previousAbs = Math.abs(previous);
        double currentAbs = Math.abs(current);

        if (currentAbs > previousAbs) {
            for (double threshold = 0.5; threshold <= 1000.0; threshold += (threshold < 1.0 ? 0.5 : 1.0)) {
                if (threshold > previousAbs && threshold <= currentAbs) return threshold;
            }
        }

        if (currentAbs < previousAbs) {
            for (double threshold = 1000.0; threshold >= 0.5; threshold -= (threshold <= 1.0 ? 0.5 : 1.0)) {
                if (threshold < previousAbs && threshold >= currentAbs) return threshold;
            }
        }

        return 0.0;
    }

    private static String thresholdKey(double value) {
        return String.format(Locale.US, "%.1f", value).replace('.', '_');
    }

    private static String formatThreshold(double value) {
        if (value == Math.rint(value)) return Long.toString((long) value);
        return String.format(Locale.US, "%.1f", value);
    }

    private static List<Transaction> parseTransactions(JSONArray array) {
        List<Transaction> out = new ArrayList<>();
        if (array == null) return out;
        for (int i = 0; i < array.length(); i++) {
            JSONObject o = array.optJSONObject(i);
            if (o == null) continue;
            String type = o.optString("type", "");
            if (!"buy".equals(type) && !"sell".equals(type)) continue;
            double amount = o.optDouble("amount", Double.NaN);
            double price = o.optDouble("price", Double.NaN);
            String date = o.optString("date", "");
            if (!Double.isFinite(amount) || !Double.isFinite(price) || amount <= 0 || price < 0 || date.isEmpty()) continue;
            out.add(new Transaction(type, amount, price, date));
        }
        return out;
    }

    private static List<Transaction> topSixHighestPricePurchases(List<Transaction> transactions) {
        List<Transaction> buys = new ArrayList<>();
        for (Transaction t : transactions) if ("buy".equals(t.type)) buys.add(t);
        buys.sort((a, b) -> Double.compare(b.price, a.price));
        return new ArrayList<>(buys.subList(0, Math.min(6, buys.size())));
    }

    private static List<Transaction> getLastPeriod(List<Transaction> transactions) {
        List<Transaction> current = new ArrayList<>();
        List<List<Transaction>> periods = buildPeriods(transactions);
        if (!periods.isEmpty()) current.addAll(periods.get(periods.size() - 1));
        return current;
    }

    private static List<List<Transaction>> buildPeriods(List<Transaction> transactions) {
        List<List<Transaction>> periods = new ArrayList<>();
        List<Transaction> current = null;
        for (Transaction t : transactions) {
            if ("buy".equals(t.type)) {
                if (current != null && !current.isEmpty()) periods.add(current);
                current = new ArrayList<>();
                current.add(t);
            } else if ("sell".equals(t.type) && current != null) {
                current.add(t);
            }
        }
        if (current != null && !current.isEmpty()) periods.add(current);
        return periods;
    }

    private static PortfolioResult calculatePortfolio(
            List<Transaction> transactions,
            double currentPrice,
            String today,
            List<BankRatePeriod> bankRatePeriods
    ) {
        List<Lot> lots = new ArrayList<>();

        for (Transaction t : transactions) {
            if ("buy".equals(t.type)) {
                lots.add(new Lot(t.amount, t.price, t.date));
            } else if ("sell".equals(t.type)) {
                double remaining = t.amount;
                while (remaining > 0 && !lots.isEmpty()) {
                    Lot lot = lots.get(0);
                    double used = Math.min(remaining, lot.amount);
                    lot.amount -= used;
                    remaining -= used;
                    if (lot.amount <= 0.0000001) lots.remove(0);
                }
            }
        }

        double currentQuantity = 0.0;
        double paidAmount = 0.0;
        for (Lot lot : lots) {
            currentQuantity += lot.amount;
            paidAmount += lot.amount * lot.price;
        }

        if (currentQuantity <= 0) return new PortfolioResult(0.0);

        double bankValue = 0.0;
        for (Lot lot : lots) {
            bankValue += compoundValue(lot.amount * lot.price, lot.date, today, bankRatePeriods);
        }

        double currentValue = currentQuantity * currentPrice;
        double unrealizedBankAmount = currentValue - bankValue;
        double plBank = bankValue > 0 ? (unrealizedBankAmount / bankValue) * 100.0 : 0.0;
        return new PortfolioResult(plBank);
    }

    private static List<BankRatePeriod> parseBankRatePeriods(JSONArray array) {
        if (array == null || array.length() == 0) {
            throw new IllegalArgumentException(
                    "data.json must contain a non-empty bankRatePeriods array."
            );
        }

        List<BankRatePeriod> periods = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            JSONObject item = array.optJSONObject(i);
            if (item == null) continue;

            String from = item.optString("from", "").trim();
            double rate = item.optDouble("rate", Double.NaN);
            if (!from.matches("\\d{4}/\\d{2}/\\d{2}") ||
                    !Double.isFinite(rate) || rate < 0) {
                continue;
            }
            periods.add(new BankRatePeriod(from, rate));
        }

        if (periods.isEmpty()) {
            throw new IllegalArgumentException(
                    "bankRatePeriods contains no valid rate periods."
            );
        }

        Collections.sort(periods, Comparator.comparingInt(p -> dateNumber(p.from)));
        for (int i = 1; i < periods.size(); i++) {
            if (periods.get(i).from.equals(periods.get(i - 1).from)) {
                throw new IllegalArgumentException(
                        "Duplicate bankRatePeriods date: " + periods.get(i).from
                );
            }
        }
        return periods;
    }

    private static double activeBankRate(List<BankRatePeriod> periods, String date) {
        BankRatePeriod active = periods.get(0);
        int target = dateNumber(date);
        for (BankRatePeriod period : periods) {
            if (dateNumber(period.from) <= target) active = period;
            else break;
        }
        return active.rate;
    }

    private static double compoundValue(
            double principal,
            String startDate,
            String endDate,
            List<BankRatePeriod> bankRatePeriods
    ) {
        if (principal <= 0) return 0.0;

        long startDay = jalaliEpochDay(startDate);
        long endDay = jalaliEpochDay(endDate);
        if (endDay < startDay) return principal;

        double value = principal;
        for (int i = 0; i < bankRatePeriods.size(); i++) {
            BankRatePeriod period = bankRatePeriods.get(i);
            long periodStart = jalaliEpochDay(period.from);
            long nextPeriodStart = i + 1 < bankRatePeriods.size()
                    ? jalaliEpochDay(bankRatePeriods.get(i + 1).from)
                    : Long.MAX_VALUE;

            long segmentStart = Math.max(startDay, periodStart);
            long segmentEnd = Math.min(endDay, nextPeriodStart - 1L);
            if (segmentEnd >= segmentStart) {
                long days = segmentEnd - segmentStart + 1L;
                value *= Math.pow(1.0 + period.rate / 100.0 / 365.0, days);
            }
        }
        return value;
    }

    private static long jalaliEpochDay(String date) {
        String[] p = date.split("/");
        if (p.length != 3) throw new IllegalArgumentException("Invalid Jalali date: " + date);
        return gregorianEpochDayFromJalali(
                Integer.parseInt(p[0]),
                Integer.parseInt(p[1]),
                Integer.parseInt(p[2])
        );
    }

    private static int daysBetweenInclusive(String start, String end) {
        try {
            String[] s = start.split("/");
            String[] e = end.split("/");
            if (s.length != 3 || e.length != 3) return 0;
            long startDay = gregorianEpochDayFromJalali(Integer.parseInt(s[0]), Integer.parseInt(s[1]), Integer.parseInt(s[2]));
            long endDay = gregorianEpochDayFromJalali(Integer.parseInt(e[0]), Integer.parseInt(e[1]), Integer.parseInt(e[2]));
            return (int) Math.max(0L, endDay - startDay + 1L);
        } catch (Exception ignored) {
            return 0;
        }
    }

    /** Matches the Gregorian/Jalali conversion used by index.html. */
    private static long gregorianEpochDayFromJalali(int jy, int jm, int jd) {
        int j = jy - 979;
        long days = 365L * j
                + (j / 33) * 8L
                + ((j % 33 + 3) / 4);
        for (int i = 1; i < jm; i++) days += i <= 6 ? 31 : 30;
        days += jd - 1L;

        int gy = 1600 + 400 * (int) (days / 146097L);
        days %= 146097L;
        if (days > 36524L) {
            gy += 100 * (int) (--days / 36524L);
            days %= 36524L;
            if (days >= 365L) days++;
        }
        gy += 4 * (int) (days / 1461L);
        days %= 1461L;
        if (days > 365L) {
            gy += (int) ((days - 1L) / 365L);
            days = (days - 1L) % 365L;
        }
        int gd = (int) days + 1;

        return epochDay(gy, monthFromDayOfYear(gy, gd), dayFromDayOfYear(gy, gd));
    }

    private static int monthFromDayOfYear(int year, int dayOfYear) {
        int[] md = {31, isLeap(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int m = 1;
        int d = dayOfYear;
        while (m <= 12 && d > md[m - 1]) { d -= md[m - 1]; m++; }
        return m;
    }

    private static int dayFromDayOfYear(int year, int dayOfYear) {
        int[] md = {31, isLeap(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int m = 1;
        int d = dayOfYear;
        while (m <= 12 && d > md[m - 1]) { d -= md[m - 1]; m++; }
        return d;
    }

    private static boolean isLeap(int y) {
        return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    }

    private static long epochDay(int year, int month, int day) {
        int y = year;
        long total = 0;
        for (int yy = 1970; yy < y; yy++) total += isLeap(yy) ? 366 : 365;
        if (y < 1970) {
            total = 0;
            for (int yy = y; yy < 1970; yy++) total -= isLeap(yy) ? 366 : 365;
        }
        int[] md = {31, isLeap(y) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        for (int m = 1; m < month; m++) total += md[m - 1];
        return total + day - 1;
    }

    private static String todayJalali() {
        java.util.Calendar now = java.util.Calendar.getInstance();
        int gy = now.get(java.util.Calendar.YEAR);
        int gm = now.get(java.util.Calendar.MONTH) + 1;
        int gd = now.get(java.util.Calendar.DAY_OF_MONTH);
        int[] gdm = {0,31,59,90,120,151,181,212,243,273,304,334};
        int jy = gy > 1600 ? 979 : 0;
        int y = gy > 1600 ? gy - 1600 : gy - 621;
        int days = 365 * y
                + (int)Math.floor((y + 3) / 4.0)
                - (int)Math.floor((y + 99) / 100.0)
                + (int)Math.floor((y + 399) / 400.0)
                - 80 + gd + gdm[gm - 1];
        jy += 33 * (days / 12053);
        days %= 12053;
        jy += 4 * (days / 1461);
        days %= 1461;
        if (days > 365) {
            jy += (days - 1) / 365;
            days = (days - 1) % 365;
        }
        int jm;
        int jd;
        if (days < 186) {
            jm = 1 + days / 31;
            jd = 1 + days % 31;
        } else {
            jm = 7 + (days - 186) / 30;
            jd = 1 + (days - 186) % 30;
        }
        return jy + "/" + String.format(Locale.US, "%02d", jm) + "/" + String.format(Locale.US, "%02d", jd);
    }

    private static int dateNumber(String date) {
        try {
            String[] p = date.split("/");
            if (p.length != 3) return 0;
            return Integer.parseInt(p[0]) * 10000 + Integer.parseInt(p[1]) * 100 + Integer.parseInt(p[2]);
        } catch (Exception ignored) {
            return 0;
        }
    }

    private static int signalValue(Object value) {
        if (value == null || JSONObject.NULL.equals(value)) return 0;
        if (value instanceof JSONObject) {
            JSONObject o = (JSONObject) value;
            String[] keys = {"signal", "type", "action", "direction", "value", "score"};
            for (String k : keys) if (o.has(k)) return signalValue(o.opt(k));
        }
        String text = String.valueOf(value).trim().toUpperCase(Locale.ROOT);
        if (text.contains("BUY") || text.contains("BULL") || text.contains("LONG")) return 1;
        if (text.contains("SELL") || text.contains("BEAR") || text.contains("SHORT")) return -1;
        try {
            double n = Double.parseDouble(text);
            return n > 0 ? 1 : n < 0 ? -1 : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Signal Matrix recovery policy: primary worker, Supabase fallback, then
     * one final primary retry. Always probing primary first detects recovery
     * immediately instead of remaining pinned to a stale fallback.
     */
    private static String httpGetSignalMatrixWithFailover(String query) throws Exception {
        String[] bases = {SIGNAL_MATRIX_WORKER_URL, SIGNAL_MATRIX_FALLBACK_URL};
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

    private static String httpGetWithFallback(String target) throws Exception {
        if (target == null || target.isEmpty()) throw new IllegalArgumentException("Empty URL");
        List<String> urls = new ArrayList<>();
        urls.add(target);
        if (!target.contains("proxy.cors.dev/")) urls.add("https://proxy.cors.dev/" + target);
        String encoded = Uri.encode(target);
        urls.add("https://api.cors.syrins.tech/?url=" + encoded);
        urls.add("https://api.allorigins.win/raw?url=" + encoded);
        Exception last = null;
        for (String candidate : urls) {
            try {
                return httpGet(candidate);
            } catch (Exception e) {
                last = e;
            }
        }
        if (last != null) throw last;
        throw new IllegalStateException("No API URL available");
    }

    private static double fetchNobitexPrice(String url, double divisor) {
        try {
            JSONObject data = new JSONObject(httpGetWithFallback(url));
            JSONArray asks = data.optJSONArray("asks");
            if (asks == null || asks.length() == 0) return Double.NaN;
            double best = Double.POSITIVE_INFINITY;
            for (int i = 0; i < asks.length(); i++) {
                Object item = asks.opt(i);
                double price = Double.NaN;
                if (item instanceof JSONArray) price = ((JSONArray) item).optDouble(0, Double.NaN);
                else if (item instanceof JSONObject) { JSONObject row = (JSONObject) item; price = row.optDouble("price", row.optDouble("rate", row.optDouble("value", Double.NaN))); }
                if (isPositive(price) && price < best) best = price;
            }
            return best == Double.POSITIVE_INFINITY ? Double.NaN : best / (divisor > 0 ? divisor : 1.0);
        } catch (Exception e) { return Double.NaN; }
    }

    private static double fetchExirPrice(String ignoredUrl) {
        // Primary source from the dashboard. It returns a plain numeric price.
        try {
            String raw = httpGetWithFallback("https://tether.fridnika.workers.dev/").trim().replace(",", "");
            double value = Double.parseDouble(raw);
            if (isPositive(value)) return value;
        } catch (Exception ignored) {}
        // Same fallback order as index.html: ticker first, then orderbook asks.
        try {
            JSONObject ticker = new JSONObject(httpGetWithFallback("https://api.exir.io/v2/ticker?symbol=usdt-irt"));
            JSONObject data = ticker.optJSONObject("data");
            JSONObject result = ticker.optJSONObject("result");
            JSONObject dataResult = data == null ? null : data.optJSONObject("result");
            JSONObject[] candidates = new JSONObject[]{ticker, data, result, dataResult, ticker.optJSONObject("ticker")};
            String[] pairKeys = new String[]{"usdt-irt", "USDT-IRT", "USDT_IRT", "usdt_irt"};
            String[] keys = new String[]{"last", "close", "price", "lastPrice"};
            for (JSONObject candidate : candidates) {
                if (candidate == null) continue;
                JSONObject pair = candidate;
                for (String pairKey : pairKeys) { if (candidate.optJSONObject(pairKey) != null) { pair = candidate.optJSONObject(pairKey); break; } }
                for (String key : keys) {
                    Object raw = pair.opt(key);
                    if (raw == null || raw == JSONObject.NULL) continue;
                    try { double value = Double.parseDouble(String.valueOf(raw).replace(",", "").trim()); if (isPositive(value)) return value; } catch (Exception ignored) {}
                }
            }
        } catch (Exception ignored) {}
        try {
            JSONObject data = new JSONObject(httpGetWithFallback("https://api.exir.io/v2/orderbook?symbol=usdt-irt"));
            JSONObject pair = data.optJSONObject("usdt-irt");
            JSONArray asks = pair == null ? null : pair.optJSONArray("asks");
            if (asks == null || asks.length() == 0) return Double.NaN;
            double lowest = Double.POSITIVE_INFINITY;
            for (int i = 0; i < asks.length(); i++) {
                Object item = asks.opt(i);
                double price = item instanceof JSONArray ? ((JSONArray)item).optDouble(0, Double.NaN) : item instanceof JSONObject ? ((JSONObject)item).optDouble("price", ((JSONObject)item).optDouble("rate", ((JSONObject)item).optDouble("value", Double.NaN))) : Double.NaN;
                if (isPositive(price) && price < lowest) lowest = price;
            }
            return lowest == Double.POSITIVE_INFINITY ? Double.NaN : lowest;
        } catch (Exception e) { return Double.NaN; }
    }

    private static double fetchBitpinPrice(String url) {
        try {
            JSONObject data = new JSONObject(httpGetWithFallback("https://api.bitpin.org/api/v1/mth/orderbook/USDT_IRT/"));
            JSONArray asks = data.optJSONArray("asks");
            if (asks == null || asks.length() == 0) return Double.NaN;
            double best = Double.POSITIVE_INFINITY;
            for (int i = 0; i < asks.length(); i++) {
                Object item = asks.opt(i);
                double price = item instanceof JSONArray ? ((JSONArray)item).optDouble(0, Double.NaN) : item instanceof JSONObject ? ((JSONObject)item).optDouble("price", ((JSONObject)item).optDouble("rate", ((JSONObject)item).optDouble("value", Double.NaN))) : Double.NaN;
                if (isPositive(price) && price < best) best = price;
            }
            return best == Double.POSITIVE_INFINITY ? Double.NaN : best;
        } catch (Exception e) { return Double.NaN; }
    }

    private static boolean isPositive(double value) {
        return Double.isFinite(value) && value > 0.0;
    }

    private static double numberOr(Object value, double fallback) {
        try {
            double d = Double.parseDouble(String.valueOf(value));
            return Double.isFinite(d) && d != 0.0 ? d : fallback;
        } catch (Exception e) {
            return fallback;
        }
    }

    private static String httpGet(String urlString) throws Exception {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(urlString);
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout((int) HTTP_TIMEOUT_MS);
            connection.setReadTimeout((int) HTTP_TIMEOUT_MS);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json,text/plain,*/*");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/120.0 Mobile Safari/537.36");
            if ("cdn.tsetmc.com".equalsIgnoreCase(url.getHost())) {
                connection.setRequestProperty("Referer", "https://www.tsetmc.com/");
                connection.setRequestProperty("Origin", "https://www.tsetmc.com");
                connection.setRequestProperty("Accept-Language", "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7");
            }
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

    private static final class BankRatePeriod {
        final String from;
        final double rate;

        BankRatePeriod(String from, double rate) {
            this.from = from;
            this.rate = rate;
        }
    }

    private static final class Transaction {
        final String type;
        final double amount;
        final double price;
        final String date;
        Transaction(String type, double amount, double price, String date) {
            this.type = type;
            this.amount = amount;
            this.price = price;
            this.date = date;
        }
    }

    private static final class Lot {
        double amount;
        final double price;
        final String date;
        Lot(double amount, double price, String date) {
            this.amount = amount;
            this.price = price;
            this.date = date;
        }
    }

    private static final class PortfolioResult {
        final double plBank;
        PortfolioResult(double plBank) { this.plBank = plBank; }
    }
}
