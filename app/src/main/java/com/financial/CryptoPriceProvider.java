package com.financial;

import android.content.Context;
import android.content.SharedPreferences;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * One sticky crypto-price failover chain used by native background/widget paths.
 *
 * Order:
 *   MEXC -> Binance -> CoinGecko -> Nestex
 *
 * The selected source is persisted per coin. A working fallback is therefore
 * reused directly instead of probing MEXC and every other API on every poll.
 */
public final class CryptoPriceProvider {
    private static final String PREFS = "financial_crypto_price_provider";
    private static final String STATE_PREFIX = "source_";
    private static final String RETRY_PREFIX = "retry_";
    private static final String MEXC_PROBE_PREFIX = "mexc_probe_";
    private static final long RETRY_ALL_MS = 30_000L;
    // While a fallback is active, periodically check MEXC so the app automatically
    // returns to the primary source as soon as it recovers.
    private static final long MEXC_PROBE_MS = 60_000L;
    private static final int TIMEOUT_MS = 5_000;

    private CryptoPriceProvider() {}

    public static final class PriceData {
        public final double price;
        public final double previousClose;
        public final double changePct;
        public final String source;
        public final long time;

        PriceData(double price, double previousClose, double changePct, String source) {
            this.price = price;
            this.previousClose = previousClose;
            this.changePct = changePct;
            this.source = source;
            this.time = System.currentTimeMillis();
        }

        public boolean hasPreviousClose() {
            return Double.isFinite(previousClose) && previousClose > 0;
        }

        public boolean hasChangePct() {
            return Double.isFinite(changePct);
        }
    }

    private static final String[] SOURCE_IDS = {"mexc", "binance", "coingecko", "nestex"};

    private static final Map<String, String> COINGECKO_IDS = new HashMap<>();
    static {
        COINGECKO_IDS.put("BTC", "bitcoin");
        COINGECKO_IDS.put("BNB", "binancecoin");
        COINGECKO_IDS.put("ETH", "ethereum");
        COINGECKO_IDS.put("DOGE", "dogecoin");
        COINGECKO_IDS.put("ADA", "cardano");
        COINGECKO_IDS.put("SOL", "solana");
        COINGECKO_IDS.put("POL", "polygon-ecosystem-token");
        COINGECKO_IDS.put("TRX", "tron");
        COINGECKO_IDS.put("AVAX", "avalanche-2");
    }

    public static PriceData get(Context context, String asset) throws Exception {
        String coin = normalize(asset);
        // GRAM is supported through exchange spot pairs (GRAMUSDT) and
        // Nestex (gram_USDT). It intentionally has no guessed CoinGecko ID.
        if (!COINGECKO_IDS.containsKey(coin) && !"GRAM".equals(coin)) {
            throw new IllegalArgumentException("Unsupported crypto asset: " + coin);
        }
        final String[] sources = COINGECKO_IDS.containsKey(coin)
                ? SOURCE_IDS
                : new String[] {"mexc", "binance", "nestex"};

        SharedPreferences sp = context.getApplicationContext()
                .getSharedPreferences(PREFS, Context.MODE_PRIVATE);

        int active = Math.max(0, Math.min(sources.length - 1,
                sp.getInt(STATE_PREFIX + coin, 0)));
        long retryAt = sp.getLong(RETRY_PREFIX + coin, 0L);
        long now = System.currentTimeMillis();

        // MEXC is always the primary source. If a fallback is active, do NOT
        // probe MEXC on every price request; probe it only periodically.
        // As soon as it responds with a valid price, switch the coin back to MEXC.
        if (active > 0) {
            long lastProbe = sp.getLong(MEXC_PROBE_PREFIX + coin, 0L);
            if (now - lastProbe >= MEXC_PROBE_MS) {
                sp.edit().putLong(MEXC_PROBE_PREFIX + coin, now).apply();
                try {
                    PriceData primary = fetch(sources[0], coin);
                    sp.edit()
                            .putInt(STATE_PREFIX + coin, 0)
                            .putLong(RETRY_PREFIX + coin, 0L)
                            .apply();
                    return primary;
                } catch (Exception ignored) {
                    // MEXC is still unavailable. Keep using the current fallback.
                }
            }
        }

        if (retryAt > now && active > 0) {
            // During a short recovery pause, use only the currently selected
            // fallback. MEXC recovery is handled by the periodic health check above.
            return fetch(sources[active], coin);
        }

        Exception last = null;
        int start = active;
        for (int index = start; index < sources.length; index++) {
            String source = sources[index];
            try {
                PriceData result = fetch(source, coin);
                sp.edit()
                        .putInt(STATE_PREFIX + coin, index)
                        .putLong(RETRY_PREFIX + coin, 0L)
                        .apply();
                return result;
            } catch (Exception error) {
                last = error;
                // Advance immediately and persist. The next poll starts from
                // the next available source instead of repeatedly retrying the
                // failed source.
                int next = Math.min(index + 1, sources.length - 1);
                sp.edit()
                        .putInt(STATE_PREFIX + coin, next)
                        .apply();
            }
        }

        sp.edit()
                .putInt(STATE_PREFIX + coin, Math.min(active + 1, sources.length - 1))
                .putLong(RETRY_PREFIX + coin, System.currentTimeMillis() + RETRY_ALL_MS)
                .apply();

        if (last != null) throw last;
        throw new Exception("All crypto price sources unavailable for " + coin);
    }

    private static PriceData fetch(String source, String coin) throws Exception {
        String url;
        if ("mexc".equals(source)) {
            url = "https://api.mexc.com/api/v3/ticker/24hr?symbol="
                    + coin + "USDT";
        } else if ("binance".equals(source)) {
            url = "https://api.binance.com/api/v3/ticker/24hr?symbol="
                    + coin + "USDT";
        } else if ("coingecko".equals(source)) {
            url = "https://api.coingecko.com/api/v3/simple/price?ids="
                    + COINGECKO_IDS.get(coin) + "&vs_currencies=usd";
        } else {
            url = "https://api.nestex.one/cg/tickers/"
                    + coin.toLowerCase(Locale.ROOT) + "_USDT";
        }

        JSONObject payload = new JSONObject(httpGet(url));
        double price;
        double previousClose = Double.NaN;
        double changePct = Double.NaN;

        if ("mexc".equals(source) || "binance".equals(source)) {
            price = number(payload.opt("price"));
            if (!(price > 0)) price = number(payload.opt("lastPrice"));
        } else if ("coingecko".equals(source)) {
            JSONObject row = payload.optJSONObject(COINGECKO_IDS.get(coin));
            price = row == null ? Double.NaN : number(row.opt("usd"));
        } else {
            price = number(payload.opt("last_price"));
            if (!(price > 0)) price = number(payload.opt("lastPrice"));
            previousClose = number(payload.opt("oldltp24h"));
            if (price > 0 && previousClose > 0) {
                changePct = ((price - previousClose) / previousClose) * 100.0;
            }
        }

        if (!Double.isFinite(price) || price <= 0) {
            throw new Exception(source + " returned invalid " + coin + " price");
        }

        return new PriceData(price, previousClose, changePct, source);
    }

    private static String normalize(String asset) {
        String value = String.valueOf(asset == null ? "" : asset)
                .replace("-", "")
                .replace("_", "")
                .replace("/", "")
                .trim()
                .toUpperCase(Locale.ROOT);
        if (value.endsWith("USDT")) value = value.substring(0, value.length() - 4);
        return value;
    }

    private static double number(Object value) {
        try {
            double d = Double.parseDouble(String.valueOf(value));
            return Double.isFinite(d) ? d : Double.NaN;
        } catch (Exception ignored) {
            return Double.NaN;
        }
    }

    private static String httpGet(String urlString) throws Exception {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(urlString).openConnection();
            connection.setConnectTimeout(TIMEOUT_MS);
            connection.setReadTimeout(TIMEOUT_MS);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("User-Agent", "FinancialDashboard/1.0 (Android)");
            connection.setUseCaches(false);

            int code = connection.getResponseCode();
            InputStream stream = code >= 200 && code < 300
                    ? connection.getInputStream() : connection.getErrorStream();
            if (stream == null) throw new Exception("HTTP " + code);

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                StringBuilder out = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) out.append(line);
                if (code < 200 || code >= 300) throw new Exception("HTTP " + code);
                return out.toString();
            }
        } finally {
            if (connection != null) connection.disconnect();
        }
    }
}
