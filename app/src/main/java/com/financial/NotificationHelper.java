package com.financial;

import android.Manifest;
import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.content.Context;
import android.content.Intent;
import android.app.PendingIntent;
import android.content.pm.PackageManager;
import android.media.AudioAttributes;
import android.net.Uri;
import android.os.Build;
import android.os.VibrationEffect;
import android.os.VibrationAttributes;
import android.os.Vibrator;
import android.os.VibratorManager;

/**
 * Central Android notification helper.
 * Keeps the existing custom notification sounds and adds a notification-scoped
 * vibration. The direct vibration uses Notification vibration attributes so it
 * is valid while the app is running in the background.
 */
public final class NotificationHelper {

    private static final String CHANNEL_PREFIX = "financial_";

    // Exactly seven user-facing notification categories.
    // Each Android notification channel maps to one of these categories.
    public static final String CHANNEL_CRYPTO_SIGNAL = CHANNEL_PREFIX + "crypto_signal_v3";
    public static final String CHANNEL_SIGNAL_MATRIX = CHANNEL_PREFIX + "signal_matrix_v7";
    public static final String CHANNEL_SIGNAL_MATRIX_BLUE = CHANNEL_PREFIX + "signal_matrix_blue_v2";
    public static final String CHANNEL_SIGNAL_MATRIX_WHITE = CHANNEL_PREFIX + "signal_matrix_white_v2";
    public static final String CHANNEL_CROSSOVER = CHANNEL_PREFIX + "crossover_v3";
    public static final String CHANNEL_DASHBOARD_USD = CHANNEL_PREFIX + "dashboard_usd_v9";
    public static final String CHANNEL_GLOBAL = CHANNEL_PREFIX + "global_v2";
    public static final String CHANNEL_FEAR_GREED = CHANNEL_PREFIX + "fear_greed_v2";
    public static final String CHANNEL_APP_ERROR = CHANNEL_PREFIX + "app_error_v3";


    // Delay, on, off, on, off, on
    private static final long[] VIBRATION_PATTERN =
            new long[]{0L, 180L, 100L, 380L, 120L, 180L};

    // All notification producers (WebView bridge, native checker, and any
    // background receiver) pass through this class. Keep a short, persistent
    // deduplication window so two producers racing on the same event can never
    // create two visible notifications.
    private static final String DEDUP_PREFS = "notification_dedup";
    private static final long DEDUP_WINDOW_MS = 5L * 60L * 1000L;
    // Hard burst guard: regardless of which producer generated the event,
    // the app will not publish more than one non-ongoing notification within
    // this short window. This prevents Crossover bursts from flooding the TV/phone.
    private static final long NOTIFICATION_BURST_GUARD_MS = 1500L;
    private static final String KEY_LAST_NOTIFICATION_AT = "last_notification_at";
    private static final Object DEDUP_LOCK = new Object();

    private NotificationHelper() {}

    public static void createChannels(Context context) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return;

        NotificationManager manager =
                (NotificationManager) context.getSystemService(Context.NOTIFICATION_SERVICE);
        if (manager == null) return;

        // Keep the notification settings intentionally simple: exactly seven
        // user-facing categories for the whole application.
        manager.createNotificationChannel(
                createChannel(context, CHANNEL_CRYPTO_SIGNAL, "Crypto Signal", null, true)
        );
        manager.createNotificationChannel(
                createChannel(context, CHANNEL_SIGNAL_MATRIX, "Signal Matrix", R.raw.signal, true)
        );
        manager.createNotificationChannel(
                createChannel(context, CHANNEL_SIGNAL_MATRIX_BLUE, "Signal Matrix • Blue", R.raw.matrisabi, true)
        );
        manager.createNotificationChannel(
                createChannel(context, CHANNEL_SIGNAL_MATRIX_WHITE, "Signal Matrix • White", R.raw.matrissefid, true)
        );
        manager.createNotificationChannel(
                createChannel(context, CHANNEL_CROSSOVER, "Crossover", R.raw.crossover, true)
        );
        manager.createNotificationChannel(
                createChannel(context, CHANNEL_DASHBOARD_USD, "Dashboard / USD", R.raw.usd, true)
        );
        manager.createNotificationChannel(
                createChannel(context, CHANNEL_GLOBAL, "Global", R.raw.global, true)
        );
        manager.createNotificationChannel(
                createChannel(context, CHANNEL_FEAR_GREED, "Fear & Greed", R.raw.global, true)
        );
        manager.createNotificationChannel(
                createChannel(context, CHANNEL_APP_ERROR, "Application Error", R.raw.babyding, true)
        );

        // Remove every older/duplicate channel so Android Settings does not
        // continue to expose the previous, more granular notification list.
        try {
            String[] legacyChannels = new String[]{
                    CHANNEL_PREFIX + "monitor_v6",
                    CHANNEL_PREFIX + "monitor_alert_v1",
                    CHANNEL_PREFIX + "trading_legacy_v1",
                    CHANNEL_PREFIX + "trading_v1",
                    CHANNEL_PREFIX + "buy_v8",
                    CHANNEL_PREFIX + "sell_v8",
                    CHANNEL_PREFIX + "buy_v9",
                    CHANNEL_PREFIX + "sell_v9",
                    "financial_buy_v2",
                    "financial_sell_v2",
                    "financial_usd_v2",
                    CHANNEL_PREFIX + "usd_v8",
                    CHANNEL_PREFIX + "signal_matrix_v3",
                    CHANNEL_PREFIX + "signal_matrix_v4",
                    CHANNEL_PREFIX + "signal_matrix_v5",
                    CHANNEL_PREFIX + "signal_matrix_v6",
                    CHANNEL_PREFIX + "signal_matrix_blue_v1",
                    CHANNEL_PREFIX + "signal_matrix_white_v1",

                    CHANNEL_PREFIX + "crossover_v1",
                    CHANNEL_PREFIX + "crossover_v2",
                    CHANNEL_PREFIX + "global_pulse_v1",
                    CHANNEL_PREFIX + "global_time_v1",
                    CHANNEL_PREFIX + "fear_greed_v1",
                    CHANNEL_PREFIX + "app_error_v1",
                    CHANNEL_PREFIX + "app_error_v2"
            };
            for (String id : legacyChannels) {
                manager.deleteNotificationChannel(id);
            }
        } catch (Exception ignored) {}
    }

    private static NotificationChannel createChannel(
            Context context,
            String id,
            String name,
            Integer soundRes,
            boolean important
    ) {
        int importance = important
                ? NotificationManager.IMPORTANCE_HIGH
                : NotificationManager.IMPORTANCE_LOW;

        NotificationChannel channel = new NotificationChannel(id, name, importance);
        channel.setDescription("Financial application notifications");
        channel.setShowBadge(true);
        channel.setLockscreenVisibility(Notification.VISIBILITY_PUBLIC);

        if (important) {
            channel.enableVibration(true);
            channel.setVibrationPattern(VIBRATION_PATTERN);
        }

        if (soundRes != null) {
            Uri soundUri = Uri.parse(
                    "android.resource://" + context.getPackageName() + "/" + soundRes
            );
            AudioAttributes audioAttributes = new AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_NOTIFICATION)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
                    .build();
            channel.setSound(soundUri, audioAttributes);
        } else {
            channel.setSound(null, null);
        }

        return channel;
    }

    public static String channelForType(String type) {
        if ("BUY".equalsIgnoreCase(type) || "SELL".equalsIgnoreCase(type)
                || "CRYPTO_SIGNAL".equalsIgnoreCase(type)) {
            return CHANNEL_CRYPTO_SIGNAL;
        }
        if ("RFKJ_BLUE".equalsIgnoreCase(type)) return CHANNEL_SIGNAL_MATRIX_BLUE;
        if ("RFKJ_WHITE".equalsIgnoreCase(type)) return CHANNEL_SIGNAL_MATRIX_WHITE;
        if ("SIGNAL_MATRIX".equalsIgnoreCase(type) || "RFKJ".equalsIgnoreCase(type)) {
            return CHANNEL_SIGNAL_MATRIX;
        }
        if ("CROSSOVER".equalsIgnoreCase(type)) return CHANNEL_CROSSOVER;
        if ("USD".equalsIgnoreCase(type) || "DASHBOARD".equalsIgnoreCase(type)) {
            return CHANNEL_DASHBOARD_USD;
        }
        if ("GLOBAL_PULSE".equalsIgnoreCase(type) || "GLOBAL_TIME".equalsIgnoreCase(type)
                || "GLOBAL".equalsIgnoreCase(type)) {
            return CHANNEL_GLOBAL;
        }
        if ("FEAR_GREED".equalsIgnoreCase(type)) return CHANNEL_FEAR_GREED;

        // Application errors and monitor/internet notifications are kept in
        // the final application-level category so there are never extra
        // notification categories exposed to the user.
        if ("APP_ERROR".equalsIgnoreCase(type) || "MONITOR".equalsIgnoreCase(type)) {
            return CHANNEL_APP_ERROR;
        }
        return CHANNEL_DASHBOARD_USD;
    }

    public static boolean canPostNotifications(Context context) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
            return true;
        }
        return context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS)
                == PackageManager.PERMISSION_GRANTED;
    }

    public static void show(Context context, String type, String title, String message) {
        show(context, type, title, message, false);
    }

    public static void show(
            Context context,
            String type,
            String title,
            String message,
            boolean ongoing
    ) {
        createChannels(context);

        if (!canPostNotifications(context)) return;

        if (!ongoing && isDuplicate(context, type, title, message)) {
            return;
        }

        // Time-sensitive market alerts must never be discarded merely because
        // another section notified a fraction of a second earlier. Crossover was
        // already exempt; Signal Matrix and Crypto Signal are now exempt too.
        boolean criticalMarketAlert =
                "CROSSOVER".equalsIgnoreCase(type)
                        || "RFKJ_BLUE".equalsIgnoreCase(type)
                        || "RFKJ_WHITE".equalsIgnoreCase(type)
                        || "SIGNAL_MATRIX".equalsIgnoreCase(type)
                        || "RFKJ".equalsIgnoreCase(type)
                        || "BUY".equalsIgnoreCase(type)
                        || "SELL".equalsIgnoreCase(type)
                        || "CRYPTO_SIGNAL".equalsIgnoreCase(type);
        if (!ongoing && !criticalMarketAlert && isBurstBlocked(context)) {
            return;
        }

        NotificationManager manager =
                (NotificationManager) context.getSystemService(Context.NOTIFICATION_SERVICE);
        if (manager == null) return;

        String channelId = ongoing ? CHANNEL_APP_ERROR : channelForType(type);

        Notification.Builder builder = Build.VERSION.SDK_INT >= Build.VERSION_CODES.O
                ? new Notification.Builder(context, channelId)
                : new Notification.Builder(context);

        builder
                .setSmallIcon(R.drawable.icon)
                .setContentTitle(title)
                .setContentText(message)
                .setStyle(new Notification.BigTextStyle().bigText(message))
                .setAutoCancel(!ongoing)
                .setOngoing(ongoing)
                .setOnlyAlertOnce(ongoing)
                .setCategory(ongoing
                        ? Notification.CATEGORY_SERVICE
                        : Notification.CATEGORY_EVENT)
                .setPriority(ongoing
                        ? Notification.PRIORITY_LOW
                        : Notification.PRIORITY_HIGH)
                .setVisibility(Notification.VISIBILITY_PUBLIC);

        // Signal Matrix notifications open the app when tapped, including
        // notifications generated while the app is closed/backgrounded.
        if (!ongoing && ("SIGNAL_MATRIX".equalsIgnoreCase(type)
                || "RFKJ".equalsIgnoreCase(type))) {
            Intent openAppIntent = new Intent(context, MainActivity.class);
            openAppIntent.addFlags(
                    Intent.FLAG_ACTIVITY_CLEAR_TOP
                            | Intent.FLAG_ACTIVITY_SINGLE_TOP
                            | Intent.FLAG_ACTIVITY_NEW_TASK
            );

            int pendingFlags = PendingIntent.FLAG_UPDATE_CURRENT;
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                pendingFlags |= PendingIntent.FLAG_IMMUTABLE;
            }

            PendingIntent contentIntent = PendingIntent.getActivity(
                    context,
                    4181,
                    openAppIntent,
                    pendingFlags
            );
            builder.setContentIntent(contentIntent);
        }

        if (!ongoing && Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            builder.setVibrate(VIBRATION_PATTERN);

            int soundRes = soundForType(type);
            Uri soundUri = Uri.parse(
                    "android.resource://" + context.getPackageName() + "/" + soundRes
            );
            builder.setSound(soundUri);
        }

        int id = ongoing
                ? 40181
                : (int) (System.currentTimeMillis() & 0x7fffffff);

        manager.notify(id, builder.build());

        // On Android 8+ the channel owns notification haptics. We also trigger
        // the same pattern directly so the notification has a consistent haptic
        // even when the channel is customized by the device. The usage is marked
        // explicitly as NOTIFICATION because this call runs from a background
        // foreground-service/receiver path.
        if (!ongoing) {
            vibrateAsNotification(context);
        }
    }

    private static boolean isBurstBlocked(Context context) {
        long now = System.currentTimeMillis();
        synchronized (DEDUP_LOCK) {
            android.content.SharedPreferences prefs =
                    context.getApplicationContext()
                            .getSharedPreferences(DEDUP_PREFS, Context.MODE_PRIVATE);
            long lastTime = prefs.getLong(KEY_LAST_NOTIFICATION_AT, 0L);
            if (lastTime > 0L && now - lastTime < NOTIFICATION_BURST_GUARD_MS) {
                return true;
            }
            prefs.edit().putLong(KEY_LAST_NOTIFICATION_AT, now).apply();
            return false;
        }
    }

    private static boolean isDuplicate(
            Context context,
            String type,
            String title,
            String message
    ) {
        String canonical = normalize(type) + "\u001f"
                + normalize(title) + "\u001f"
                + normalize(message);
        String key = "event_" + Integer.toHexString(canonical.hashCode());
        long now = System.currentTimeMillis();

        synchronized (DEDUP_LOCK) {
            android.content.SharedPreferences prefs =
                    context.getApplicationContext()
                            .getSharedPreferences(DEDUP_PREFS, Context.MODE_PRIVATE);
            long lastTime = prefs.getLong(key, 0L);
            if (lastTime > 0L && now - lastTime < DEDUP_WINDOW_MS) {
                return true;
            }
            prefs.edit().putLong(key, now).apply();
            return false;
        }
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().replaceAll("\\s+", " ");
    }

    private static void vibrateAsNotification(Context context) {
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                VibratorManager manager =
                        (VibratorManager) context.getSystemService(Context.VIBRATOR_MANAGER_SERVICE);
                if (manager == null) return;

                Vibrator vibrator = manager.getDefaultVibrator();
                if (vibrator == null || !vibrator.hasVibrator()) return;

                VibrationEffect effect =
                        VibrationEffect.createWaveform(VIBRATION_PATTERN, -1);

                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                    VibrationAttributes attributes =
                            VibrationAttributes.createForUsage(
                                    VibrationAttributes.USAGE_NOTIFICATION
                            );
                    vibrator.vibrate(effect, attributes);
                } else {
                    AudioAttributes audioAttributes = new AudioAttributes.Builder()
                            .setUsage(AudioAttributes.USAGE_NOTIFICATION)
                            .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
                            .build();
                    vibrator.vibrate(effect, audioAttributes);
                }
                return;
            }

            Vibrator vibrator =
                    (Vibrator) context.getSystemService(Context.VIBRATOR_SERVICE);
            if (vibrator == null || !vibrator.hasVibrator()) return;

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                VibrationEffect effect =
                        VibrationEffect.createWaveform(VIBRATION_PATTERN, -1);
                AudioAttributes audioAttributes = new AudioAttributes.Builder()
                        .setUsage(AudioAttributes.USAGE_NOTIFICATION)
                        .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
                        .build();
                vibrator.vibrate(effect, audioAttributes);
            } else {
                vibrator.vibrate(VIBRATION_PATTERN, -1);
            }
        } catch (SecurityException ignored) {
            // VIBRATE permission is declared in AndroidManifest.xml.
        } catch (Throwable ignored) {
            // Notification delivery must never fail because the haptic API is unavailable.
        }
    }

    private static void playNotificationSound(Context context, int soundRes) {
        try {
            android.media.MediaPlayer player =
                    android.media.MediaPlayer.create(context.getApplicationContext(), soundRes);
            if (player == null) return;
            player.setOnCompletionListener(mp -> {
                try { mp.release(); } catch (Exception ignored) {}
            });
            player.setOnErrorListener((mp, what, extra) -> {
                try { mp.release(); } catch (Exception ignored) {}
                return true;
            });
            player.start();
        } catch (Throwable ignored) {
            // Notification delivery must never fail because audio playback is unavailable.
        }
    }

    private static int soundForType(String type) {
        if ("BUY".equalsIgnoreCase(type)) return R.raw.buy;
        if ("SELL".equalsIgnoreCase(type)) return R.raw.sell;
        if ("RFKJ_BLUE".equalsIgnoreCase(type)) return R.raw.matrisabi;
        if ("CROSSOVER".equalsIgnoreCase(type)) return R.raw.crossover;
        if ("GLOBAL_PULSE".equalsIgnoreCase(type) || "GLOBAL_TIME".equalsIgnoreCase(type)
                || "FEAR_GREED".equalsIgnoreCase(type)) return R.raw.global;
        if ("RFKJ_WHITE".equalsIgnoreCase(type)) return R.raw.matrissefid;
        if ("SIGNAL_MATRIX".equalsIgnoreCase(type) || "RFKJ".equalsIgnoreCase(type)) {
            return R.raw.signal;
        }
        return R.raw.usd;
    }
}
