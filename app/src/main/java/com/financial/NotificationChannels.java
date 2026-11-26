package com.financial;

/**
 * Legacy scheduler compatibility.
 * All user-facing notification channels are created centrally by NotificationHelper.
 */
public final class NotificationChannels {
    public static final String TRADING = NotificationHelper.CHANNEL_CRYPTO_SIGNAL;
    public static final String USD = NotificationHelper.CHANNEL_DASHBOARD_USD;

    private NotificationChannels() {}

    public static void ensure(android.content.Context context) {
        NotificationHelper.createChannels(context);
    }
}
