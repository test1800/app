package com.financial;

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.os.Build;

public final class BackgroundCheckScheduler {
    private static final int REQUEST = 7721;
    private static final long INTERVAL_MS = 60_000L;

    private BackgroundCheckScheduler() {}

    public static void schedule(Context context) {
        NotificationChannels.ensure(context);
        AlarmManager am = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
        if (am == null) return;

        Intent intent = new Intent(context, BackgroundCheckReceiver.class);
        intent.setAction(BackgroundCheckReceiver.ACTION_RESTART_MONITOR_SERVICE);
        PendingIntent pi = PendingIntent.getBroadcast(
                context, REQUEST, intent,
                Build.VERSION.SDK_INT >= 23
                        ? PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
                        : PendingIntent.FLAG_UPDATE_CURRENT
        );

        long triggerAt = System.currentTimeMillis() + INTERVAL_MS;
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                am.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, triggerAt, pi);
            } else {
                am.set(AlarmManager.RTC_WAKEUP, triggerAt, pi);
            }
        } catch (Exception ignored) {
        }
    }
}
