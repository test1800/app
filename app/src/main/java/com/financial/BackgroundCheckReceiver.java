package com.financial;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.os.Build;

/**
 * Alarm receiver used only to keep the foreground monitor service alive.
 *
 * IMPORTANT: This class must not implement a second notification engine.
 * Previously it independently fetched signals/data and could race with
 * NativeNotificationChecker, producing duplicate notifications.
 */
public class BackgroundCheckReceiver extends BroadcastReceiver {

    public static final String ACTION_RESTART_MONITOR_SERVICE =
            "com.financial.ACTION_RESTART_MONITOR_SERVICE";

    @Override
    public void onReceive(Context context, Intent intent) {
        Context app = context.getApplicationContext();

        // The alarm and the service restart path now share the same native
        // notification engine. NativeNotificationChecker itself decides
        // whether it is currently the active notification owner.
        NativeNotificationChecker.checkIfLocked(app);

        restartMonitorService(app);
        BackgroundCheckScheduler.schedule(app);
    }

    private static void restartMonitorService(Context context) {
        Intent serviceIntent = new Intent(context, NetworkMonitorService.class);
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(serviceIntent);
            } else {
                context.startService(serviceIntent);
            }
        } catch (Exception ignored) {
        }
    }
}
