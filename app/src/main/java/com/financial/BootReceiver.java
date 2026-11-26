package com.financial;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.os.Build;

public class BootReceiver extends BroadcastReceiver {

    private static final String PREFS = "financial_native_state";
    private static final String KEY_MONITOR_ENABLED = "monitor_enabled";

    @Override
    public void onReceive(Context context, Intent intent) {
        if (!Intent.ACTION_BOOT_COMPLETED.equals(intent.getAction())) return;

        boolean enabled = context
                .getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                .getBoolean(KEY_MONITOR_ENABLED, false);

        if (!enabled) return;

        Intent serviceIntent = new Intent(context, NetworkMonitorService.class);

        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(serviceIntent);
            } else {
                context.startService(serviceIntent);
            }
        } catch (Exception ignored) {}
    }
}
