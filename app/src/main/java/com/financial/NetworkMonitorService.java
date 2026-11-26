package com.financial;

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.net.NetworkRequest;
import android.os.Build;
import android.os.IBinder;
import android.os.Handler;
import android.os.Looper;

public class NetworkMonitorService extends Service {

    private static final String PREFS = "financial_native_state";
    private static final String KEY_LAST_ONLINE = "last_online";

    private ConnectivityManager connectivityManager;
    private ConnectivityManager.NetworkCallback networkCallback;
    private boolean registered = false;

    private final Handler dashboardHandler = new Handler(Looper.getMainLooper());
    private final Runnable dashboardRefreshRunnable = new Runnable() {
        @Override public void run() {
            // Use exactly one notification engine at a time.
            // Foreground + unlocked: keep the original WebView notification logic.
            // PIN-locked, Android-locked, or Activity no longer resumed (minimized/closed):
            // use the native engine, which does not depend on WebView/JavaScript.
            boolean activityResumed = MainActivity.isActivityResumed();
            boolean appLocked = MainActivity.isAppLocked(NetworkMonitorService.this);

            if (activityResumed && !appLocked) {
                MainActivity.requestDashboardBackgroundRefresh();
            } else {
                // The WebView is no longer available when the app is minimized/closed.
                // Keep the home-screen widget alive from the native foreground service
                // instead of relying on the WebView timers or the widget host's own
                // periodic scheduling. This refreshes ALL widget values from their
                // existing cached/network sources while the app UI is not visible.
                NativeNotificationChecker.checkIfLocked(NetworkMonitorService.this);
                try {
                    UltraCompactWidget.updateAll(NetworkMonitorService.this);
                } catch (Throwable ignored) {
                    // Widget refresh must never stop the background monitor.
                }
            }

            dashboardHandler.postDelayed(this, 10_000L);
        }
    };


    // Signal Matrix is the most time-sensitive Widget value. Keep a dedicated
    // fast poller so it is not tied to the 10-second full-dashboard refresh.
    // Crossover has its own fast background cadence so it does not wait for
    // the 10-second full-dashboard refresh.
    private final Handler crossoverHandler = new Handler(Looper.getMainLooper());
    private final Runnable crossoverRefreshRunnable = new Runnable() {
        @Override public void run() {
            try {
                NativeNotificationChecker.checkCrossoversIfBackground(NetworkMonitorService.this);
            } catch (Throwable ignored) {
            }
            crossoverHandler.postDelayed(this, 5_000L);
        }
    };


    private final Handler scheduleNotificationHandler = new Handler(Looper.getMainLooper());
    private final Runnable scheduleNotificationRunnable = new Runnable() {
        @Override public void run() {
            try {
                NativeNotificationChecker.checkScheduleIfBackground(NetworkMonitorService.this);
            } catch (Throwable ignored) {
            }
            scheduleNotificationHandler.postDelayed(this, 5_000L);
        }
    };

    private final Handler cryptoSignalHandler = new Handler(Looper.getMainLooper());
    private final Runnable cryptoSignalRefreshRunnable = new Runnable() {
        @Override public void run() {
            try {
                NativeNotificationChecker.checkCryptoSignalsIfBackground(NetworkMonitorService.this);
            } catch (Throwable ignored) {
            }
            cryptoSignalHandler.postDelayed(this, 5_000L);
        }
    };

    private final Handler signalMatrixHandler = new Handler(Looper.getMainLooper());
    private final Runnable signalMatrixRefreshRunnable = new Runnable() {
        @Override public void run() {
            try {
                UltraCompactWidget.updateSignalMatrixOnly(NetworkMonitorService.this);
            } catch (Throwable ignored) {
            }
            try {
                NativeNotificationChecker.checkSignalMatrixIfBackground(NetworkMonitorService.this);
            } catch (Throwable ignored) {
            }
            signalMatrixHandler.postDelayed(this, 2_000L);
        }
    };


    private static final int RESTART_REQUEST = 99381;

    private void scheduleSelfRestart() {
        try {
            AlarmManager alarmManager = (AlarmManager) getSystemService(ALARM_SERVICE);
            if (alarmManager == null) return;

            Intent intent = new Intent(this, BackgroundCheckReceiver.class);
            intent.setAction(BackgroundCheckReceiver.ACTION_RESTART_MONITOR_SERVICE);

            PendingIntent pendingIntent = PendingIntent.getBroadcast(
                    this,
                    RESTART_REQUEST,
                    intent,
                    Build.VERSION.SDK_INT >= Build.VERSION_CODES.M
                            ? PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE
                            : PendingIntent.FLAG_UPDATE_CURRENT
            );

            long triggerAt = System.currentTimeMillis() + 2_000L;
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                alarmManager.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, triggerAt, pendingIntent);
            } else {
                alarmManager.set(AlarmManager.RTC_WAKEUP, triggerAt, pendingIntent);
            }
        } catch (Exception ignored) {
        }
    }

    @Override
    public void onCreate() {
        super.onCreate();

        NotificationHelper.createChannels(this);

        NotificationHelper.show(
                this,
                "USD",
                "Financial",
                "Internet monitoring is active",
                true
        );

        startForeground(40181, buildForegroundNotification());

        connectivityManager =
                (ConnectivityManager) getSystemService(Context.CONNECTIVITY_SERVICE);

        boolean onlineNow = isInternetAvailable();
        getSharedPreferences(PREFS, MODE_PRIVATE)
                .edit()
                .putBoolean(KEY_LAST_ONLINE, onlineNow)
                .apply();

        registerNetworkCallback();
        dashboardHandler.postDelayed(dashboardRefreshRunnable, 10_000L);
        // Start immediately; do not wait for the first 10-second dashboard tick.
        signalMatrixHandler.post(signalMatrixRefreshRunnable);
        crossoverHandler.post(crossoverRefreshRunnable);
        cryptoSignalHandler.post(cryptoSignalRefreshRunnable);
        scheduleNotificationHandler.post(scheduleNotificationRunnable);
    }

    private android.app.Notification buildForegroundNotification() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            return new android.app.Notification.Builder(this, NotificationHelper.CHANNEL_APP_ERROR)
                    .setSmallIcon(R.drawable.icon)
                    .setContentTitle("Financial")
                    .setContentText("Internet monitoring is active")
                    .setOngoing(true)
                    .setOnlyAlertOnce(true)
                    .setCategory(android.app.Notification.CATEGORY_SERVICE)
                    .setVisibility(android.app.Notification.VISIBILITY_PUBLIC)
                    .build();
        }

        return new android.app.Notification.Builder(this)
                .setSmallIcon(R.drawable.icon)
                .setContentTitle("Financial")
                .setContentText("Internet monitoring is active")
                .setOngoing(true)
                .setOnlyAlertOnce(true)
                .build();
    }

    private boolean isInternetAvailable() {
        if (connectivityManager == null) return false;

        Network network = connectivityManager.getActiveNetwork();
        if (network == null) return false;

        NetworkCapabilities capabilities =
                connectivityManager.getNetworkCapabilities(network);
        if (capabilities == null) return false;

        return capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                && capabilities.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED);
    }

    private void handlePossibleNetworkChange() {
        boolean online = isInternetAvailable();
        boolean previous = getSharedPreferences(PREFS, MODE_PRIVATE)
                .getBoolean(KEY_LAST_ONLINE, online);

        if (!previous && online) {
            NotificationHelper.show(
                    this,
                    "MONITOR",
                    "Financial Dashboard",
                    "Internet connection restored. Dashboard can refresh its data.",
                    false
            );
        }

        getSharedPreferences(PREFS, MODE_PRIVATE)
                .edit()
                .putBoolean(KEY_LAST_ONLINE, online)
                .apply();
    }

    private void registerNetworkCallback() {
        if (connectivityManager == null || registered) return;

        NetworkRequest request = new NetworkRequest.Builder()
                .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                .build();

        networkCallback = new ConnectivityManager.NetworkCallback() {
            @Override
            public void onAvailable(Network network) {
                handlePossibleNetworkChange();
            }

            @Override
            public void onLost(Network network) {
                handlePossibleNetworkChange();
            }

            @Override
            public void onCapabilitiesChanged(
                    Network network,
                    NetworkCapabilities capabilities
            ) {
                handlePossibleNetworkChange();
            }
        };

        try {
            connectivityManager.registerNetworkCallback(request, networkCallback);
            registered = true;
        } catch (Exception ignored) {
            registered = false;
        }
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        return START_STICKY;
    }

    @Override
    public void onTaskRemoved(Intent rootIntent) {
        // Some Android/OEM task managers terminate the app process when its task is
        // swiped away even though a foreground service is expected to continue.
        // Schedule an independent restart so the native notification engine keeps running.
        scheduleSelfRestart();
        super.onTaskRemoved(rootIntent);
    }

    @Override
    public void onDestroy() {
        signalMatrixHandler.removeCallbacksAndMessages(null);
        cryptoSignalHandler.removeCallbacksAndMessages(null);
        crossoverHandler.removeCallbacksAndMessages(null);
        scheduleNotificationHandler.removeCallbacksAndMessages(null);
        if (connectivityManager != null && networkCallback != null && registered) {
            try {
                connectivityManager.unregisterNetworkCallback(networkCallback);
            } catch (Exception ignored) {}
        }
        registered = false;
        dashboardHandler.removeCallbacksAndMessages(null);
        super.onDestroy();
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }
}
