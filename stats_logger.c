#include "stats_logger.h"

#include <glib.h>

void stats_logger_init(stats_logger_t *sl,
                       const char *role,
                       uint64_t target_ops,
                       uint64_t total_expected,
                       gint64 interval_us) {
    gint64 now = g_get_monotonic_time();

    sl->role = role;
    sl->target_ops = target_ops;
    sl->total_expected = total_expected;
    sl->interval_us = interval_us;
    sl->start_time_us = now;
    sl->interval_start_us = now;
    sl->interval_start_count = 0;
}

void stats_logger_maybe_log(stats_logger_t *sl,
                            uint64_t current_count,
                            gint64 current_lag_us) {
    gint64 now = g_get_monotonic_time();
    gint64 interval_elapsed_us = now - sl->interval_start_us;
    gint64 elapsed_us;
    uint64_t interval_count;
    double average_ops = 0.0;
    double interval_ops = 0.0;

    if (interval_elapsed_us < sl->interval_us) {
        return;
    }

    elapsed_us = now - sl->start_time_us;
    interval_count = current_count - sl->interval_start_count;

    if (elapsed_us > 0) {
        average_ops = ((double)current_count * G_USEC_PER_SEC) / (double)elapsed_us;
    }

    if (interval_elapsed_us > 0) {
        interval_ops = ((double)interval_count * G_USEC_PER_SEC) /
                       (double)interval_elapsed_us;
    }

    if (sl->target_ops > 0) {
        g_message("Progress: %s=%" G_GUINT64_FORMAT "/%" G_GUINT64_FORMAT
                  " target=%" G_GUINT64_FORMAT " ops interval=%.2f msg/s avg=%.2f msg/s lag=%" G_GINT64_FORMAT "us",
                  sl->role,
                  current_count,
                  sl->total_expected,
                  sl->target_ops,
                  interval_ops,
                  average_ops,
                  current_lag_us);
    } else {
        g_message("Progress: %s=%" G_GUINT64_FORMAT "/%" G_GUINT64_FORMAT
                  " interval=%.2f msg/s avg=%.2f msg/s",
                  sl->role,
                  current_count,
                  sl->total_expected,
                  interval_ops,
                  average_ops);
    }

    sl->interval_start_us = now;
    sl->interval_start_count = current_count;
}
