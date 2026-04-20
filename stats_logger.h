#ifndef STATS_LOGGER_H
#define STATS_LOGGER_H

#include <glib.h>
#include <stdint.h>

typedef struct {
    const char *role;
    uint64_t    target_ops;
    uint64_t    total_expected;
    gint64      interval_us;
    gint64      start_time_us;
    gint64      interval_start_us;
    uint64_t    interval_start_count;
} stats_logger_t;

void stats_logger_init(stats_logger_t *sl,
                       const char *role,
                       uint64_t target_ops,
                       uint64_t total_expected,
                       gint64 interval_us);

void stats_logger_maybe_log(stats_logger_t *sl,
                            uint64_t current_count,
                            gint64 current_lag_us);

#endif
