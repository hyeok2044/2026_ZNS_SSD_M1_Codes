#ifndef RATE_PACER_H
#define RATE_PACER_H

#include <glib.h>
#include <stdint.h>

typedef void (*rate_pacer_idle_cb)(void *opaque, int timeout_ms);

typedef struct {
    uint64_t ops;
    gint64   start_time_us;
    uint64_t count;
    rate_pacer_idle_cb idle_cb;
    void    *idle_opaque;
} rate_pacer_t;

void   rate_pacer_init(rate_pacer_t *p, uint64_t ops,
                       rate_pacer_idle_cb idle_cb, void *idle_opaque);
gint64 rate_pacer_wait(rate_pacer_t *p);
void   rate_pacer_tick(rate_pacer_t *p);

#endif
