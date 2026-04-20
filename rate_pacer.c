#include "rate_pacer.h"

#include <glib.h>

void rate_pacer_init(rate_pacer_t *p, uint64_t ops,
                     rate_pacer_idle_cb idle_cb, void *idle_opaque) {
    p->ops = ops;
    p->start_time_us = g_get_monotonic_time();
    p->count = 0;
    p->idle_cb = idle_cb;
    p->idle_opaque = idle_opaque;
}

/* N번째 이벤트의 목표 시각: start + N / ops 초.
 * 매번 "직전 시각 + 간격"이 아니라 start 기준으로 재계산하므로 오차가 누적되지 않는다.
 */
static gint64 target_time_us(const rate_pacer_t *p) {
    return p->start_time_us +
           (gint64)((p->count * (uint64_t)G_USEC_PER_SEC) / p->ops);
}

gint64 rate_pacer_wait(rate_pacer_t *p) {
    gint64 target_us;

    if (p->ops == 0) {
        return 0;
    }

    target_us = target_time_us(p);

    while (1) {
        gint64 now = g_get_monotonic_time();
        gint64 remaining_us = target_us - now;

        /* 이미 타깃을 지났으면 기다리지 않고 뒤처진 양(us)을 반환한다.
         * 호출부가 곧바로 다음 처리를 시도해 스케줄을 따라잡는다.
         */
        if (remaining_us <= 0) {
            return -remaining_us;
        }

        /* 1ms 미만은 sleep 해상도가 부족하므로 타임아웃 0으로 busy-poll.
         * 그 이상이면 idle_cb(배경 작업 처리용)를 호출하되, 반응성 유지를 위해
         * 한 번 대기하는 시간은 100ms로 상한을 둔다.
         */
        if (p->idle_cb) {
            int timeout_ms = 0;
            if (remaining_us >= 1000) {
                timeout_ms = (int)MIN((remaining_us / 1000), 100);
            }
            p->idle_cb(p->idle_opaque, timeout_ms);
        } else if (remaining_us >= 1000) {
            g_usleep((gulong)MIN(remaining_us, 100 * 1000));
        }
    }
}

void rate_pacer_tick(rate_pacer_t *p) {
    p->count++;
}
