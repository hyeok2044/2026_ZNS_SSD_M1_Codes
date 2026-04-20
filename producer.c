#include <glib.h>
#include <librdkafka/rdkafka.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "common.c"
#include "producer_options.h"

/* Optional per-message delivery callback (triggered by poll() or flush())
 * 메시지 발송이 성공하거나 (재시도 후에도) 실패하였을 때 호출됩니다.
 */
static void dr_msg_cb(rd_kafka_t *kafka_handle,
                      const rd_kafka_message_t *rkmessage,
                      void *opaque) {
    if (rkmessage->err) {
        g_error("Message delivery failed: %s", rd_kafka_err2str(rkmessage->err));
    }
}

/* Kafka Server와 연결을 맺고 Producer를 반환합니다.
 */
static rd_kafka_t *create_producer(const producer_options_t *options) {
    rd_kafka_t *producer;
    rd_kafka_conf_t *conf;
    char errstr[512];

    conf = rd_kafka_conf_new();

    set_config(conf, "bootstrap.servers", (char *)options->bootstrap_servers);
    set_config(conf, "acks", (char *)options->acks);
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

    producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer) {
        g_error("Failed to create new producer: %s", errstr);
        return NULL;
    }

    return producer;
}

/* Payload로 사용될 문자열 배열을 생성합니다.
 */
static char *create_payload(const producer_options_t *options) {
    char *payload = (char *)malloc(options->payload_size + 1);

    if (!payload) {
        g_error("Failed to allocate payload");
        return NULL;
    }

    memset(payload, 'A', options->payload_size);
    payload[options->payload_size] = '\0';
    return payload;
}

/* 메시지를 발행(Produce)합니다.
 */
static rd_kafka_resp_err_t produce_message(rd_kafka_t *producer,
                                           const producer_options_t *options,
                                           const char *payload) {
    while (1) {
        rd_kafka_resp_err_t err;

        err = rd_kafka_producev(producer,
                                RD_KAFKA_V_TOPIC(options->topic),
                                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                RD_KAFKA_V_VALUE((void *)payload,
                                                 options->payload_size),
                                RD_KAFKA_V_OPAQUE(NULL),
                                RD_KAFKA_V_END);

        if (err != RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            return err;
        }

        rd_kafka_poll(producer, 100);
    }
}

static gint64 get_target_send_time_us(const producer_options_t *options,
                                      gint64 start_time_us,
                                      uint64_t sent_count) {
    if (options->ops == 0) {
        return start_time_us;
    }

    return start_time_us +
           (gint64)((sent_count * (uint64_t)G_USEC_PER_SEC) / options->ops);
}

static gint64 wait_for_next_send_slot(rd_kafka_t *producer,
                                      const producer_options_t *options,
                                      gint64 start_time_us,
                                      uint64_t sent_count) {
    gint64 target_time_us;

    if (options->ops == 0) {
        return 0;
    }

    target_time_us = get_target_send_time_us(options, start_time_us, sent_count);

    while (1) {
        gint64 now = g_get_monotonic_time();
        gint64 remaining_us = target_time_us - now;

        if (remaining_us <= 0) {
            return -remaining_us;
        }

        if (remaining_us >= 1000) {
            int poll_timeout_ms = (int)MIN((remaining_us / 1000), 100);
            rd_kafka_poll(producer, poll_timeout_ms);
        } else {
            rd_kafka_poll(producer, 0);
        }
    }
}

static void log_stats(const producer_options_t *options,
                      uint64_t sent_count,
                      gint64 start_time_us,
                      gint64 interval_start_us,
                      uint64_t interval_start_count,
                      gint64 current_lag_us) {
    gint64 now = g_get_monotonic_time();
    gint64 elapsed_us = now - start_time_us;
    gint64 interval_elapsed_us = now - interval_start_us;
    uint64_t interval_count = sent_count - interval_start_count;
    double average_ops = 0.0;
    double interval_ops = 0.0;

    if (elapsed_us > 0) {
        average_ops = ((double)sent_count * G_USEC_PER_SEC) / (double)elapsed_us;
    }

    if (interval_elapsed_us > 0) {
        interval_ops = ((double)interval_count * G_USEC_PER_SEC) /
                       (double)interval_elapsed_us;
    }

    if (options->ops > 0) {
        g_message("Progress: sent=%" G_GUINT64_FORMAT "/%" G_GUINT64_FORMAT
                  " target=%" G_GUINT64_FORMAT " ops interval=%.2f msg/s avg=%.2f msg/s lag=%" G_GINT64_FORMAT "us",
                  sent_count,
                  options->message_count,
                  options->ops,
                  interval_ops,
                  average_ops,
                  current_lag_us);
        return;
    }

    g_message("Progress: sent=%" G_GUINT64_FORMAT "/%" G_GUINT64_FORMAT
              " interval=%.2f msg/s avg=%.2f msg/s",
              sent_count,
              options->message_count,
              interval_ops,
              average_ops);
}

/* 지정된 횟수만큼 메시지를 발행합니다.
 */
static uint64_t produce_messages(rd_kafka_t *producer,
                                 const producer_options_t *options,
                                 const char *payload) {
    gint64 start_time_us = g_get_monotonic_time();
    gint64 interval_start_us = start_time_us;
    uint64_t interval_start_count = 0;
    uint64_t sent_count = 0;
    gint64 current_lag_us = 0;

    while (sent_count < options->message_count) {
        rd_kafka_resp_err_t err;

        current_lag_us = wait_for_next_send_slot(producer,
                                                 options,
                                                 start_time_us,
                                                 sent_count);

        err = produce_message(producer, options, payload);
        if (err) {
            g_error("Failed to produce to topic %s: %s",
                    options->topic,
                    rd_kafka_err2str(err));
            return sent_count;
        }

        sent_count++;
        rd_kafka_poll(producer, 0);

        if ((g_get_monotonic_time() - interval_start_us) >=
            (gint64)options->stats_interval_us) {
            log_stats(options,
                      sent_count,
                      start_time_us,
                      interval_start_us,
                      interval_start_count,
                      current_lag_us);
            interval_start_us = g_get_monotonic_time();
            interval_start_count = sent_count;
        }
    }

    return sent_count;
}

/* 프로그램을 종료하기 전에 내부 큐에 남아있는 메시지를 모두 Flush합니다.
 */
static void flush_producer(rd_kafka_t *producer) {
    g_message("Flushing final messages..");
    rd_kafka_flush(producer, 10 * 1000);

    if (rd_kafka_outq_len(producer) > 0) {
        g_error("%d message(s) were not delivered", rd_kafka_outq_len(producer));
    }
}

int main(int argc, char **argv) {
    char *payload;
    rd_kafka_t *producer;
    producer_options_t options;
    uint64_t sent_count;

    init_default_producer_options(&options);
    if (!parse_producer_options(argc, argv, &options)) {
        return 1;
    }

    payload = create_payload(&options);
    producer = create_producer(&options);

    sent_count = produce_messages(producer, &options, payload);
    flush_producer(producer);

    g_message("%" G_GUINT64_FORMAT " messages were produced to topic %s.",
              sent_count,
              options.topic);

    free(payload);
    rd_kafka_destroy(producer);

    return 0;
}
