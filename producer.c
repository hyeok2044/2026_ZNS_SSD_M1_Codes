#include <glib.h>
#include <librdkafka/rdkafka.h>
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

/* 진행 현황을 출력합니다.
 */
static void log_progress(rd_kafka_t *producer,
                               const producer_options_t *options,
                               int count) {
    if (count % options->progress_interval == 0) {
        g_message("Progress: (%d/%d)", count, options->produce_iterations);
        rd_kafka_flush(producer, 10 * 1000);
    }
}

/* 메시지를 발행(Produce)합니다.
 */
static rd_kafka_resp_err_t produce_message(rd_kafka_t *producer,
                                           const producer_options_t *options,
                                           const char *payload) {
    return rd_kafka_producev(producer,
                             RD_KAFKA_V_TOPIC(options->topic),
                             RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                             RD_KAFKA_V_VALUE((void *)payload,
                                              options->payload_size),
                             RD_KAFKA_V_OPAQUE(NULL),
                             RD_KAFKA_V_END);
}

/* 지정된 횟수만큼 메시지를 말행합니다.
 */
static void produce_messages(rd_kafka_t *producer,
                             const producer_options_t *options,
                             const char *payload) {
    int count = 0;

    while (++count <= options->produce_iterations) {
        rd_kafka_resp_err_t err;

        err = produce_message(producer, options, payload);
        log_progress(producer, options, count);

        if (err) {
            g_error("Failed to produce to topic %s: %s",
                    options->topic,
                    rd_kafka_err2str(err));
            return;
        }

        rd_kafka_poll(producer, 0);
    }
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

    init_default_producer_options(&options);
    if (!parse_producer_options(argc, argv, &options)) {
        return 1;
    }

    payload = create_payload(&options);
    producer = create_producer(&options);

    produce_messages(producer, &options, payload);
    flush_producer(producer);

    g_message("%d events were produced to topic %s.",
              options.produce_iterations,
              options.topic);

    free(payload);
    rd_kafka_destroy(producer);

    return 0;
}
