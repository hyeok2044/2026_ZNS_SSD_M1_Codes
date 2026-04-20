#include <glib.h>
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
#include <string.h>

#include "common.c"

typedef struct {
    const char *bootstrap_servers;
    const char *acks;
    const char *topic;
    int message_count;
    int produce_iterations;
    int progress_interval;
    size_t payload_size;
} producer_options_t;

/* Optional per-message delivery callback (triggered by poll() or flush())
 * when a message has been successfully delivered or permanently
 * failed delivery (after retries).
 */
static void dr_msg_cb(rd_kafka_t *kafka_handle,
                      const rd_kafka_message_t *rkmessage,
                      void *opaque) {
    if (rkmessage->err) {
        g_error("Message delivery failed: %s", rd_kafka_err2str(rkmessage->err));
    }
}

static void init_default_options(producer_options_t *options) {
    options->bootstrap_servers = "localhost:9092";
    options->acks = "all";
    options->topic = "ext4-test";
    options->message_count = 10;
    options->produce_iterations = 100000000;
    options->progress_interval = 100000;
    options->payload_size = 1000;
}

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

static char *create_payload(const producer_options_t *options) {
    char *payload = (char *)malloc(options->payload_size);

    if (!payload) {
        g_error("Failed to allocate payload");
        return NULL;
    }

    memset(payload, 'A', options->payload_size);
    payload[options->payload_size - 1] = '\0';
    return payload;
}

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

static void maybe_log_progress(rd_kafka_t *producer,
                               const producer_options_t *options,
                               int count) {
    if (count % options->progress_interval == 0) {
        g_message("Progress: (%d/%d)", count, options->produce_iterations);
        rd_kafka_flush(producer, 10 * 1000);
    }
}

static void produce_messages(rd_kafka_t *producer,
                             const producer_options_t *options,
                             const char *payload) {
    int count = 0;

    while (++count <= options->produce_iterations) {
        rd_kafka_resp_err_t err;

        err = produce_message(producer, options, payload);
        maybe_log_progress(producer, options, count);

        if (err) {
            g_error("Failed to produce to topic %s: %s",
                    options->topic,
                    rd_kafka_err2str(err));
            return;
        }

        rd_kafka_poll(producer, 0);
    }
}

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

    init_default_options(&options);

    payload = create_payload(&options);
    producer = create_producer(&options);

    produce_messages(producer, &options, payload);
    flush_producer(producer);

    g_message("%d events were produced to topic %s.",
              options.message_count,
              options.topic);

    free(payload);
    rd_kafka_destroy(producer);

    return 0;
}
