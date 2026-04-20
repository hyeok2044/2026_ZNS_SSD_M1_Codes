#include <glib.h>
#include <librdkafka/rdkafka.h>

#include "common.c"

#define ARR_SIZE(arr) ( sizeof((arr)) / sizeof((arr[0])) )


/* Optional per-message delivery callback (triggered by poll() or flush())
 * when a message has been successfully delivered or permanently
 * failed delivery (after retries).
 */
static void dr_msg_cb (rd_kafka_t *kafka_handle,
                       const rd_kafka_message_t *rkmessage,
                       void *opaque) {
    if (rkmessage->err) {
        g_error("Message delivery failed: %s", rd_kafka_err2str(rkmessage->err));
    }
}

int main (int argc, char **argv) {
    rd_kafka_t *producer;
    rd_kafka_conf_t *conf;
    char errstr[512];

    // Create client configuration
    conf = rd_kafka_conf_new();

    // User-specific properties that you must set
    set_config(conf, "bootstrap.servers", "localhost:9092");

    // Fixed properties
    set_config(conf, "acks", "all");

    // Install a delivery-error callback.
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

    // Create the Producer instance.
    producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer) {
        g_error("Failed to create new producer: %s", errstr);
        return 1;
    }

    // Configuration object is now owned, and freed, by the rd_kafka_t instance.
    conf = NULL;

    // Produce data by selecting random values from these lists.
    int message_count = 10;
    const char *topic = "ext4-test";
    int partition = RD_KAFKA_PARTITION_UA;
    char* c = (char*)malloc(1000);
    memset(c, 'A', 1000);
    c[999] = '\0';

    int cnt = 0;
    while (++cnt <= 100000000) {
        size_t key_len = 0;
        size_t value_len = 1000;

        rd_kafka_resp_err_t err;

        err = rd_kafka_producev(producer,
                                RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                RD_KAFKA_V_VALUE((void*)c, value_len),
                                RD_KAFKA_V_OPAQUE(NULL),
                                RD_KAFKA_V_END);

	if(cnt%100000 == 0) {
        g_message("Progress: (%d/%d)", cnt, 100000000);
	    rd_kafka_flush(producer, 10 * 1000);
	}

    if (err) {
        g_error("Failed to produce to topic %s: %s", topic, rd_kafka_err2str(err));
        return 1;
	}
//        else {
//            g_message("Produced event to topic %s: value = %12s", topic, value);
//        }

        rd_kafka_poll(producer, 0);
    }

    // Block until the messages are all sent.
    g_message("Flushing final messages..");
    rd_kafka_flush(producer, 10 * 1000);

    if (rd_kafka_outq_len(producer) > 0) {
        g_error("%d message(s) were not delivered", rd_kafka_outq_len(producer));
    }

    g_message("%d events were produced to topic %s.", message_count, topic);

    rd_kafka_destroy(producer);

    return 0;
}
