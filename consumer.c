#include <glib.h>
#include <librdkafka/rdkafka.h>
#include <stdint.h>

#include "common.c"
#include "consumer_options.h"

static volatile sig_atomic_t run = 1;

/**
 * @brief Signal termination of program
 */
static void stop(int sig)
{
  run = 0;
}

int main(int argc, char **argv)
{
  rd_kafka_t         *consumer;
  rd_kafka_conf_t    *conf;
  rd_kafka_resp_err_t err;
  consumer_options_t  options;
  char                errstr[512];

  init_default_consumer_options(&options);
  if (!parse_consumer_options(argc, argv, &options)) {
    return 1;
  }

  // Create client configuration
  conf = rd_kafka_conf_new();

  // User-specific properties that you must set
  set_config(conf, "bootstrap.servers", (char *)options.bootstrap_servers);

  // Fixed properties
  set_config(conf, "group.id", (char *)options.group_id);
  set_config(conf, "auto.offset.reset", (char *)options.auto_offset_reset);

  // Create the Consumer instance.
  consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (!consumer) {
    g_error("Failed to create new consumer: %s", errstr);
    return 1;
  }
  rd_kafka_poll_set_consumer(consumer);

  // Configuration object is now owned, and freed, by the rd_kafka_t instance.
  conf = NULL;

  // Convert the list of topics to a format suitable for librdkafka.
  rd_kafka_topic_partition_list_t *subscription =
      rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_list_add(
      subscription, options.topic, RD_KAFKA_PARTITION_UA);

  // Subscribe to the list of topics.
  err = rd_kafka_subscribe(consumer, subscription);
  if (err) {
    g_error("Failed to subscribe to %d topics: %s",
            subscription->cnt,
            rd_kafka_err2str(err));
    rd_kafka_topic_partition_list_destroy(subscription);
    rd_kafka_destroy(consumer);
    return 1;
  }

  rd_kafka_topic_partition_list_destroy(subscription);

  // Install a signal handler for clean shutdown.
  signal(SIGINT, stop);

  uint64_t received          = 0;
  uint64_t interval_count    = 0;
  gint64   interval_start_us = g_get_monotonic_time();
  // Start polling for messages.
  while (run) {
    rd_kafka_message_t *consumer_message;

    consumer_message = rd_kafka_consumer_poll(consumer, 500);
    if (!consumer_message) {
      continue;
    }

    if (consumer_message->err) {
      if (consumer_message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        /* We can ignore this error - it just means we've read
         * everything and are waiting for more data.
         */
      } else {
        g_message("Consumer error: %s",
                  rd_kafka_message_errstr(consumer_message));
        return 1;
      }
    } else {
      received++;
      interval_count++;
    }

    // Free the message when we're done.
    rd_kafka_message_destroy(consumer_message);
    gint64 now_us     = g_get_monotonic_time();
    gint64 elapsed_us = now_us - interval_start_us;

    if (elapsed_us >= G_USEC_PER_SEC) {
      double consume_mps =
          ((double)interval_count * G_USEC_PER_SEC) / (double)elapsed_us;

      g_print("{"
              "\"timestamp_us\":%" G_GINT64_FORMAT ","
              "\"payload_size\":%zu,"
              "\"consume_mps\":%.2f,"
              "\"consumed_count\":%" G_GUINT64_FORMAT ","
              "\"state\":\"running\""
              "}\n",
              g_get_real_time(),
              options.payload_size,
              consume_mps,
              interval_count);

      interval_start_us = now_us;
      interval_count    = 0;
    }
  }

  // Close the consumer: commit final offsets and leave the group.
  g_message("Closing consumer");
  rd_kafka_consumer_close(consumer);

  // Destroy the consumer.
  rd_kafka_destroy(consumer);

  return 0;
}
