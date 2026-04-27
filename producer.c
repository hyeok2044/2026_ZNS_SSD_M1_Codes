#include <glib.h>
#include <librdkafka/rdkafka.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "common.c"
#include "producer_options.h"
#include "rate_pacer.h"
#include "stats_logger.h"

#define PRODUCER_POLL_EVERY_N 1000

typedef struct {
  uint64_t    target_mps;
  uint64_t    sent_count;
  uint64_t    duration_sec;
  double      actual_mps;
  const char *state;
} producer_phase_result_t;

/* Optional per-message delivery callback (triggered by poll() or flush())
 * 메시지 발송이 성공하거나 (재시도 후에도) 실패하였을 때 호출됩니다.
 */
static void dr_msg_cb(rd_kafka_t               *kafka_handle,
                      const rd_kafka_message_t *rkmessage,
                      void                     *opaque)
{
  if (rkmessage->err) {
    g_error("Message delivery failed: %s", rd_kafka_err2str(rkmessage->err));
  }
}

static void
print_producer_phase_result_json(const producer_options_t      *options,
                                 const producer_phase_result_t *result)
{
  gint64 timestamp_us = g_get_real_time();

  g_print("{"
          "\"timestamp_us\":%" G_GINT64_FORMAT ","
          "\"scenario\":\"%s\","
          "\"payload_size\":%zu,"
          "\"target_mps\":%" G_GUINT64_FORMAT ","
          "\"actual_mps\":%.2f,"
          "\"latency_avg_us\":0,"
          "\"latency_p50_us\":0,"
          "\"latency_p90_us\":0,"
          "\"latency_p99_us\":0,"
          "\"sent_count\":%" G_GUINT64_FORMAT ","
          "\"acked_count\":0,"
          "\"duration_sec\":%" G_GUINT64_FORMAT ","
          "\"state\":\"%s\""
          "}\n",
          timestamp_us,
          options->scenario,
          options->payload_size,
          result->target_mps,
          result->actual_mps,
          result->sent_count,
          result->duration_sec,
          result->state);
}

/* Kafka Server와 연결을 맺고 Producer를 반환합니다.
 */
static rd_kafka_t *create_producer(const producer_options_t *options)
{
  rd_kafka_t      *producer;
  rd_kafka_conf_t *conf;
  char             errstr[512];

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
static char *create_payload(const producer_options_t *options)
{
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
static rd_kafka_resp_err_t produce_message(rd_kafka_t               *producer,
                                           const producer_options_t *options,
                                           const char               *payload)
{
  while (1) {
    rd_kafka_resp_err_t err;

    err = rd_kafka_producev(
        producer,
        RD_KAFKA_V_TOPIC(options->topic),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE((void *)payload, options->payload_size),
        RD_KAFKA_V_OPAQUE(NULL),
        RD_KAFKA_V_END);

    if (err != RD_KAFKA_RESP_ERR__QUEUE_FULL) {
      return err;
    }

    rd_kafka_poll(producer, 100);
  }
}

static void producer_idle_cb(void *opaque, int timeout_ms)
{
  rd_kafka_poll((rd_kafka_t *)opaque, timeout_ms);
}

/* 지정된 횟수만큼 메시지를 발행합니다.
 */
static producer_phase_result_t
run_producer_phase(rd_kafka_t               *producer,
                   const producer_options_t *options,
                   const char               *payload,
                   uint64_t                  target_mps,
                   uint64_t                  duration_sec,
                   const char               *state)
{
  rate_pacer_t            pacer;
  stats_logger_t          stats;
  gint64                  start_us;
  gint64                  end_us;
  uint64_t                expected_count;
  producer_phase_result_t result;

  expected_count = target_mps * duration_sec;

  rate_pacer_init(&pacer, target_mps, producer_idle_cb, producer);
  stats_logger_init(&stats,
                    "sent",
                    target_mps,
                    expected_count,
                    (gint64)options->stats_interval_us);

  start_us = g_get_monotonic_time();
  end_us   = start_us + ((gint64)duration_sec * G_USEC_PER_SEC);

  while (g_get_monotonic_time() < end_us) {
    gint64              lag_us;
    rd_kafka_resp_err_t err;

    lag_us = rate_pacer_wait(&pacer);

    err = produce_message(producer, options, payload);
    if (err) {
      g_error("Failed to produce to topic %s: %s",
              options->topic,
              rd_kafka_err2str(err));
      break;
    }

    rate_pacer_tick(&pacer);

    if ((pacer.count % PRODUCER_POLL_EVERY_N) == 0) {
      rd_kafka_poll(producer, 0);
    }

    stats_logger_maybe_log(&stats, pacer.count, lag_us);
  }

  rd_kafka_poll(producer, 0);

  result.target_mps   = target_mps;
  result.sent_count   = pacer.count;
  result.duration_sec = duration_sec;
  result.actual_mps   = ((double)pacer.count) / (double)duration_sec;
  result.state        = state;

  return result;
}

/* 프로그램을 종료하기 전에 내부 큐에 남아있는 메시지를 모두 Flush합니다.
 */
static void flush_producer(rd_kafka_t *producer)
{
  g_message("Flushing final messages..");
  rd_kafka_flush(producer, 10 * 1000);

  if (rd_kafka_outq_len(producer) > 0) {
    g_error("%d message(s) were not delivered", rd_kafka_outq_len(producer));
  }
}

/**
 * MPS를 선형 증가시키며 warmup/measurement를 수행하는 ramp-up 실험.
 * 각 phase 결과를 출력하고, 총 전송 메시지 수를 반환한다.
 */
static uint64_t run_producer_ramp(rd_kafka_t               *producer,
                                  const producer_options_t *options,
                                  const char               *payload)
{
  uint64_t total_sent = 0;

  for (uint64_t target_mps = options->initial_mps;
       target_mps <= options->max_mps;
       target_mps += options->incr_mps) {

    producer_phase_result_t warmup_result;
    producer_phase_result_t measurement_result;

    // Warm up phase
    g_message("Starting warmup: target_mps=%" G_GUINT64_FORMAT, target_mps);

    warmup_result = run_producer_phase(
        producer, options, payload, target_mps, options->warmup_sec, "warmup");

    print_producer_phase_result_json(options, &warmup_result);
    total_sent += warmup_result.sent_count;

    // Measurement Phase
    g_message("Starting measurement: target_mps=%" G_GUINT64_FORMAT,
              target_mps);

    measurement_result = run_producer_phase(producer,
                                            options,
                                            payload,
                                            target_mps,
                                            options->measurement_sec,
                                            "measurement");

    print_producer_phase_result_json(options, &measurement_result);
    total_sent += measurement_result.sent_count;
  }

  return total_sent;
}

int main(int argc, char **argv)
{
  char              *payload;
  rd_kafka_t        *producer;
  producer_options_t options;
  uint64_t           sent_count;

  init_default_producer_options(&options);
  if (!parse_producer_options(argc, argv, &options)) {
    return 1;
  }

  payload  = create_payload(&options);
  producer = create_producer(&options);

  sent_count = run_producer_ramp(producer, &options, payload);
  flush_producer(producer);

  g_message("%" G_GUINT64_FORMAT " messages were produced to topic %s.",
            sent_count,
            options.topic);

  free(payload);
  rd_kafka_destroy(producer);

  return 0;
}
