#include "producer_options.h"

#include <errno.h>
#include <getopt.h>
#include <glib.h>
#include <stdint.h>
#include <stdlib.h>

static void print_usage(const char *program)
{
  g_printerr("Usage: %s "
             "--bootstrap-servers HOST:PORT "
             "--topic TOPIC "
             "--scenario producer_only|producer_consumer "
             "--payload-size BYTES "
             "--initial-mps N "
             "--incr-mps N "
             "--max-mps N "
             "--warmup-sec N "
             "--measurement-sec N\n",
             program);
}

static int parse_positive_uint64(const char *value, uint64_t *result)
{
  char              *end = NULL;
  unsigned long long parsed;

  errno  = 0;
  parsed = strtoull(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' || parsed == 0) {
    return 0;
  }

  *result = (uint64_t)parsed;
  return 1;
}

void init_default_producer_options(producer_options_t *options)
{
  options->bootstrap_servers = "localhost:9092";
  options->acks              = "all";
  options->topic             = "ext4-test";
  options->scenario          = "producer_only";

  options->initial_mps = 1000;
  options->incr_mps    = 1000;
  options->max_mps     = 50000;

  options->warmup_sec      = 20;
  options->measurement_sec = 30;

  options->stats_interval_us = G_USEC_PER_SEC;
  options->payload_size      = 1000;
}

int parse_producer_options(int argc, char **argv, producer_options_t *options)
{
  static struct option long_options[] = {
      {"bootstrap-servers", required_argument, NULL, 'b'},
      {"scenario",          required_argument, NULL, 'c'},
      {"initial-mps",       required_argument, NULL, 'i'},
      {"incr-mps",          required_argument, NULL, 'r'},
      {"max-mps",           required_argument, NULL, 'x'},
      {"warmup-sec",        required_argument, NULL, 'w'},
      {"measurement-sec",   required_argument, NULL, 's'},
      {"payload-size",      required_argument, NULL, 'p'},
      {"topic",             required_argument, NULL, 't'},
      {0,                   0,                 0,    0  },
  };

  int      opt;
  int      option_index = 0;
  uint64_t parsed_value;

  optind = 1;

  while ((opt = getopt_long(argc, argv, "", long_options, &option_index)) !=
         -1) {
    switch (opt) {
    case 'b':
      if (optarg[0] == '\0') {
        g_printerr("Bootstrap servers must not be empty\n");
        print_usage(argv[0]);
        return 0;
      }
      options->bootstrap_servers = optarg;
      break;
    case 'c':
      if (g_strcmp0(optarg, "producer_only") != 0 &&
          g_strcmp0(optarg, "producer_consumer") != 0) {
        g_printerr("Invalid scenario: %s\n", optarg);
        print_usage(argv[0]);
        return 0;
      }
      options->scenario = optarg;
      break;
    case 'i':
      if (!parse_positive_uint64(optarg, &parsed_value)) {
        g_printerr("Invalid initial mps: %s\n", optarg);
        print_usage(argv[0]);
        return 0;
      }
      options->initial_mps = parsed_value;
      break;

    case 'r':
      if (!parse_positive_uint64(optarg, &parsed_value)) {
        g_printerr("Invalid incr mps: %s\n", optarg);
        print_usage(argv[0]);
        return 0;
      }
      options->incr_mps = parsed_value;
      break;

    case 'x':
      if (!parse_positive_uint64(optarg, &parsed_value)) {
        g_printerr("Invalid max mps: %s\n", optarg);
        print_usage(argv[0]);
        return 0;
      }
      options->max_mps = parsed_value;
      break;

    case 'w':
      if (!parse_positive_uint64(optarg, &parsed_value)) {
        g_printerr("Invalid warmup sec: %s\n", optarg);
        print_usage(argv[0]);
        return 0;
      }
      options->warmup_sec = parsed_value;
      break;

    case 's':
      if (!parse_positive_uint64(optarg, &parsed_value)) {
        g_printerr("Invalid measurement sec: %s\n", optarg);
        print_usage(argv[0]);
        return 0;
      }
      options->measurement_sec = parsed_value;
      break;

    case 'p':
      if (!parse_positive_uint64(optarg, &parsed_value) ||
          parsed_value > SIZE_MAX) {
        g_printerr("Invalid payload size: %s\n", optarg);
        print_usage(argv[0]);
        return 0;
      }
      options->payload_size = (size_t)parsed_value;
      break;

    case 't':
      if (optarg[0] == '\0') {
        g_printerr("Topic must not be empty\n");
        print_usage(argv[0]);
        return 0;
      }
      options->topic = optarg;
      break;

    default:
      print_usage(argv[0]);
      return 0;
    }
  }

  if (optind != argc) {
    print_usage(argv[0]);
    return 0;
  }

  if (options->initial_mps > options->max_mps) {
    g_printerr("initial-mps must be <= max-mps\n");
    print_usage(argv[0]);
    return 0;
  }

  return 1;
}
