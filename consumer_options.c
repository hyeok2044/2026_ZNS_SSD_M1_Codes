#include "consumer_options.h"

#include <errno.h>
#include <getopt.h>
#include <glib.h>
#include <stdint.h>
#include <stdlib.h>

static void print_usage(const char *program_name)
{
  g_printerr("Usage: %s "
             "--bootstrap-servers HOST:PORT "
             "--topic TOPIC "
             "--group-id GROUP_ID "
             "--auto-offset-reset earliest|latest "
             "--payload-size BYTES "
             "--warmup-sec N "
             "--measurement-sec N\n",
             program_name);
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

void init_default_consumer_options(consumer_options_t *options)
{
  options->bootstrap_servers = "localhost:9092";
  options->topic             = "ext4-test";
  options->group_id          = "fs-test-consumer";
  options->auto_offset_reset = "earliest";

  options->warmup_sec        = 10;
  options->measurement_sec   = 30;
  options->stats_interval_us = G_USEC_PER_SEC;
  options->payload_size      = 1024;
}

int parse_consumer_options(int argc, char **argv, consumer_options_t *options)
{
  static struct option long_options[] = {
      {"bootstrap-servers", required_argument, NULL, 'b'},
      {"topic",             required_argument, NULL, 't'},
      {"group-id",          required_argument, NULL, 'g'},
      {"auto-offset-reset", required_argument, NULL, 'o'},
      {"payload-size",      required_argument, NULL, 'p'},
      {"warmup-sec",        required_argument, NULL, 'w'},
      {"measurement-sec",   required_argument, NULL, 'm'},
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

    case 't':
      if (optarg[0] == '\0') {
        g_printerr("Topic must not be empty\n");
        print_usage(argv[0]);
        return 0;
      }
      options->topic = optarg;
      break;

    case 'g':
      if (optarg[0] == '\0') {
        g_printerr("Group ID must not be empty\n");
        print_usage(argv[0]);
        return 0;
      }
      options->group_id = optarg;
      break;

    case 'o':
      if (g_strcmp0(optarg, "earliest") != 0 &&
          g_strcmp0(optarg, "latest") != 0) {
        g_printerr("Invalid auto-offset-reset: %s\n", optarg);
        print_usage(argv[0]);
        return 0;
      }
      options->auto_offset_reset = optarg;
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

    case 'w':
      if (!parse_positive_uint64(optarg, &parsed_value)) {
        g_printerr("Invalid warmup sec: %s\n", optarg);
        print_usage(argv[0]);
        return 0;
      }
      options->warmup_sec = parsed_value;
      break;

    case 'm':
      if (!parse_positive_uint64(optarg, &parsed_value)) {
        g_printerr("Invalid measurement sec: %s\n", optarg);
        print_usage(argv[0]);
        return 0;
      }
      options->measurement_sec = parsed_value;
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

  return 1;
}
