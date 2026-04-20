#include "producer_options.h"

#include <errno.h>
#include <getopt.h>
#include <glib.h>
#include <stdint.h>
#include <stdlib.h>

static void print_usage(const char *program_name) {
    g_printerr("Usage: %s [--bootstrap-servers HOSTS] [--payload-size BYTES] [--topic TOPIC] [--ops N] [--message-count N]\n",
               program_name);
}

static int parse_positive_uint64(const char *value, uint64_t *result) {
    char *end = NULL;
    unsigned long long parsed;

    errno = 0;
    parsed = strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed == 0) {
        return 0;
    }

    *result = (uint64_t)parsed;
    return 1;
}

void init_default_producer_options(producer_options_t *options) {
    options->bootstrap_servers = "localhost:9092";
    options->acks = "all";
    options->topic = "ext4-test";
    options->message_count = 100000000;
    options->ops = 0;
    options->stats_interval_us = G_USEC_PER_SEC;
    options->payload_size = 1000;
}

int parse_producer_options(int argc, char **argv, producer_options_t *options) {
    static struct option long_options[] = {
        {"bootstrap-servers", required_argument, NULL, 'b'},
        {"message-count", required_argument, NULL, 'm'},
        {"ops", required_argument, NULL, 'o'},
        {"payload-size", required_argument, NULL, 'p'},
        {"topic", required_argument, NULL, 't'},
        {0, 0, 0, 0},
    };
    int opt;
    int option_index = 0;
    uint64_t parsed_value;

    optind = 1;

    while ((opt = getopt_long(argc, argv, "", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'b':
                if (optarg[0] == '\0') {
                    g_printerr("Bootstrap servers must not be empty\n");
                    print_usage(argv[0]);
                    return 0;
                }
                options->bootstrap_servers = optarg;
                break;
            case 'm':
                if (!parse_positive_uint64(optarg, &parsed_value)) {
                    g_printerr("Invalid message count: %s\n", optarg);
                    print_usage(argv[0]);
                    return 0;
                }
                options->message_count = parsed_value;
                break;
            case 'o':
                if (!parse_positive_uint64(optarg, &parsed_value)) {
                    g_printerr("Invalid ops: %s\n", optarg);
                    print_usage(argv[0]);
                    return 0;
                }
                options->ops = parsed_value;
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

    return 1;
}
