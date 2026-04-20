#include "producer_options.h"

#include <errno.h>
#include <getopt.h>
#include <glib.h>
#include <stdint.h>
#include <stdlib.h>

static void print_usage(const char *program_name) {
    g_printerr("Usage: %s [--bootstrap-servers HOSTS] [--payload-size BYTES] [--topic TOPIC]\n",
               program_name);
}

static int parse_payload_size(const char *value, size_t *payload_size) {
    char *end = NULL;
    unsigned long long parsed;

    errno = 0;
    parsed = strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' ||
        parsed == 0 || parsed > SIZE_MAX) {
        return 0;
    }

    *payload_size = (size_t)parsed;
    return 1;
}

void init_default_producer_options(producer_options_t *options) {
    options->bootstrap_servers = "localhost:9092";
    options->acks = "all";
    options->topic = "ext4-test";
    options->produce_iterations = 100000000;
    options->progress_interval = 100000;
    options->payload_size = 1000;
}

int parse_producer_options(int argc, char **argv, producer_options_t *options) {
    static struct option long_options[] = {
        {"bootstrap-servers", required_argument, NULL, 'b'},
        {"payload-size", required_argument, NULL, 'p'},
        {"topic", required_argument, NULL, 't'},
        {0, 0, 0, 0},
    };
    int opt;
    int option_index = 0;

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
            case 'p':
                if (!parse_payload_size(optarg, &options->payload_size)) {
                    g_printerr("Invalid payload size: %s\n", optarg);
                    print_usage(argv[0]);
                    return 0;
                }
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
