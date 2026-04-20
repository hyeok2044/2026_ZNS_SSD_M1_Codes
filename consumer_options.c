#include "consumer_options.h"

#include <getopt.h>
#include <glib.h>

static void print_usage(const char *program_name) {
    g_printerr("Usage: %s [--bootstrap-servers HOSTS] [--topic TOPIC]\n",
               program_name);
}

void init_default_consumer_options(consumer_options_t *options) {
    options->bootstrap_servers = "localhost:9092";
    options->topic = "ext4-test";
    options->group_id = "kafka-c-getting-started";
    options->auto_offset_reset = "earliest";
}

int parse_consumer_options(int argc, char **argv, consumer_options_t *options) {
    static struct option long_options[] = {
        {"bootstrap-servers", required_argument, NULL, 'b'},
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
