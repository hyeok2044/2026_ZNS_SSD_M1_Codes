#ifndef PRODUCER_OPTIONS_H
#define PRODUCER_OPTIONS_H

#include <stddef.h>
#include <stdint.h>

typedef struct {
    const char *bootstrap_servers;
    const char *acks;
    const char *topic;
    uint64_t message_count;
    uint64_t ops;
    uint64_t stats_interval_us;
    size_t payload_size;
} producer_options_t;

void init_default_producer_options(producer_options_t *options);
int parse_producer_options(int argc, char **argv, producer_options_t *options);

#endif
