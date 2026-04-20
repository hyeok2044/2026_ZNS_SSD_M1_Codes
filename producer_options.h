#ifndef PRODUCER_OPTIONS_H
#define PRODUCER_OPTIONS_H

#include <stddef.h>

typedef struct {
    const char *bootstrap_servers;
    const char *acks;
    const char *topic;
    int produce_iterations;
    int progress_interval;
    size_t payload_size;
} producer_options_t;

void init_default_producer_options(producer_options_t *options);
int parse_producer_options(int argc, char **argv, producer_options_t *options);

#endif
