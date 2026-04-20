#ifndef CONSUMER_OPTIONS_H
#define CONSUMER_OPTIONS_H

typedef struct {
    const char *bootstrap_servers;
    const char *topic;
    const char *group_id;
    const char *auto_offset_reset;
} consumer_options_t;

void init_default_consumer_options(consumer_options_t *options);
int parse_consumer_options(int argc, char **argv, consumer_options_t *options);

#endif
