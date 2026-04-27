#ifndef CONSUMER_OPTIONS_H
#define CONSUMER_OPTIONS_H

#include <stddef.h>
#include <stdint.h>

typedef struct {
  const char *bootstrap_servers;
  const char *topic;
  const char *group_id;
  const char *auto_offset_reset;

  uint64_t warmup_sec;
  uint64_t measurement_sec;

  uint64_t stats_interval_us;
  size_t   payload_size;
} consumer_options_t;

void init_default_consumer_options(consumer_options_t *options);
int  parse_consumer_options(int argc, char **argv, consumer_options_t *options);

#endif
