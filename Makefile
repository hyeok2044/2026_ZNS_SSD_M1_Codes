.PHONY: ALL clean

ALL: producer consumer

CFLAGS = -Wall $(shell pkg-config --cflags glib-2.0 rdkafka)
LDLIBS = $(shell pkg-config --libs glib-2.0 rdkafka)

producer: producer.c producer_options.c producer_options.h rate_pacer.c rate_pacer.h stats_logger.c stats_logger.h
	$(CC) $(CFLAGS) producer.c producer_options.c rate_pacer.c stats_logger.c -o $@ $(LDLIBS)

consumer: consumer.c consumer_options.c consumer_options.h
	$(CC) $(CFLAGS) consumer.c consumer_options.c -o $@ $(LDLIBS)

clean:
	rm -f producer consumer
