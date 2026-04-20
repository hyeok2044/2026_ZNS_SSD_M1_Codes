.PHONY: ALL clean

ALL: producer consumer

CFLAGS = -Wall $(shell pkg-config --cflags glib-2.0 rdkafka)
LDLIBS = $(shell pkg-config --libs glib-2.0 rdkafka)

producer: producer.c producer_options.c producer_options.h
	$(CC) $(CFLAGS) producer.c producer_options.c -o $@ $(LDLIBS)

consumer: consumer.c
	$(CC) $(CFLAGS) $< -o $@ $(LDLIBS)

clean:
	rm -f producer consumer
