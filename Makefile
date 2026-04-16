.PHONY: ALL clean

ALL: producer consumer

CFLAGS = -Wall $(shell pkg-config --cflags glib-2.0 rdkafka)
LDLIBS = $(shell pkg-config --libs glib-2.0 rdkafka)

producer: producer.c
	$(CC) $(CFLAGS) $< -o $@ $(LDLIBS)

consumer: consumer.c
	$(CC) $(CFLAGS) $< -o $@ $(LDLIBS)

clean:
	rm -f producer consumer
