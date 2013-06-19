include ../make.defines

client: client.c client.h
	${CC} ${CFLAGS} $< `pkg-config fuse --libs --cflags` -o $@

clean:
	rm -rf ../common/*.o
	rm -rf client
