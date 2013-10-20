include ../make.defines

client: client.c client.h cache.h cache.c
	${CC} ${CFLAGS} $< cache.c `pkg-config fuse --libs --cflags` -o $@

clean:
	rm -rf ../common/*.o
	rm -rf client
	rm -rf *.dSYM/
