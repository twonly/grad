include ../make.defines

api_test: api_test.c ../common/ppcomm.c ../common/sockets.c ${HEADERS}
	${CC} ${CFLAGS} $< ../common/ppcomm.c ../common/sockets.c -o $@

clean:
	rm -rf ../common/*.o
	rm -rf api_test
