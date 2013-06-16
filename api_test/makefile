include ../make.defines

api_test: api_test.c ../common/ppcomm.c ${HEADERS}
	${CC} ${CFLAGS} $< ../common/ppcomm.c -o $@

clean:
	rm -rf ../common/*.o
	rm -rf api_test
