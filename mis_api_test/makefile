include ../make.defines

mis_api_test: mis_api_test.c ../common/ppcomm.c ${HEADERS}
	${CC} ${CFLAGS} $< ../common/ppcomm.c -o $@

clean:
	rm -rf ../common/*.o
	rm -rf mis_api_test
