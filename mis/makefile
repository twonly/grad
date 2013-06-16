include ../make.defines

mis: mis.c mis.h ${LIBS} ${HEADERS}
	${CC} ${CFLAGS} $< ${LIBS} -o $@

clean:
	rm -rf ../common/*.o
	rm -rf mis

