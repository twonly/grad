include ../make.defines

mis: clean mis.c mis.h mis_fs.c mis_fs.h ${LIBS} ${HEADERS}
	${CC} ${CFLAGS} mis.c mis_fs.c ${LIBS} -o $@

clean:
	rm -rf ../common/*.o
	rm -rf mis

