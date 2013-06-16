include ../make.defines

mds: mds.c mds_fs.c mds.h init.h ${LIBS} ${HEADERS}
	${CC} ${CFLAGS} mds.c mds_fs.c ${LIBS} -o $@

clean:
	rm -rf ../common/*.o
	rm -rf mds

