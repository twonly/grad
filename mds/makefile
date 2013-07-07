include ../make.defines

mds: clean mds.c mdmd.c mdmd.h mds_fs.c mdscs.c mdscs.h mds.h init.h chunks.c chunks.h ${LIBS} ${HEADERS}
	${CC} ${CFLAGS} mds.c mds_fs.c mdmd.c mdscs.c chunks.c ${LIBS} -o $@

clean:
	rm -rf ../common/*.o
	rm -rf mds

