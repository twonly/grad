include ../make.defines

mds: mds.c mds_fs.c mds_cs.c mds_cs.h mds.h init.h ${LIBS} ${HEADERS}
	${CC} ${CFLAGS} mds.c mds_fs.c mds_cs.c ${LIBS} -o $@

clean:
	rm -rf ../common/*.o
	rm -rf mds

