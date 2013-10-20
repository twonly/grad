include ../make.defines

mds: clean mds.c mdmd.c mdmd.h mds_fs.c mdscs.c mdscs.h mds.h init.h chunks.c chunks.h mdmd_stat.c mdmd_stat.h ${LIBS} ${HEADERS}
	${CC} ${CFLAGS} mds.c mds_fs.c mdmd.c mdscs.c chunks.c mdmd_stat.c ${LIBS} -o $@

clean:
	rm -rf ../common/*.o
	rm -rf mds
	rm -rf *.dSYM/

