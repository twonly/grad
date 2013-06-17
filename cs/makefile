include ../make.defines

cs: cs_cl.c cs_cl.h cs_mds.c cs_mds.h init.h ${LIBS} ${HEADERS}
	${CC} ${CFLAGS} cs_cl.c cs_mds.c ${LIBS} -o $@

clean:
	rm -rf ../common/*.o
	rm -rf mds

