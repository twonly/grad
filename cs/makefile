include ../make.defines

cs: clean cscl.c cscl.h csmds.c chunks.c csmds.h chunks.h init.h ${LIBS} ${HEADERS}
	${CC} ${CFLAGS} cscl.c csmds.c chunks.c ${LIBS} -o $@

clean:
	rm -rf ../common/*.o
	rm -rf cs

