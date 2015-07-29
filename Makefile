# 
# VERSION CHANGES
#

LOCATION=/usr/local
CFLAGS=-Wall -g -I. -O2
#CFLAGS=-Wall -ggdb -I. -O0

OBJ=undark
OFILES=varint.o 
default: undark

.c.o:
	${CC} ${CFLAGS} $(COMPONENTS) -c $*.c

all: ${OBJ} 

undark: ${OFILES} undark.c 
#	ctags *.[ch]
#	clear
	${CC} ${CFLAGS} $(COMPONENTS) undark.c ${OFILES} -o undark ${LIBS}

install: ${OBJ}
	cp undark ${LOCATION}/bin/
	cp undark.1  ${LOCATION}/man/man1

clean:
	rm -f *.o *core ${OBJ}
