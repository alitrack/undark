# 
# VERSION CHANGES
#

LOCATION=/usr/local
#CFLAGS=-Wall -g -I. -O2
CFLAGS=-Wall -ggdb -I. -Og

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

run:
	./undark --cellcount-min=50 --cellcount-max=60 --no-blobs --rowsize-min=420 --rowsize-max=1000 -i ../sms.db >sms-data.csv
