#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
//#include <arpa/inet.h>

#include "varint.h"

int varint_decode(uint64_t *result, char *varint_p, char **end) {
	char *p;
	int shift;
	int length;
	uint64_t value;

	p = varint_p;
	length = 0;
	value = 0;
	shift = 0;
	for (;;) {
		value <<= shift;
		value |= ((*p & 0x7f));
		length++;
		if ((*p & 0x80) == 0x0) {
			break;
		}
		p++;
		shift += 7;
	}

	if (end != NULL) {
		*end = ++p;
	}

	*result = value;

	return length;
}


