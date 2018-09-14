/**
* undark - generic data puller from SQLite DBs.
*
* Rather CPU intensive likely, relies on the correlation
* that the SQLite length of payload should be the same
* as the summation of the payload cell sizes.
*
* Written by Paul L Daniels (pldaniels@pldaniels.com)
*
* BSD Revised licence ( see LICENCE )
*
* Original version released October 6, 2013
*
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
//#include <winsock2.h>

#include <ctype.h>
#include <time.h>
#include <errno.h>

#include "varint.h"

#define FL __FILE__,__LINE__
#define VERBOSE if (g->verbose)
#define DEBUG if (g->debug)

#define DECODE_MODE_FREESPACE 1
#define DECODE_MODE_NORMAL 0

#define PAYLOAD_SIZE_MINIMUM 10
#define PAYLOAD_CELLS_MAX 1000
#define OVERFLOW_PAGES_MAX 10000

#define PARAM_VERSION "--version"
#define PARAM_HELP "--help"
#define PARAM_FINE_SEARCH "--fine-search"
#define PARAM_FREESPACE_ONLY "--freespace"
#define PARAM_FREESPACE_MINIMUM "--freespace-minimum="
#define PARAM_NO_BLOBS "--no-blobs"
#define PARAM_BLOB_SIZE_LIMIT "--blob-size-limit="
#define PARAM_CELLCOUNT_MIN "--cellcount-min="
#define PARAM_CELLCOUNT_MAX "--cellcount-max="
#define PARAM_ROWSIZE_MIN "--rowsize-min="
#define PARAM_ROWSIZE_MAX "--rowsize-max="
#define PARAM_PAGE_SIZE "--page-size="
#define PARAM_PAGE_START "--page-start="  // add to 0.5
#define PARAM_PAGE_END "--page-end=" // add to 0.5
#define PARAM_REMOVED_ONLY "--removed-only"



struct globals {
	uint8_t debug;
	uint8_t verbose;

	char *input_file; // actual file name
	char *db_origin;	// the mmap'd file origin
	char *db_end; // the computed end of the mmap'd file ( based on file size )
	char *db_cfp; // current file position
	char *db_cpp; // current page position
	char *db_cpp_limit; // end of the current page
	size_t db_size;

	uint32_t page_size, page_count, page_number;
	uint32_t page_start, page_end;

	uint32_t freelist_first_page, freelist_page_count;
	uint32_t *freelist_pages;
	uint32_t freelist_pages_current_index;
	int freelist_space_only;
	int removed_only;
	size_t freespace_minimum;

	time_t date_upper, date_lower; // deprecated - now that Undark has become a generic tool
	int cc_min, cc_max;  // cell count limits
	size_t rs_min, rs_max; // row/payload limits
	int report_blobs; // do we even handle blob data
	size_t blob_size_limit; // at which point do we cut over to dumping to *.blob files?

	int blob_count;
	int fine_search;
};




struct cell {
	int t; // serial
	int o; // offset
	int s; // size
};



struct sql_payload {
	uint64_t prefix_length;
	uint64_t length;
	uint64_t rowid;
	uint64_t header_size;
	int cell_count;
	int cell_page;
	int cell_page_offset;
	struct cell cells[PAYLOAD_CELLS_MAX+1];
	uint32_t overflow_pages[OVERFLOW_PAGES_MAX+1];
	char *mapped_data, *mapped_data_endpoint;
};

struct sqlite_leaf_header {
	int page_number;
	int page_byte;
	uint16_t freeblock_offset;
	uint16_t freeblock_size;
	uint16_t freeblock_next;
	int cellcount;
	int cell_offset;
	int freebytes;
};


char version[] = "undark version 0.7.1, by Paul L Daniels ( pldaniels@pldaniels.com )\n";
char help[] = "-i <sqlite DB> [-d] [-v] [-V|--version] [--cellcount-min=<count>] [--cellcount-max=<count>] [--rowsize-min=<bytes>] [--rowsize-max=<bytes>] [--no-blobs] [--blob-size-limit=<bytes>] [--page-size=<bytes>] [--page-start=<number>] [--page-end=<number>] [--freespace] [--freespace-minimum=<bytes>]\n"
"\t-i: input SQLite3 format database\n"
"\t-d: enable debugging output (very large dumps)\n"
"\t-v: enable verbose output\n"
"\t-V|--version: show version of software\n"
"\t-h|--help: show this help\n"
"\t--cellcount-min: define the minimum number of cells a row must have to be extracted\n"
"\t--cellcount-max: define the maximum number of cells a row must have to be extracted\n"
"\t--rowsize-min: define the minimum number of bytes a row must have to be extracted\n"
"\t--rowsize-max: define the maximum number of bytes a row must have to be extracted\n"
"\t--no-blobs: disable the dumping of blob data\n"
"\t--blob-size-limit: all blobs larger than this size are dumped to .blob files\n"
"\t--fine-search: search DB shifting one byte at a time, rather than records\n"
"\t--page-size: hard code the page size for the DB (useful when header is damaged)\n"
"\t--removed-only: Dumps rows that have their key set to -1\n"
//"\t--page-start: starting page to scan in db\n"
//"\t--page-end: ending page to scan in db\n"
"\t--freespace: search for rows in the freespace\n"
//"\t--freespace-minimum: smallest freespace size to search in\n"
;

/*-----------------------------------------------------------------\
Date Code:	: 20131023-105927
Function Name	: UNDARK_init
Returns Type	: int
----Parameter List
1. struct globals *g , 
------------------
Exit Codes	: 
Side Effects	: 
--------------------------------------------------------------------
Comments:

--------------------------------------------------------------------
Changes:

\------------------------------------------------------------------*/
int UNDARK_init( struct globals *g ) {

	/**
	* Initialise our globals 
	*/
	g->page_size = 0;
	g->page_count = 0;
	g->page_number = 1;
	g->debug = 0;
	g->verbose = 0;
	g->input_file = NULL;
	g->date_lower = 0;
	g->date_upper = 0;
	g->cc_max = PAYLOAD_CELLS_MAX;
	g->cc_min = 2;
	g->rs_max = SIZE_MAX;
	g->rs_min = 10;
	g->blob_count = 0;
	g->report_blobs = 1;
	g->blob_size_limit = SIZE_MAX; // C99 
	g->fine_search = 0;
	g->freelist_space_only = 0;
	g->removed_only = 0;
	g->freespace_minimum = SIZE_MAX; // C99
	g->page_start = 0;
	g->page_end = 0;

	g->db_cfp = NULL;
	g->db_cpp = NULL;

	return 0;
}




/*-----------------------------------------------------------------\
Date Code:	: 20131023-105933
Function Name	: UNDARK_parse_parameters
Returns Type	: int
----Parameter List
1. int argc, 
2.  char **argv, 
3.  struct globals *g , 
------------------
Exit Codes	: 
Side Effects	: 
--------------------------------------------------------------------
Comments:

--------------------------------------------------------------------
Changes:

\------------------------------------------------------------------*/
int UNDARK_parse_parameters( int argc, char **argv, struct globals *g ) {

	int param;

	/**
	* We need at least some additional parameters 
	*
	*/
	if (argc < 2) {
		fprintf(stderr,"%s", help);
		fprintf(stderr,"Sizeof double = %ld,  long double = %ld\n", sizeof(double), sizeof(long double));
		exit(1);
	}


	/**
	* Decode the input parameters.
	* Yes, I know, I should do this using gnu params etc.
	*/
	for (param = 1; param < argc; param++) {
		char *p = argv[param];

		if (strcmp(p, "-V") == 0) { fprintf(stdout,"%s", version); exit(0); }
		if (strcmp(p, "-h") == 0) { fprintf(stdout,"%s %s", argv[0], help); exit(0); }
		if (strcmp(p, "-d") == 0) g->debug = 1;
		if (strcmp(p, "-v") == 0) g->verbose = 1;
		if (strcmp(p, "-i") == 0) {
			param++; 
			if (param < argc) { 
				g->input_file = argv[param]; 
			} else { 
				fprintf(stderr,"Not enough paramters\n"); 
				exit(1); 
			}
		} else if (strncmp(p,"--", 2) == 0) {

			DEBUG fprintf(stderr,"Parameter: '%s' %d\n", p, (int)strlen(PARAM_BLOB_SIZE_LIMIT));
			// extended parameters 

			if (strncmp(p,PARAM_VERSION, strlen(PARAM_VERSION))==0) {
				fprintf(stderr,"%s", version);
				exit(0);

			} else if (strncmp(p,PARAM_HELP, strlen(PARAM_HELP))==0) {
				fprintf(stderr,"%s %s", argv[0], help);
				exit(0);


			} else if (strncmp(p,PARAM_NO_BLOBS, strlen(PARAM_NO_BLOBS))==0) {
				g->report_blobs = 0;

			} else if (strncmp(p,PARAM_BLOB_SIZE_LIMIT, strlen(PARAM_BLOB_SIZE_LIMIT))==0) {
				p = p +strlen(PARAM_BLOB_SIZE_LIMIT);
				g->blob_size_limit = strtol( p, NULL, 10 );

			} else if (strncmp(p,PARAM_PAGE_START, strlen(PARAM_PAGE_START))==0) {
				p = p +strlen(PARAM_PAGE_START);
				g->page_start = strtol( p, NULL, 10 );

			} else if (strncmp(p,PARAM_PAGE_END, strlen(PARAM_PAGE_END))==0) {
				p = p +strlen(PARAM_PAGE_END);
				g->page_end = strtol( p, NULL, 10 );

			} else if (strncmp(p,PARAM_PAGE_SIZE, strlen(PARAM_PAGE_SIZE))==0) {
				p = p +strlen(PARAM_PAGE_SIZE);
				g->page_size = strtol( p, NULL, 10 );

			} else if (strncmp(p,PARAM_FREESPACE_MINIMUM, strlen(PARAM_FREESPACE_MINIMUM))==0) {
				p = p +strlen(PARAM_FREESPACE_MINIMUM);
				g->freespace_minimum = strtol( p, NULL, 10 );

			} else if (strncmp(p,PARAM_CELLCOUNT_MIN, strlen(PARAM_CELLCOUNT_MIN))==0) {
				p = p +strlen(PARAM_CELLCOUNT_MIN);
				g->cc_min = strtol( p, NULL, 10 );

			} else if (strncmp(p,PARAM_CELLCOUNT_MAX, strlen(PARAM_CELLCOUNT_MAX))==0) {
				p = p +strlen(PARAM_CELLCOUNT_MAX);
				g->cc_max = strtol( p, NULL, 10 );

			} else if (strncmp(p,PARAM_ROWSIZE_MIN, strlen(PARAM_ROWSIZE_MIN))==0) {
				p = p +strlen(PARAM_ROWSIZE_MIN);
				g->rs_min = strtol( p, NULL, 10 );

			} else if (strncmp(p,PARAM_ROWSIZE_MAX, strlen(PARAM_ROWSIZE_MAX))==0) {
				p = p +strlen(PARAM_ROWSIZE_MAX);
				g->rs_max = strtol( p, NULL, 10 );

			} else if (strncmp(p,PARAM_FINE_SEARCH, strlen(PARAM_FINE_SEARCH))==0) {
				g->fine_search = 1;

			} else if (strncmp(p,PARAM_FREESPACE_ONLY, strlen(PARAM_FREESPACE_ONLY))==0) {
				g->freelist_space_only = 1;

			} else if (strncmp(p,PARAM_REMOVED_ONLY, strlen(PARAM_REMOVED_ONLY))==0) {
				g->removed_only = 1;

			} else {
				fprintf(stderr,"Cannot interpret extended parameter: \"%s\"\n",p);
				exit(1);
			}

		}
	}

	if (g->input_file == NULL) {
		fprintf(stderr,"ERROR: Need input file\n");
		exit(1);
	}


	return 0;
}




/*-----------------------------------------------------------------\
Date Code:	: 20131006-121932
Function Name	: to_signed_byte
Returns Type	: char
----Parameter List
1. unsigned char value, 
------------------
Exit Codes	: 
Side Effects	: 
--------------------------------------------------------------------
Comments:

Converts 2's compliment byte to signed integer

--------------------------------------------------------------------
Changes:

\------------------------------------------------------------------*/
char to_signed_byte(unsigned char value) {
	int signed_value = value;
	if (value >> 7) signed_value |= -1 << 7;
	return signed_value;
}

int to_signed_int( unsigned int value ) {
	int signed_value = value;

	if (value >> 15) signed_value |= -1 << 15;
	return signed_value;
}

long int to_signed_long( unsigned long int value ) {
	long int signed_value = value;

	if (value >> 31) signed_value |= -1 << 31;
	return signed_value;
}





/*-----------------------------------------------------------------\
Date Code:	: 20131002-220244
Function Name	: tdump
Returns Type	: int
----Parameter List
1. char *p, 
2.  uint16_t l , 
------------------
Exit Codes	: 
Side Effects	: 
--------------------------------------------------------------------
Comments:

--------------------------------------------------------------------
Changes:

\------------------------------------------------------------------*/
int tdump( char *p, uint16_t l ) {

	while (l--) {
		if (isprint(*p)) fprintf(stdout,"%c", *p); else fprintf(stdout,".");
		p++;
	}

	return 0;
}




/*-----------------------------------------------------------------\
Date Code:	: 20131006-122020
Function Name	: sqltdump
Returns Type	: int
----Parameter List
1. char *p, 
2.  uint16_t l , 
------------------
Exit Codes	: 
Side Effects	: 
--------------------------------------------------------------------
Comments:
Dumps text in a SQL friendly format ( doubling of single quotes )

--------------------------------------------------------------------
Changes:

\------------------------------------------------------------------*/
int sqltdump( char *p, uint16_t l ) {

	fprintf(stdout,"\"");
	while (l--) {
		if (*p == '\"') fprintf(stdout,"\"");
		if (isprint(*p)) fprintf(stdout,"%c", *p); else fprintf(stdout,".");
		p++;
	}
	fprintf(stdout,"\"");

	return 0;
}




/*-----------------------------------------------------------------\
Date Code:	: 20131007-004650
Function Name	: blob_dump
Returns Type	: int
----Parameter List
1. unsigned char *p, 
2.  uint16_t l , 
------------------
Exit Codes	: 
Side Effects	: 
--------------------------------------------------------------------
Comments:

--------------------------------------------------------------------
Changes:

\------------------------------------------------------------------*/
int blob_dump( unsigned char *p, uint16_t l ) {

	fprintf(stdout,"x'");
	while (l--) {
		fprintf(stdout,"%02X", ( unsigned char)*p);
		p++;
	}
	fprintf(stdout,"'");

	return 0;
}




/*-----------------------------------------------------------------\
Date Code:	: 20131002-220250
Function Name	: hdump
Returns Type	: int
----Parameter List
1. char *p, 
2.  uint16_t l , 
------------------
Exit Codes	: 
Side Effects	: 
--------------------------------------------------------------------
Comments:

Combo hex + text dump, 16 byte wide rows

--------------------------------------------------------------------
Changes:

\------------------------------------------------------------------*/
int hdump( unsigned char *p, uint16_t length, char *msg ) {

	int oc = 0;
	int ll = length;

	fprintf(stdout,"%s: Hexdumping %d bytes from %p\n", msg, ll, p);
	uint16_t c = 0;

	if (p == NULL) {
		fprintf(stdout,"ERROR: NULL passed.\n");
		//		exit(1);
	}

	while (ll > 0) {
		int br;
		unsigned char *op;

		fprintf(stdout,"%04X [%06d] ",oc, ll);
		oc+=16;

		br = ll;
		op = p;
		while (ll--) {
			fprintf(stdout,"%02X ", *p);
			c++;
			p++;
			if (c%16 == 0) break;
		}

		ll = br;
		p = op;
		c = 0;

		fprintf(stdout, "  [%06d]", ll );
		while (ll--) {
			fprintf(stdout,"%c", isprint(*p)?*p:'.');
			c++;
			p++;
			if (c%16 == 0)  break;
		}

		fprintf(stdout," %d\n",ll);
	}

	fprintf(stdout,"\n");


	return 0;
}




/*-----------------------------------------------------------------\
Date Code:	: 20131007-184003
Function Name	: blob_dump_to_file
Returns Type	: int
----Parameter List
1. struct globals *glob, 
2.  char *p, 
3.  size_t l , 
------------------
Exit Codes	: 
Side Effects	: 
--------------------------------------------------------------------
Comments:

--------------------------------------------------------------------
Changes:

\------------------------------------------------------------------*/
int blob_dump_to_file( struct globals *g, char *p, size_t l ) {
	int f;
	ssize_t written;
	char fn[1024];

	snprintf(fn, sizeof(fn), "%d.blob", g->blob_count);
	DEBUG fprintf(stdout,"%s:%d:DEBUG: Writing %ld bytes to %s\n", FL , l, fn );
	f = open(fn, O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR );
	if (!f) { fprintf(stderr,"Cannot open %s (%s)\n", fn, strerror(errno)); return 1; }
	written = write(f, p, l);
	if ( written != l ) {
		fprintf(stderr,"Wrote %ld of %ld bytes to %s ( %s )\n", written, l, fn, strerror(errno));
		close(f);
		return 1;
	}
	close(f);
	return 0;
}




/*-----------------------------------------------------------------\
Date Code:	: 20131003-223556
Function Name	: *bstrstr
Returns Type	: char
----Parameter List
1. char *needle, 
2.  char *haystack, 
3.  char *limit , 
------------------
Exit Codes	: 
Side Effects	: 
--------------------------------------------------------------------
Comments:
Searches for the needle among a haystack possibly containing
\0 delimeted data.

--------------------------------------------------------------------
Changes:

\------------------------------------------------------------------*/
char *bstrstr( char *haystack, char *needle, char *limit ) {

	char *p;

	if ((!needle)||(*needle == '\0')) return NULL;
	if (!haystack) return NULL;
	if ((limit == NULL)||(limit <=haystack)) return NULL;

	p = haystack;
	while (p < limit) {
		char *tp;
		char *tn;

		tn = needle;
		tp = p;
		while ((*tn) && (tp < limit) && (*tp == *tn)) {
			tn++;
			tp++;
			if (*tn == '\0') return p;
		}
		p++;
	}

	return NULL;
}




/*-----------------------------------------------------------------\
Date Code:	: 20131004-175721
Function Name	: decode_row_meta
Returns Type	: int
----Parameter List
1. uint8_t *p, 
2.  struct sql_payload *payload , 
------------------
Exit Codes	: 
Side Effects	: 
--------------------------------------------------------------------
Comments:

Decodes the payload header data so that we can then later
pull the actual data from the file.

--------------------------------------------------------------------
Changes:
added 'mode',  standard, or freespace

\------------------------------------------------------------------*/
int decode_row( struct globals *g, char *p, char *data_endpoint, struct sql_payload *payload, int mode, size_t forced_length ) {
	int t = 0, offset;
	char *plh_ep; // payload header end point
	char *base = p;

	DEBUG {
		fprintf(stdout,"%s:%d:DEBUG:DECODING ROW-------------------------MODE:%s\n", FL, (mode?"Freespace":"Standard"));
		hdump((unsigned char *)p, 16, "Decode_row start data");
	}

	payload->overflow_pages[0] = 0;
	payload->cell_count = 0;

	if ( mode == DECODE_MODE_FREESPACE ) {
		payload->length = forced_length -4; // and we still have to deduct the payload header size
	} else {
		varint_decode( &(payload->length), p, &p );
	}

	if (payload->length > g->db_size) return 0;
	if (payload->length < g->rs_min) return 0;
	if (payload->length > g->rs_max) return 0;

	DEBUG fprintf(stdout,"%s:%d:DEBUG:Payload size: %lu\n", FL, (unsigned long int)payload->length);

	if ( mode == DECODE_MODE_FREESPACE ) {
		payload->rowid = 1;
	} else {
		varint_decode( &(payload->rowid), p, &p );
	}

	if (payload->rowid < 1) return 0;

	payload->prefix_length = p -base; // store this so we know how many bytes the length + Row ID took up.

	plh_ep = p; // first set up the beginning of the payload header array size.
	varint_decode( &(payload->header_size), p, &p );
	if (payload->header_size > g->page_size) return 0;

	if (mode == DECODE_MODE_FREESPACE) {
		payload->length -= payload->header_size;
		DEBUG fprintf(stdout,"%s:%d:DEBUG: Looking for %lu bytes of data after the payload header\n", FL , (long unsigned int)payload->length);
		fflush(stdout);
	}

	// If the payload size exceeds the page_size, then we have to do some more checking

    // page size: 4096, payload->length: 4121 to 5229 CRASH
	if (payload->length > (g->page_size -35)) {
        //printf("Payload length: %ld, Page size: %u\n", payload->length, g->page_size);
        //printf("this payload > page size\n");
		uint32_t tmp, ovp;
		int ovpi = 1;

		// get the FIRST overflow page
		memcpy(&tmp, data_endpoint -4, 4);
		ovp = payload->overflow_pages[0] = ntohl(tmp);

		// if the page is beyond the file range, then we've just got defective input data
		if (ovp > g->page_count) return 0;
		DEBUG fprintf(stdout,"%s:%d:DEBUG: First overflow page = %lu\n", FL , (long unsigned int)ovp);
		DEBUG hdump((unsigned char *)(data_endpoint -16), 16, "First overflow page start data");


		while (ovp > 0) {

			void *calculated_address;

			calculated_address = g->db_origin +( (ovp -1) *g->page_size);
			DEBUG fprintf(stdout,"%s:%d:DEBUG: Calculated address: %p\n", FL, calculated_address);

			// test for seeking beyond the db limit
			//if ((g->db_origin +((ovp -1) *g->page_size)) > (g->db_end -4)) {

			if ( calculated_address > (void *)(g->db_end -4)) { //PLD:20141220-0000
				DEBUG	fprintf(stdout,"%s:%d:ERROR: Seek beyond end of data looking for overflow page (%p > %p)\n", FL, calculated_address, g->db_end);
				break;
			} 

			if ( calculated_address < (void *)(g->db_origin)) { //PLD:20141220-0000
				DEBUG	fprintf(stdout,"%s:%d:ERROR: Seek before DB starts (%p < %p)\n", FL, calculated_address, g->db_origin);
				break;
			} 

			memcpy(&tmp, calculated_address, 4);
			ovp = payload->overflow_pages[ovpi] = ntohl(tmp);
			DEBUG fprintf(stdout,"%s:%d:DEBUG: overflow page[%d] = %d\n", FL , ovpi, ovp);
			DEBUG fflush(stdout);
			ovpi++;
			if (ovpi > OVERFLOW_PAGES_MAX) {
				fprintf(stdout,"ERROR: No more space for overflow pages\n");
				fflush(stdout);
				payload->overflow_pages[0] = 0;
				break;
			}
			payload->overflow_pages[ovpi] = 0;
		}


		DEBUG {
			fprintf(stdout,"DEBUG: Total of %d overflow pages\n",ovpi);
			ovpi = 0;
			while (payload->overflow_pages[ovpi]) {
				fprintf(stdout,"DEBUG: Overflow %d->%d\n", ovpi, payload->overflow_pages[ovpi]);
				ovpi++;
			}
		}
		}  // overflow handling

		if (payload->header_size > g->page_size) return 0; // sorry, no can do with the way we're playing this decoding game.
		if (payload->header_size < 2) return 0; // need at least 2 bytes

		plh_ep += payload->header_size; // if we got a sane value, then we can use this for the full decode size ( includes the size of the first varint telling us the size )

		DEBUG { fprintf(stdout,"[L:%lld][id:%lld][PLHz:%lld]",(long long int) payload->length, (long long int)payload->rowid, (long long int)payload->header_size); }

		t = 0;
		offset = 0;


		while (1) {
			uint64_t s;
			int vil;

			vil = varint_decode( &s, p, &p ); 

			if (vil > 8) return 0; // no var int should be bigger than 8 bytes.

			payload->cells[t].t = s; // set the type
			switch (s) {
				case 0: s = 0; break;
				case 1: s = 1; break;
				case 2: s = 2; break;
				case 3: s = 3; break;
				case 4: s = 4; break;
				case 5: s = 6; break;
				case 6: case 7: s = 8; break;
				case 8: case 9: s = 0; break;
				case 10: case 11: DEBUG fprintf(stdout,"%s:%d:DEBUG: celltype 10/11 reserved, aborting row.\n",FL); s = 0; return 0; break;
				default: 
										if ((s >= 12)&&((s&0x01)==0)) { payload->cells[t].t = 12; s = (s-12)/2; }
										else if ((s >= 13)&&((s&0x01)==1)) { payload->cells[t].t = 13; s = (s-13)/2; }
										break;
			}

			payload->cells[t].s = s; // set the size/length
			payload->cells[t].o = (plh_ep +offset) -base;
			offset += payload->cells[t].s;
			if (offset > payload->length) return 0;

			DEBUG { fprintf(stdout,"[%d:%d:%d-%d(%ld)]", t, payload->cells[t].t, payload->cells[t].s, payload->cells[t].o, plh_ep -p ); }

			if (p >= plh_ep) break;
			t++;
			payload->cell_count++;
			if ( t > g->cc_max ) return 0;
		} // while decoding the cells

		if (p == plh_ep) {
			DEBUG {
				fprintf(stdout,"DEBUG: Payload head size match. (%ld =? %ld)\n ", p -base,plh_ep -base);
				fprintf(stdout,"DEBUG: Data size by cell meta sum = %d\n ", offset );
			}
		} else {
			DEBUG {
				fprintf(stdout,"DEBUG: Payload scan end point, and predicted end point didn't match, difference %ld \n", p -plh_ep );
			}
		}

		if ( t < g->cc_min )  {
			DEBUG fprintf(stdout,"%s:%d:DEBUG: cell count under the minimum, so aborting\n", FL );
			return 0;
		}

		DEBUG fprintf(stdout,"Offset [%u] + headersize [%lu] = length check [%lu]... \n", offset, (unsigned long int)payload->header_size, (unsigned long int)payload->length);

		if (mode == DECODE_MODE_FREESPACE) {
			/** there can often be multiple entries within freespace, so we have to be
			* a little looser with our acceptance criterion
			*/
			if (offset <= payload->length) {
				DEBUG fprintf(stdout,"%s:%d:DEBUG: FREESPACE SUBMATCH FOUND ( %u of %lu used )\n", FL , offset, (long unsigned int) payload->length);
				return (offset +payload->header_size +4);
			}
		}

		if (offset + payload->header_size  == payload->length) {
			DEBUG fprintf(stdout,"\nMATCH FOUND!\n");
			return 1;
		}

		return 0;
	}


	/*-----------------------------------------------------------------\
	Date Code:	: 20150723-210259
	Function Name	: ntonll
	Returns Type	: uint64_t
	----Parameter List
	1. uint64_t value, 
	------------------
	Exit Codes	: 
	Side Effects	: 
	--------------------------------------------------------------------
Comments:

--------------------------------------------------------------------
Changes:

\------------------------------------------------------------------*/

	uint64_t swap64(uint64_t x) {
		uint8_t i;
		uint64_t y ;
		uint8_t *px, *py;

		px = (uint8_t *)&x;
		py = (uint8_t *)&y;

		for (i=0; i<8; i++) {
			*(py+i) = *(px +(7-i));
		}
		return y;
	}

	uint64_t ntohll(uint64_t value) {
		uint64_t t;
//		hdump( &value, 8, "\nUxx: ");
		if (1==ntohl(1)) {
			return value;
		} else {
			t = swap64(value);
//			hdump( &t, 8, "\nSWP: ");
			return t;
		}
	}

	/*-----------------------------------------------------------------\
	Date Code:	: 20131008-182215
	Function Name	: dump_row
	Returns Type	: int
	----Parameter List
	1. struct globals *glob, 
	2.  char *p, 
	3.  char *data_endpoint, 
	4.  struct sql_payload *payload, 
	5.  int decode , 
	------------------
	Exit Codes	: 
	Side Effects	: 
	--------------------------------------------------------------------
Comments:

--------------------------------------------------------------------
Changes:

\------------------------------------------------------------------*/
	int dump_row( struct globals *g, char *base, char *data_endpoint, struct sql_payload *payload, int mode ) {
		int t = 0;
		int ovpi;
		void *addr;


		DEBUG fprintf(stdout,"\n-DUMPING ROW------------------\n");
		DEBUG hdump((unsigned char *)base, 16, "Dump_row starting data");

		if ( payload->length > g->db_size ) {
			DEBUG fprintf(stdout,"%s:%d:ERROR: Nonsensical payload length of %ld requested, ignoring.\n", FL, (long int)payload->length);
			return -1;
		}

		if (payload->overflow_pages[0] == 0) {
			payload->mapped_data = base;
			payload->mapped_data_endpoint = data_endpoint;

		} else {
			int msize = (payload->length)*sizeof(uint8_t);

			// count size of overflow pages (if any)
			ovpi = 0;
			while (payload->overflow_pages[ovpi]) {
				addr = g->db_origin +((payload->overflow_pages[ovpi]-1) *g->page_size) +4; //PLD:20141221-2240 segfault fix
				if (( addr < (void *)g->db_origin) || ( addr+4 > (void *)g->db_end)) {
					DEBUG fprintf(stdout,"%s:%d:dump_row:ERROR: page seek request outside of boundaries of file (%p < %p > %p)\n", FL, g->db_origin, addr, g->db_end);
					return -1;
				}
				msize += g->page_size -4;
				ovpi++;
			}

			printf("plength %ld, total: %d\n", payload->length, msize);
			//__asm__("int $3");
			payload->mapped_data = malloc(msize);
			if ( !payload->mapped_data ) {
				fprintf(stderr,"%s:%d:ERROR: Cannot allocate %ld bytes for mapped data\n", FL, (long int)payload->length +100);
				return -1;
			}
			DEBUG fprintf(stdout,"ALLOCATED %d bytes to mapped data\n", (int)(payload->length +100) );
			if (!payload->mapped_data){ fprintf(stderr,"ERROR: Cannot allocate %d bytes for payload\n", (int)(payload->length +1)); return 0; }
			memset( payload->mapped_data, 'X', payload->length +1 );

			// load in the first, default page.
			DEBUG fprintf(stdout,"Copying data for initial page\n");
			memcpy(payload->mapped_data, base, data_endpoint -base );
			payload->mapped_data_endpoint = payload->mapped_data +(data_endpoint -base -4);
			//		DEBUG hdump( (unsigned char *)payload->mapped_data, payload->mapped_data_endpoint -payload->mapped_data +4  );

			// Load in the overflow pages (if any)
			ovpi = 0;
			while (payload->overflow_pages[ovpi]) {
				DEBUG fprintf(stdout,"Copying data from file to memory for page %d to offset [%d]\n", payload->overflow_pages[ovpi], (int)(payload->mapped_data_endpoint -payload->mapped_data));

				addr = g->db_origin +((payload->overflow_pages[ovpi]-1) *g->page_size) +4; //PLD:20141221-2240 segfault fix
				if (( addr < (void *)g->db_origin) || ( addr+4 > (void *)g->db_end)) {
					DEBUG fprintf(stdout,"%s:%d:dump_row:ERROR: page seek request outside of boundaries of file (%p < %p > %p)\n", FL, g->db_origin, addr, g->db_end);
					return -1;
				}

				memcpy(payload->mapped_data_endpoint, addr, g->page_size -4);
				payload->mapped_data_endpoint += g->page_size -4;

				//	DEBUG hdump( (unsigned char *)payload->mapped_data, payload->mapped_data_endpoint -payload->mapped_data );

				ovpi++;
			}
		}

		DEBUG hdump((unsigned char *)payload->mapped_data, payload->mapped_data_endpoint -payload->mapped_data, "Payload mapped data" );

		if (mode == DECODE_MODE_FREESPACE) {
			t = 0;
			fprintf(stdout,"-1");

		} else t = -1;

		while (t <= payload->cell_count) {
			DEBUG fprintf(stdout,"%s:%d:DEBUG: Cell[%d], Type:%d, size:%d, offset:%d\n", FL , t, payload->cells[t].t, payload->cells[t].s, payload->cells[t].o);
			if (t == -1) fprintf(stdout,"%ld", (long unsigned int) payload->rowid);
			if (t>=0) { fprintf(stdout,",");
				switch (payload->cells[t].t) {
					case 0: fprintf(stdout,"NULL"); break;
					case 1: fprintf(stdout,"x%d", to_signed_byte(*(payload->mapped_data +payload->cells[t].o)) ); break;
					case 2: {
								uint16_t n;
								memcpy(&n, payload->mapped_data +payload->cells[t].o, 2 );
								fprintf(stdout,"%d" , to_signed_int(ntohs(n)));
							}
							break;

					case 3: {
								uint32_t n;
								memcpy(&n, payload->mapped_data +payload->cells[t].o, 3 );
								fprintf(stdout,"%ld", to_signed_long(ntohl(n)));
							}
							break;

					case 4: {
								uint32_t n;
								memcpy(&n, payload->mapped_data +payload->cells[t].o, 4 );
								fprintf(stdout,"%ld", to_signed_long(ntohl(n)));
							}
							break;

					case 5: fprintf(stdout,"%d", ntohl(*(payload->mapped_data +payload->cells[t].o))); break;
					case 6: fprintf(stdout,"%d", ntohl(*(payload->mapped_data +payload->cells[t].o))); break;
					case 7: 
							{
								uint64_t n;
								uint64_t nn;
								double *zz;
								memcpy(&n, payload->mapped_data +payload->cells[t].o, 8 );
								nn = (double) ntohll(n);
//									hdump( &nn, 8, "\nFPPP: ");
									zz = (double *)&nn;
								fprintf(stdout,"%f",*zz); 
							}
							break;

					case 8: fprintf(stdout,"0" ); break;
					case 9: fprintf(stdout,"1" ); break;
					case 12: 
							if ( g->report_blobs) {
								if (payload->cells[t].s < g->blob_size_limit) {
									DEBUG fprintf(stdout,"%s:%d:DEBUG:Not Dumping data to blob file, keeping in CSV\n", FL );
									blob_dump((unsigned char *) (payload->mapped_data +payload->cells[t].o), payload->cells[t].s );
								} else {
									// dump the blob to a file.
									DEBUG fprintf(stdout,"%s:%d:DEBUG:Dumping data to %d.blob [%d bytes]\n", FL ,g->blob_count, payload->cells[t].s);
									blob_dump_to_file( g, (payload->mapped_data +payload->cells[t].o), payload->cells[t].s );
									DEBUG fprintf(stdout,"\"%d.blob\"", g->blob_count);
								}
							}
							g->blob_count++;
							break;

					case 13:
							DEBUG fprintf(stdout,"%s:%d:DEBUG: Dumping text-13\n", FL );
							sqltdump( payload->mapped_data +payload->cells[t].o, payload->cells[t].s ); 
							break;
					default:
							fprintf(stderr,"Invalid cell type '%d'", payload->cells[t].t);
							DEBUG fprintf(stdout,"%s:%d:DEBUG: Invalid cell type '%d'", FL, payload->cells[t].t);
							DEBUG hdump( (unsigned char *) base, 128, "Invalid cell type" );
							return 0;
							break;
				} // switch cell type
			}

			t++;

		} // while decoding the cells

		fprintf(stdout,"\n");
		fflush(stdout);
		if (payload->overflow_pages[0] != 0) {
			//__asm__("int $3");
			free( payload->mapped_data );
		}

		return 0;
	}




	/*-----------------------------------------------------------------\
	Date Code:	: 20131004-211659
	Function Name	: *find_next_sms
	Returns Type	: char
	----Parameter List
	1. char *s, 
	2.  char *end_point , 
	------------------
	Exit Codes	: 
	Side Effects	: 
	--------------------------------------------------------------------
Comments:

Finds rows within a block.

--------------------------------------------------------------------
Changes:


\------------------------------------------------------------------*/
	char *find_next_row( struct globals *g, char *s, char *end_point, char *global_start, int mode, size_t forced_length ) {

		char *p;
		struct sql_payload sql;

		DEBUG fprintf(stdout,"find_next_row: MODE: %d\n", mode );
		if (s == NULL) fprintf(stdout,"ERROR: NULL passed as search-space parameter\n");
		p = s;
		do {
			int row;

			row = decode_row( g, p, end_point, &sql, mode, forced_length );
			if (row) {
				DEBUG fprintf(stdout,"ROWID: %ld found [+%ld] record size: %d bytes\n", (unsigned long int)sql.rowid, p -global_start, (unsigned int)( sql.length+sql.prefix_length ));
				fflush(stdout);

				/** If we're only wanting the removed, no-key-value rows, then 
				* continue to the next row 
				*/
				if ((g->removed_only)&&(row >= 0)) {
					p++;
					continue;
				}



				if ((mode == DECODE_MODE_NORMAL)&&( g->freelist_space_only == 1)) {
					// do nothing
				} else  {
					dump_row( g, p, end_point, &sql, mode );
				}

				fflush(stdout);
				if (mode == DECODE_MODE_NORMAL) {
					if (g->fine_search) p++;
					else p+= sql.length;
				} else {
					if (row >= forced_length) {
						DEBUG fprintf(stdout,"%s:%d:DEBUG: No more data left in freespace block to examine\n", FL);
						p = end_point;
						break;
					} else {
						p+=row; forced_length -= row;
						DEBUG hdump((unsigned char *)p,64, "After freespace decode");
					}
				}
			} else {
				p++;
			}

		} while (p < end_point -PAYLOAD_SIZE_MINIMUM);

		return NULL;

	}




	/*-----------------------------------------------------------------\
	Date Code:	: 20131002-220317
	Function Name	: main
	Returns Type	: int
	----Parameter List
	1. int argc, 
	2.  char **argv , 
	------------------
	Exit Codes	: 
	Side Effects	: 
	--------------------------------------------------------------------
Comments:

--------------------------------------------------------------------
Changes:

\------------------------------------------------------------------*/
	int main( int argc, char **argv ) {

		int fd;
		struct globals globo, *g;
		struct stat st;
		char *p;
		int stat_result;

		/**
		* Set up our global struct.
		*
		* We do this as a local var, rather than global so that it forces
		* us to pass it through the functions, rather than _assuming_ it's
		* available globally, which makes it a lot easier to migrate things
		* to other libs/modules later
		*
		*/
		g = &globo;



		UNDARK_init( g );
		UNDARK_parse_parameters( argc, argv, g );

		/**
		* Check our input file sanity
		*
		*/
		stat_result = stat( g->input_file, &st );
		if (stat_result != 0) {
			fprintf(stderr,"ERROR: Cannot access input file '%s' ( %s )\n", g->input_file, strerror(errno));
			exit(1);
		}


		/**
		* Map our input file to memory, makes it a lot easier
		* to jump around if we need to and saves us having to
		* handle buffer limits - leave it to the OS to manage :)
		*/
		fd = open( g->input_file, O_RDONLY );
		g->db_size = st.st_size;
		g->db_origin = mmap( NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0 );
		g->db_end = g->db_origin +st.st_size -1;

		//fprintf(stderr,"DB origin: %p\nDB end: %p\n", g->db_origin, g->db_end );

		/**
		* Start decoding the database
		*
		* Though it's not really required for us to care about the
		* SQLite page sizes, it can be useful in case we get boundary
		* situations and try to follow the data across a page
		*
		* If the page size is already set via parameter, then skip
		*
		*/
		if (g->page_size == 0) {
			p = g->db_origin +16;
			g->page_size =	(*(p+1)) | ((*p)<<8);
		}

		/**
		* Get the number of pages that are supposed to be in the database, though
		* we can ignore this and simply parse through the whole DB page at a time
		* until we reach the end
		*/
		p = g->db_origin +28;
		memcpy( &g->page_count, g->db_origin +28, 4 ); // copy the page count from the header
		g->page_count = ntohl( g->page_count ); // convert to local format

		DEBUG fprintf(stdout,"Pagesize: %u, Pagecount: %u\n", g->page_size, g->page_count);

		/** 
		* Get the free list meta data
		*
		*/
		memcpy( &g->freelist_first_page, g->db_origin +32, 4 ); // copy the page count from the header
		g->freelist_first_page = ntohl( g->freelist_first_page );
		DEBUG fprintf(stdout,"First page of freelist trunk: %d\n", g->freelist_first_page );

		memcpy( &g->freelist_page_count, g->db_origin +36, 4 ); // copy the page count from the header
		g->freelist_page_count = ntohl( g->freelist_page_count );
		DEBUG fprintf(stdout,"Freelist page count: %d\n", g->freelist_page_count );


		/**
		* Get the actual free list pages
		*
		*/
			g->db_cfp = g->db_cpp = g->db_origin;

			DEBUG fprintf(stdout,"%s:%d:DEBUG: Commence decoding data\n", FL );
			fflush(stdout);

			while (g->db_cfp < g->db_end ) {
				struct sqlite_leaf_header leaf;
				int freeblock_mode = 0;

				/* load the next page from the file in to the scratch pad */
				g->db_cfp = g->db_cpp;
				g->db_cpp_limit = g->db_cpp +g->page_size ; // was -1 ?

				DEBUG fprintf(stdout,"\n\n%s:%d:-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=START.\n", FL);

				/* process the block, mostly this is just removing any 0-bytes
					from the block so our strstr() calls aren't prematurely terminated.
				*/
				DEBUG {
					char *p;
					size_t l;
					int bc = 0;

					fprintf(stdout,"%s:%d:Dumping main block in RAW... [ Page No: %lu, Offset: %lu (0x%X),  size : %d ]\n"
							, FL
							, (long unsigned int)g->page_number
							, (long unsigned int)(g->db_cpp -g->db_origin)
							, (unsigned int)(g->db_cpp -g->db_origin)
							,  g->page_size
							);

					p = g->db_cfp;
					l = g->page_size;
					while ((l--)&&(p)) {
						{ if (isprint(*p)) { fprintf(stdout,"%c", *p); } else fprintf(stdout,"_");}
						p++;
						bc++;
						if (bc%128 == 0) fprintf(stdout,"\n");
					}
					fprintf(stdout,"\n");
					fflush(stdout);
				} // debug



				leaf.freeblock_offset = 0;
				leaf.freeblock_size = 0;
				leaf.freeblock_next = 0;
				leaf.page_number = g->page_number;

				/* Decode the page header */
				if (*(g->db_cfp) == 13) { 

					DEBUG fprintf(stdout,"%s:%d:DEBUG: Decoding page header for page %d\n", FL , g->page_number );
					fflush(stdout);
					leaf.page_byte = 13;

					/**
					* Get freeblock offset and determine if we have a free block in this
					* page that needs to be inspected.  This is one of the more commonly
					* needed parts of data for our row recovery 
					*
					*/
					memcpy( &(leaf.freeblock_offset), (g->db_cfp +1), 2 );
					leaf.freeblock_offset = ntohs( leaf.freeblock_offset );
					if (leaf.freeblock_offset > 0) {
						uint16_t next, sz, off;

						freeblock_mode = 1;
						off = leaf.freeblock_offset;

						DEBUG fprintf(stdout,"%s:%d:DEBUG: FREEBLOCK mode ON: header decode [offset=%u]\n", FL , leaf.freeblock_offset);

						do {
							DEBUG hdump((unsigned char *)(g->db_cfp +off), 16, "Freeblock header data");

							memcpy( &next, ( g->db_cfp +off ), 2 );
							next = ntohs( next );
							memcpy( &sz, ( g->db_cfp +off +2 ), 2 );
							sz = ntohs( sz );

							DEBUG fprintf(stdout,"%s:%d:DEBUG: Freeblock size = %u, next position = %u\n", FL, sz, next );

							if (next) off = next;
						} while (next);
						DEBUG fprintf(stdout,"%s:%d:DEBUG: END OF FREEBLOCK TRACE\n", FL);

						memcpy( &(leaf.freeblock_next), ( g->db_cfp +leaf.freeblock_offset ), 2 );
						leaf.freeblock_next = ntohs( leaf.freeblock_next );
						memcpy( &(leaf.freeblock_size), ( g->db_cfp +leaf.freeblock_offset +2 ), 2 );
						leaf.freeblock_size = ntohs( leaf.freeblock_size );
					}

					DEBUG fprintf(stdout,"%s:%d:DEBUG: Freeblock offset = %u, size = %u, next block = %u \n", FL , leaf.freeblock_offset, leaf.freeblock_size, leaf.freeblock_next );
					if (leaf.freeblock_size > 0) {
						DEBUG fprintf(stdout,"%s:%d:DEBUG: Freeblock data [ %d bytes total [4 bytes for header] ]\n", FL, leaf.freeblock_size );
						DEBUG hdump( (unsigned char *)(g->db_cfp +leaf.freeblock_offset+4), leaf.freeblock_size-4, "Actual data in free block" );
					}
					fflush(stdout);
					//				leaf.freeblock_offset = ntohs( ta );
					leaf.cellcount = ntohs(*(g->db_cfp+3));
					leaf.cell_offset = ntohs(*(g->db_cfp+5));
					leaf.freebytes = (*(g->db_cfp+7));

					DEBUG fprintf(stdout,"%s:%d:DEBUG: PAGEHEADER:%d pagebyte: %d, freeblock offset: %d, cell count: %d, first cell offset %d, free bytes %d\n", FL 
							, leaf.page_number
							, leaf.page_byte
							, leaf.freeblock_offset
							, leaf.cellcount
							, leaf.cell_offset
							, leaf.freebytes
							);

					/**
					* If we're wanting free block sourced data, then simply jump
					* to the start of the free block space and commence the searching
					* in the next section ( find_next_row ).
					*
					* After this the g->db_cfp pointer should be sitting on the first
					* varint of the payload header which defines the header length
					* (inclusive)
					*
					* Detecting rows in the freeblocks is done differently to the 
					* normal data, so 
					*
					*/
					if (g->freelist_space_only) {

						if ((leaf.freeblock_offset > 0) && (leaf.freeblock_size > 0)) {

							DEBUG fprintf(stdout,"%s:%d:DEBUG: Shifting to freespace at %d from page start\n", FL , leaf.freeblock_offset);
							g->db_cfp = g->db_cfp + leaf.freeblock_offset +4;

							DEBUG fprintf(stdout,"%s:%d:DEBUG: New position = %p\n", FL , g->db_cfp);
							DEBUG hdump((unsigned char *)g->db_cfp -4,32, "Scratch pointer at freespace data start (including 4 byte header)");
							DEBUG fflush(stdout);
						}
					}

					fflush(stdout);
				} // if we have a leaf page, which we can decode the header on.




				//if ((leaf.page_byte == 13)) {
				if (1) {

					char *row;
					row = g->db_cfp;
					DEBUG fprintf(stdout,"%s:%d:DEBUG: g->db_cfp search at = %p\n", FL , g->db_cfp);
					do {

						if ((row > g->db_origin)&&(row < g->db_end)) {

							row = find_next_row( g, row, g->db_cpp_limit, g->db_cfp, freeblock_mode, leaf.freeblock_size );

							//if (row > g->db_end) fprintf(stdout,"ERROR: beyond end point\n");
							if (row > g->db_cpp_limit) fprintf(stdout,"ERROR: beyond end point\n");
							if (row < g->db_cfp) DEBUG fprintf(stdout,"%s:%d:DEBUG: Row location not in g->db_cfp page\n", FL );
							if (row == NULL) DEBUG fprintf(stdout,"%s:%d:DEBUG: Row has been returned as NULL\n", FL );
							DEBUG fprintf(stdout,"%s:%d:DEBUG: ROW found at offset: %ld\n", FL, row-g->db_cfp);
						} else {

							break;
						}

					} while (row && (row < g->db_cpp_limit ));
					//} while (row && (row < g->db_cpp_limit ) && (row < g->db_end) );

					DEBUG fprintf(stdout,"%s:%d:DEBUG: Finished searching for rows in DB page %d\n", FL , g->page_number);
			}


			{
				g->db_cpp += g->page_size;
				g->page_number++;
			}

			if (g->page_count < g->page_number)
				break;

			} // while (data < endpoint)

			close(fd);

			return 0;
			}


			/** END **/
