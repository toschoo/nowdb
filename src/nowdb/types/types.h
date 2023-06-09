/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Fundamental Types
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_types_decl
#define nowdb_types_decl

#include <nowdb/types/version.h>
#include <stdint.h>

#ifdef __GNUC__
#define likely(x)   __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)
#define popcount32 __builtin_popcount
#define popcount64 __builtin_popcountll
#else
#define likely(x) x
#define unlikely(x) x
#define popcount32
#define popcount64
#endif

#ifdef _NOWDB_WITH_PYTHON
#define NOWDB_INC_PYTHON <python2.7/Python.h>
#endif

#define NOWDB_MAGIC 0x450099db

#define NOWDB_DB_VERSION 1

#define NOWDB_OS_PAGE  4096
#define NOWDB_IDX_PAGE 8192
#define NOWDB_CMP_PAGE 8192
#define NOWDB_MAX_PATH 4096
#define NOWDB_MAX_NAME  256

#define NOWDB_BLACK_PAGE (void*)0x1

#define NOWDB_KILO 1024
#define NOWDB_MEGA 1048576
#define NOWDB_GIGA 1073741824

typedef char nowdb_bool_t;

#define TRUE 1
#define FALSE 0

nowdb_bool_t nowdb_init();
void nowdb_close();

void *nowdb_lib();

typedef uint8_t  nowdb_bitmap8_t;
typedef uint16_t nowdb_bitmap16_t;
typedef uint32_t nowdb_bitmap32_t;
typedef uint64_t nowdb_bitmap64_t;

#define NOWDB_BITMAP8_ALL 0xff
#define NOWDB_BITMAP16_ALL 0xffff
#define NOWDB_BITMAP32_ALL 0xffffffff
#define NOWDB_BITMAP64_ALL 0xffffffffffffffff

/* any value used as actor or event key */
typedef uint64_t nowdb_key_t;

/* time counts nano-seconds since the epoch */
typedef int64_t nowdb_time_t;

typedef uint32_t nowdb_version_t;

typedef uint32_t nowdb_roleid_t;

typedef uint8_t nowdb_content_t;

#define NOWDB_CONT_UNKNOWN 0
#define NOWDB_CONT_VERTEX  1
#define NOWDB_CONT_EDGE    2

/* polymorphic value       */
typedef uint64_t nowdb_value_t;

/* possible types of value */
typedef uint32_t nowdb_type_t;

#define NOWDB_TYP_NOTHING  0
#define NOWDB_TYP_TEXT     1
#define NOWDB_TYP_DATE     2
#define NOWDB_TYP_TIME     3
#define NOWDB_TYP_FLOAT    4
#define NOWDB_TYP_INT      5
#define NOWDB_TYP_UINT     6
#define NOWDB_TYP_COMPLEX  7
#define NOWDB_TYP_LONGTEXT 8
#define NOWDB_TYP_BOOL     9
#define NOWDB_TYP_SHORT   99

int nowdb_strtoval(char *str, nowdb_type_t typ, void *value);
int nowdb_strtotype(char *str);
int nowdb_correctType(nowdb_type_t good,
                      nowdb_type_t *bad,
                      void      *value);

#define NOWDB_EOR       0xa

#define NOWDB_STATUS    0x21
#define NOWDB_REPORT    0x22
#define NOWDB_ROW       0x23
#define NOWDB_CURSOR    0x24

#define NOWDB_DELIM     0x3b

#define NOWDB_ACK       0x4f
#define NOWDB_NOK       0x4e

#define NOWDB_UMAX ULLONG_MAX
#define NOWDB_IMAX LLONG_MAX
#define NOWDB_FMAX 9007199254740992ll

#define NOWDB_UMIN 0
#define NOWDB_IMIN LLONG_MIN;
#define NOWDB_FMIN -9007199254740992ll

typedef uint32_t nowdb_fileid_t;
typedef uint64_t nowdb_pageid_t;
typedef uint64_t nowdb_rowid_t;

extern char nowdb_nullrec[1024];

int nowdb_sizeByOff(nowdb_content_t cont, uint16_t off);

/* ------------------------------------------------------------------------
 * Offsets
 * ------------------------------------------------------------------------
 */
#define NOWDB_OFF_VERTEX 0
#define NOWDB_OFF_ORIGIN 0
#define NOWDB_OFF_DESTIN 8
#define NOWDB_OFF_STAMP  16
#define NOWDB_OFF_VSTAMP  8

/* to be removed */
#define NOWDB_OFF_ROLE 128
#define NOWDB_OFF_PROP 256
#define NOWDB_OFF_VTYPE 512
#define NOWDB_OFF_VALUE 1024

int nowdb_vertex_offByName(char *field);

/* ------------------------------------------------------------------------
 * Edge Offsets
 * ------------------------------------------------------------------------
 */

int nowdb_edge_offByName(char *field);

// records per page for a given record size
uint32_t nowdb_recordsPerPage(uint32_t recsz);

// compute the size of the page control block
// which registers records in a page
// which are to be considered / to be ignored
uint32_t nowdb_pagectrlSize(uint32_t recsz);

char *nowdb_typename(nowdb_type_t typ);

// compute the size of the attribute control block
// which registers attributes that are NULL/NOT NULL
static inline uint32_t nowdb_attctrlSize(uint16_t atts) {
	return (atts%8==0?atts/8:atts/8+1);
}

// get start offset in record where ctrl block starts
static inline uint32_t nowdb_ctrlStart(uint16_t atts) {
	return atts*8;
}

// get attribute control bit and byte for specific offset
static inline void nowdb_getCtrl(uint32_t off, uint8_t *bit, uint16_t *byte) {
	uint32_t o = off/8;
	*byte = o/8;
	*bit  = o%8;
}

static inline uint32_t nowdb_recSize(uint16_t atts) {
	return(8*atts+nowdb_attctrlSize(atts));
}

#endif
