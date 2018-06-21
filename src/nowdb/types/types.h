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

#define NOWDB_MAGIC 0x450099db

#define NOWDB_VERSION 1

#define NOWDB_OS_PAGE  4096
#define NOWDB_IDX_PAGE 8192
#define NOWDB_CMP_PAGE 8192
#define NOWDB_MAX_PATH 4096
#define NOWDB_MAX_NAME  256

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

typedef uint32_t nowdb_fileid_t;
typedef uint64_t nowdb_pageid_t;
typedef uint64_t nowdb_rowid_t;

extern char nowdb_nullrec[64];

/* ------------------------------------------------------------------------
 * Vertex Offsets
 * ------------------------------------------------------------------------
 */
#define NOWDB_OFF_VERTEX 0
#define NOWDB_OFF_PROP   8
#define NOWDB_OFF_VALUE 16
#define NOWDB_OFF_VTYPE 24
#define NOWDB_OFF_ROLE  28

/* ------------------------------------------------------------------------
 * Vertex Property
 * ---------------
 * Storetype of properties of a vertex
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_key_t   vertex; /* id of the vertex          */
	nowdb_key_t property; /* id of the vertex property */
	nowdb_value_t  value; /* property value            */
	nowdb_type_t   vtype; /* type of the value         */
	nowdb_roleid_t  role; /* role identifier           */
} nowdb_vertex_t;

void nowdb_vertex_writeValue(nowdb_vertex_t *v, nowdb_type_t typ, void *value);
void nowdb_vertex_readValue(nowdb_vertex_t *v, nowdb_type_t typ, void *value);
int nowdb_vertex_strtov(nowdb_vertex_t *v, nowdb_type_t typ, char *value);

int nowdb_vertex_offByName(char *field);

/* ------------------------------------------------------------------------
 * Edge Offsets
 * ------------------------------------------------------------------------
 */
#define NOWDB_OFF_EDGE     0
#define NOWDB_OFF_ORIGIN   8
#define NOWDB_OFF_DESTIN  16
#define NOWDB_OFF_LABEL   24
#define NOWDB_OFF_TMSTMP  32
#define NOWDB_OFF_WEIGHT  40
#define NOWDB_OFF_WEIGHT2 48
#define NOWDB_OFF_WTYPE   56
#define NOWDB_OFF_WTYPE2  60

/* ------------------------------------------------------------------------
 * Edge
 * ----
 * Storetype of connections bewteen vertices
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_key_t        edge; /* id of the edge          */
	nowdb_key_t      origin; /* id of the left  vertex  */
	nowdb_key_t      destin; /* id of the right vertex  */
	nowdb_key_t       label; /* id of the primary label */
	nowdb_time_t  timestamp; /* timestamp               */
	nowdb_value_t    weight; /* first weight component  */
	nowdb_value_t   weight2; /* secnd weight component  */
	nowdb_type_t   wtype[2]; /* type of the weights     */
} nowdb_edge_t;

void nowdb_edge_writeWeight(nowdb_edge_t *e, nowdb_type_t typ, void *value);
void nowdb_edge_writeWeight2(nowdb_edge_t *e, nowdb_type_t typ, void *value);

void nowdb_edge_readWeight(nowdb_edge_t *e, nowdb_type_t typ, void *value);
void nowdb_edge_readWeight2(nowdb_edge_t *e, nowdb_type_t typ, void *value);

int nowdb_edge_strtow(nowdb_edge_t *e, nowdb_type_t typ, char *value);
int nowdb_edge_strtow2(nowdb_edge_t *e, nowdb_type_t typ, char *value);

int nowdb_edge_offByName(char *field);

#endif
