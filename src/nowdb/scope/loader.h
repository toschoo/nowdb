/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Loader: CSV Loader for the canonical formal (context and vertex)
 *   TODO: The loader concept should be generic.
 *         In particular, it should be able to:
 *         - load CSV ('canoncial format' with numerical identifiers,
 *                     that's what we have)
 *         - load CSV ('canonical format' with strings, using the DBs
 *                     datamodel)
 *         - load binary formats (using, e.g., avro)
 * ========================================================================
 */
#ifndef nowdb_loader_decl
#define nowdb_loader_decl

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#include <nowdb/types/types.h>
#include <nowdb/types/time.h>
#include <nowdb/store/store.h>

/* ------------------------------------------------------------------------
 * Flags for CSV loader
 * ------------------------------------------------------------------------
 */
#define NOWDB_CSV_VERTEX          2
#define NOWDB_CSV_HAS_HEADER      8
#define NOWDB_CSV_USE_HEADER     16
#define NOWDB_CSV_GIVE_FEEDBACK  64

/* ------------------------------------------------------------------------
 * Number of fields in edge
 * ------------------------------------------------------------------------
 */
#define NOWDB_EDGE_FIELDS 9

/* ------------------------------------------------------------------------
 * Field numbers in edge
 * ------------------------------------------------------------------------
 */
#define NOWDB_FIELD_EDGE      0
#define NOWDB_FIELD_ORIGIN    1
#define NOWDB_FIELD_DESTIN    2
#define NOWDB_FIELD_LABEL     3 
#define NOWDB_FIELD_TIMESTAMP 4
#define NOWDB_FIELD_WEIGHT    5
#define NOWDB_FIELD_WEIGHT2   6
#define NOWDB_FIELD_WTYPE     7
#define NOWDB_FIELD_WTYPE2    8

/* ------------------------------------------------------------------------
 * Number of fields in vertex
 * ------------------------------------------------------------------------
 */
#define NOWDB_VERTEX_FIELDS 5

/* ------------------------------------------------------------------------
 * Field numbers in vertex
 * ------------------------------------------------------------------------
 */
#define NOWDB_FIELD_VERTEX   0
#define NOWDB_FIELD_PROPERTY 1
#define NOWDB_FIELD_VALUE    2
#define NOWDB_FIELD_VTYPE    3
#define NOWDB_FIELD_ROLE     4

/* ------------------------------------------------------------------------
 * Field and row callbacks
 * ------------------------------------------------------------------------
 */
typedef void (*nowdb_csv_field_t)(void*, size_t, void*);
typedef void (*nowdb_csv_row_t)(int,void*);

/* ------------------------------------------------------------------------
 * Predeclaration of csv state
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_csv_st nowdb_csv_t;

/* ------------------------------------------------------------------------
 * Loader
 * ------------------------------------------------------------------------
 */
typedef struct {
	/* model */
	FILE            *stream; /* the input stream  */
	FILE           *ostream; /* the output stream */
	nowdb_store_t    *store; /* the output store  */
	nowdb_bitmap32_t  flags; /* flags             */
	nowdb_csv_field_t fproc; /* field callback    */
	nowdb_csv_row_t   rproc; /* row   callback    */
	char              delim; /* csv delimiter     */
	nowdb_csv_t        *csv; /* csv state         */
	nowdb_err_t         err; /* error occurred    */
	uint64_t         loaded; /* rows loaded       */
	uint64_t         errors; /* rows rejected     */
	nowdb_time_t    runtime; /* running time      */
} nowdb_loader_t;

/* ------------------------------------------------------------------------
 * Initialise loader
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_loader_init(nowdb_loader_t    *ldr,
                              FILE           *stream,
                              FILE          *ostream,
                              nowdb_store_t   *store,
                              nowdb_bitmap32_t flags);

/* ------------------------------------------------------------------------
 * Destroy loader
 * ------------------------------------------------------------------------
 */
void nowdb_loader_destroy(nowdb_loader_t *ldr);

/* ------------------------------------------------------------------------
 * Run the loader
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_loader_run(nowdb_loader_t *ldr);

/* ------------------------------------------------------------------------
 * Standard csv field callbacks for context and vertex
 * ------------------------------------------------------------------------
 */
void nowdb_csv_field_context(void *data, size_t len, void *rsc);
void nowdb_csv_field_vertex(void *data, size_t len, void *rsc);

/* ------------------------------------------------------------------------
 * Standard csv row callback
 * ------------------------------------------------------------------------
 */
void nowdb_csv_row(int c, void *rsc);
#endif

