/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Filter: generic data filter
 *         used to compare a field value with a given value
 * ========================================================================
 */
#ifndef nowdb_filter_decl
#define nowdb_filter_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>

#include <stdint.h>
#include <stdio.h>

/* ------------------------------------------------------------------------
 * Node type
 * ------------------------------------------------------------------------
 */
#define NOWDB_FILTER_COMPARE 1
#define NOWDB_FILTER_BOOL    2

/* ------------------------------------------------------------------------
 * Compare Operations
 * ------------------------------------------------------------------------
 */
#define NOWDB_FILTER_EQ 1
#define NOWDB_FILTER_LE 2
#define NOWDB_FILTER_GE 3
#define NOWDB_FILTER_LT 4
#define NOWDB_FILTER_GT 5
#define NOWDB_FILTER_NE 6

/* ------------------------------------------------------------------------
 * Boolean Operations
 * ------------------------------------------------------------------------
 */
#define NOWDB_FILTER_TRUE  1
#define NOWDB_FILTER_FALSE 2
#define NOWDB_FILTER_JUST  3
#define NOWDB_FILTER_NOT   4
#define NOWDB_FILTER_AND   5
#define NOWDB_FILTER_OR    6

/* ------------------------------------------------------------------------
 * Filter node
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_filter_st {
	int        ntype; /* node type                              */
	int           op; /* operation (compare or boolean)         */
	uint32_t     off; /* offset of the field into the structure */
	uint32_t    size; /* size of the field                      */
	nowdb_type_t typ; /* type of the field                      */
	void        *val; /* value to compare with                  */
	char         own; /* value is to be destroyed               */
	struct nowdb_filter_st *left;  /* left kid                  */
	struct nowdb_filter_st *right; /* right kid                 */
} nowdb_filter_t;

/* ------------------------------------------------------------------------
 * Create a compare node
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_filter_newCompare(nowdb_filter_t **filter, int op,
                                        uint32_t off, uint32_t size,
                                        nowdb_type_t typ, void *val);

/* ------------------------------------------------------------------------
 * Filter owns the value
 * ------------------------------------------------------------------------
 */
void nowdb_filter_own(nowdb_filter_t *filter);

/* ------------------------------------------------------------------------
 * Create a boolean node
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_filter_newBool(nowdb_filter_t **filter, int op);

/* ------------------------------------------------------------------------
 * Destroy node
 * ------------------------------------------------------------------------
 */
void nowdb_filter_destroy(nowdb_filter_t *filter);

/* ------------------------------------------------------------------------
 * Evaluate filter
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_filter_eval(nowdb_filter_t *filter, void *data);

/* ------------------------------------------------------------------------
 * Copy filter
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_filter_copy(nowdb_filter_t *src, nowdb_filter_t *trg);

/* ------------------------------------------------------------------------
 * Extract period from filter
 * ------------------------------------------------------------------------
 */
void nowdb_filter_period(nowdb_filter_t *filter,
                         nowdb_time_t   *start,
                         nowdb_time_t   *end);

/* ------------------------------------------------------------------------
 * Print filter to stream
 * ------------------------------------------------------------------------
 */
void nowdb_filter_show(nowdb_filter_t *filter, FILE *stream);
#endif

