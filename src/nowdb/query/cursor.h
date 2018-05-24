/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Cursor: DQL executor
 * ========================================================================
 */
#ifndef nowdb_cursor_decl
#define nowdb_cursor_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/scope/scope.h>
#include <nowdb/reader/reader.h>
#include <nowdb/query/plan.h>

/* ------------------------------------------------------------------------
 * Pair of (store, files obtained from that store)
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_store_t *store;
	ts_algo_list_t files;
} nowdb_storefile_t;

/* ------------------------------------------------------------------------
 * Cursor 
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t         numr; /* number of readers             */
	nowdb_reader_t **rdrs; /* array of pointers to readers  */
	nowdb_storefile_t stf; /* should be a list of stf! */
	uint32_t          off; /* offset in the current reader  */
	uint32_t      recsize; /* record size                   */
	/* iterator ?               */
	/* projection !             */
	/* grouping, ordering, etc. */
	/* sub-queries?             */
} nowdb_cursor_t;

/* ------------------------------------------------------------------------
 * Create a new cursor 
 * -------------------
 * Expecting a scope and an execution plan.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_new(nowdb_scope_t  *scope,
                             ts_algo_list_t  *plan,
                             nowdb_cursor_t **cur);

/* ------------------------------------------------------------------------
 * Destroy a new cursor 
 * ------------------------------------------------------------------------
 */
void nowdb_cursor_destroy(nowdb_cursor_t *cur);

/* ------------------------------------------------------------------------
 * Open cursor 
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_open(nowdb_cursor_t *cur);

/* ------------------------------------------------------------------------
 * Fetch from cursor
 * -----------------
 * Copy records corresponding to the query into the user-provided buffer
 * of size sz (in bytes). The number of bytes written to the buffer is
 * communicated back through osz.
 * When the cursor is exhausted, fetch will return the error 'eof'.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_fetch(nowdb_cursor_t   *cur,
                              char *buf, uint32_t sz,
                                      uint32_t *osz);

/* ------------------------------------------------------------------------
 * Reset cursor to start position
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_rewind(nowdb_cursor_t *cur);

#endif
