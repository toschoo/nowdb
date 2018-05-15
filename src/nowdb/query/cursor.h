/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Cursor: DQL executor
 * TODO:
 * - better to ask what not to do...
 * ========================================================================
 */
#ifndef nowdb_cursor_decl
#define nowdb_cursor_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/scope/scope.h>
#include <nowdb/reader/reader.h>
#include <nowdb/query/plan.h>

typedef struct {
	uint32_t         numr;
	nowdb_reader_t **rdrs;
	uint32_t         disc;

	/* projection !             */
	/* grouping, ordering, etc. */
	/* sub-queries?             */
} nowdb_cursor_t;

nowdb_err_t nowdb_cursor_new(nowdb_scope_t  *scope,
                             ts_algo_list_t  *plan,
                             nowdb_cursor_t **cur);

nowdb_err_t nowdb_cursor_destroy(nowdb_cursor_t *cur);

nowdb_err_t nowdb_cursor_fetch(nowdb_cursor_t   *cur,
                              char *buf, uint32_t sz,
                                      uint32_t *osz);

nowdb_err_t nowdb_cursor_rewind(nowdb_cursor_t *cur);

#endif
