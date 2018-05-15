/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Cursor: DQL executor
 * ========================================================================
 */

#include <nowdb/query/cursor.h>

#define INVALIDPLAN(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s)

static char *OBJECT = "cursor";

static inline nowdb_err_t initReader(nowdb_scope_t *scope,
                                     nowdb_cursor_t  *cur,
                                     int idx,
                                     nowdb_plan_t *plan)  {

	// if vertex, get files from vertex
	// if context, get context and get files from context
	// create reader

	return NOWDB_OK;
}

nowdb_err_t nowdb_cursor_new(nowdb_scope_t  *scope,
                             ts_algo_list_t  *plan,
                             nowdb_cursor_t **cur) {
	int i;
	ts_algo_list_node_t *runner;
	nowdb_plan_t *stp;
	nowdb_err_t   err;

	runner=plan->head;
	stp = runner->cont;

	if (stp->ntype != NOWDB_PLAN_SUMMARY) 
		INVALIDPLAN("no summary in plan");

	*cur = calloc(1, sizeof(nowdb_cursor_t));
	if (*cur == NULL) 
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                 "allocating cusor");

	(*cur)->rdrs = calloc(stp->target, sizeof(nowdb_reader_t));
	if ((*cur)->rdrs == NULL) {
		free(*cur); *cur = NULL;
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                               "allocating readers");
	}
	(*cur)->numr = stp->target;

	runner = runner->nxt;
	err = initReader(scope, *cur, i, runner->cont);
	if (err != NOWDB_OK) return err;
	
	return NOWDB_OK;
}

nowdb_err_t nowdb_cursor_destroy(nowdb_cursor_t *cur);

nowdb_err_t nowdb_cursor_fetch(nowdb_cursor_t   *cur,
                              char *buf, uint32_t sz,
                                      uint32_t *osz);

nowdb_err_t nowdb_cursor_rewind(nowdb_cursor_t *cur);


