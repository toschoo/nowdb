/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Cursor: DQL executor
 * ========================================================================
 */

#include <nowdb/reader/reader.h>
#include <nowdb/query/cursor.h>

#define INVALIDPLAN(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s)

static char *OBJECT = "cursor";

typedef struct {
	nowdb_store_t *store;
	ts_algo_list_t *list;
} storefile_t;

static inline nowdb_err_t initReader(nowdb_scope_t *scope,
                                     nowdb_cursor_t  *cur,
                                     int idx,
                                     nowdb_plan_t *plan)  {
	nowdb_err_t      err;
	nowdb_store_t *store;
	nowdb_context_t *ctx;

	if (plan->stype == NOWDB_AST_VERTEX) store = &scope->vertices;
	else {
		err = nowdb_scope_getContext(scope, plan->name, &ctx);
		if (err != NOWDB_OK) return err;
		store = &ctx->store;
	}
	/* check period! */
	err = nowdb_store_getFiles(store, &cur->stf.files, NOWDB_TIME_DAWN,
	                                                   NOWDB_TIME_DUSK);
	if (err != NOWDB_OK) return err;
	
	err = nowdb_reader_fullscan(cur->rdrs+idx, &cur->stf.files, NULL);
	if (err != NOWDB_OK) {
		nowdb_store_destroyFiles(store, &cur->stf.files);
		return err;
	}
	cur->stf.store = store;
	return NOWDB_OK;
}

nowdb_err_t nowdb_cursor_new(nowdb_scope_t  *scope,
                             ts_algo_list_t  *plan,
                             nowdb_cursor_t **cur) {
	int i=0;
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

	/* we should check how many readers are on the same store 
	 * and create a list for that store!!! */
	(*cur)->stf.store = NULL;
	ts_algo_list_init(&(*cur)->stf.files);

	runner = runner->nxt;
	err = initReader(scope, *cur, i, runner->cont);
	if (err != NOWDB_OK) {
		free((*cur)->rdrs); (*cur)->rdrs = NULL;
		free(*cur); *cur = NULL;
		return err;
	}
	
	return NOWDB_OK;
}

void nowdb_cursor_destroy(nowdb_cursor_t *cur) {
	if (cur->rdrs != NULL) {
		for(int i=0; i<cur->numr; i++) {
			nowdb_reader_destroy(cur->rdrs[i]);
			cur->rdrs[i] = NULL;
		}
		free(cur->rdrs); cur->rdrs = NULL;
	}
	if (cur->stf.store != NULL) {
		nowdb_store_destroyFiles(cur->stf.store, &cur->stf.files);
		cur->stf.store = NULL;
	}
}

nowdb_err_t nowdb_cursor_fetch(nowdb_cursor_t   *cur,
                              char *buf, uint32_t sz,
                                      uint32_t *osz);

nowdb_err_t nowdb_cursor_rewind(nowdb_cursor_t *cur);


