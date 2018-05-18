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

	if (plan->target == NOWDB_AST_VERTEX) {
		cur->recsize = 32;
		store = &scope->vertices;
	} else {
		cur->recsize = 64;
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

	/*
	fprintf(stderr, "[CURSOR] Targets: %d\n", stp->target);
	*/

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

	runner = runner->nxt;
	if (runner == NULL) return NOWDB_OK;
	stp = runner->cont;

	/* this should be sent per reader and added
	 * to the specific reader... */
	if (stp->ntype == NOWDB_PLAN_FILTER) {
		(*cur)->rdrs[0]->filter = stp->load;
	}
	return NOWDB_OK;
}

void nowdb_cursor_destroy(nowdb_cursor_t *cur) {
	if (cur->rdrs != NULL) {
		for(int i=0; i<cur->numr; i++) {
			if (cur->rdrs[i]->filter != NULL) {
				nowdb_filter_destroy(cur->rdrs[i]->filter);
				free(cur->rdrs[i]->filter);
				cur->rdrs[i]->filter = NULL;
			}
			nowdb_reader_destroy(cur->rdrs[i]);
			free(cur->rdrs[i]); cur->rdrs[i] = NULL;
		}
		free(cur->rdrs); cur->rdrs = NULL;
	}
	if (cur->stf.store != NULL) {
		nowdb_store_destroyFiles(cur->stf.store, &cur->stf.files);
		cur->stf.store = NULL;
	}
}

nowdb_err_t nowdb_cursor_open(nowdb_cursor_t *cur) {
	nowdb_err_t err = NOWDB_OK;

	cur->off = 0;

	for (int i=0; i<cur->numr;i++) {
		err = nowdb_reader_move(cur->rdrs[0]);
		if (err != NOWDB_OK) break;
	}
	return err;
}

static inline nowdb_err_t simplefetch(nowdb_cursor_t *cur,
                                   char *buf, uint32_t sz,
                                            uint32_t *osz) 
{
	nowdb_err_t err;
	uint32_t x = 0;
	uint32_t recsz = cur->rdrs[0]->recsize;
	nowdb_filter_t *filter = cur->rdrs[0]->filter;
	char *src = nowdb_reader_page(cur->rdrs[0]);

	*osz = 0;
	for(uint32_t i=0;i<sz;i+=recsz) {
		if (cur->off >= NOWDB_IDX_PAGE) {
			err = nowdb_reader_move(cur->rdrs[0]);
			if (err != NOWDB_OK) return err;
			src = nowdb_reader_page(cur->rdrs[0]);
			cur->off = 0;
		}
		if (memcmp(src+cur->off, nowdb_nullrec, recsz) == 0) {
			cur->off = NOWDB_IDX_PAGE; continue;
		}
		if (filter != NULL &&
		    !nowdb_filter_eval(filter, src+cur->off)) {
			cur->off += recsz; continue;
		}
		memcpy(buf+x, src+cur->off, recsz);
		x += recsz; cur->off += recsz; *osz += recsz;
	}
	return NOWDB_OK;
}

nowdb_err_t nowdb_cursor_fetch(nowdb_cursor_t   *cur,
                              char *buf, uint32_t sz,
                                       uint32_t *osz)
{
	if (cur->numr == 1) return simplefetch(cur, buf, sz, osz);
	return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
	             "only simple fetch is implemented :-(\n");
}

nowdb_err_t nowdb_cursor_rewind(nowdb_cursor_t *cur);


