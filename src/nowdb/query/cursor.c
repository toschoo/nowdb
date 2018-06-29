/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Cursor: DQL executor
 * ========================================================================
 */
#include <nowdb/reader/reader.h>
#include <nowdb/query/cursor.h>

/* ------------------------------------------------------------------------
 * Macro for the frequent error "invalid plan"
 * ------------------------------------------------------------------------
 */
#define INVALIDPLAN(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s)

static char *OBJECT = "cursor";

/* ------------------------------------------------------------------------
 * Init Reader 
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t initReader(nowdb_scope_t *scope,
                                     nowdb_cursor_t  *cur,
                                     int              idx,
                                     nowdb_plan_t   *plan)  {
	nowdb_err_t      err;
	nowdb_store_t *store;
	nowdb_context_t *ctx;
	nowdb_plan_idx_t *pidx;

	/* target is vertex */
	if (plan->helper == NOWDB_AST_VERTEX) {
		cur->recsize = 32;
		store = &scope->vertices;

	/* target is a context */
	} else {
		cur->recsize = 64;
		err = nowdb_scope_getContext(scope, plan->name, &ctx);
		if (err != NOWDB_OK) return err;
		store = &ctx->store;
	}

	/* we still need to get the period from the filter */
	err = nowdb_store_getFiles(store, &cur->stf.files,
		                          NOWDB_TIME_DAWN,
	                                  NOWDB_TIME_DUSK);
	if (err != NOWDB_OK) return err;

	/* create an index search reader */
	if (plan->stype == NOWDB_READER_SEARCH_) {
		pidx = plan->load;
		err = nowdb_reader_search(cur->rdrs+idx,
		                         &cur->stf.files,
		                          pidx->idx,
		                          pidx->keys,NULL);
		free(pidx->keys); free(pidx);
	
	/* create a fullscan reader */
	} else {
		err = nowdb_reader_fullscan(cur->rdrs+idx,
		                    &cur->stf.files, NULL);
	}
	if (err != NOWDB_OK) {
		nowdb_store_destroyFiles(store, &cur->stf.files);
		return err;
	}

	/* remember where the files came from */
	cur->stf.store = store;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create new cursor
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_new(nowdb_scope_t  *scope,
                             ts_algo_list_t  *plan,
                             nowdb_cursor_t  **cur) {
	int i=0;
	ts_algo_list_node_t *runner;
	nowdb_plan_t *stp;
	nowdb_err_t   err;

	/* point to head of plan */
	runner=plan->head;
	stp = runner->cont;

	/* we expect a summary node */
	if (stp->ntype != NOWDB_PLAN_SUMMARY) 
		INVALIDPLAN("no summary in plan");

	/* allocate the cursor */
	*cur = calloc(1, sizeof(nowdb_cursor_t));
	if (*cur == NULL) 
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                 "allocating cusor");

	/*
	fprintf(stderr, "[CURSOR] Targets: %d\n", stp->helper);
	*/

	/* allocate the readers */
	(*cur)->rdrs = calloc(stp->helper, sizeof(nowdb_reader_t));
	if ((*cur)->rdrs == NULL) {
		free(*cur); *cur = NULL;
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                               "allocating readers");
	}
	(*cur)->numr = stp->helper;

	/* we should check how many readers are on the same store 
	 * and create a list for that store!!! */
	(*cur)->stf.store = NULL;
	ts_algo_list_init(&(*cur)->stf.files);

	/* now pass on to the readers */
	runner = runner->nxt;
	err = initReader(scope, *cur, i, runner->cont);
	if (err != NOWDB_OK) {
		free((*cur)->rdrs); (*cur)->rdrs = NULL;
		free(*cur); *cur = NULL;
		return err;
	}

	/* pass on to the filter (in fact, there should/may be,
	 * one filter per reader */
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

/* ------------------------------------------------------------------------
 * Destroy cursor
 * ------------------------------------------------------------------------
 */
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

/* ------------------------------------------------------------------------
 * Open cursor
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_open(nowdb_cursor_t *cur) {
	nowdb_err_t err = NOWDB_OK;

	cur->off = 0;

	/* initialise readers */
	for (int i=0; i<cur->numr; i++) {
		err = nowdb_reader_move(cur->rdrs[0]);
		if (err != NOWDB_OK) break;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Check whether this position is "in"
 * ------------------------------------------------------------------------
 */
static inline char checkpos(nowdb_reader_t *r, uint32_t pos) {
	uint64_t k = 1;
	if (r->cont == NULL) return 1;
	if (pos < 64) {
		k <<= pos;
		if (r->cont[0] & k) return 1;
	} else  {
		k <<= (pos-64);
		if (r->cont[1] &k) return 1;
	}
	return 0;
	
}

/* ------------------------------------------------------------------------
 * Single reader fetch (no iterator!)
 * ------------------------------------------------------------------------
 */
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
		/* we have reached the end of the current page */
		if (cur->off >= NOWDB_IDX_PAGE) {
			err = nowdb_reader_move(cur->rdrs[0]);
			if (err != NOWDB_OK) return err;
			src = nowdb_reader_page(cur->rdrs[0]);
			cur->off = 0;
		}

		/* we hit the nullrecord and pass on to the next page */
		if (memcmp(src+cur->off, nowdb_nullrec, recsz) == 0) {
			cur->off = NOWDB_IDX_PAGE; continue;
		}

		/* check content */
		if (!checkpos(cur->rdrs[0], cur->off/recsz)) {
			cur->off += recsz; continue;
		}

		/* apply filter */
		if (filter != NULL &&
		    !nowdb_filter_eval(filter, src+cur->off)) {
			cur->off += recsz; continue;
		}

		/* copy the record to the output buffer */
		memcpy(buf+x, src+cur->off, recsz);

		/* increment buf index and reader offset */
		x += recsz; cur->off += recsz;
	}
	*osz = x;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Fetch
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_fetch(nowdb_cursor_t   *cur,
                              char *buf, uint32_t sz,
                                       uint32_t *osz)
{
	if (cur->numr == 1) return simplefetch(cur, buf, sz, osz);
	return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
	             "only simple fetch is implemented :-(\n");
}

/* ------------------------------------------------------------------------
 * Rewind
 * TODO
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_rewind(nowdb_cursor_t *cur);
