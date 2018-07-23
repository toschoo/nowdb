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
	if (plan->helper == NOWDB_AST_VERTEX ||
	    plan->helper == NOWDB_AST_TYPE) {
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

	} else if (plan->stype == NOWDB_READER_RANGE_) {
		pidx = plan->load;
		err = nowdb_reader_range(cur->rdrs+idx,
		                       &cur->stf.files,
		                        pidx->idx, NULL,
		                        NULL, NULL); /* range ! */
		free(pidx); /* the index should be destroyed by plan */
	
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
 * Helper: filter pending files from list of files
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getPending(ts_algo_list_t *files,
                                     ts_algo_list_t *pending) {
	ts_algo_list_node_t *runner;
	nowdb_file_t *file;

	for(runner=files->head;runner!=NULL;runner=runner->nxt) {
		file = runner->cont;
		if (!(file->ctrl & NOWDB_FILE_SORT)) {
			if (ts_algo_list_append(pending, file) != TS_ALGO_OK)
			{
				return nowdb_err_get(nowdb_err_no_mem,
				        FALSE, OBJECT, "list.append");
			}
		}
	}
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
	nowdb_plan_t *stp, *rstp;
	nowdb_err_t   err;
	uint32_t rn=0;

	/* point to head of plan */
	runner=plan->head;
	stp = runner->cont;

	/* we expect a summary node */
	if (stp->ntype != NOWDB_PLAN_SUMMARY) 
		INVALIDPLAN("no summary in plan");

	rn = stp->helper;

	/* get first reader node (this should be a loop!) */
	runner = runner->nxt;
	if (runner == NULL) INVALIDPLAN("no reader");
	rstp = runner->cont;

	/* we expect a reader node */
	if (rstp->ntype != NOWDB_PLAN_READER) {
		INVALIDPLAN("reader expected in plan");
	}
	if (rstp->stype == NOWDB_READER_SEARCH_) rn++;

	/* allocate the cursor */
	*cur = calloc(1, sizeof(nowdb_cursor_t));
	if (*cur == NULL) 
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                 "allocating cusor");

	/*
	fprintf(stderr, "[CURSOR] Targets: %d\n", stp->helper);
	*/

	/* allocate the readers */
	(*cur)->rdrs = calloc(rn, sizeof(nowdb_reader_t));
	if ((*cur)->rdrs == NULL) {
		free(*cur); *cur = NULL;
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                               "allocating readers");
	}
	(*cur)->numr = rn;
	if (rn > 1 && rstp->stype == NOWDB_READER_SEARCH_) {
		(*cur)->disc = NOWDB_ITER_SEQ;
	}

	/* we should check how many readers are on the same store 
	 * and create a list for that store!!! */
	(*cur)->stf.store = NULL;
	ts_algo_list_init(&(*cur)->stf.files);
	ts_algo_list_init(&(*cur)->stf.pending);

	err = initReader(scope, *cur, i, rstp);
	if (err != NOWDB_OK) {
		free((*cur)->rdrs); (*cur)->rdrs = NULL;
		free(*cur); *cur = NULL;
		return err;
	}
	i++;

	if (rstp->stype == NOWDB_READER_SEARCH_) {
		err = getPending(&(*cur)->stf.files,
		                 &(*cur)->stf.pending);
		if (err != NOWDB_OK) {
			ts_algo_list_destroy(&(*cur)->stf.pending);
			free((*cur)->rdrs); (*cur)->rdrs = NULL;
			free(*cur); *cur = NULL;
		}
		err = nowdb_reader_fullscan((*cur)->rdrs+i,
		                &(*cur)->stf.pending, NULL);
		if (err != NOWDB_OK) {
			ts_algo_list_destroy(&(*cur)->stf.pending);
			free((*cur)->rdrs); (*cur)->rdrs = NULL;
			free(*cur); *cur = NULL;
		}
	}

	/* In case of range:
	 * create a bufscanner */

	/* pass on to the filter (in fact, there should/may be,
	 * one filter per reader */
	runner = runner->nxt;
	if (runner!=NULL) {
		stp = runner->cont;

		/* this should be sent per reader and added
		 * to the corresponding reader... */
		if (stp->ntype == NOWDB_PLAN_FILTER) {
			(*cur)->filter = stp->load;
			for(i=0;i<rn;i++) {
				(*cur)->rdrs[i]->filter = stp->load;
			}
		}
	}

	/* pass on to projection or order by or group by */
	runner = runner->nxt;
	if (runner == NULL) return NOWDB_OK;
	stp = runner->cont;

	if (stp->ntype == NOWDB_PLAN_ORDERING) {
		runner = runner->nxt;
		if (runner == NULL) return NOWDB_OK;
		stp = runner->cont;
	}

	/* group by */
	if (stp->ntype == NOWDB_PLAN_GROUPING) {

		/* what do we need for grouping? */

		runner = runner->nxt;
		if (runner == NULL) {
			if (stp->stype == NOWDB_PLAN_GROUPING) {
				nowdb_cursor_destroy(*cur); free(*cur);
				INVALIDPLAN("grouping without projection");	
			}
			return NOWDB_OK;
		}
		stp = runner->cont;
	}

	if (stp->ntype == NOWDB_PLAN_PROJECTION) {
		(*cur)->row = calloc(1, sizeof(nowdb_row_t));
		if ((*cur)->row == NULL) {
			nowdb_cursor_destroy(*cur); free(*cur);
			return err;
		}
		err = nowdb_row_init((*cur)->row, scope, stp->load);
		if (err != NOWDB_OK) {
			free((*cur)->row); (*cur)->row = NULL;
			nowdb_cursor_destroy(*cur); free(*cur);
			return err;
		}
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
			nowdb_reader_destroy(cur->rdrs[i]);
			free(cur->rdrs[i]); cur->rdrs[i] = NULL;
		}
		free(cur->rdrs); cur->rdrs = NULL;
	}
	if (cur->filter != NULL) {
		nowdb_filter_destroy(cur->filter);
		free(cur->filter);
		cur->filter = NULL;
	}
	if (cur->stf.store != NULL) {
		nowdb_store_destroyFiles(cur->stf.store, &cur->stf.files);
		ts_algo_list_destroy(&cur->stf.pending);
		cur->stf.store = NULL;
	}
	if (cur->row != NULL) {
		nowdb_row_destroy(cur->row);
		free(cur->row); cur->row = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Open cursor
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_open(nowdb_cursor_t *cur) {
	char x = 0;
	nowdb_err_t err = NOWDB_OK;

	cur->cur = 0;
	cur->off = 0;

	/* initialise readers */
	for (int i=0; i<cur->numr; i++) {
		err = nowdb_reader_move(cur->rdrs[i]);
		if (nowdb_err_contains(err, nowdb_err_eof)) {
			nowdb_err_release(err); continue;
		}
		if (err != NOWDB_OK) break;
		x=1;
	}
	if (x==0) return nowdb_err_get(nowdb_err_eof, FALSE, OBJECT, NULL);
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
                                            uint32_t *osz,
                                          uint32_t *count) 
{
	nowdb_err_t err;
	uint32_t x = 0;
	uint32_t r = cur->cur;
	uint32_t recsz = cur->rdrs[r]->recsize;
	nowdb_filter_t *filter = cur->rdrs[r]->filter;
	char *src = nowdb_reader_page(cur->rdrs[r]);
	char complete=0, cc=0;

	if (src == NULL) return nowdb_err_get(nowdb_err_eof,
		                        FALSE, OBJECT, NULL);

	*osz = 0;
	*count = 0;
	for(uint32_t i=0;i<sz;i+=recsz) {
		/* we have reached the end of the current page */
		if (cur->off >= NOWDB_IDX_PAGE) {
			err = nowdb_reader_move(cur->rdrs[r]);
			if (err != NOWDB_OK) return err;
			src = nowdb_reader_page(cur->rdrs[r]);
			cur->off = 0;
		}

		/* we hit the nullrecord and pass on to the next page */
		if (memcmp(src+cur->off, nowdb_nullrec, recsz) == 0) {
			cur->off = NOWDB_IDX_PAGE; continue;
		}

		/* check content
		 * -------------
		 * here's potential for imporvement:
		 * 1) we can immediately advance to the next marked record
		 * 2) if we have read all records, we can leave */
		if (!checkpos(cur->rdrs[r], cur->off/recsz)) {
			cur->off += recsz; continue;
		}

		/* apply filter */
		if (filter != NULL &&
		    !nowdb_filter_eval(filter, src+cur->off)) {
			cur->off += recsz; continue;
		}

		/* copy the record to the output buffer */
		if (cur->row == NULL) {
			memcpy(buf+x, src+cur->off, recsz);
			x += recsz; cur->off += recsz;
			(*count)++;

		/* project ... */
		} else {
			err = nowdb_row_project(cur->row,
			                        src+cur->off, recsz,
			                        buf, sz, &x, &cc,
			                        &complete);
			if (complete) {
				cur->off += recsz;
				(*count)+=cc;
			}
		}
	}
	*osz = x;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Multi reader: sequence fetch
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t seqfetch(nowdb_cursor_t *cur,
                                char *buf, uint32_t sz,
                                         uint32_t *osz,
                                         uint32_t *cnt) 
{
	nowdb_err_t err;

	for(;;) {
		err = simplefetch(cur, buf, sz, osz, cnt);
		if (err == NOWDB_OK) return NOWDB_OK;
		if (err->errcode == nowdb_err_eof) {
			cur->cur++;
			if (cur->cur == cur->numr) {
				cur->cur = 0; return err;
			}
			nowdb_err_release(err);
			continue;
		}
		if (err != NOWDB_OK) return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Fetch
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_fetch(nowdb_cursor_t   *cur,
                              char *buf, uint32_t sz,
                                       uint32_t *osz,
                                       uint32_t *cnt)
{
	if (cur->numr == 1) return simplefetch(cur, buf, sz, osz, cnt);
	switch(cur->disc) {
	case NOWDB_ITER_SEQ: return seqfetch(cur, buf, sz, osz, cnt);
	case NOWDB_ITER_MERGE:
	case NOWDB_ITER_JOIN:
	default:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
	              "only simple and seq fetch is implemented :-(\n");
	}
}

/* ------------------------------------------------------------------------
 * Rewind
 * TODO
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_rewind(nowdb_cursor_t *cur);
