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

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

static char *OBJECT = "cursor";

/* ------------------------------------------------------------------------
 * Helper: Index contains model identifier
 * ------------------------------------------------------------------------
 */
static inline char hasId(nowdb_plan_idx_t *pidx) {
	nowdb_index_keys_t *keys;
	keys = nowdb_index_getResource(pidx->idx);
	for(int i=0; i<keys->sz; i++) {
		if (keys->off[i] == NOWDB_OFF_EDGE) return 1;
		if (keys->off[i] == NOWDB_OFF_VERTEX) return 1;
	}
	return 0;
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
 * Create merge reader
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t createMerge(nowdb_cursor_t    *cur,
                                      int               type,
                                      nowdb_plan_idx_t *pidx) {
	nowdb_err_t err;
	nowdb_reader_t *buf;
	nowdb_reader_t *range;
	ts_algo_list_t pfiles;

	ts_algo_list_init(&pfiles);
	err = getPending(&cur->stf.files, &pfiles);
	if (err != NOWDB_OK) {
		ts_algo_list_destroy(&pfiles);
		return err;
	}
	
	switch(type) {
	case NOWDB_PLAN_FRANGE_:
		err = nowdb_reader_bufidx(&buf, &pfiles, pidx->idx, NULL,
		                               NOWDB_ORD_ASC, NULL, NULL);
		break;
	case NOWDB_PLAN_KRANGE_:
	case NOWDB_PLAN_CRANGE_:
	default:
		ts_algo_list_destroy(&pfiles);
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
		                                    "KRANGE or CRANGE");
	}
	if (err != NOWDB_OK) {
		ts_algo_list_destroy(&pfiles);
		return err;
	}

	switch(type) {
	case NOWDB_PLAN_FRANGE_:
		err = nowdb_reader_frange(&range, &cur->stf.files, pidx->idx,
		                                            NULL, NULL, NULL);
		break;
	case NOWDB_PLAN_KRANGE_:
	case NOWDB_PLAN_CRANGE_:
	default:
		ts_algo_list_destroy(&pfiles);
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
		                                    "KRANGE or CRANGE");
	}
	if (err != NOWDB_OK) {
		nowdb_reader_destroy(buf);
		return err;
	}

	err = nowdb_reader_merge(cur->rdrs, 2, range, buf);
	if (err != NOWDB_OK) {
		nowdb_reader_destroy(buf);
		nowdb_reader_destroy(range);
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Init Reader 
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t initReader(nowdb_scope_t *scope,
                                     nowdb_cursor_t  *cur,
                                     int              idx,
                                     nowdb_time_t   start,
                                     nowdb_time_t     end,
                                     nowdb_plan_t  *rplan) {
	nowdb_err_t      err;
	nowdb_store_t *store;
	nowdb_context_t *ctx;
	nowdb_plan_idx_t *pidx;

	/* target is vertex */
	if (rplan->helper == NOWDB_AST_VERTEX ||
	    rplan->helper == NOWDB_AST_TYPE) {
		cur->recsize = 32;
		store = &scope->vertices;

	/* target is a context */
	} else {
		cur->recsize = 64;
		err = nowdb_scope_getContext(scope, rplan->name, &ctx);
		if (err != NOWDB_OK) return err;
		store = &ctx->store;
	}

	cur->hasid = TRUE;

	/* get files */
	err = nowdb_store_getFiles(store, &cur->stf.files, start, end);
	if (err != NOWDB_OK) return err;

	/* create an index search reader */
	if (rplan->stype == NOWDB_PLAN_SEARCH_) {
		fprintf(stderr, "SEARCH\n");
		pidx = rplan->load;
		err = nowdb_reader_search(cur->rdrs+idx,
		                         &cur->stf.files,
		                          pidx->idx,
		                          pidx->keys,NULL);

	} else if (rplan->stype == NOWDB_PLAN_FRANGE_) {
		fprintf(stderr, "FRANGE\n");
		pidx = rplan->load;
		err = createMerge(cur, rplan->stype, pidx);

	} else if (rplan->stype == NOWDB_PLAN_KRANGE_) {
		fprintf(stderr, "KRANGE\n");
		pidx = rplan->load;
		cur->hasid = hasId(pidx);
		err = nowdb_reader_krange(cur->rdrs+idx,
		                        &cur->stf.files,
		                        pidx->idx, NULL,
		                        NULL, NULL); /* range ! */

	} else if (rplan->stype == NOWDB_PLAN_CRANGE_) {
		fprintf(stderr, "CRANGE\n");
		pidx = rplan->load;
		cur->hasid = hasId(pidx);
		err = nowdb_reader_crange(cur->rdrs+idx,
		                        &cur->stf.files,
		                        pidx->idx, NULL,
		                        NULL, NULL); /* range ! */
	
	/* create a fullscan reader */
	} else {
		err = nowdb_reader_fullscan(cur->rdrs+idx,
		                    &cur->stf.files, NULL);
	}
	if (err != NOWDB_OK) {
		nowdb_store_destroyFiles(store, &cur->stf.files);
		return err;
	}

	nowdb_reader_setPeriod(cur->rdrs[idx], start, end);

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
	nowdb_plan_t *stp=NULL, *rstp=NULL;
	nowdb_err_t   err;
	uint32_t rn=0;
	nowdb_time_t start = NOWDB_TIME_DAWN;
	nowdb_time_t end = NOWDB_TIME_DUSK;
	nowdb_plan_idx_t *pidx=NULL;

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
	if (rstp->stype == NOWDB_PLAN_SEARCH_) rn++;
	//    rstp->stype == NOWDB_PLAN_FRANGE_) rn++;

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
	if (rn > 1) {
		if (rstp->stype == NOWDB_PLAN_SEARCH_) {
			(*cur)->disc = NOWDB_ITER_SEQ;
		} else if (rstp->stype == NOWDB_PLAN_FRANGE_) {
			(*cur)->disc = NOWDB_ITER_MERGE;
			pidx = rstp->load;
			(*cur)->cmp = nowdb_index_getCompare(pidx->idx);
			(*cur)->k  = nowdb_index_getResource(pidx->idx);
			(*cur)->key = NULL;
			(*cur)->tmp = calloc((*cur)->k->sz, 8);
			if ((*cur)->tmp == NULL) {
				NOMEM("allocating keys");
				return err;
			}
		}
	}

	/* we should check how many readers are on the same store 
	 * and create a list for that store!!! */
	(*cur)->stf.store = NULL;
	ts_algo_list_init(&(*cur)->stf.files);
	ts_algo_list_init(&(*cur)->stf.pending);

	/* pass on to the filter (in fact, there should/may be,
	 * one filter per reader */
	runner = runner->nxt;
	if (runner!=NULL) {
		stp = runner->cont;
		if (stp->ntype == NOWDB_PLAN_FILTER) {
			(*cur)->filter = stp->load;
			nowdb_filter_period(stp->load, &start, &end);
			runner = runner->nxt;
			stp->load = NULL;
		}
	}

	err = initReader(scope, *cur, i, start, end, rstp);
	if (err != NOWDB_OK) {
		free((*cur)->rdrs); (*cur)->rdrs = NULL;
		free(*cur); *cur = NULL;
		return err;
	}
	i++;

	if (rstp->stype == NOWDB_PLAN_SEARCH_) { // ||
	//     rstp->stype == NOWDB_PLAN_FRANGE_) {
		err = getPending(&(*cur)->stf.files,
		                 &(*cur)->stf.pending);
		if (err != NOWDB_OK) {
			ts_algo_list_destroy(&(*cur)->stf.pending);
			free((*cur)->rdrs); (*cur)->rdrs = NULL;
			free(*cur); *cur = NULL;
		}
		if (rstp->stype == NOWDB_PLAN_SEARCH_) {
			err = nowdb_reader_fullscan((*cur)->rdrs+i,
			               &(*cur)->stf.pending, NULL);
		} else {
			err = nowdb_reader_bufidx((*cur)->rdrs+i,
			                    &(*cur)->stf.pending,
			                         pidx->idx, NULL,
			                           NOWDB_ORD_ASC,
			                             NULL, NULL);
		}
		if (err != NOWDB_OK) {
			ts_algo_list_destroy(&(*cur)->stf.pending);
			free((*cur)->rdrs); (*cur)->rdrs = NULL;
			free(*cur); *cur = NULL;
		}
	}
	if ((*cur)->filter != NULL) {
		for(i=0;i<rn;i++) {
			(*cur)->rdrs[i]->filter = (*cur)->filter;
		}
	}

	/* In case of range:
	 * create a bufscanner */

	/* pass on to projection or order by or group by */
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
	if (cur->key != NULL) {
		free(cur->key); cur->key = NULL;
	}
	if (cur->tmp != NULL) {
		free(cur->tmp); cur->tmp = NULL;
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
	uint32_t r = cur->cur;
	uint32_t recsz = cur->rdrs[r]->recsize;
	nowdb_filter_t *filter = cur->rdrs[r]->filter;
	char *src = nowdb_reader_page(cur->rdrs[r]);
	char complete=0, cc=0;
	uint32_t mx;

	if (src == NULL) return nowdb_err_get(nowdb_err_eof,
		                        FALSE, OBJECT, NULL);
	mx = cur->rdrs[r]->type != NOWDB_READER_KRANGE &&
	     cur->rdrs[r]->type != NOWDB_READER_CRANGE?
	     NOWDB_IDX_PAGE:recsz;

	while(*osz < sz) {
		// fprintf(stderr, "fetch %u %u\n", cur->off, *osz);
		/* we have reached the end of the current page */
		if (cur->off >= mx) {
			// fprintf(stderr, "move %u %u\n", cur->off, *osz);
			err = nowdb_reader_move(cur->rdrs[r]);
			if (err != NOWDB_OK) return err;
			src = nowdb_reader_page(cur->rdrs[r]);
			cur->off = 0;
		}

		/* we hit the nullrecord and pass on to the next page */
		if (memcmp(src+cur->off, nowdb_nullrec, recsz) == 0) {
			cur->off = mx; continue;
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
		if (cur->row == NULL || !cur->hasid) {
			memcpy(buf+(*osz), src+cur->off, recsz);
			*osz += recsz; cur->off += recsz;
			(*count)++;

		/* project ... */
		} else {
			err = nowdb_row_project(cur->row,
			                        src+cur->off, recsz,
			                        buf, sz, osz, &cc,
			                        &complete);
			if (err != NOWDB_OK) return err;
			if (complete) {
				cur->off+=recsz;
				(*count)+=cc;
			}
			if (cc == 0) break;
		}
	}
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
			cur->cur++; cur->off=0;
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
 * Helper: find next key in reader
 * ------------------------------------------------------------------------
 */
static inline void findOff(nowdb_cursor_t *cur) {
	char *src = nowdb_reader_page(cur->rdrs[cur->cur]);
	for(int i=0; i<NOWDB_IDX_PAGE; i+=cur->recsize) {
		// compare cur->key with edge at idx
		if (cur->recsize == 64) {
			nowdb_index_grabEdgeKeys(cur->k,
			                src+i,cur->tmp);
		} else {
			nowdb_index_grabVertexKeys(cur->k,
			                  src+i,cur->tmp);
		}
		if (cur->cmp(cur->key, cur->tmp, cur->k)
		                      == BEET_CMP_EQUAL) {
			cur->off = i; break;
		}
	}
}

/* ------------------------------------------------------------------------
 * Helper: find next key
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t findNextKey(nowdb_cursor_t *cur) {
	nowdb_err_t err;
	char x;
	char *src;
	void *key=NULL;
	cur->cur = 0;
	cur->off = 0;
	for(int i=0; i<cur->numr; i++) {
		src = nowdb_reader_page(cur->rdrs[i]);
		if (src == NULL) continue;
		// if (cur->rdrs[i]->type != NOWDB_READER_BUF) continue; // DEBUGING
		if (cur->key != NULL) {
			x = cur->cmp(cur->rdrs[i]->key, cur->key, cur->k);
			/*
			fprintf(stderr, "comparing %u %lu and %lu: %d\n", i,
			                *(uint64_t*)(cur->rdrs[i]->key),
			                *(uint64_t*)(cur->key), x);
			*/
		}
		if (cur->key == NULL || x != BEET_CMP_LESS) {
			if (key == NULL) {
				key = cur->rdrs[i]->key;
				cur->cur = i;
			} else if (cur->cmp(cur->rdrs[i]->key, key, cur->k)
			                                  == BEET_CMP_LESS) {
				key = cur->rdrs[i]->key;
				cur->cur = i;
			}
		}
	}
	if (key == NULL) return nowdb_err_get(nowdb_err_eof,
	                                FALSE, OBJECT, NULL);
	if (cur->key == NULL) {
		cur->key = calloc(cur->k->sz, 8);
		if (cur->key == NULL) {
			NOMEM("allocating keys");
			return err;
		}
	}
	memcpy(cur->key, key, cur->k->sz*8);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Multi reader: merge fetch
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t mergefetch(nowdb_cursor_t *cur,
                                  char *buf, uint32_t sz,
                                           uint32_t *osz,
                                           uint32_t *cnt)
{
	nowdb_err_t err;

	// find minimal key greater than cur->key
	err = findNextKey(cur);
	if (err != NOWDB_OK) return err;

	/*
	fprintf(stderr, "we now have the keys %lu and %lu\n",
	                ((uint64_t*)(cur->rdrs[cur->cur]->key))[0],
	                ((uint64_t*)(cur->key))[0]);
	*/

	findOff(cur);

	/*
	fprintf(stderr, "%u.%u: %lu\n", cur->cur, cur->off,
	                          ((uint64_t*)cur->key)[0]);
	*/

	// exhaust that key on that reader
	return simplefetch(cur, buf, sz, osz, cnt);
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
	*osz = 0;
	*cnt = 0;

	if (cur->numr == 1) return simplefetch(cur, buf, sz, osz, cnt);
	switch(cur->disc) {
	case NOWDB_ITER_SEQ: return seqfetch(cur, buf, sz, osz, cnt);
	case NOWDB_ITER_MERGE: return mergefetch(cur, buf, sz, osz, cnt);

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
