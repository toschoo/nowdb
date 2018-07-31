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
 * Create sequence reader
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t createSeq(nowdb_cursor_t    *cur,
                                    int               type,
                                    nowdb_plan_idx_t *pidx) {
	nowdb_err_t err;
	nowdb_reader_t *full;
	nowdb_reader_t *search;

	err = getPending(&cur->stf.files, &cur->stf.pending);
	if (err != NOWDB_OK) {
		return err;
	}
	
	switch(type) {
	case NOWDB_PLAN_SEARCH_:
		err = nowdb_reader_fullscan(&full, &cur->stf.pending, NULL);
		break;
	default:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
		                                    "unknown seq type");
	}
	if (err != NOWDB_OK) return err;

	switch(type) {
	case NOWDB_PLAN_SEARCH_:
		err = nowdb_reader_search(&search, &cur->stf.files,
		                       pidx->idx, pidx->keys, NULL);
		break;
	default:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
		                                    "unknown seq type");
	}
	if (err != NOWDB_OK) {
		nowdb_reader_destroy(full);
		return err;
	}

	err = nowdb_reader_seq(&cur->rdr, 2, search, full);
	if (err != NOWDB_OK) {
		nowdb_reader_destroy(full);
		nowdb_reader_destroy(search);
		return err;
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

	err = getPending(&cur->stf.files, &cur->stf.pending);
	if (err != NOWDB_OK) {
		return err;
	}
	
	switch(type) {
	case NOWDB_PLAN_FRANGE_:
		err = nowdb_reader_bufidx(&buf, &cur->stf.pending,
		                                  pidx->idx, NULL,
		                        NOWDB_ORD_ASC, NULL, NULL);
		break;

	case NOWDB_PLAN_KRANGE_:
		err = nowdb_reader_bkrange(&buf, &cur->stf.pending,
		                                   pidx->idx, NULL,
		                         NOWDB_ORD_ASC, NULL, NULL);
		break;

	case NOWDB_PLAN_CRANGE_:
	default:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
		                                    "KRANGE or CRANGE");
	}
	if (err != NOWDB_OK) return err;

	switch(type) {
	case NOWDB_PLAN_FRANGE_:
		err = nowdb_reader_frange(&range, &cur->stf.files, pidx->idx,
		                                            NULL, NULL, NULL);
		break;

	case NOWDB_PLAN_KRANGE_:
		err = nowdb_reader_krange(&range, &cur->stf.files, pidx->idx,
		                                            NULL, NULL, NULL);
		if (range->ikeys != NULL) {
			cur->tmp = calloc(1, cur->recsize);
			if (cur->tmp == NULL) {
				NOMEM("allocating temporary buffer");
				nowdb_reader_destroy(buf);
				nowdb_reader_destroy(range);
				return err;
			}
		}
		break;

	case NOWDB_PLAN_CRANGE_:
	default:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
		                                    "KRANGE or CRANGE");
	}
	if (err != NOWDB_OK) {
		nowdb_reader_destroy(buf);
		return err;
	}

	err = nowdb_reader_merge(&cur->rdr, 2, range, buf);
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
		err = createSeq(cur, rplan->stype, pidx);

	} else if (rplan->stype == NOWDB_PLAN_FRANGE_) {
		fprintf(stderr, "FRANGE\n");
		pidx = rplan->load;
		err = createMerge(cur, rplan->stype, pidx);

	} else if (rplan->stype == NOWDB_PLAN_KRANGE_) {
		fprintf(stderr, "KRANGE\n");
		pidx = rplan->load;
		cur->hasid = hasId(pidx);
		err = createMerge(cur, rplan->stype, pidx);

	} else if (rplan->stype == NOWDB_PLAN_CRANGE_) {
		fprintf(stderr, "CRANGE\n");
		pidx = rplan->load;
		cur->hasid = hasId(pidx);
		err = nowdb_reader_crange(&cur->rdr,
		                          &cur->stf.files,
		                          pidx->idx, NULL,
		                          NULL, NULL); // range !
	
	/* create a fullscan reader */
	} else {
		err = nowdb_reader_fullscan(&cur->rdr,
		                &cur->stf.files, NULL);
	}
	if (err != NOWDB_OK) {
		nowdb_store_destroyFiles(store, &cur->stf.files);
		return err;
	}

	nowdb_reader_setPeriod(cur->rdr, start, end);

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
	ts_algo_list_node_t *runner;
	nowdb_plan_t *stp=NULL, *rstp=NULL;
	nowdb_err_t   err;
	nowdb_time_t start = NOWDB_TIME_DAWN;
	nowdb_time_t end = NOWDB_TIME_DUSK;

	/* point to head of plan */
	runner=plan->head;
	stp = runner->cont;

	/* we expect a summary node */
	if (stp->ntype != NOWDB_PLAN_SUMMARY) 
		INVALIDPLAN("no summary in plan");

	/* get reader node */
	runner = runner->nxt;
	if (runner == NULL) INVALIDPLAN("no reader");
	rstp = runner->cont;

	/* we expect a reader node */
	if (rstp->ntype != NOWDB_PLAN_READER) {
		INVALIDPLAN("reader expected in plan");
	}

	/* allocate the cursor */
	*cur = calloc(1, sizeof(nowdb_cursor_t));
	if (*cur == NULL) {
		NOMEM("allocating cursor");
		return err;
	} 

	(*cur)->rdr = NULL;
	(*cur)->tmp = NULL;
	(*cur)->stf.store = NULL;
	ts_algo_list_init(&(*cur)->stf.files);
	ts_algo_list_init(&(*cur)->stf.pending);

	/* pass on to the filter (in fact, there should/may be,
	 * one filter per subreader */
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

	err = initReader(scope, *cur, start, end, rstp);
	if (err != NOWDB_OK) {
		free((*cur)->rdr); (*cur)->rdr = NULL;
		free(*cur); *cur = NULL;
		return err;
	}
	if ((*cur)->filter != NULL) {
		(*cur)->rdr->filter = (*cur)->filter;
	}

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
	if (cur == NULL) return;
	if (cur->rdr != NULL) {
		nowdb_reader_destroy(cur->rdr);
		free(cur->rdr); cur->rdr = NULL;
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
	if (cur->tmp != NULL) {
		free(cur->tmp); cur->tmp = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Open cursor
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_open(nowdb_cursor_t *cur) {

	cur->off = 0;

	/* initialise readers */
	return nowdb_reader_move(cur->rdr);
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
	uint32_t recsz = cur->rdr->recsize;
	nowdb_filter_t *filter = cur->rdr->filter;
	char *src = nowdb_reader_page(cur->rdr);
	char complete=0, cc=0;
	nowdb_cmp_t x;
	uint32_t mx;

	if (src == NULL) return nowdb_err_get(nowdb_err_eof,
		                        FALSE, OBJECT, NULL);

	mx = cur->rdr->ko?recsz:NOWDB_IDX_PAGE;

	while(*osz < sz) {
		/* we have reached the end of the current page */
		if (cur->off >= mx) {
			// fprintf(stderr, "move %u %u\n", cur->off, *osz);
			err = nowdb_reader_move(cur->rdr);
			if (err != NOWDB_OK) return err;
			src = nowdb_reader_page(cur->rdr);
			cur->off = 0;
		}

		/* we hit the nullrecord and pass on to the next page */
		if (memcmp(src+cur->off, nowdb_nullrec, recsz) == 0) {
			cur->off = mx; continue;
		}

		/* check content
		 * -------------
		 * here's potential for improvement:
		 * 1) we can immediately advance to the next marked record
		 * 2) if we have read all records, we can leave */
		if (!checkpos(cur->rdr, cur->off/recsz)) {
			cur->off += recsz; continue;
		}

		/* apply filter */
		if (filter != NULL &&
		    !nowdb_filter_eval(filter, src+cur->off)) {
			cur->off += recsz; continue;
		}

		/* if keys-only: keep unique
		 * but watch out for CRANGE --
		 * there we have to process
		 * all duplicates */
		if (cur->rdr->ko && cur->tmp != NULL) {
			if (memcmp(cur->tmp, nowdb_nullrec,
			                cur->recsize) == 0) {
				memcpy(cur->tmp, src+cur->off, cur->recsize);
			} else {
				x = cur->recsize == NOWDB_EDGE_SIZE?
				    nowdb_sort_edge_keys_compare(cur->tmp,
				                             src+cur->off,
				                          cur->rdr->ikeys):
				    nowdb_sort_vertex_keys_compare(cur->tmp,
				                               src+cur->off,
				                            cur->rdr->ikeys);
				if (x == NOWDB_SORT_EQUAL) {
					cur->off += recsz; continue;
				}
				memcpy(cur->tmp, src+cur->off, cur->recsize);
			}
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

			/* this is an awful hack to force
			 * a krange reader to present us
			 * the same record once again */
			} else if (cur->tmp != 0) {
				memcpy(cur->tmp, nowdb_nullrec,
				       cur->recsize);
			}
			if (cc == 0) break;
		}
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
	*osz = 0;
	*cnt = 0;

	return simplefetch(cur, buf, sz, osz, cnt);
}

/* ------------------------------------------------------------------------
 * Rewind
 * TODO
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_rewind(nowdb_cursor_t *cur);
