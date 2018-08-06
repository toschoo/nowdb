/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Cursor: DQL executor
 * ========================================================================
 */
#include <nowdb/reader/reader.h>
#include <nowdb/query/cursor.h>

/* ------------------------------------------------------------------------
 * Macros for the frequent error "invalid plan" and "no memory"
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
 * Helper: get range for range reader
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getRange(nowdb_filter_t *filter,
                                   nowdb_plan_idx_t *pidx,
                                   char         **fromkey,
                                   char           **tokey) {
	char x = 0;
	nowdb_index_keys_t *keys;
	nowdb_err_t err;

	if (pidx->keys != NULL) {
		*fromkey = pidx->keys;
		*tokey = pidx->keys;
		return NOWDB_OK;
	}
	keys = nowdb_index_getResource(pidx->idx);
	if (keys == NULL) return NOWDB_OK;

	*fromkey = calloc(1,keys->sz*8);
	if (*fromkey == NULL) {
		NOMEM("allocating keys");
		return err;
	}
	*tokey = calloc(1,keys->sz*8);
	if (*tokey == NULL) {
		free(*fromkey);
		NOMEM("allocating keys");
		return err;
	}
	x = nowdb_filter_range(filter, keys->sz, keys->off,
	                                 *fromkey, *tokey);
	if (!x) {
		free(*fromkey); *fromkey = NULL;
		free(*tokey); *tokey = NULL;
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

	if (pidx == NULL) {
		INVALIDPLAN("no index for merge scan");
	}

	err = getRange(cur->filter, pidx,
	              &cur->fromkey,
	              &cur->tokey);
	if (err != NOWDB_OK) return err;
	
	switch(type) {
	case NOWDB_PLAN_FRANGE_:
		err = nowdb_reader_bufidx(&buf, &cur->stf.pending,
		                                  pidx->idx, NULL,
		                                    NOWDB_ORD_ASC,
		                         cur->fromkey, cur->tokey);
		break;

	case NOWDB_PLAN_KRANGE_:
		err = nowdb_reader_bkrange(&buf, &cur->stf.pending,
		                                   pidx->idx, NULL,
		                                     NOWDB_ORD_ASC,
		                         cur->fromkey, cur->tokey);
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
		                              NULL, cur->fromkey, cur->tokey);
		break;

	case NOWDB_PLAN_KRANGE_:
		err = nowdb_reader_krange(&range, &cur->stf.files, pidx->idx,
		                              NULL, cur->fromkey, cur->tokey);
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

	pidx = rplan->load;

	/* create an index search reader */
	if (rplan->stype == NOWDB_PLAN_SEARCH_) {
		fprintf(stderr, "SEARCH\n");
		err = createSeq(cur, rplan->stype, pidx);

	} else if (rplan->stype == NOWDB_PLAN_FRANGE_) {
		fprintf(stderr, "FRANGE\n");
		err = createMerge(cur, rplan->stype, pidx);

	} else if (rplan->stype == NOWDB_PLAN_KRANGE_) {
		fprintf(stderr, "KRANGE\n");
		cur->hasid = hasId(pidx);
		err = createMerge(cur, rplan->stype, pidx);

	} else if (rplan->stype == NOWDB_PLAN_CRANGE_) {
		fprintf(stderr, "CRANGE\n");
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
	(*cur)->row   = NULL;
	(*cur)->group = NULL;
	(*cur)->nogrp = NULL;
	(*cur)->stf.store = NULL;
	ts_algo_list_init(&(*cur)->stf.files);
	ts_algo_list_init(&(*cur)->stf.pending);

	/* pass on to the filter (in fact, there should/may be
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

		runner = runner->nxt;
		if (runner == NULL) {
			nowdb_cursor_destroy(*cur); free(*cur);
			INVALIDPLAN("grouping without projection");
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

	/* aggregates */
	runner = runner->nxt;
	if (runner != NULL) {
		stp = runner->cont;
		if (stp->ntype == NOWDB_PLAN_AGGREGATES) {

			/* create temporary variable for groupswitch */
			(*cur)->tmp2 = calloc(1, (*cur)->recsize);
			if ((*cur)->tmp2 == NULL) {
				NOMEM("allocating temporary buffer");
				nowdb_cursor_destroy(*cur); free(*cur);
				return err;
			}

			if ((*cur)->tmp == NULL) {
				(*cur)->tmp = calloc(1, (*cur)->recsize);
			}
			if ((*cur)->tmp == NULL) {
				NOMEM("allocating temporary buffer");
				nowdb_cursor_destroy(*cur); free(*cur);
				return err;
			}

			/* create group */
			if ((*cur)->rdr->type == NOWDB_READER_MERGE) {
				err = nowdb_group_fromList(&(*cur)->group,
				                               stp->load);
			} else {
				err = nowdb_group_fromList(&(*cur)->nogrp,
				                               stp->load);
			}
			if (err != NOWDB_OK) {
				nowdb_cursor_destroy(*cur);
				free(*cur); return err;
			}
			stp->load = NULL;
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
	if (cur->tmp2 != NULL) {
		free(cur->tmp2); cur->tmp2 = NULL;
	}
	if (cur->fromkey != NULL) {
		free(cur->fromkey); cur->fromkey = NULL;
	}
	if (cur->tokey != NULL) {
		free(cur->tokey); cur->tokey = NULL;
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
 * Ungrouped aggregates
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t nogroup(nowdb_cursor_t *cur,
                                  nowdb_content_t ctype,
                                  uint32_t        recsz,
                                  char           *src) {
	nowdb_err_t err;

	if (memcmp(cur->tmp, nowdb_nullrec, recsz) == 0) {
		memcpy(cur->tmp, src+cur->off, recsz);
		memcpy(cur->tmp2, cur->tmp, recsz);
	}
	err = nowdb_group_map(cur->nogrp, ctype, src+cur->off);
	if (err != NOWDB_OK) return err;
	cur->off += recsz;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Group switch
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t groupswitch(nowdb_cursor_t   *cur,
                                      nowdb_content_t ctype,
                                      uint32_t        recsz,
                                      char *src,    char *x) {
	nowdb_err_t err;
	nowdb_cmp_t cmp;

	*x = 1;

	if (memcmp(cur->tmp, nowdb_nullrec, cur->recsize) == 0) {
		memcpy(cur->tmp, src+cur->off, cur->recsize);
		if (cur->group != NULL) {
			*x=0;
			memcpy(cur->tmp2, cur->tmp, cur->recsize);
			cur->off+=cur->recsize;
		}
		return NOWDB_OK;
	}
	cmp = cur->recsize == NOWDB_EDGE_SIZE?
	      nowdb_sort_edge_keys_compare(cur->tmp,
		                       src+cur->off,
		                    cur->rdr->ikeys):
	      nowdb_sort_vertex_keys_compare(cur->tmp,
		                         src+cur->off,
		                      cur->rdr->ikeys);
	if (cmp == NOWDB_SORT_EQUAL) {
		if (cur->group != NULL) {
			err = nowdb_group_map(cur->group, ctype,
			                          src+cur->off);
			if (err != NOWDB_OK) return err;
		}
		cur->off += recsz; *x=0;
		return NOWDB_OK;
	}
	if (cur->group != NULL) {
		err = nowdb_group_map(cur->group, ctype,
			                      cur->tmp2);
		if (err != NOWDB_OK) return err;
		err = nowdb_group_reduce(cur->group, ctype);
		if (err != NOWDB_OK) return err;

		memcpy(cur->tmp2, cur->tmp, cur->recsize);
	}
	memcpy(cur->tmp, src+cur->off, cur->recsize);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * The last turn
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t handleEOF(nowdb_cursor_t *cur,
                                    nowdb_err_t     old,
                                  nowdb_content_t ctype,
                                         uint32_t recsz,
                                 char *buf, uint32_t sz,
                                          uint32_t *osz,
                                        uint32_t *count) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_group_t *g;
	char complete=0, cc=0;
	char x;
	
	if (old == NOWDB_OK || old->errcode != nowdb_err_eof) return old;
	if (cur->group == NULL && cur->nogrp == NULL) return old;

	if (cur->nogrp != NULL) {
		g = cur->nogrp;
		// err = nogroup(cur, ctype, recsz, cur->tmp2);
	} else {
		g = cur->group;
		err = groupswitch(cur, ctype, recsz, cur->tmp2, &x);
	}
	if (err != NOWDB_OK) return err;

	err = nowdb_row_project(cur->row, g,
	                          cur->tmp2,
	                  cur->rdr->recsize,
		          buf, sz, osz, &cc,
			          &complete);
	if (err != NOWDB_OK) {
		nowdb_err_release(old);
		return err;
	}
	if (complete) {
		(*count)+=cc;
		return old;
	}
	memcpy(cur->tmp, nowdb_nullrec, recsz);
	nowdb_err_release(old);
	return NOWDB_OK;
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
	char complete=0, cc=0, x=1;
	uint32_t mx;

	/* the reader should store the content type */
	nowdb_content_t ctype = recsz == NOWDB_EDGE_SIZE?
	                                 NOWDB_CONT_EDGE:
	                                 NOWDB_CONT_VERTEX;

	if (src == NULL) return nowdb_err_get(nowdb_err_eof,
		                        FALSE, OBJECT, NULL);

	mx = cur->rdr->ko?recsz:NOWDB_IDX_PAGE;

	while(*osz < sz) {
		/* we have reached the end of the current page */
		if (cur->off >= mx) {
			// fprintf(stderr, "move %u %u\n", cur->off, *osz);
			err = nowdb_reader_move(cur->rdr);
			if (err != NOWDB_OK) {
				return handleEOF(cur, err,
				             ctype, recsz,
				                  buf, sz,
				               osz, count);
			}
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

		/* if keys-only, group or no-group aggregates */
		if (cur->tmp != NULL) {
			if (cur->nogrp != NULL) {
				err = nogroup(cur, ctype, recsz, src);
				if (err != NOWDB_OK) return err;
				continue;
			}

			if (cur->group != NULL || cur->rdr->ko) {
				err = groupswitch(cur, ctype, recsz, src, &x);
				if (err != NOWDB_OK) return err;
				if (!x) continue;
			}
		}

		/* copy the record to the output buffer */
		if (cur->row == NULL || !cur->hasid) {
			memcpy(buf+(*osz), src+cur->off, recsz);
			*osz += recsz; cur->off += recsz;
			(*count)++;

		/* project ... */
		} else {
			if (cur->group == NULL) {
				err = nowdb_row_project(cur->row, NULL,
				                   src+cur->off, recsz,
				                     buf, sz, osz, &cc,
				                            &complete);
			} else {
				err = nowdb_row_project(cur->row,
				                      cur->group,
				                cur->tmp2, recsz,
				               buf, sz, osz, &cc,
				                      &complete);
			}
			if (err != NOWDB_OK) return err;
			if (complete) {
				cur->off+=recsz;
				(*count)+=cc;

			/* this is an awful hack to force
			 * a krange reader to present us
			 * the same record once again */
			} else if (cur->tmp != 0) {
				memcpy(cur->tmp, nowdb_nullrec, cur->recsize);
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
