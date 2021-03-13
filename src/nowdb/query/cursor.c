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
		// if (keys->off[i] == NOWDB_OFF_EDGE) return 1;
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
static inline nowdb_err_t getRange(nowdb_expr_t    filter,
                                   nowdb_plan_idx_t *pidx,
                                   char         **fromkey,
                                   char           **tokey) {
	char x = 0;
	nowdb_index_keys_t *keys;
	nowdb_err_t err;

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
	x = nowdb_expr_range(filter, keys->sz, keys->off,
	                               *fromkey, *tokey);
	if (!x) {
		free(*fromkey); *fromkey = NULL;
		free(*tokey); *tokey = NULL;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Some local helpers
 * ------------------------------------------------------------------------
 */
#define DESTROYREADERS(n,rs) \
	if (rs != NULL) {\
		for(int i=0; i<n; i++) { \
			if (rs[n] != NULL) { \
				nowdb_reader_destroy(rs[n]); free(rs[n]); \
			} \
		} \
	} \
	free(rs);rs=NULL;

#define DESTROYFILES(f) \
	for(ts_algo_list_node_t *rr=f->head; \
	               rr!=NULL; rr=rr->nxt){\
		nowdb_file_destroy(rr->cont); \
		free(rr->cont);  \
	} \
	ts_algo_list_destroy(f); free(f);

#define COPYFILES(src,trg) \
	nowdb_file_t *file; \
	files = calloc(1, sizeof(ts_algo_list_t)); \
	if (files == NULL) { \
		NOMEM("allocating new file list"); \
		break; \
	} \
	ts_algo_list_init(files); \
	for(ts_algo_list_node_t *run= cur->stf.files.head; \
	    run!=NULL; run=run->nxt) { \
		file = malloc(sizeof(nowdb_file_t)); \
		if (file == NULL) { \
			NOMEM("allocating file"); \
			DESTROYFILES(files); \
			break; \
		} \
		nowdb_file_copy(run->cont, file); \
		if (ts_algo_list_append(files, file) != TS_ALGO_OK) { \
			NOMEM("list.append"); \
			DESTROYFILES(files);  \
			break; \
		} \
	} \
	if (err != NOWDB_OK) break;

/* ------------------------------------------------------------------------
 * Create sequence reader
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t createSeq(nowdb_cursor_t    *cur,
                                    int               type,
                                    nowdb_plan_idx_t *pidx,
                                    int              count) {
	nowdb_err_t err;
	nowdb_reader_t **rds;

	rds = calloc(count+1, sizeof(nowdb_reader_t*));
	if (rds == NULL) {
		NOMEM("allocating readers");
		return err;
	}

	err = getPending(&cur->stf.files, &cur->stf.pending);
	if (err != NOWDB_OK) {
		free(rds); return err;
	}
	
	switch(type) {
	case NOWDB_PLAN_SEARCH_:
		err = nowdb_reader_fullscan(&rds[0], &cur->stf.pending, NULL);
		break;
	default:
		free(rds);
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
		                                    "unknown seq type");
	}
	if (err != NOWDB_OK) {
		free(rds); return err;
	}
	for(int i=0; i<count; i++) {
		ts_algo_list_t *files;

		switch(type) {
		case NOWDB_PLAN_SEARCH_:
			if (i>0) {
				// each reader needs its
				// own file descriptor
				COPYFILES(&cur->stf.files, files);
			} else {
				files = &cur->stf.files;
			}
			if (pidx[i].maps == NULL) {
				err = nowdb_reader_search(&rds[i+1], files,
			           	pidx[i].idx, pidx[i].keys, NULL);
			} else {
				err = nowdb_reader_mrange(&rds[i+1], files,
				           pidx[i].idx, NULL, pidx[i].maps,
				                               NULL, NULL);
			}
			break;
		default:
			return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
			                                    "unknown seq type");
		}
		if (err != NOWDB_OK) {
			DESTROYREADERS(i,rds);
			return err;
		}
		// destroy your own private file descriptor
		if (i > 0) rds[i+1]->ownfiles = 1;
	}
	
	err = nowdb_reader_vseq(&cur->rdr, count+1, rds);
	if (err != NOWDB_OK) {
		// DESTROYREADERS(count+1,rds);
		DESTROYREADERS(count,rds);
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
	if (err != NOWDB_OK) return err;

	if (pidx == NULL) {
		ts_algo_list_destroy(&cur->stf.pending);
		INVALIDPLAN("no index for merge scan");
	}

	err = getRange(cur->filter, pidx,
	              &cur->fromkey,
	              &cur->tokey);
	if (err != NOWDB_OK) {
		ts_algo_list_destroy(&cur->stf.pending);
		return err;
	}
	
	switch(type) {
	case NOWDB_PLAN_FRANGE_:
	case NOWDB_PLAN_MRANGE_: // we need to handle this one!
		err = nowdb_reader_bufidx(&buf, &cur->stf.pending,
		                pidx->idx, cur->filter, cur->eval,
		                                    NOWDB_ORD_ASC,
		                        cur->fromkey, cur->tokey);
		if (err == NOWDB_OK) buf->maps = pidx->maps;
		break;

	case NOWDB_PLAN_KRANGE_:
		err = nowdb_reader_bkrange(&buf, &cur->stf.pending,
		                 pidx->idx, cur->filter, cur->eval,
		                                     NOWDB_ORD_ASC,
		                         cur->fromkey, cur->tokey);
		break;

	case NOWDB_PLAN_CRANGE_:
	default:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
		                                    "KRANGE or CRANGE");
	}
	if (err != NOWDB_OK) {
		ts_algo_list_destroy(&cur->stf.pending);
		return err;
	}

	switch(type) {
	case NOWDB_PLAN_FRANGE_:
		err = nowdb_reader_frange(&range, &cur->stf.files, pidx->idx,
		                              NULL, cur->fromkey, cur->tokey);
		break;

	case NOWDB_PLAN_MRANGE_:
		err = nowdb_reader_mrange(&range, &cur->stf.files, pidx->idx,
		                 NULL, pidx->maps, cur->fromkey, cur->tokey);
		break;

	case NOWDB_PLAN_KRANGE_:
		err = nowdb_reader_krange(&range, &cur->stf.files, pidx->idx,
		                              NULL, cur->fromkey, cur->tokey);
		if (range->ikeys != NULL) {
			cur->tmp = calloc(1, cur->recsz);
			if (cur->tmp == NULL) {
				ts_algo_list_destroy(&cur->stf.pending);
				NOMEM("allocating temporary buffer");
				nowdb_reader_destroy(buf);
				nowdb_reader_destroy(range);
				return err;
			}
		}
		break;

	case NOWDB_PLAN_CRANGE_:
	default:
		ts_algo_list_destroy(&cur->stf.pending);
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
		                                    "KRANGE or CRANGE");
	}
	if (err != NOWDB_OK) {
		ts_algo_list_destroy(&cur->stf.pending);
		nowdb_reader_destroy(buf);
		return err;
	}

	err = nowdb_reader_merge(&cur->rdr, 2, range, buf);
	if (err != NOWDB_OK) {
		ts_algo_list_destroy(&cur->stf.pending);
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
                                     nowdb_context_t *ctx,
                                     nowdb_cursor_t  *cur,
                                     nowdb_time_t   start,
                                     nowdb_time_t     end,
                                     nowdb_plan_t  *rplan) {
	nowdb_err_t      err;
	nowdb_store_t *store;
	nowdb_plan_idx_t *pidx;

	store = &ctx->store;
	cur->recsz = store->recsize;
	cur->content = store->cont;

	/* content is vertex */
	if (rplan->helper == NOWDB_AST_VERTEX ||
	    rplan->helper == NOWDB_AST_TYPE) {

		// fprintf(stderr, "CONTENT NAME: %s\n", rplan->name);

		if (rplan->name != NULL) {
			err = nowdb_model_getVertexByName(scope->model,
			                                  rplan->name,
			                                  &cur->v);
			if (err != NOWDB_OK) return err;
		}
	}

	cur->hasid = TRUE;

	/* get files */
	err = nowdb_store_getFiles(store, &cur->stf.files, start, end);
	if (err != NOWDB_OK) return err;

	pidx = rplan->load;

	/* create an index search reader */
	if (rplan->stype == NOWDB_PLAN_SEARCH_) {
		// fprintf(stderr, "SEARCH\n");
		err = createSeq(cur, rplan->stype, pidx, 1);
		pidx->maps = NULL;

	} else if (rplan->stype == NOWDB_PLAN_FRANGE_ ||
	           rplan->stype == NOWDB_PLAN_MRANGE_ ||
	          (rplan->stype == NOWDB_PLAN_KRANGE_ && !hasId(pidx))) {
		if (pidx->maps == NULL) {
			// fprintf(stderr, "FRANGE\n");
			err = createMerge(cur, NOWDB_PLAN_FRANGE_, pidx);
		} else {
			// fprintf(stderr, "MRANGE\n");
			err = createMerge(cur, NOWDB_PLAN_MRANGE_, pidx);
			pidx->maps = NULL;
		}

	// KRANGE only allowd with model id
	} else if (rplan->stype == NOWDB_PLAN_KRANGE_) {
		// fprintf(stderr, "KRANGE\n");
		cur->hasid = hasId(pidx);
		err = createMerge(cur, rplan->stype, pidx);

	} else if (rplan->stype == NOWDB_PLAN_CRANGE_) {
		// fprintf(stderr, "CRANGE\n");
		cur->hasid = hasId(pidx);
		err = nowdb_reader_crange(&cur->rdr,
		                          &cur->stf.files,
		                          pidx->idx, NULL,
		                               NULL, NULL); // range !
	/* create a fullscan reader */
	} else {
		// fprintf(stderr, "FULLSCAN\n");
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
	nowdb_context_t *ctx;
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

	err = nowdb_scope_getContext(scope, rstp->name, &ctx);
	if (err != NOWDB_OK) return err;

	/* allocate the cursor */
	*cur = calloc(1, sizeof(nowdb_cursor_t));
	if (*cur == NULL) {
		NOMEM("allocating cursor");
		return err;
	} 

	(*cur)->model = scope->model;
	(*cur)->filter = NULL;
	(*cur)->rdr = NULL;
	(*cur)->tmp = NULL;
	(*cur)->row   = NULL;
	(*cur)->group = NULL;
	(*cur)->nogrp = NULL;
	(*cur)->eval = NULL;
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
			nowdb_expr_period(stp->load, &start, &end);
			runner = runner->nxt;
			stp->load = NULL;
		}
	}

	// initialise eval helper
	(*cur)->eval = calloc(1, sizeof(nowdb_eval_t));
	if ((*cur)->eval == NULL) {
		NOMEM("allocating evaluation helper");
		nowdb_cursor_destroy(*cur);
		free(*cur); *cur = NULL;
		return err;
	}

	(*cur)->eval->model = scope->model;
	(*cur)->eval->text = scope->text;
	(*cur)->eval->tlru = NULL;

	(*cur)->eval->tlru = calloc(1, sizeof(nowdb_ptlru_t));
	if ((*cur)->eval->tlru == NULL) {
		NOMEM("allocating text lru");
		nowdb_cursor_destroy(*cur);
		free(*cur); *cur = NULL;
		return err;
	}
	err = nowdb_ptlru_init((*cur)->eval->tlru, 100000);
	if (err != NOWDB_OK) {
		free((*cur)->eval->tlru);
		(*cur)->eval->tlru=NULL;
		nowdb_cursor_destroy(*cur);
		free(*cur); *cur = NULL;
		return err;
	}

	err = initReader(scope, ctx, *cur, start, end, rstp);
	if (err != NOWDB_OK) {
		free((*cur)->rdr); (*cur)->rdr = NULL;
		nowdb_cursor_destroy(*cur);
		free(*cur); *cur = NULL;
		return err;
	}
	if ((*cur)->filter != NULL) {
		/*
		nowdb_expr_show((*cur)->filter, stderr);
		fprintf(stderr, "\n");
		*/
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
		(*cur)->grouping = 1;
		if ((*cur)->tmp == NULL) {
			(*cur)->tmp = calloc(1, (*cur)->recsz);
		}
		if ((*cur)->tmp == NULL) {
			NOMEM("allocating temporary buffer");
			nowdb_cursor_destroy(*cur); free(*cur);
			return err;
		}
	}

	/* projection */
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
			(*cur)->tmp2 = calloc(1, (*cur)->recsz);
			if ((*cur)->tmp2 == NULL) {
				NOMEM("allocating temporary buffer");
				nowdb_cursor_destroy(*cur); free(*cur);
				return err;
			}
			if ((*cur)->tmp == NULL) {
				(*cur)->tmp = calloc(1, (*cur)->recsz);
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
				if ((*cur)->group != NULL &&
				    (*cur)->row   != NULL) {
					nowdb_group_setEval((*cur)->group,
					              &(*cur)->row->eval);
				}
			} else {
				err = nowdb_group_fromList(&(*cur)->nogrp,
				                               stp->load);
				if ((*cur)->nogrp != NULL &&
				    (*cur)->row   != NULL) {
					nowdb_group_setEval((*cur)->nogrp,
					              &(*cur)->row->eval);
				}
			}
			if (err != NOWDB_OK) {
				nowdb_cursor_destroy(*cur);
				free(*cur); return err;
			}
			ts_algo_list_destroy(stp->load);
			free(stp->load);
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
		nowdb_expr_destroy(cur->filter);
		free(cur->filter);
		cur->filter = NULL;
	}
	if (cur->eval != NULL) {
		nowdb_eval_destroy(cur->eval);
		free(cur->eval); cur->eval = NULL;
	}
	if (cur->stf.store != NULL) {
		nowdb_store_destroyFiles(cur->stf.store, &cur->stf.files);
		ts_algo_list_destroy(&cur->stf.pending);
		cur->stf.store = NULL;
	}
	if (cur->group != NULL) {
		nowdb_group_destroy(cur->group);
		free(cur->group); cur->group = NULL;
	}
	if (cur->nogrp != NULL) {
		nowdb_group_destroy(cur->nogrp);
		free(cur->nogrp); cur->nogrp = NULL;
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

	// initialise offset
	cur->off = 0;

	// initialise readers
	return nowdb_reader_move(cur->rdr);
}

/* ------------------------------------------------------------------------
 * Check whether this position is "in"
 * 1) It will be even more complicated in the future:
 *    we will then need to combine cont with the ctrlblock in the file,
 *    i.e. r->cont[y] & f->hdr->cont[y] & (k<<i)
 * 2) we should encapsulate this somewhere;
 * ------------------------------------------------------------------------
 */
static inline char checkpos(nowdb_reader_t *r, uint32_t pos) {
	int y,i;
	uint8_t k = 1;
	if (r->cont == NULL) return 1;
	y = pos/8;
	i = pos%8;
	if (r->cont[y] & (k<<i)) return 1;
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
		memcpy(cur->tmp, src, recsz);
		memcpy(cur->tmp2, cur->tmp, recsz);
	}
	err = nowdb_group_map(cur->nogrp, ctype, src);
	if (err != NOWDB_OK) return err;
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

	/* group not yet initialised */
	if (memcmp(cur->tmp, nowdb_nullrec, cur->recsz) == 0) {
		memcpy(cur->tmp, src, cur->recsz);
		if (cur->group != NULL) {
			*x=0;
			memcpy(cur->tmp2, cur->tmp, cur->recsz);
		}
		return NOWDB_OK;
	}
	cmp = ctype == NOWDB_CONT_EDGE?
	      nowdb_sort_edge_keys_compare(cur->tmp, src,
		                           cur->rdr->ikeys):
	      nowdb_sort_vertex_keys_compare(cur->tmp, src,
		                             cur->rdr->ikeys);
	/* no group switch, just map */
	if (cmp == NOWDB_SORT_EQUAL) {
		if (cur->group != NULL) {
			err = nowdb_group_map(cur->group, ctype, src);
			if (err != NOWDB_OK) return err;
		}
		*x=0;
		return NOWDB_OK;
	}
	/* we actually switch the group */
	if (cur->group != NULL) {
		err = nowdb_group_map(cur->group, ctype, cur->tmp2);
		if (err != NOWDB_OK) return err;
		err = nowdb_group_reduce(cur->group, ctype);
		if (err != NOWDB_OK) return err;
	}
	memcpy(cur->tmp, src, cur->recsz);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Finalise the group
 * ------------------------------------------------------------------------
 */
static inline void finalizeGroup(nowdb_cursor_t *cur, char *src) {
	if (cur->tmp2 != NULL && src != cur->tmp2 && cur->tmp != NULL) {
		memcpy(cur->tmp2, cur->tmp, cur->recsz);
	}
	nowdb_group_reset(cur->group);
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
	char complete=0, cc=0, full=0;
	
	if (old == NOWDB_OK || old->errcode != nowdb_err_eof) return old;

	cur->eof = 1;

	if (cur->group == NULL && cur->nogrp == NULL) return old;
	if (cur->nogrp != NULL) {
		err = nowdb_group_reduce(cur->nogrp, ctype);
	} else if (cur->group != NULL) {
		cur->off = 0;
		// if (*osz == 0) return old; // is this correct???
		err = nowdb_group_map(cur->group, ctype, cur->tmp2);
		if (err != NOWDB_OK) return err;
		err = nowdb_group_reduce(cur->group, ctype);
	}
	if (err != NOWDB_OK) {
		return nowdb_err_cascade(err, old);
	}

	err = nowdb_row_project(cur->row,
	                        cur->tmp2,
	                cur->rdr->recsize,
		      buf, sz, osz, &full,
                          &cc, &complete);
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

#define AFTERMOVE() \
	src = nowdb_reader_page(cur->rdr); \
	cur->recsz = cur->rdr->recsize; \
	recsz = cur->recsz; \
	mx = cur->rdr->ko?recsz:(NOWDB_IDX_PAGE/recsz)*recsz; \
	filter = cur->rdr->filter; \
	ctype = cur->rdr->content;

#define CHECKEOF() \
	if (cur->eof) { \
		return nowdb_err_get(nowdb_err_eof, FALSE, OBJECT, NULL); \
	}

#define MKEOF() \
	err = nowdb_err_get(nowdb_err_eof, FALSE, OBJECT, NULL); 

#define FREESRC(err) \
	if (cur->freesrc) { \
		free(realsrc); realsrc = NULL; \
		cur->leftover = NULL; \
		cur->freesrc = 0; \
	}

/* ------------------------------------------------------------------------
 * Fetch
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t fetch(nowdb_cursor_t *cur,
                             char *buf, uint32_t sz,
                                      uint32_t *osz,
                                    uint32_t *count) 
{
	nowdb_err_t err;
	uint32_t recsz;
	uint32_t realsz;
	char *realsrc=NULL;
	nowdb_expr_t filter;
	char *src;
	nowdb_content_t ctype;
	char complete=0, cc=0, x=1, full=0;
	uint32_t mx;

	// we have already reached eof
	// CHECKEOF()

	// initialise like after move
	AFTERMOVE()

	for(;;) {
		// handle leftovers
		if (cur->leftover != NULL) {

			realsz = cur->recsz;
			realsrc = cur->leftover;
			cur->leftover = NULL;

			goto projection;

		}
		// END OF FILE
		if (cur->eof) {
			fprintf(stderr, "EOF indeed: %d\n", cur->eof);
			MKEOF();
			return handleEOF(cur, err, ctype,
				         recsz, buf, sz,
				         osz, count);
		}
		// END OF PAGE
		if (cur->off >= mx) {
			err = nowdb_reader_move(cur->rdr);
			if (err != NOWDB_OK) {
				if (!nowdb_err_contains(err, nowdb_err_eof))
					return err;

				cur->eof = 1;
				continue;
			}

			cur->off = 0;
			AFTERMOVE()
		}
		// NULLRECORD
		if (memcmp(src+cur->off, nowdb_nullrec, recsz) == 0) {
			cur->off = mx; continue;
		}
		// check content
		// -------------
		// here's potential for improvement:
		// 1) we can immediately advance to the next marked record
		// 2) if we have read all records, we can leave
		if (!checkpos(cur->rdr, cur->off/recsz)) {
			cur->off += recsz; continue;
		}
		// fprintf(stderr, "NOT DELETED\n");
		// FILTER
		if (filter != NULL) {
			// fprintf(stderr, "FILTER???\n");
			void *v=NULL;
			nowdb_type_t  t;
			err = nowdb_expr_eval(filter, cur->eval,
			                  src+cur->off, &t, &v);
			if (err != NOWDB_OK) return err;
			if (t == NOWDB_TYP_NOTHING) {
				cur->off += recsz; continue;
			} else if (t == NOWDB_TYP_TEXT) {
				if (v == NULL || strlen(v) == 0) {
					cur->off += recsz; continue;
				}
			} else if (!(*(nowdb_value_t*)v)) {
				cur->off += recsz; continue;
			}
		}
		realsz = recsz;
		realsrc = src+cur->off;
grouping:
		// if keys-only, group or no-group aggregates
		if (cur->tmp != NULL) {
			if (cur->nogrp != NULL) {
				err = nogroup(cur, ctype, realsz, realsrc);
				FREESRC();
				if (err != NOWDB_OK) return err;
				cur->off += recsz;
				continue;
			}
			if (cur->grouping) {
				// review for vertex !
				err = groupswitch(cur, ctype, realsz,
				                        realsrc, &x);
				FREESRC();
				if (err != NOWDB_OK) return err;
				if (!x) {
					cur->off+=cur->recsz;
					continue;
				}
			}
		}
projection:
		// fprintf(stderr, "PROJECTING\n");
		if (cur->group == NULL) {
			// fprintf(stderr, "NO GROUP\n");
			err = nowdb_row_project(cur->row,
			                 realsrc, realsz,
			                         buf, sz,
			                      osz, &full,
 			                 &cc, &complete);
		} else {
			err = nowdb_row_project(cur->row,
			               cur->tmp2, realsz,
			                         buf, sz,
			                      osz, &full,
			                 &cc, &complete);
		}
		if (err != NOWDB_OK) return err;
		if (complete) {

			// finalise the group (if there is one)
			finalizeGroup(cur, realsrc);

			FREESRC();

			(*count)+=cc;
			cur->off+=recsz;
		} else {
			// remember if we have to free the leftover!
			cur->leftover = realsrc;
			cur->recsz = realsz;
			recsz = cur->recsz;

			// this is still ugly
			if (realsrc != cur->vrtx    &&
			    realsrc != src+cur->off &&
			    realsrc != src+cur->off-cur->recsz) cur->freesrc=1;
		}
		if (full) break;
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
	*osz = 0; *cnt = 0;
	return fetch(cur, buf, sz, osz, cnt);
}

/* ------------------------------------------------------------------------
 * Rewind
 * TODO
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_rewind(nowdb_cursor_t *cur);

/* ------------------------------------------------------------------------
 * EOF
 * ------------------------------------------------------------------------
 */
char nowdb_cursor_eof(nowdb_cursor_t *cur) {
	return cur->eof;
}
