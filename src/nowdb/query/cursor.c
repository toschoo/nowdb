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

static uint64_t fullmap = NOWDB_BITMAP64_ALL;

#define VINDEX "_vindex"

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
		cur->content = NOWDB_CONT_VERTEX;
		cur->recsz = 32;
		store = &scope->vertices;

		// fprintf(stderr, "CONTENT NAME: %s\n", rplan->name);

		if (rplan->name != NULL) {
			err = nowdb_model_getVertexByName(scope->model,
			                                  rplan->name,
			                                  &cur->v);
			if (err != NOWDB_OK) return err;
		}

	/* content is edge */
	} else {
		cur->content = NOWDB_CONT_EDGE;
		err = nowdb_scope_getContext(scope, rplan->name, &ctx);
		if (err != NOWDB_OK) return err;
		store = &ctx->store;
		cur->recsz = store->recsize;
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
 * init vertex props for where
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t initWRow(nowdb_cursor_t *cur) {
	nowdb_err_t err;
	if (cur->v == NULL) INVALIDPLAN("no vertex type in cursor");
	err = nowdb_vrow_fromFilter(cur->v->roleid, &cur->wrow,
	                               cur->filter, cur->eval);
	if (err != NOWDB_OK) return err;
	// nowdb_vrow_autoComplete(cur->wrow);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * init vertex props for where
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t initPRow(nowdb_cursor_t *cur) {
	nowdb_err_t err;
	nowdb_model_prop_t *p;
	nowdb_expr_t px;

	if (cur->v == NULL) INVALIDPLAN("no vertex type in cursor");
	if (cur->row == NULL) return NOWDB_OK;

	err = nowdb_vrow_new(cur->v->roleid, &cur->prow, cur->eval);
	if (err != NOWDB_OK) return err;

	// not yet working
	// nowdb_vrow_autoComplete(cur->prow);

	for(int i=0; i<cur->row->sz; i++) {
		err = nowdb_vrow_addExpr(cur->prow, cur->row->fields[i]);
		if (err != NOWDB_OK) return err;
	}

	// Always add pk
	err = nowdb_model_getPK(cur->model, cur->v->roleid, &p);
	if (err != NOWDB_OK) return err;

	err = nowdb_expr_newVertexField(&px, p->name,
	         cur->v->roleid, p->propid, p->value);
	if (err != NOWDB_OK) return err;

	err = nowdb_vrow_addExpr(cur->prow, px);
	nowdb_expr_destroy(px); free(px);
	if (err != NOWDB_OK) return err;

	return NOWDB_OK;
}

#define FIELD(x) \
	NOWDB_EXPR_TOFIELD(x)

/* ------------------------------------------------------------------------
 * Helper: check whether filter contains nothing but vid
 * ------------------------------------------------------------------------
 */
static nowdb_err_t hasOnlyVidR(nowdb_row_t    *row,
                               nowdb_expr_t filter,
                               char           *yes) {
	nowdb_err_t err;
	ts_algo_list_t fields;
	ts_algo_list_node_t *run;
	nowdb_expr_t node;

	if (filter == NULL) return NOWDB_OK;

	ts_algo_list_init(&fields);
	err = nowdb_expr_filter(filter, NOWDB_EXPR_FIELD, &fields);
	if (err != NOWDB_OK) {
		ts_algo_list_destroy(&fields);
		return err;
	}
	for(run=fields.head;run!=NULL;run=run->nxt) {
		node=run->cont;
		// should we check for PK?
		if (FIELD(node)->off != NOWDB_OFF_ROLE &&
		    FIELD(node)->off != NOWDB_OFF_VERTEX) {
			*yes=0; break;
		}
	}
	ts_algo_list_destroy(&fields);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: check whether filter contains nothing but vid
 * ------------------------------------------------------------------------
 */
static nowdb_err_t hasOnlyVid(nowdb_row_t  *row,
                              nowdb_expr_t  filter,
                              char         *yes)
{
	*yes = 1;
	if (filter == NULL) return NOWDB_OK;
	return hasOnlyVidR(row, filter, yes);
}

/* ------------------------------------------------------------------------
 * Some local helpers
 * ------------------------------------------------------------------------
 */
#define DESTROYVIDS(v) \
	for(ts_algo_list_node_t *run=v.head; \
	             run!=NULL; run=run->nxt) \
	{ \
		free(run->cont); \
	} \
	ts_algo_list_destroy(&v);

/* ------------------------------------------------------------------------
 * Helper: Build the filter for the vertex as 'in (...)'
 * ------------------------------------------------------------------------
 */
static nowdb_err_t makeVidCompare(nowdb_cursor_t *cur,
                                 ts_algo_tree_t *vids) {
	nowdb_err_t err;
	nowdb_expr_t f, o1, o2;
	nowdb_expr_t r, rc, v, vc;

	// vertex constant
	// we make this first, so the memory management for vids is done
	err = nowdb_expr_constFromTree(&vc, vids, NOWDB_TYP_UINT);
	if (err != NOWDB_OK) {
		ts_algo_tree_destroy(vids); free(vids);
		return err;
	}

	// role field
	err = nowdb_expr_newVertexOffField(&r, NOWDB_OFF_ROLE);
	if (err != NOWDB_OK) {
		nowdb_expr_destroy(vc); free(vc);
		return err;
	}

	// role constant
	err = nowdb_expr_newConstant(&rc, &cur->v->roleid,
	                                 NOWDB_TYP_SHORT);
	if (err != NOWDB_OK) {
		nowdb_expr_destroy(vc); free(vc);
		nowdb_expr_destroy(r); free(r);
		return err;
	}

	// roleid = role
	err = nowdb_expr_newOp(&o1, NOWDB_EXPR_OP_EQ, r, rc);
	if (err != NOWDB_OK) {
		nowdb_expr_destroy(vc); free(vc);
		nowdb_expr_destroy(r); free(r);
		nowdb_expr_destroy(rc); free(rc);
		return err;
	}

	// vid field
	err = nowdb_expr_newVertexOffField(&v, NOWDB_OFF_VERTEX);
	if (err != NOWDB_OK) {
		nowdb_expr_destroy(vc); free(vc);
		nowdb_expr_destroy(o1); free(o1);
		return err;
	}

	// vid in vids
	err = nowdb_expr_newOp(&o2, NOWDB_EXPR_OP_IN, v, vc);
	if (err != NOWDB_OK) {
		nowdb_expr_destroy(o1); free(o1);
		nowdb_expr_destroy(v); free(v);
		nowdb_expr_destroy(vc); free(vc);
		return err;
	}

	// and
	err = nowdb_expr_newOp(&f, NOWDB_EXPR_OP_AND, o1, o2);
	if (err != NOWDB_OK) {
		nowdb_expr_destroy(o1); free(o1);
		nowdb_expr_destroy(o2); free(o2);
		return err;
	}

	// nowdb_filter_show(f,stderr);fprintf(stderr, "\n");

	// destroy the old filter
	if (cur->filter != NULL) {
		nowdb_expr_destroy(cur->filter);
		free(cur->filter); cur->filter = NULL;
	}

	// set the new filter
	cur->filter = f;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Some local helpers
 * ------------------------------------------------------------------------
 */
#define DESTROYPIDX(n,ps) \
	for(int i=0; i<n; i++) { \
		if (ps[i].keys != NULL) { \
			free(ps[i].keys); \
		} \
		if (ps[i].maps != NULL) { \
			free(ps[i].maps); \
		} \
	} \
	free(ps);

/* ------------------------------------------------------------------------
 * Helper: Build mrange reader for vid 'in (...)'
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t makeVidRange(nowdb_scope_t  *scope,
                                       nowdb_cursor_t *cur,
                                       ts_algo_tree_t *vtree) {
	nowdb_err_t err;
	nowdb_plan_idx_t *pidx;
	nowdb_index_desc_t *desc;

	// get the internal index on vertex
	err = nowdb_index_man_getByName(scope->iman,
	                              VINDEX, &desc);
	if (err != NOWDB_OK) return err;

	// one plan index per vid
	pidx = calloc(1, sizeof(nowdb_plan_idx_t));
	if (pidx == NULL) {
		NOMEM("allocating plan index");
		NOWDB_IGNORE(nowdb_index_enduse(desc->idx));
		return err;
	}

	// create index keys
	pidx->idx = desc->idx;
	pidx->keys = malloc(12);
	if (pidx->keys == NULL) {
		NOMEM("allocating keys");
		NOWDB_IGNORE(nowdb_index_enduse(desc->idx));
		DESTROYPIDX(0,pidx);
		return err;
	}

	memcpy(pidx->keys, &cur->v->roleid, 4);

	pidx->maps = calloc(2, sizeof(ts_algo_tree_t*));
	if (pidx->maps == NULL) {
		NOMEM("allocating keys");
		NOWDB_IGNORE(nowdb_index_enduse(desc->idx));
		DESTROYPIDX(1,pidx);
		return err;
	}

	pidx->maps[0] = NULL;
	pidx->maps[1] = vtree;

	err = createSeq(cur, NOWDB_PLAN_SEARCH_, pidx, 1);
	pidx->maps = NULL;
	DESTROYPIDX(1,pidx);
	if (err != NOWDB_OK) {
		NOWDB_IGNORE(nowdb_index_enduse(desc->idx));
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: Build search readers, one reader per vid 'in (...)'
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t makeVidSearch(nowdb_scope_t  *scope,
                                        nowdb_cursor_t *cur,
                                        ts_algo_list_t *vlst) {
	nowdb_err_t err;
	nowdb_plan_idx_t *pidx;
	nowdb_index_desc_t *desc;
	ts_algo_list_node_t *run;
	int i=0;

	// get the internal index on vertex
	err = nowdb_index_man_getByName(scope->iman,
	                              VINDEX, &desc);
	if (err != NOWDB_OK) return err;

	// one plan index per vid
	pidx = calloc(vlst->len, sizeof(nowdb_plan_idx_t));
	if (pidx == NULL) {
		NOMEM("allocating plan index");
		NOWDB_IGNORE(nowdb_index_enduse(desc->idx));
		return err;
	}

	// create index keys
	for(run=vlst->head;run!=NULL;run=run->nxt) {
		pidx[i].idx = desc->idx;
		pidx[i].keys = malloc(12);
		if (pidx[i].keys == NULL) {
			NOMEM("allocating keys");
			NOWDB_IGNORE(nowdb_index_enduse(desc->idx));
			DESTROYPIDX(i,pidx);
			return err;
		}
		memcpy(pidx[i].keys, &cur->v->roleid, 4);
		memcpy(pidx[i].keys+4, run->cont, 8); i++;
	}

	// create index keys
	err = createSeq(cur, NOWDB_PLAN_SEARCH_, pidx, vlst->len);
	DESTROYPIDX(vlst->len,pidx);
	if (err != NOWDB_OK) {
		NOWDB_IGNORE(nowdb_index_enduse(desc->idx));
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: Build search readers, one reader per vid 'in (...)'
 * ------------------------------------------------------------------------
 */
static nowdb_err_t makeVidReader(nowdb_scope_t  *scope,
                                 nowdb_cursor_t *cur,
                                 ts_algo_list_t *vlst,
                                 ts_algo_tree_t *vids) {
	nowdb_err_t err;

	// first destroy existing reader
	if (cur->rdr != NULL) {
		nowdb_reader_destroy(cur->rdr);
		free(cur->rdr); cur->rdr = NULL;
	}

	// destroy existing files
	if (cur->stf.store != NULL) {
		nowdb_store_destroyFiles(cur->stf.store, &cur->stf.files);
		ts_algo_list_destroy(&cur->stf.pending);
		ts_algo_list_init(&cur->stf.files);
		ts_algo_list_init(&cur->stf.pending);
		cur->stf.store = NULL;
	}
	
	// get the relevant files 
	cur->stf.store = &scope->vertices;
	err = nowdb_store_getFiles(cur->stf.store, &cur->stf.files,
	                          NOWDB_TIME_DAWN, NOWDB_TIME_DUSK);
	if (err != NOWDB_OK) return err;

	// this magical number is not based on any benchmarks!
	if (vlst->len > 31) {
		// fprintf(stderr, "MRANGE\n");
		err = makeVidRange(scope, cur, vids);
	} else {
		// fprintf(stderr, "SEARCH: %d\n", vlst->len);
		err = makeVidSearch(scope, cur, vlst);
	}
	if (err != NOWDB_OK) {
		nowdb_store_destroyFiles(cur->stf.store, &cur->stf.files);
		ts_algo_list_destroy(&cur->stf.pending);
		cur->stf.store = NULL;
		return err;
	}

	// set the filter to the new reader
	if (cur->filter != NULL) {
		cur->rdr->filter = cur->filter;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: Get the vids according to the original query,
 *         put them in a list and then create new second
 *         cursor identical to the first, but with
 *         filter and readers accepting all vertices
 *         (of this type) that are 'in (...)' the vertices.
 *
 * Here are the callbacks for the tree...
 * ------------------------------------------------------------------------
 */
#define VID(x) \
	(*(uint64_t*)x)

static ts_algo_cmp_t vidcompare(void *ingore, void *one, void *two) {
	if (VID(one) < VID(two)) return ts_algo_cmp_less;
	if (VID(one) > VID(two)) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	free(n);
	return TS_ALGO_OK;
}

static void videstroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	free(*n); *n = NULL;
}

/* ------------------------------------------------------------------------
 * Helper: here is it
 * ------------------------------------------------------------------------
 */
#define BUFSIZE 8192
static nowdb_err_t getVids(nowdb_scope_t *scope,
                           nowdb_cursor_t  *mom) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_cursor_t *cur;
	char *buf=NULL;
	uint32_t osz=0, cnt=0;
	ts_algo_tree_t *vids=NULL;
	ts_algo_list_t *vlst=NULL;

	// create a new cursor,
	// which is the same as the old
	// but projects only the primary key
	cur = calloc(1, sizeof(nowdb_cursor_t));
	if (cur == NULL) {
		NOMEM("allocating cursor");
		return err;
	}

	// we could just memcpy the whole thing
	cur->content = mom->content;
	cur->rdr = mom->rdr;
	memcpy(&cur->stf, &mom->stf, sizeof(nowdb_storefile_t));
	cur->model = mom->model;
	cur->recsz = mom->recsz;
	cur->filter = mom->filter;
	cur->eval = mom->eval;
	cur->v = mom->v;
	cur->fromkey = mom->fromkey;
	cur->tokey = mom->tokey;

	cur->row = NULL;
	cur->group = NULL;
	cur->nogrp = NULL;
	cur->tmp = NULL;
	cur->tmp2 = NULL;

	cur->hasid = 1;
	cur->eof = 0;

	// initialise the vertex row handler
	err = initWRow(cur);
	if (err != NOWDB_OK) goto cleanup;

	/*
	if (cur->wrow != NULL) {
		nowdb_expr_show(cur->wrow->filter,stderr);
		fprintf(stderr, "\n");
	}
	*/

	// open it
	err = nowdb_cursor_open(cur);
	if (err != NOWDB_OK) goto cleanup;

	// the output buffer
	buf = malloc(BUFSIZE);
	if (buf == NULL) {
		NOMEM("allocating buffer");
		goto cleanup;
	}

	// we store the vid uniquely
	vids = ts_algo_tree_new(&vidcompare, NULL,
	                        &noupdate, &videstroy,
	                                   &videstroy);
	if (vids == NULL) {
		NOMEM("tree.init");
		goto cleanup;
	}

	// fetch
	for(;;) {
		err = nowdb_cursor_fetch(cur, buf, BUFSIZE, &osz, &cnt);
		if (err != NOWDB_OK) {
			if (err->errcode == nowdb_err_eof) {
				nowdb_err_release(err);
				err = NOWDB_OK;
				if (cnt == 0) break;
			} else goto cleanup;
		}
		for(int i=0; i<osz; i+=cur->recsz) {
			uint64_t *vid;
			vid = malloc(8);
			if (vid == NULL) {
				NOMEM("allocating vid");
				goto cleanup;
			}
			memcpy(vid, buf+i, 8);
			// fprintf(stderr, "got vid %lu (%u)\n", *vid, cur->recsz);
			if (ts_algo_tree_insert(vids, vid) != TS_ALGO_OK) {
				free(vid);
				ts_algo_tree_destroy(vids); free(vids);
				NOMEM("tree.insert");
				goto cleanup;
			}
		}
	}

	// nothing found
	if (vids->count == 0) {
		err = nowdb_err_get(nowdb_err_eof, FALSE, OBJECT,
		                                 "preprocessing");
		ts_algo_tree_destroy(vids); free(vids);
		goto cleanup;
	}

	// fprintf(stderr, "HAVING %d\n", vids->count);

	// tree to list
	vlst = ts_algo_tree_toList(vids);
	if (vlst == NULL) {
		ts_algo_tree_destroy(vids); free(vids);
		NOMEM("tree.toList");
		goto cleanup;
	}

	// create filter with in compare
	err = makeVidCompare(mom, vids);
	if (err != NOWDB_OK) goto cleanup;

	// make reader (using the built-in index if possible)
	err = makeVidReader(scope, mom, vlst, vids);
	if (err != NOWDB_OK) goto cleanup;

cleanup:
	if (vlst != NULL) {
		ts_algo_list_destroy(vlst); free(vlst);
	}
	if (buf != NULL) free(buf);

	// destroy the cursor
	if (cur->wrow != NULL) {
		nowdb_vrow_destroy(cur->wrow);
		free(cur->wrow); cur->wrow = NULL;
	}
	free(cur);

	mom->eof = 0;

	return err;
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

	err = initReader(scope, *cur, start, end, rstp);
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
	// well, we have a cursor.
	// if it is on vertex and the projection
	// contains things beyond the primary key,
	// we are not yet done.
	if ((*cur)->content == NOWDB_CONT_VERTEX) {
		char ok;

		/*
		nowdb_expr_show((*cur)->filter, stderr);
		fprintf(stderr, "\n");
		*/

		// make mom->prow from field list
		err = initPRow(*cur);
		if (err != NOWDB_OK) {
			nowdb_cursor_destroy(*cur);
			free(*cur); return err;
		}

	        err = hasOnlyVid((*cur)->row, (*cur)->filter, &ok);
		if (err != NOWDB_OK) {
			nowdb_cursor_destroy(*cur);
			free(*cur); return err;
		}
		if (ok) return NOWDB_OK;

		// get vids
		err = getVids(scope, *cur);
		if (err != NOWDB_OK) {
			nowdb_cursor_destroy(*cur);
			free(*cur); return err;
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
	if (cur->wrow != NULL) {
		nowdb_vrow_destroy(cur->wrow);
		free(cur->wrow); cur->wrow = NULL;
	}
	if (cur->prow != NULL) {
		nowdb_vrow_destroy(cur->prow);
		free(cur->prow); cur->prow = NULL;
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
 * REVIEW!!!!
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
                                  uint64_t        pmap,
                                  char           *src) {
	nowdb_err_t err;

	if (memcmp(cur->tmp, nowdb_nullrec, recsz) == 0) {
		memcpy(cur->tmp, src, recsz);
		memcpy(cur->tmp2, cur->tmp, recsz);
	}
	err = nowdb_group_map(cur->nogrp, ctype, pmap, src);
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
                                      uint64_t         pmap,
                                      char *src,    char *x) {
	nowdb_err_t err;
	nowdb_cmp_t cmp;

	*x = 1;

	/*
	fprintf(stderr, "groupswitch on %p+%u %u bytes to %p\n",
	                 src, cur->off, cur->recsz, cur->tmp);
	*/

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
			err = nowdb_group_map(cur->group, ctype, pmap, src);
			if (err != NOWDB_OK) return err;
		}
		*x=0;
		return NOWDB_OK;
	}
	/* we actually switch the group */
	if (cur->group != NULL) {
		err = nowdb_group_map(cur->group, ctype,
			                pmap, cur->tmp2);
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
		if (*osz == 0) return old; // is this correct???
		err = nowdb_group_map(cur->group, ctype,
			              fullmap,cur->tmp2);
		if (err != NOWDB_OK) return err;
		err = nowdb_group_reduce(cur->group, ctype);
	}
	if (err != NOWDB_OK) {
		return nowdb_err_cascade(err, old);
	}

	err = nowdb_row_project(cur->row,
	                        cur->tmp2,
	                cur->rdr->recsize,
	                          fullmap,
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
	uint64_t pmap=fullmap;
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
			pmap = cur->pmap;
			realsrc = cur->leftover;
			cur->leftover = NULL;

			goto projection;

		}
		// handle complete prows
		if (cur->prow != NULL) {
			if (nowdb_vrow_complete(cur->prow,
			                        &realsz,
			                        &pmap,
                                                &realsrc)) {
				cur->freesrc = 1;
				goto grouping;
			}
		}
		// handle leftover from wrows
		if (cur->eof && cur->wrow != NULL) {
			err = nowdb_vrow_force(cur->wrow);
			if (err != NOWDB_OK) return err;

			// memset(cur->vrtx, 0, 32);
			err = nowdb_vrow_eval(cur->wrow,
                                (nowdb_key_t*)cur->vrtx, &x);
			if (err != NOWDB_OK) return err;
			if (x) {
				realsrc = cur->vrtx;
				realsz = 32;
				goto projection;
			}
			if (!nowdb_vrow_empty(cur->wrow)) continue;
		}
		// END OF FILE
		if (cur->eof) {
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
				if (cur->prow != NULL) {
					nowdb_err_release(err);
					err = nowdb_vrow_force(cur->prow);
					if (err != NOWDB_OK) return err;
				}
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
		// FILTER
		if (cur->wrow != NULL) {
			char x=0;

			// we add one more property
			err = nowdb_vrow_add(cur->wrow,
			    (nowdb_vertex_t*)(src+cur->off), &x);
			if (err != NOWDB_OK) return err;
			if (!x) {
				cur->off += recsz; continue;
			}
			// the only record that could have been completed
			// by that is the one to which the current belongs
			err = nowdb_vrow_eval(cur->wrow,
			    (nowdb_key_t*)cur->vrtx, &x);
			if (err != NOWDB_OK) return err;
			if (!x) {
				cur->off += recsz; continue;
			}

		} else if (filter != NULL) {
			void *v;
			nowdb_type_t  t;
			err = nowdb_expr_eval(filter, cur->eval, fullmap,
			                           src+cur->off, &t, &v);
			if (err != NOWDB_OK) return err;
			if (!(*(nowdb_value_t*)v)) {
				cur->off += recsz; continue;
			}
		}
		// PROW
		if (cur->prow != NULL) {
			err = nowdb_vrow_add(cur->prow,
			      (nowdb_vertex_t*)(src+cur->off), &x);
			if (err != NOWDB_OK) return err;
			if (!nowdb_vrow_complete(cur->prow,
			         &realsz, &pmap, &realsrc)) 
			{
				cur->off += recsz; continue;
			}
			cur->freesrc = 1;
		} else {
			realsz = recsz;
			realsrc = src+cur->off;
		}
grouping:
		// if keys-only, group or no-group aggregates
		if (cur->tmp != NULL) {
			if (cur->nogrp != NULL) {
				err = nogroup(cur, ctype, realsz,
				              pmap, realsrc);
				FREESRC();
				if (err != NOWDB_OK) return err;
				cur->off += recsz;
				continue;
			}
			if (cur->grouping) {
				// review for vertex !
				err = groupswitch(cur, ctype, realsz,
				                  pmap, realsrc, &x);
				FREESRC();
				if (err != NOWDB_OK) return err;
				if (!x) {
					cur->off+=cur->recsz;
					continue;
				}
			}
		}
projection:
		if (cur->group == NULL) {
			err = nowdb_row_project(cur->row,
			                 realsrc, realsz,
			                   pmap, buf, sz,
			                      osz, &full,
 			                 &cc, &complete);
		} else {
			err = nowdb_row_project(cur->row,
			               cur->tmp2, realsz,
			                   pmap, buf, sz,
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
			cur->pmap = pmap;

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
