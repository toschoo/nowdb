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
static inline nowdb_err_t getRange(nowdb_filter_t *filter,
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
	x = nowdb_filter_range(filter, keys->sz, keys->off,
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
	for(int i=0; i<n; i++) { \
		if (rs[n] != NULL) { \
			nowdb_reader_destroy(rs[n]); free(rs[n]); \
		} \
	} \
	free(rs);

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
			err = nowdb_reader_search(&rds[i+1], files,
			           pidx[i].idx, pidx[i].keys, NULL);
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
		DESTROYREADERS(count+1,rds);
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
		                           pidx->idx, cur->filter,
		                                    NOWDB_ORD_ASC,
		                         cur->fromkey, cur->tokey);
		break;

	case NOWDB_PLAN_KRANGE_:
		err = nowdb_reader_bkrange(&buf, &cur->stf.pending,
		                            pidx->idx, cur->filter,
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
		cur->target = NOWDB_TARGET_VERTEX;
		cur->recsize = 32;
		store = &scope->vertices;

		// fprintf(stderr, "TARGET NAME: %s\n", rplan->name);

		if (rplan->name != NULL) {
			err = nowdb_model_getVertexByName(scope->model,
			                                  rplan->name,
			                                  &cur->v);
			if (err != NOWDB_OK) return err;
		}

	/* target is a context */
	} else {
		cur->target = NOWDB_TARGET_EDGE;
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
		// fprintf(stderr, "SEARCH\n");
		err = createSeq(cur, rplan->stype, pidx, 1);

	} else if (rplan->stype == NOWDB_PLAN_FRANGE_ ||
	          (rplan->stype == NOWDB_PLAN_KRANGE_ && !hasId(pidx))) {
		// fprintf(stderr, "FRANGE\n");
		err = createMerge(cur, NOWDB_PLAN_FRANGE_, pidx);

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
	if (cur->v == NULL) INVALIDPLAN("no vertex type in cursor");
	return nowdb_vrow_fromFilter(cur->v->roleid, &cur->wrow, cur->filter);
}

/* ------------------------------------------------------------------------
 * init vertex props for where
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t initPRow(nowdb_cursor_t *cur) {
	nowdb_err_t err;

	if (cur->v == NULL) INVALIDPLAN("no vertex type in cursor");
	if (cur->row == NULL) return NOWDB_OK;

	err = nowdb_vrow_new(cur->v->roleid, &cur->prow);
	if (err != NOWDB_OK) return err;

	for(int i=0; i<cur->row->sz; i++) {
		err = nowdb_vrow_addExpr(cur->prow, cur->row->fields[i]);
		if (err != NOWDB_OK) return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: check whether filter contains nothing but vid
 * ------------------------------------------------------------------------
 */
static nowdb_err_t hasOnlyVid(nowdb_row_t       *row,
                              nowdb_filter_t *filter,
                              char              *yes)
{
	*yes = 1;

	if (filter == NULL) return NOWDB_OK;

	// nowdb_filter_show(filter, stderr); fprintf(stderr, "\n");

	// we search all of a kind
	if (filter->ntype == NOWDB_FILTER_COMPARE &&
	    filter->op == NOWDB_FILTER_EQ         &&
	    filter->off == NOWDB_OFF_ROLE) return NOWDB_OK;

	// we search for a specific vid, i.e.
	// (role = mytype) and (vid = myvid)
	if (!(filter->ntype == NOWDB_FILTER_BOOL &&
	      filter->op    == NOWDB_FILTER_AND)) {
		*yes = 0;
	} else {
		if (filter->left == NULL) {
			INVALIDPLAN("incomplete filter");
		}
		if (filter->right == NULL) {
			INVALIDPLAN("incomplete filter");
		}
		if (!(filter->left->ntype == NOWDB_FILTER_COMPARE &&
		      (filter->left->op == NOWDB_FILTER_EQ ||
		       filter->left->op == NOWDB_FILTER_IN))) *yes = 0;
		if (!(filter->right->ntype == NOWDB_FILTER_COMPARE &&
		      (filter->right->op == NOWDB_FILTER_EQ ||
		       filter->right->op == NOWDB_FILTER_IN))) *yes = 0;
		if (filter->left->off != NOWDB_OFF_ROLE &&
		    filter->right->off != NOWDB_OFF_ROLE) *yes = 0;
		if (filter->left->off != NOWDB_OFF_VERTEX &&
		    filter->right->off != NOWDB_OFF_VERTEX) *yes = 0;
	}

	if (*yes == 1) return NOWDB_OK;	

	/* This is irrelevant
	  
	*yes = 1;

	for(int i=0; i<row->sz; i++) {
		if (row->fields[i].target != NOWDB_TARGET_VERTEX) continue;
		if (row->fields[i].name == NULL) continue;
		fprintf(stderr, "%u.%s (%lu)\n", row->fields[i].roleid,
		                                 row->fields[i].name,
		                                 row->fields[i].propid);
		err = nowdb_model_getPropById(row->model,
		                              row->fields[i].roleid,
		                              row->fields[i].propid,
		                              &p);
		if (err != NOWDB_OK) return err;
		if (p->pk) {
			row->fields[i].pk = 1;
		} else {
			*yes=0;
		} 
	}
	*/
	return NOWDB_OK;
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
                                 ts_algo_list_t *vlst) {
	nowdb_err_t err;
	nowdb_filter_t *f;
	ts_algo_list_t vids;
	ts_algo_list_node_t *run;
	uint64_t *role;

	// we allocate the role, so the filter can own it
	role = malloc(8);
	if (role == NULL) {
		NOMEM("allocating role");
		return err;
	}
	*role = cur->v->roleid;

	// copy the list of vids (which is owned by the filter)
	ts_algo_list_init(&vids);
	for(run=vlst->head;run!=NULL;run=run->nxt) {
		uint64_t *vid = malloc(8);
		if (vid == NULL) {
			NOMEM("allocating vid");
			DESTROYVIDS(vids);
			return err;
		}
		memcpy(vid, run->cont, 8);
		if (ts_algo_list_append(&vids, vid) != TS_ALGO_OK) {
			NOMEM("list.append");
			DESTROYVIDS(vids);
			return err;
		}
	}

	// create a boolean filter:
	// (role = mytype) and (vid in (...))
	err = nowdb_filter_newBool(&f, NOWDB_FILTER_AND);
	if (err != NOWDB_OK) return err;

	// role = mytype
	err = nowdb_filter_newCompare(&f->left,
	                      NOWDB_FILTER_EQ,
	                      NOWDB_OFF_ROLE, 4,
	                      NOWDB_TYP_UINT, role, NULL);
	if (err != NOWDB_OK) {
		nowdb_filter_destroy(f); free(f);
		DESTROYVIDS(vids);
		return err;
	}
	nowdb_filter_own(f->left);

	// vid in (...)
	err = nowdb_filter_newCompare(&f->right,
	                      NOWDB_FILTER_IN,
	                      NOWDB_OFF_VERTEX, 8,
	                      NOWDB_TYP_UINT, NULL, &vids);
	if (err != NOWDB_OK) {
		nowdb_filter_destroy(f->left); free(f->left);
		nowdb_filter_destroy(f); free(f);
		DESTROYVIDS(vids);
		return err;
	}

	// destroy the list but *only* the list
	// its content is now owned by the filter
	ts_algo_list_destroy(&vids);

	// nowdb_filter_show(f,stderr);fprintf(stderr, "\n");

	// destroy the old filter
	if (cur->filter != NULL) {
		nowdb_filter_destroy(cur->filter);
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
		free(ps[i].keys); \
	} \
	free(ps);

/* ------------------------------------------------------------------------
 * Helper: Build search readers, on reader per vid 'in (...)'
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

	fprintf(stderr, "SEARCH\n");

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
                                 ts_algo_list_t *vlst) {
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

	// this magical number reflects an internal of reader
        // (which, currently, can have only 64 subreaders). 
	// On the long run, we should improve readers
	// so that we can have a meaningful limit or
	// threshold, preferrably a ratio with the number
	// of keys in the type.
	// Furthermore, instead of performing a fullscan
	// we should use a MRANGE merge reader, i.e.
	// a reader that presents only relevant keys (vid),
	// i.e. keys that correspond to the 'in' list.
	if (vlst->len > 61) {
		err = nowdb_reader_fullscan(&cur->rdr,
		                &cur->stf.files, NULL);
	} else {
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
 *         put then in a list and then create new second
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
	cur->target = mom->target;
	cur->rdr = mom->rdr;
	memcpy(&cur->stf, &mom->stf, sizeof(nowdb_storefile_t));
	cur->model = mom->model;
	cur->recsize = mom->recsize;
	cur->filter = mom->filter;
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
			} else break;
		}
		for(int i=0; i<osz; i+=cur->recsize) {
			uint64_t *vid;
			vid = malloc(8);
			if (vid == NULL) {
				NOMEM("allocating vid");
				goto cleanup;
			}
			memcpy(vid, buf+i, 8);
			// fprintf(stderr, "%lu\n", *(uint64_t*)vid);
			if (ts_algo_tree_insert(vids, vid) != TS_ALGO_OK) {
				NOMEM("tree.insert");
				goto cleanup;
			}
		}
	}

	// nothing found
	if (vids->count == 0) {
		err = nowdb_err_get(nowdb_err_eof, FALSE, OBJECT,
		                                 "preprocessing");
		goto cleanup;
	}

	// tree to list
	vlst = ts_algo_tree_toList(vids);
	if (vlst == NULL) {
		NOMEM("tree.toList");
		goto cleanup;
	}

	// create filter with in compare
	err = makeVidCompare(mom, vlst);
	if (err != NOWDB_OK) goto cleanup;

	// make reader (using the built-in index if possible)
	err = makeVidReader(scope, mom, vlst);
	if (err != NOWDB_OK) goto cleanup;

cleanup:
	if (vids != NULL) {
		ts_algo_tree_destroy(vids); free(vids);
	}
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
		/*
		nowdb_filter_show((*cur)->filter, stderr);
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
			ts_algo_list_destroy(stp->load);
			free(stp->load);
			stp->load = NULL;
		}
	}
	// well, we have a cursor.
	// if it is on vertex and the projection
	// contains things beyond the primary key,
	// we are not yet done.
	if ((*cur)->target == NOWDB_TARGET_VERTEX) {
		char ok;

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
		nowdb_filter_destroy(cur->filter);
		free(cur->filter);
		cur->filter = NULL;
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

	/*
	fprintf(stderr, "groupswitch on %p+%u %u bytes to %p\n",
	                 src, cur->off, cur->recsize, cur->tmp);
	*/

	/* group not yet initialised */
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
	/* no group switch, just map */
	if (cmp == NOWDB_SORT_EQUAL) {
		if (cur->group != NULL) {
			err = nowdb_group_map(cur->group, ctype,
			                          src+cur->off);
			if (err != NOWDB_OK) return err;
		}
		cur->off += recsz; *x=0;
		return NOWDB_OK;
	}
	/* we actually switch the group */
	if (cur->group != NULL) {
		err = nowdb_group_map(cur->group, ctype,
			                      cur->tmp2);
		if (err != NOWDB_OK) return err;
		err = nowdb_group_reduce(cur->group, ctype);
		if (err != NOWDB_OK) return err;
	}
	memcpy(cur->tmp, src+cur->off, cur->recsize);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Finalise the group
 * ------------------------------------------------------------------------
 */
static inline void finalizeGroup(nowdb_cursor_t *cur, char *src) {
	if (cur->tmp2 != NULL && src != cur->tmp2) {
		memcpy(cur->tmp2, cur->tmp, cur->recsize);
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
	nowdb_group_t *g;
	char complete=0, cc=0, full=0;
	char x;
	
	if (old == NOWDB_OK || old->errcode != nowdb_err_eof) return old;

	cur->eof = 1;

	if (cur->group == NULL && cur->nogrp == NULL) return old;

	if (cur->nogrp != NULL) {
		g = cur->nogrp;
		err = nowdb_group_reduce(cur->nogrp, ctype);
	} else {
		g = cur->group;
		cur->off = 0;
		err = groupswitch(cur, ctype, recsz, cur->tmp2, &x);
	}
	if (err != NOWDB_OK) {
		return nowdb_err_cascade(err, old);
	}

	err = nowdb_row_project(cur->row, g,
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
	uint32_t realsz;
	char *realsrc=NULL;
	nowdb_filter_t *filter = cur->rdr->filter;
	char *src = nowdb_reader_page(cur->rdr);
	char complete=0, cc=0, x=1, full=0;
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
			/*
			fprintf(stderr, "move %u %u (%u)\n",
			                 cur->off, *osz, mx);
			*/
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

		/* apply filter to vertex row */
		if (cur->wrow != NULL) {
			nowdb_key_t vid;
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
			if (!nowdb_vrow_eval(cur->wrow, &vid)) {
				cur->off += recsz; continue;
			}

		/* apply filter directly */
		} else if (filter != NULL &&
		    !nowdb_filter_eval(filter, src+cur->off)) {
			cur->off += recsz; continue;
		}

		// add vertex property to prow
		// NOTE:
		// as soon as we *always* use the internal index
		// for vertices, we can enforce completion, i.e.
		// we complete the previous vertex when we see
		// a new vid
		if (cur->prow != NULL) {
			err = nowdb_vrow_add(cur->prow,
			              (nowdb_vertex_t*)(src+cur->off), &x);
			if (err != NOWDB_OK) return err;
		}

		// review!!!
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

		if (cur->prow != NULL) {
			if (!nowdb_vrow_complete(cur->prow,
			                 &realsz, &realsrc)) 
			{
				cur->off += recsz; continue;
			}
		} else {
			realsz = recsz;
			realsrc = src+cur->off;
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
				                       realsrc, realsz,
				                   buf, sz, osz, &full,
 				                       &cc, &complete);
			} else {
				err = nowdb_row_project(cur->row,
				                      cur->group,
				                cur->tmp2, recsz,
				             buf, sz, osz, &full,
				                 &cc, &complete);
			}
			if (realsrc != (src+cur->off)) free(realsrc);
			if (err != NOWDB_OK) return err;
			if (complete) {
				cur->off+=recsz;
				(*count)+=cc;

				// finalise the group (if there is one)
				finalizeGroup(cur, src);

			/* this is an awful hack to force
			 * a krange reader to present us
			 * the same record once again */
			} else if (cur->tmp != 0) {
				memcpy(cur->tmp, nowdb_nullrec, cur->recsize);
			}
			if (full) break;
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
	*osz = 0; *cnt = 0;
	return simplefetch(cur, buf, sz, osz, cnt);
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
