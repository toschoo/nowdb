/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Scope: a collection of contexts
 * ========================================================================
 */
#include <nowdb/scope/scope.h>
#include <nowdb/io/dir.h>

static char *OBJECT = "scope";

/* ------------------------------------------------------------------------
 * Macro: scope NULL
 * ------------------------------------------------------------------------
 */
#define SCOPENULL() \
	if (scope == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                          FALSE, OBJECT, "scope is NULL");

/* ------------------------------------------------------------------------
 * Tree callbacks for contexts: compare
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t ctxcompare(void *ignore, void *left, void *right) {
	int cmp = strcmp(((nowdb_context_t*)left)->name,
	                 ((nowdb_context_t*)right)->name);
	if (cmp < 0) return ts_algo_cmp_less;
	if (cmp > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Tree callbacks for contexts: update (do nothing!)
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	/*
	fprintf(stderr, "reinserting context '%s' ('%s')\n",
	       ((nowdb_context_t*)n)->name,
	       ((nowdb_context_t*)o)->name);
	*/
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Tree callbacks for contexts: delete and destroy
 * ------------------------------------------------------------------------
 */
static void ctxdestroy(void *ignore, void **n) {
	if (*n != NULL) {
		nowdb_context_destroy((nowdb_context_t*)(*n));
		free(*n); *n = NULL;
	}
}

/* -----------------------------------------------------------------------
 * Helper: make generic context path
 * -----------------------------------------------------------------------
 */
static nowdb_err_t mkctxpath(nowdb_scope_t *scope, char **path) {
	*path = nowdb_path_append(scope->path, "context");
	if (*path == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                          "allocating context path");
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: make context path for specific context
 * -----------------------------------------------------------------------
 */
static nowdb_err_t mkthisctxpath(nowdb_scope_t *scope,
                                           char *name,
                                          char **path) {
	char *tmp;

	tmp = nowdb_path_append(scope->path, "context");
	if (tmp == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                          "allocating context path");
	}
	*path = nowdb_path_append(tmp, name); free(tmp);
	if (*path == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                          "allocating context path");
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: make vertex path
 * -----------------------------------------------------------------------
 */
static nowdb_err_t mkvtxpath(nowdb_scope_t *scope, char **path) {

	*path = nowdb_path_append(scope->path, "vertex");
	if (*path == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                           "allocating vertex path");
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: append 'index' to anything
 * -----------------------------------------------------------------------
 */
static nowdb_err_t mkidxpath(char *p, char **path) {

	*path = nowdb_path_append(p, "index");
	if (*path == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                            "allocating index path");
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: append anything to anything
 * -----------------------------------------------------------------------
 */
static nowdb_err_t mkanypath(char *p1, char *p2, char **path) {

	*path = nowdb_path_append(p1,p2); 
	if (*path == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                  "allocating path");
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: initialise index manager
 * -----------------------------------------------------------------------
 */
static nowdb_err_t initIndexMan(nowdb_scope_t *scope) {
	nowdb_err_t err;
	char *imanpath;
	char *ctxpath;
	char *vtxpath;

	scope->iman = calloc(1,sizeof(nowdb_index_man_t));
	if (scope->iman == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                     FALSE, OBJECT, "allocating index manager");

	imanpath = nowdb_path_append(scope->path, "icat");
	if (imanpath == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                     "allocating index catalog path");
	}

	err = mkctxpath(scope, &ctxpath);
	if (err != NOWDB_OK) {
		free(scope->iman); scope->iman = NULL;
		free(imanpath);
		return err;
	}
	err = mkvtxpath(scope, &vtxpath);
	if (err != NOWDB_OK) {
		free(scope->iman); scope->iman = NULL;
		free(imanpath);free(ctxpath);
		return err;
	}
	err = nowdb_index_man_init(scope->iman,
	                          &scope->contexts,
	                           nowdb_lib(),
	                           imanpath,
	                           ctxpath,
	                           vtxpath);

	free(imanpath); free(ctxpath); free(vtxpath);

	if (err != NOWDB_OK) {
		free(scope->iman); scope->iman = NULL;
		return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Allocate and initialise a new scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_new(nowdb_scope_t **scope,
                            nowdb_path_t     path,
                            nowdb_version_t   ver)
{
	nowdb_err_t err;
	if (scope == NULL) return nowdb_err_get(nowdb_err_invalid,
	        FALSE, OBJECT, "pointer to scope object is NULL");

	*scope = malloc(sizeof(nowdb_scope_t));
	if (*scope == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "allocating scope object");
	err = nowdb_scope_init(*scope, path, ver);
	if (err != NOWDB_OK) {
		free(*scope); *scope = NULL; return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Initialise an already allocated scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_init(nowdb_scope_t *scope,
                             nowdb_path_t    path,
                             nowdb_version_t  ver) {
	nowdb_err_t err;
	nowdb_path_t  p;
	size_t        s;

	if (scope == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "scope object is NULL");
	if (path  == NULL) return nowdb_err_get(nowdb_err_invalid,
		                   FALSE, OBJECT, "path is NULL");

	/* set defaults */
	scope->path = NULL;
	scope->iman = NULL;
	scope->ver  = ver;
	scope->state = NOWDB_SCOPE_CLOSED;

	/* path */
	s = strnlen(path, NOWDB_MAX_PATH+1);
	if (s >= NOWDB_MAX_NAME) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                 "path too long (max: 4096)");
	}
	scope->path = malloc(s+1);
	if (scope->path == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                            "allocating scope path");
	}
	strcpy(scope->path, path);

	/* catalog */
	scope->catalog = nowdb_path_append(scope->path, "catalog");
	if (scope->catalog == NULL) {
		free(scope->path); scope->path = NULL;
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                    "allocating scope catalog path");
	}
	
	/* lock */
	err = nowdb_rwlock_init(&scope->lock);
	if (err != NOWDB_OK) return err;
	
	/* context tree */
	if (ts_algo_tree_init(&scope->contexts,
 	                      &ctxcompare, NULL,
	                      &noupdate,
	                      &ctxdestroy,
	                      &ctxdestroy) != TS_ALGO_OK)
	{
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                       "initialising context tree");
		free(scope->path); scope->path = NULL;
		free(scope->catalog); scope->catalog = NULL;
		nowdb_rwlock_destroy(&scope->lock);
		return err;
	}

	/* vertex store */
	p = nowdb_path_append(path, "vertex");
	if (p == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                         "allocating context path");
		free(scope->path); scope->path = NULL;
		free(scope->catalog); scope->catalog = NULL;
		nowdb_rwlock_destroy(&scope->lock);
		ts_algo_tree_destroy(&scope->contexts);
		return err;
	}
	err = nowdb_store_init(&scope->vertices, p, ver,
	                       sizeof(nowdb_vertex_t),
	                       NOWDB_FILE_MAPSIZE,
	                       NOWDB_MEGA *
	                       sizeof(nowdb_vertex_t));
	if (err != NOWDB_OK) {
		free(scope->path); scope->path = NULL; free(p);
		free(scope->catalog); scope->catalog = NULL;
		nowdb_rwlock_destroy(&scope->lock);
		ts_algo_tree_destroy(&scope->contexts);
		return err;
	}
	free(p); 

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Destroy scope
 * -----------------------------------------------------------------------
 */
void nowdb_scope_destroy(nowdb_scope_t *scope) {
	if (scope == NULL) return;
	if (scope->iman != NULL) {
		nowdb_index_man_destroy(scope->iman);
		free(scope->iman); scope->iman = NULL;
	}
	if (scope->path != NULL) {
		free(scope->path); scope->path = NULL;
	}
	if (scope->catalog != NULL) {
		free(scope->catalog); scope->catalog = NULL;
	}
	nowdb_rwlock_destroy(&scope->lock);
	nowdb_store_destroy(&scope->vertices);
	ts_algo_tree_destroy(&scope->contexts);
}

/* ------------------------------------------------------------------------
 * Helper: compute catalog size
 * ------------------------------------------------------------------------
 */
static inline uint32_t computeCatalogSize(nowdb_scope_t *scope) {
	/* once:
	 * MAGIC + version
	 *
	 * per line:
	 * ---------
	 * alloc size        4 
	 * large size        4 
	 * nm sorters        4 
	 * compression       4 
	 * encryption        4 
	 * context name    255
	 */
	uint32_t once = 8;
	uint32_t perline = 275;
	uint32_t nCtx   = 0;

	nCtx += scope->contexts.count;
	return once + nCtx * perline + 1;
}

/* ------------------------------------------------------------------------
 * Helper: write one line into the catalog
 * ------------------------------------------------------------------------
 */
static inline void writeCatalogLine(nowdb_context_t *ctx,
                                char *buf, uint32_t *off) {

	uint32_t s = strlen(ctx->name)+1;

	/*
	if (ctx->store.filesize == 0) *((char*)0x0) = 1;
	*/

	memcpy(buf+*off, &ctx->store.filesize, 4); *off += 4;
	memcpy(buf+*off, &ctx->store.largesize, 4); *off += 4;
	memcpy(buf+*off, &ctx->store.tasknum, 4); *off += 4;
	memcpy(buf+*off, &ctx->store.comp, 4); *off += 4;
	memset(buf+*off, 0, 4); *off += 4;
	memcpy(buf+*off, ctx->name, s); *off += s;
}

/* ------------------------------------------------------------------------
 * Helper: compute catalog size and alloc buf
 *         write files to buffer and write buffers to disk
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t storeCatalog(nowdb_scope_t *scope) {
	nowdb_err_t err;
	uint32_t magic = NOWDB_MAGIC;
	char *buf = NULL;
	uint32_t off=8;
	ts_algo_list_node_t *runner;
	ts_algo_list_t *tmp;
	nowdb_context_t *ctx;
	uint32_t sz;

	sz = computeCatalogSize(scope);
	buf = malloc(sz);
	if (buf == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                   FALSE, OBJECT, "allocating buffer");

	memcpy(buf, &magic, 4);
	memcpy(buf+4, &scope->ver, 4);

	if (scope->contexts.count > 0) {
		tmp = ts_algo_tree_toList(&scope->contexts);
		if (tmp == NULL) {
			err = nowdb_err_get(nowdb_err_no_mem,
	                        FALSE, OBJECT, "tree.toList");
			free(buf); return err;
		}
		for(runner=tmp->head; runner!=NULL; runner=runner->nxt) {
			ctx = runner->cont;
			writeCatalogLine(ctx, buf, &off);
		}
		ts_algo_list_destroy(tmp); free(tmp);
	}
	err = nowdb_writeFileWithBkp(scope->path, scope->catalog, buf, off);
	free(buf); return err;
}

/* -----------------------------------------------------------------------
 * Helper: Allocate and initialise a context
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t initContext(nowdb_scope_t    *scope,
                                      char              *name,
                                      nowdb_ctx_config_t *cfg,
                                      nowdb_version_t     ver,
                                      nowdb_context_t   **ctx) {
	nowdb_path_t tmp,p;
	nowdb_err_t err;
	uint32_t s;

	s = strnlen(name, NOWDB_MAX_NAME+1);
	if (s >= NOWDB_MAX_NAME) return nowdb_err_get(nowdb_err_invalid,
	                     FALSE, OBJECT, "name too long (max: 255)");

	*ctx = malloc(sizeof(nowdb_context_t));
	if (*ctx == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                        "allocating context object");
	}
	(*ctx)->name = malloc(s+1);
	if ((*ctx)->name == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                          "allocating name string");
		free(*ctx); *ctx = NULL; return err;
	}
	strcpy((*ctx)->name, name);

	tmp = nowdb_path_append(scope->path, "context");
	if (tmp == NULL) {
		free((*ctx)->name); free(*ctx); *ctx = NULL;
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                          "allocating context path");
	}
	p = nowdb_path_append(tmp, name); free(tmp);
	if (p == NULL) {
		free((*ctx)->name); free(*ctx); *ctx = NULL;
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                     "allocating context/name path");
	}
	err = nowdb_context_err(*ctx,
	      nowdb_store_init(&(*ctx)->store, p, ver,
	                         sizeof(nowdb_edge_t),
	                               cfg->allocsize,
	                              cfg->largesize));
	free(p);
	if (err != NOWDB_OK) {
		free((*ctx)->name); free(*ctx); *ctx = NULL;
		return err;
	}
	err = nowdb_store_configSort(&(*ctx)->store, &nowdb_store_edge_compare);
	if (err != NOWDB_OK) {
		nowdb_store_destroy(&(*ctx)->store);
		free((*ctx)->name); free(*ctx);
		return err;
	}
	err = nowdb_store_configCompression(&(*ctx)->store, cfg->comp);
	if (err != NOWDB_OK) {
		nowdb_store_destroy(&(*ctx)->store);
		free((*ctx)->name); free(*ctx); *ctx = NULL;
		return err;
	}
	err = nowdb_store_configIndexing(&(*ctx)->store, scope->iman, *ctx);
	if (err != NOWDB_OK) {
		nowdb_store_destroy(&(*ctx)->store);
		free((*ctx)->name); free(*ctx); *ctx = NULL;
		return err;
	}
	err = nowdb_store_configWorkers(&(*ctx)->store, cfg->sorters);
	if (err != NOWDB_OK) {
		nowdb_store_destroy(&(*ctx)->store);
		free((*ctx)->name); free(*ctx); *ctx = NULL;
		return err;
	}
	if (ts_algo_tree_insert(&scope->contexts, *ctx) != TS_ALGO_OK) {
		nowdb_store_destroy(&(*ctx)->store);
		free((*ctx)->name); free(*ctx); *ctx = NULL;
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                      "tree.insert");
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: open all contexts
 * -----------------------------------------------------------------------
 */
static nowdb_err_t openAllContexts(nowdb_scope_t *scope) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_t *list;
	ts_algo_list_node_t *runner;
	nowdb_context_t *ctx;

	if (scope->contexts.count == 0) return NOWDB_OK;

	list = ts_algo_tree_toList(&scope->contexts);
	if (list == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                          FALSE, OBJECT, "tree.toList");

	for(runner=list->head; runner != NULL; runner=runner->nxt) {
		ctx = runner->cont;

		/* fprintf(stderr, "opening %s\n", ctx->name); */

		err = nowdb_context_err(ctx,
		      nowdb_store_configIndexing(&ctx->store,
		                           scope->iman, ctx));
		if (err != NOWDB_OK) break;

		err = nowdb_context_err(ctx, nowdb_store_open(&ctx->store));
		if (err != NOWDB_OK) break;
	}
	ts_algo_list_destroy(list); free(list);
	return err;
}

/* -----------------------------------------------------------------------
 * Helper: find a context
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t findContext(nowdb_scope_t  *scope,
                                      char            *name,
                                      nowdb_context_t **ctx) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_context_t search;
	uint32_t s;

	s = strnlen(name, NOWDB_MAX_NAME+1);
	if (s >= NOWDB_MAX_NAME) return nowdb_err_get(nowdb_err_invalid,
	                     FALSE, OBJECT, "name too long (max: 255)");
	search.name = malloc(s+1); /* here a slab could be very useful */
	if (search.name == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                           "allocating name string");
	}
	strcpy(search.name, name);

	*ctx = ts_algo_tree_find(&scope->contexts, &search);
	if (*ctx == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
	                        FALSE, OBJECT, search.name);
	}
	free(search.name);
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: read one line from the catalog
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t readCatalogLine(nowdb_scope_t *scope,
                                          char *buf, uint32_t *off,
                                          nowdb_version_t ver) {
	nowdb_err_t        err;
	nowdb_context_t   *ctx;
	nowdb_ctx_config_t cfg;
	uint32_t i;

	cfg.sort = 1;
	cfg.encp = NOWDB_ENCP_NONE;

	memcpy(&cfg.allocsize, buf+*off, 4); *off += 4;
	memcpy(&cfg.largesize, buf+*off, 4); *off += 4;
	memcpy(&cfg.sorters, buf+*off, 4); *off += 4;
	memcpy(&cfg.comp, buf+*off, 4); *off += 4; *off += 4;

	for(i=0;i<255;i++) {
		if (buf[*off+i] == 0) break;
	}
	if (i > 255) return nowdb_err_get(nowdb_err_catalog, FALSE, OBJECT,
	                                                "no context name");

	err = initContext(scope, buf+*off, &cfg, ver, &ctx);
	if (err != NOWDB_OK) return err;

	*off += i + 1;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: read catalog
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t readCatalog(nowdb_scope_t *scope) {
	nowdb_err_t     err;
	uint32_t      magic;
	nowdb_version_t ver;
	char           *buf;
	uint32_t         sz;
	uint32_t      off=0;

	sz = nowdb_path_filesize(scope->catalog);
	if (sz == 0) return nowdb_err_get(nowdb_err_stat, FALSE, OBJECT,
	                                                scope->catalog); 
	buf = malloc(sz);
	if (buf == NULL) return nowdb_err_get(nowdb_err_stat, FALSE, OBJECT,
	                                               "allocating buffer");
	err = nowdb_readFile(scope->catalog, buf, sz);
	if (err != NOWDB_OK) {
		free(buf); return err;
	}
	memcpy(&magic, buf, 4); off+=4;
	if (magic != NOWDB_MAGIC) return nowdb_err_get(nowdb_err_magic,
	                                FALSE, OBJECT, scope->catalog);
	memcpy(&ver, buf+off, 4); off+=4;

	while(off < sz) {
		err = readCatalogLine(scope, buf, &off, ver);
		if (err != NOWDB_OK) break;
	}
	free(buf);
	if (err != NOWDB_OK) {
		ts_algo_tree_destroy(&scope->contexts);
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Helper: drop index
 * -----------------------------------------------------------------------
 */
static nowdb_err_t dropIndex(nowdb_scope_t     *scope,
                             nowdb_index_desc_t *desc)  {
	nowdb_err_t err;
	char *tmp, *path;
	
	if (desc->ctx != NULL) {
		err = mkthisctxpath(scope, desc->ctx->name, &path);
	} else {
		err = mkvtxpath(scope, &path);
	}
	if (err != NOWDB_OK) return err;

	err = mkidxpath(path, &tmp); free(path);
	if (err != NOWDB_OK) return err;

	err = mkanypath(tmp, desc->name, &path); free(tmp);
	if (err != NOWDB_OK) return err;

	err = nowdb_index_man_unregister(scope->iman, desc->name);
	if (err != NOWDB_OK) {
		free(path); return err;
	}
	err = nowdb_index_drop(path); free(path);
	return err;
}

/* -----------------------------------------------------------------------
 * Helper: drop all indexes of given context
 * -----------------------------------------------------------------------
 */
static nowdb_err_t dropAllIndices(nowdb_scope_t *scope,
                                  nowdb_context_t *ctx) {
	nowdb_err_t     err;
	ts_algo_list_t idxs;
	ts_algo_list_node_t *runner;

	ts_algo_list_init(&idxs);
	err = nowdb_index_man_getAllOf(scope->iman, ctx, &idxs);
	if (err != NOWDB_OK) {
		ts_algo_list_destroy(&idxs); return err;
	}
	for (runner=idxs.head; runner!=NULL; runner=runner->nxt) {
		err = dropIndex(scope, runner->cont);
		if (err != NOWDB_OK) break;
	}
	ts_algo_list_destroy(&idxs);
	return err;
}

/* -----------------------------------------------------------------------
 * Helper: brutally remove indeices from disk
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t removeAllIndices(nowdb_scope_t *scope,
                                           nowdb_context_t *ctx) {
	nowdb_err_t  err;
	char *tmp, *path;
	
	if (ctx != NULL) {
		err = mkthisctxpath(scope, ctx->name, &tmp);
	} else {
		err = mkvtxpath(scope, &tmp);
	}
	if (err != NOWDB_OK) return err;

	err = mkidxpath(tmp, &path); free(tmp);
	if (err != NOWDB_OK) return err;

	err = nowdb_path_rRemove(path); free(path);
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: drop all contexts
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t dropAllContexts(nowdb_scope_t *scope) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_t *tmp;
	ts_algo_list_node_t *runner;
	nowdb_context_t   *ctx;

	if (scope->contexts.count == 0) {
		err = readCatalog(scope);
		if (err != NOWDB_OK) return err;
	}
	if (scope->contexts.count == 0) return NOWDB_OK;

	tmp = ts_algo_tree_toList(&scope->contexts);
	if (tmp == NULL) return nowdb_err_get(nowdb_err_no_mem,
		                 FALSE, OBJECT, "tree.toList");
	for(runner=tmp->head; runner!=NULL; runner=runner->nxt) {
		ctx = runner->cont;

		NOWDB_IGNORE(nowdb_store_close(&ctx->store));

		err = removeAllIndices(scope, ctx);
		if (err != NOWDB_OK) {
			ts_algo_list_destroy(tmp); free(tmp);
			return err;
		}

		err = nowdb_context_err(ctx, nowdb_store_drop(&ctx->store));
		if (err != NOWDB_OK) {
			ts_algo_list_destroy(tmp); free(tmp);
			return err;
		}
		ts_algo_tree_delete(&scope->contexts, ctx);
	}
	ts_algo_list_destroy(tmp); free(tmp);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: close all contexts
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t closeAllContexts(nowdb_scope_t *scope) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_t *tmp;
	ts_algo_list_node_t *runner;
	nowdb_context_t   *ctx;

	if (scope->contexts.count == 0) return NOWDB_OK;

	tmp = ts_algo_tree_toList(&scope->contexts);
	if (tmp == NULL) return nowdb_err_get(nowdb_err_no_mem,
		                 FALSE, OBJECT, "tree.toList");
	for(runner=tmp->head; runner!=NULL; runner=runner->nxt) {
		ctx = runner->cont;

		/* fprintf(stderr, "closing %s\n", ctx->name); */

		err = nowdb_context_err(ctx, nowdb_store_close(&ctx->store));
		if (err != NOWDB_OK) {
			ts_algo_list_destroy(tmp); free(tmp);
			return err;
		}
		ts_algo_tree_delete(&scope->contexts, ctx);
	}
	ts_algo_list_destroy(tmp); free(tmp);
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Create a scope physically on disk
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_create(nowdb_scope_t *scope) {
	nowdb_path_t p;
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	if (scope == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "scope object is NULL");

	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) return err;

	if (nowdb_path_exists(scope->path, NOWDB_DIR_TYPE_ANY)) {
		err = nowdb_err_get(nowdb_err_create, FALSE, OBJECT,
		                                       scope->path);
		goto unlock;
	}

	/* create base path */
	err = nowdb_dir_create(scope->path);
	if (err != NOWDB_OK) goto unlock;

	/* create model dir */
	p = nowdb_path_append(scope->path, "model");
	if (p == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                 "allocating path");
		goto unlock;
	}
	err = nowdb_dir_create(p); free(p);
	if (err != NOWDB_OK) goto unlock;

	/* create context dir */
	p = nowdb_path_append(scope->path, "context");
	if (p == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                 "allocating path");
		goto unlock;
	}
	err = nowdb_dir_create(p); free(p);
	if (err != NOWDB_OK) goto unlock;

	/* create vertex store */
	err = nowdb_store_create(&scope->vertices);
	if (err != NOWDB_OK) goto unlock;

	/* write catalog */
	err = storeCatalog(scope);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Helper: remove directory (if it exists)
 * -----------------------------------------------------------------------
 */
static nowdb_err_t removeFile(nowdb_scope_t *scope, char *file) {
	nowdb_err_t err;
	char *p;
	struct stat st;

	p = nowdb_path_append(scope->path, file);
	if (p == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                  "allocating path");
	}
	if (stat(p, &st) != 0) {
		free(p); return NOWDB_OK;
	}
	err = nowdb_path_remove(p); free(p);
	return err;
}

/* -----------------------------------------------------------------------
 * Drop a scope physically from disk
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_drop(nowdb_scope_t *scope) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	if (scope == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "scope object is NULL");

	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) return err;

	if (!nowdb_path_exists(scope->path, NOWDB_DIR_TYPE_DIR)) {
		err = nowdb_err_get(nowdb_err_drop, FALSE, OBJECT,
		                                     scope->path);
		goto unlock;
	}

	/* drop contexts */
	err = dropAllContexts(scope);
	if (err != NOWDB_OK) goto unlock;

	/* remove context dir */
	err = removeFile(scope, "context");
	if (err != NOWDB_OK) goto unlock;

	/* drop vertex indices */
	err = removeAllIndices(scope, NULL);
	if (err != NOWDB_OK) goto unlock;

	/* drop vertex store */
	err = nowdb_store_drop(&scope->vertices);
	if (err != NOWDB_OK) goto unlock;

	/* remove model dir */
	err = removeFile(scope, "model");
	if (err != NOWDB_OK) goto unlock;

	/* remove icat */
	err = removeFile(scope, "icat");
	if (err != NOWDB_OK) goto unlock;

	/* remove catalog */
	err = nowdb_path_remove(scope->catalog);
	if (err != NOWDB_OK) goto unlock;

	/* remove base */
	err = nowdb_path_remove(scope->path);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Open a scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_open(nowdb_scope_t *scope) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	if (scope == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "scope object is NULL");

	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) return err;

	if (scope->state == NOWDB_SCOPE_OPEN) {
		return nowdb_unlock_write(&scope->lock);
	}

	if (!nowdb_path_exists(scope->path, NOWDB_DIR_TYPE_DIR)) {
		err = nowdb_err_get(nowdb_err_open, FALSE, OBJECT,
		                                     scope->path);
		goto unlock;
	}
	if (!nowdb_path_exists(scope->catalog, NOWDB_DIR_TYPE_FILE)) {
		err = nowdb_err_get(nowdb_err_open, FALSE, OBJECT,
		                                   scope->catalog);
		goto unlock;
	}
	
	err = nowdb_store_open(&scope->vertices);
	if (err != NOWDB_OK) goto unlock;

	err = readCatalog(scope);
	if (err != NOWDB_OK) {
		NOWDB_IGNORE(nowdb_store_close(&scope->vertices));
		goto unlock;
	}

	err = initIndexMan(scope);
	if (err != NOWDB_OK) {
		NOWDB_IGNORE(nowdb_store_close(&scope->vertices));
		goto unlock;
	}

	err = openAllContexts(scope);
	if (err != NOWDB_OK) {
		NOWDB_IGNORE(nowdb_store_close(&scope->vertices));
		goto unlock;
	}

	scope->state = NOWDB_SCOPE_OPEN;
unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Close a scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_close(nowdb_scope_t *scope) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	SCOPENULL();

	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) return err;

	if (scope->state == NOWDB_SCOPE_CLOSED) {
		return nowdb_unlock_write(&scope->lock);
	}

	err = storeCatalog(scope);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_store_close(&scope->vertices);
	if (err != NOWDB_OK) goto unlock;

	err = closeAllContexts(scope);
	if (err != NOWDB_OK) goto unlock;

	if (scope->iman != NULL) {
		nowdb_index_man_destroy(scope->iman);
		free(scope->iman); scope->iman = NULL;
	}

	scope->state = NOWDB_SCOPE_CLOSED;
unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Create a context within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_createContext(nowdb_scope_t    *scope,
                                      char              *name,
                                      nowdb_ctx_config_t *cfg) 
{
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_context_t *ctx;

	SCOPENULL();

	if (name  == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "name is NULL");
	if (cfg == NULL) return nowdb_err_get(nowdb_err_invalid,
	                 FALSE, OBJECT, "configurator is NULL");

	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) return err;

	if (scope->state == NOWDB_SCOPE_CLOSED) {
		err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                "scope is not open");
		goto unlock;
	}

	err = findContext(scope, name, &ctx);
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_key_not_found) {
			nowdb_err_release(err);
		} else {
			goto unlock;
		}
	} else {
		err = nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT, name);
		goto unlock;
	}

	err = initContext(scope, name, cfg, scope->ver, &ctx);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_context_err(ctx, nowdb_store_create(&ctx->store));
	if (err != NOWDB_OK) {
		ts_algo_tree_delete(&scope->contexts, ctx);
		goto unlock;
	}
	err = nowdb_context_err(ctx, nowdb_store_open(&ctx->store));
	if (err != NOWDB_OK) goto unlock;

	err = storeCatalog(scope);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Drop a context within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_dropContext(nowdb_scope_t *scope,
                                    char          *name) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_context_t *ctx;

	SCOPENULL();
	if (name  == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "name is NULL");

	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) goto unlock;

	if (scope->state == NOWDB_SCOPE_CLOSED) {
		err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                "scope is not open");
		goto unlock;
	}

	err = findContext(scope, name, &ctx);
	if (err != NOWDB_OK) goto unlock;

	NOWDB_IGNORE(nowdb_store_close(&ctx->store));

	err = dropAllIndices(scope, ctx);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_store_drop(&ctx->store);
	if (err != NOWDB_OK) goto unlock;

	ts_algo_tree_delete(&scope->contexts, ctx);
unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Get context within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_getContext(nowdb_scope_t   *scope,
                                   char             *name,
                                   nowdb_context_t **ctx) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	SCOPENULL();
	if (name  == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "name is NULL");
	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) goto unlock;

	if (scope->state == NOWDB_SCOPE_CLOSED) {
		err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                "scope is not open");
		goto unlock;
	}

	err = findContext(scope, name, ctx);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Create index within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_createIndex(nowdb_scope_t     *scope,
                                    char               *name,
                                    char            *context,
                                    nowdb_index_keys_t *keys,
                                    uint16_t          sizing) {
	nowdb_err_t err2,err=NOWDB_OK;
	nowdb_context_t *ctx=NULL;
	nowdb_index_desc_t *desc=NULL;
	nowdb_index_keys_t *k;
	uint16_t sz;
	char *path = NULL;
	char *tmp;

	SCOPENULL();

	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "name is NULL");
	if (keys == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "keys is NULL");

	sz = sizing==0?NOWDB_CONFIG_SIZE_BIG:sizing;

	/* currently, we lock the scope
	 * for the entire process. That's
	 * not a good solution, but ok
	 * as a first approach */
	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) return err;

	if (scope->state == NOWDB_SCOPE_CLOSED) {
		err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                "scope is not open");
		goto unlock;
	}
	if (context != NULL) {
		err = findContext(scope, context, &ctx);
		if (err != NOWDB_OK) goto unlock;

		err = mkthisctxpath(scope, ctx->name, &tmp);
		if (err != NOWDB_OK) goto unlock;
		
	} else {
		err = mkvtxpath(scope, &tmp);
		if (err != NOWDB_OK) goto unlock;
	}

	err = mkidxpath(tmp, &path); free(tmp);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_index_keys_copy(keys, &k);
	if (err != NOWDB_OK) {
		free(path); goto unlock;
	}

	err = nowdb_index_desc_create(name, ctx, k, NULL, &desc);
	if (err != NOWDB_OK) {
		nowdb_index_keys_destroy(k);
		free(path); goto unlock;
	}

	err = nowdb_index_man_register(scope->iman, desc);
	if (err != NOWDB_OK) {
		nowdb_index_desc_destroy(desc); free(desc);
		free(path); goto unlock;
	}

	err = nowdb_index_create(path, sz, desc); 
	if (err != NOWDB_OK) {
		free(path);
		NOWDB_IGNORE(nowdb_index_man_unregister(scope->iman, name));
		goto unlock;
	}

	err = nowdb_index_open(path, nowdb_lib(), desc); free(path);
	if (err != NOWDB_OK) {
		NOWDB_IGNORE(nowdb_index_man_unregister(scope->iman, name));
		goto unlock;
	}

	/* TODO: write missing data */

unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Drop index within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_dropIndex(nowdb_scope_t *scope,
                                  char          *name) {

	nowdb_err_t err2,err=NOWDB_OK;
	nowdb_index_desc_t *desc;

	SCOPENULL();
	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "name is NULL");

	/* currently, we lock the scope
	 * for the entire process. That's
	 * not a good solution, but ok
	 * as a first approach */
	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) {
		return err;
	}
	if (scope->state == NOWDB_SCOPE_CLOSED) {
		err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                "scope is not open");
		goto unlock;
	}

	err = nowdb_index_man_getByName(scope->iman, name, &desc);
	if (err != NOWDB_OK) goto unlock;

	err = dropIndex(scope, desc);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Get index within that scope by name
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_getIndexByName(nowdb_scope_t   *scope,
                                       char            *name,
                                       nowdb_index_t   **idx) {
	nowdb_err_t err2, err=NOWDB_OK;
	nowdb_index_desc_t   *desc;

	SCOPENULL();

	if (idx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "idx is NULL");
	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "name is NULL");

	err = nowdb_lock_read(&scope->lock);
	if (err != NOWDB_OK) return err;

	if (scope->state == NOWDB_SCOPE_CLOSED) {
		err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                 "scope is not open");
		goto unlock;
	}

	err = nowdb_index_man_getByName(scope->iman, name, &desc);
	if (err != NOWDB_OK) goto unlock;

	if (desc->idx == NULL) {
		err = nowdb_err_get(nowdb_err_nosuch_index, FALSE, OBJECT,
		              "index pre-registered but not yet created");
		goto unlock;
	}
	*idx = desc->idx;

unlock:
	err2 = nowdb_unlock_read(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Get index within that scope by definition
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_getIndex(nowdb_scope_t   *scope,
                                 char          *context,
                                 nowdb_index_keys_t  *k,
                                 nowdb_index_t    **idx) {
	nowdb_err_t err2, err=NOWDB_OK;
	nowdb_context_t  *ctx=NULL;
	nowdb_index_desc_t   *desc;

	SCOPENULL();

	if (idx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "idx is NULL");

	err = nowdb_lock_read(&scope->lock);
	if (err != NOWDB_OK) return err;

	if (scope->state == NOWDB_SCOPE_CLOSED) {
		err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                 "scope is not open");
		goto unlock;
	}
	if (context != NULL) {
		err = findContext(scope, context, &ctx);
		if (err != NOWDB_OK) goto unlock;
	}

	err = nowdb_index_man_getByKeys(scope->iman, ctx, k, &desc);
	if (err != NOWDB_OK) goto unlock;

	if (desc->idx == NULL) {
		err = nowdb_err_get(nowdb_err_nosuch_index, FALSE, OBJECT,
		              "index pre-registered but not yet created");
		goto unlock;
	}
	*idx = desc->idx;

unlock:
	err2 = nowdb_unlock_read(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Insert one record
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_insert(nowdb_scope_t *scope,
                               char        *context,
                               void          *data);

