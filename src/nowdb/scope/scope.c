/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Scope: a collection of contexts
 * ========================================================================
 */
#include <nowdb/scope/scope.h>
#include <nowdb/io/dir.h>
#include <nowdb/reader/reader.h>

#include <beet/types.h>
#include <beet/index.h>

static char *OBJECT = "scope";

#define VINDEX "_vindex"

/* ------------------------------------------------------------------------
 * Macro: scope NULL
 * ------------------------------------------------------------------------
 */
#define SCOPENULL() \
	if (scope == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                          FALSE, OBJECT, "scope is NULL");

#define NOMEM(x) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, x);

#define SCOPENOTOPEN() \
	if (scope->state != NOWDB_SCOPE_OPEN) { \
		err = nowdb_err_get(nowdb_err_invalid, \
		      FALSE, OBJECT, "state not open"); \
		goto unlock; \
	}

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

/* ------------------------------------------------------------------------
 * Helper: make beet error
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t makeBeetError(beet_err_t ber) {
	nowdb_err_t err, err2=NOWDB_OK;

	if (ber == BEET_OK) return NOWDB_OK;
	if (ber < BEET_OSERR_ERRNO) {
		err2 = nowdb_err_get(nowdb_err_beet, FALSE, OBJECT,
		                          (char*)beet_oserrdesc());
	}
	err = nowdb_err_get(nowdb_err_beet, FALSE, OBJECT,
	                         (char*)beet_errdesc(ber));
	if (err == NOWDB_OK) return err2; else err->cause = err2;
	return err;
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

	if (scope != NULL) {
		tmp = nowdb_path_append(scope->path, "context");
	} else {
		tmp = strdup("context");
	}
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

	if (scope != NULL) {
		*path = nowdb_path_append(scope->path, "vertex");
	} else {
		*path = strdup("vertex");
	}
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
	                           scope->path,
	                           imanpath,
	                           ctxpath,
	                           vtxpath);

	free(imanpath); free(ctxpath); free(vtxpath);

	if (err != NOWDB_OK) {
		free(scope->iman); scope->iman = NULL;
		return err;
	}

	scope->vertices.iman = scope->iman;

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: create and open model
 * -----------------------------------------------------------------------
 */
static nowdb_err_t openModel(nowdb_scope_t *scope) {
	nowdb_err_t err;
	nowdb_path_t p;

	if (scope->model != NULL) return NOWDB_OK;

	p = nowdb_path_append(scope->path, "model");
	if (p == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                            "allocating model path");
	}
	if (!nowdb_path_exists(p, NOWDB_DIR_TYPE_DIR)) {
		fprintf(stderr, "%s does not exist\n", p);
	}
	scope->model = calloc(1,sizeof(nowdb_model_t));
	if (scope->model == NULL) {
		free(p);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                 "allocating model");
	}
	err = nowdb_model_init(scope->model, p); free(p);
	if (err != NOWDB_OK) {
		free(scope->model); scope->model = NULL;
		return err;
	}
	return nowdb_model_load(scope->model);
}

/* -----------------------------------------------------------------------
 * Helper: initialise vertex
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t initVertex(nowdb_scope_t *scope,
                                     nowdb_version_t  ver) {
	nowdb_path_t p;
	nowdb_err_t err;

	err = mkvtxpath(scope, &p);
	if (err != NOWDB_OK) return err;

	err = nowdb_store_init(&scope->vertices, p,
	                       scope->evache, ver,
	                       sizeof(nowdb_vertex_t),
	                       NOWDB_FILE_MAPSIZE,
	                       NOWDB_MEGA *
	                       sizeof(nowdb_vertex_t));
	free(p);
	if (err != NOWDB_OK) return NOWDB_OK;

	err = nowdb_store_configSort(&scope->vertices,
	                  &nowdb_sort_vertex_compare);
	if (err != NOWDB_OK) {
		nowdb_store_destroy(&scope->vertices);
		return err;
	}
	/*
	err = nowdb_store_configCompression(&(*ctx)->store, cfg->comp);
	if (err != NOWDB_OK) {
		nowdb_store_destroy(&scope->vertices);
		return err;
	}
	*/
	err = nowdb_store_configIndexing(&scope->vertices, scope->iman, NULL);
	if (err != NOWDB_OK) {
		nowdb_store_destroy(&scope->vertices);
		return err;
	}
	err = nowdb_store_configWorkers(&scope->vertices, 2);
	if (err != NOWDB_OK) {
		nowdb_store_destroy(&scope->vertices);
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
	scope->model = NULL;
	scope->vindex = NULL;
	scope->evache = NULL;
	scope->ivache = NULL;
	scope->text = NULL;
	scope->pman = NULL;
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
	err = initVertex(scope, ver);
	if (err != NOWDB_OK) {
		free(scope->path); scope->path = NULL;
		free(scope->catalog); scope->catalog = NULL;
		nowdb_rwlock_destroy(&scope->lock);
		ts_algo_tree_destroy(&scope->contexts);
		return err;
	}

	/* external vertex cache */
	scope->evache = calloc(1,sizeof(nowdb_plru12_t));
	if (scope->evache == NULL) {
		nowdb_scope_destroy(scope);
		NOMEM("external vertex cache");
		return err;
	}
	err = nowdb_plru12_init(scope->evache, 100000);
	if (err != NOWDB_OK) {
		nowdb_scope_destroy(scope);
		return err;
	}

	/* internal vertex cache */
	scope->ivache = calloc(1,sizeof(nowdb_plru12_t));
	if (scope->ivache == NULL) {
		nowdb_scope_destroy(scope);
		NOMEM("internal vertex cache");
		return err;
	}
	err = nowdb_plru12_init(scope->ivache, 2500000);
	if (err != NOWDB_OK) {
		nowdb_scope_destroy(scope);
		return err;
	}
	
	/* text */
	p = nowdb_path_append(path, "text");
	if (p == NULL) {
		nowdb_scope_destroy(scope);
		NOMEM("allocating path");
		return err;
	}
	scope->text = calloc(1, sizeof(nowdb_text_t));
	if (scope->text == NULL) {
		free(p);
		nowdb_scope_destroy(scope);
		NOMEM("allocating text");
		return err;
	}
	err = nowdb_text_init(scope->text, p); free(p);
	if (err != NOWDB_OK) {
		nowdb_scope_destroy(scope);
		return err;
	}

	/* stored procedures */
	err = nowdb_procman_new(&scope->pman, path);
	if (err != NOWDB_OK) {
		nowdb_scope_destroy(scope);
		return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Destroy scope
 * -----------------------------------------------------------------------
 */
void nowdb_scope_destroy(nowdb_scope_t *scope) {
	if (scope == NULL) return;
	if (scope->evache != NULL) {
		nowdb_plru12_destroy(scope->evache);
		free(scope->evache); scope->evache = NULL;
	}
	if (scope->ivache != NULL) {
		nowdb_plru12_destroy(scope->ivache);
		free(scope->ivache); scope->ivache = NULL;
	}
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
	if (scope->model != NULL) {
		nowdb_model_destroy(scope->model);
		free(scope->model); scope->model = NULL;
	}
	if (scope->text != NULL) {
		nowdb_text_destroy(scope->text);
		free(scope->text); scope->text = NULL;
	}
	if (scope->pman != NULL) {
		nowdb_procman_destroy(scope->pman);
		free(scope->pman); scope->pman = NULL;
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
	      nowdb_store_init(&(*ctx)->store, p,
	                         NULL, ver,
	                         sizeof(nowdb_edge_t),
	                               cfg->allocsize,
	                              cfg->largesize));
	free(p);
	if (err != NOWDB_OK) {
		free((*ctx)->name); free(*ctx); *ctx = NULL;
		return err;
	}
	err = nowdb_store_configSort(&(*ctx)->store, &nowdb_sort_edge_compare);
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
	char *tmp, *path=NULL;
	
	if (desc->ctx != NULL) {
		err = mkthisctxpath(NULL, desc->ctx->name, &path);
	} else {
		err = mkvtxpath(NULL, &path);
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
	err = nowdb_index_drop(scope->path, path); free(path);
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
	char *tmp = NULL;
	char *path= NULL;
	
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

	/* text */
	err = nowdb_dir_create(scope->text->path);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_text_create(scope->text);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Helper: remove model
 * -----------------------------------------------------------------------
 */
static nowdb_err_t removeModel(nowdb_scope_t *scope) {
	nowdb_err_t err;
	nowdb_path_t  p;

	p = nowdb_path_append(scope->path, "model");
	if (p == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                      FALSE, OBJECT, "allocate path");

	err = nowdb_path_rRemove(p); free(p);
	if (err != NOWDB_OK) return err; 

	return NOWDB_OK;
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

	/* remove model */
	err = removeModel(scope);
	if (err != NOWDB_OK) goto unlock;

	/* drop text */
	err = nowdb_text_drop(scope->text);
	if (err != NOWDB_OK) goto unlock;

	/* remove icat */
	err = removeFile(scope, "icat");
	if (err != NOWDB_OK) goto unlock;

	/* remove pcat */
	err = removeFile(scope, "pcat");
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
 * Helper: create index internally
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t createIndex(nowdb_scope_t     *scope,
                                      char               *name,
                                      char            *context,
                                      nowdb_index_keys_t *keys,
                                      uint16_t          sizing) {
	nowdb_err_t err;
	nowdb_context_t *ctx=NULL;
	nowdb_index_desc_t *desc=NULL;
	nowdb_index_keys_t *k;
	char *path = NULL;
	char *tmp  = NULL;

	if (context != NULL) {
		err = findContext(scope, context, &ctx);
		if (err != NOWDB_OK) return err;

		err = mkthisctxpath(NULL, ctx->name, &tmp);
		if (err != NOWDB_OK) return err;
		
	} else {
		err = mkvtxpath(NULL, &tmp);
		if (err != NOWDB_OK) return err;
	}

	err = mkidxpath(tmp, &path); free(tmp);
	if (err != NOWDB_OK) return err;

	err = nowdb_index_keys_copy(keys, &k);
	if (err != NOWDB_OK) {
		free(path); return err;
	}

	err = nowdb_index_desc_create(name, ctx, k, NULL, &desc);
	if (err != NOWDB_OK) {
		nowdb_index_keys_destroy(k);
		free(path); return err;
	}

	err = nowdb_index_man_register(scope->iman, desc);
	if (err != NOWDB_OK) {
		nowdb_index_desc_destroy(desc); free(desc);
		free(path); return err;
	}

	err = nowdb_index_create(scope->path, path, sizing, desc); 
	if (err != NOWDB_OK) {
		free(path);
		NOWDB_IGNORE(nowdb_index_man_unregister(scope->iman, name));
		return err;
	}

	err = nowdb_index_open(scope->path, path, nowdb_lib(), desc);
	free(path);
	if (err != NOWDB_OK) {
		NOWDB_IGNORE(nowdb_index_man_unregister(scope->iman, name));
		return err;
	}

	/* TODO: write missing data */
	
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: check whether vertex index was already created
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t getVIndex(nowdb_scope_t *scope, char *x) {
	nowdb_index_desc_t *desc;
	nowdb_err_t err;

	err = nowdb_index_man_getByName(scope->iman, VINDEX, &desc);
	if (err == NOWDB_OK) {
		scope->vindex = desc->idx;
		*x = 1; return NOWDB_OK;
	}
	if (err->errcode == nowdb_err_key_not_found) {
		nowdb_err_release(err);
		*x = 0; return NOWDB_OK;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Helper: create vertex index
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t createVIndex(nowdb_scope_t *scope) {
	nowdb_err_t err;
	nowdb_index_keys_t *keys;
	char x=0;

	err = nowdb_index_keys_create(&keys, 2, NOWDB_OFF_ROLE,
	                                      NOWDB_OFF_VERTEX);
	if (err != NOWDB_OK) return err;

	err = createIndex(scope, VINDEX, NULL, keys,
	                    NOWDB_CONFIG_SIZE_TINY);
	nowdb_index_keys_destroy(keys);
	if (err != NOWDB_OK) return err;

	err = getVIndex(scope, &x);
	if (err != NOWDB_OK) return err;

	if (!x) return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT,
	                         "cannot create index on vertices");

	return NOWDB_OK;
	
}

/* -----------------------------------------------------------------------
 * Helper: fill the evache with pending vertices
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t fillEVache(nowdb_scope_t *scope) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2;
	ts_algo_list_t pending;
	nowdb_reader_t *reader=NULL;
	nowdb_key_t vid;
	nowdb_roleid_t role;
	char *page;

	ts_algo_list_init(&pending);

	err = nowdb_lock_read(&scope->vertices.lock);
	if (err != NOWDB_OK) return err;

	// this is a generic pattern and
	// shall go to a generic reader library
	err = nowdb_store_getAllWaiting(&scope->vertices, &pending);
	if (err != NULL) goto unlock;

	err = nowdb_reader_fullscan(&reader, &pending, NULL);
	if (err != NOWDB_OK) goto unlock;

	while((err = nowdb_reader_move(reader)) == NOWDB_OK) {
		page = nowdb_reader_page(reader);
		for(int i=0; i<NOWDB_IDX_PAGE; i+=NOWDB_VERTEX_SIZE) {
			char x=0;
			if (memcmp(page+i, nowdb_nullrec, 32) == 0) break;
			memcpy(&vid, page+i+NOWDB_OFF_VERTEX, 8);
			memcpy(&role, page+i+NOWDB_OFF_ROLE, 4);
			err = nowdb_plru12_get(scope->evache, role, vid, &x);
			if (err != NOWDB_OK) break;
			if (x == 1) continue;
			// add as resident!
			err = nowdb_plru12_addResident(scope->evache,
			                                  role, vid);
			if (err != NOWDB_OK) break;
		}
	}
	if (err != NOWDB_OK) {
		if (nowdb_err_contains(err, nowdb_err_eof)) {
			nowdb_err_release(err); err = NOWDB_OK;
		}
		goto unlock;
	}

unlock:
	nowdb_store_destroyFiles(&scope->vertices, &pending);
	if (reader != NULL) {
		nowdb_reader_destroy(reader); free(reader);
	}
	err2 = nowdb_unlock_read(&scope->vertices.lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
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
	char x = 0;

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

	err = readCatalog(scope);
	if (err != NOWDB_OK) goto unlock;

	err = initIndexMan(scope);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_procman_load(scope->pman);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_store_open(&scope->vertices);
	if (err != NOWDB_OK) {
		nowdb_index_man_destroy(scope->iman);
		free(scope->iman); scope->iman = NULL;
		goto unlock;
	}
	err = fillEVache(scope);
	if (err != NOWDB_OK) {
		nowdb_index_man_destroy(scope->iman);
		free(scope->iman); scope->iman = NULL;
		goto unlock;
	}

	err = openAllContexts(scope);
	if (err != NOWDB_OK) {
		nowdb_index_man_destroy(scope->iman);
		free(scope->iman); scope->iman = NULL;
		NOWDB_IGNORE(nowdb_store_close(&scope->vertices));
		goto unlock;
	}

	err = openModel(scope);
	if (err != NOWDB_OK) {
		nowdb_index_man_destroy(scope->iman);
		free(scope->iman); scope->iman = NULL;
		NOWDB_IGNORE(nowdb_store_close(&scope->vertices));
		NOWDB_IGNORE(closeAllContexts(scope));
		goto unlock;
	}

	err = nowdb_text_open(scope->text);
	if (err != NOWDB_OK) {
		nowdb_index_man_destroy(scope->iman);
		free(scope->iman); scope->iman = NULL;
		NOWDB_IGNORE(nowdb_store_close(&scope->vertices));
		NOWDB_IGNORE(closeAllContexts(scope));
		nowdb_model_destroy(scope->model);
		free(scope->model); scope->model = NULL;
		goto unlock;
	}

	err = getVIndex(scope, &x);
	if (err != NOWDB_OK) {
		nowdb_index_man_destroy(scope->iman);
		free(scope->iman); scope->iman = NULL;
		NOWDB_IGNORE(nowdb_store_close(&scope->vertices));
		NOWDB_IGNORE(closeAllContexts(scope));
		nowdb_model_destroy(scope->model);
		free(scope->model); scope->model = NULL;
		goto unlock;
	}

	if (!x) err = createVIndex(scope);
	if (err != NOWDB_OK) {
		nowdb_index_man_destroy(scope->iman);
		free(scope->iman); scope->iman = NULL;
		NOWDB_IGNORE(nowdb_store_close(&scope->vertices));
		NOWDB_IGNORE(closeAllContexts(scope));
		nowdb_model_destroy(scope->model);
		free(scope->model); scope->model = NULL;
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
	if (scope->model != NULL) {
		nowdb_model_destroy(scope->model);
		free(scope->model); scope->model = NULL;
	}
	err = nowdb_text_close(scope->text);
	if (err != NOWDB_OK) goto unlock;

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
	char t;

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

	// context does not yet exist
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

	// there is no edge nor type with that name
	err = nowdb_model_whatIs(scope->model, name, &t);
	if (err == NOWDB_OK) {
		err = nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT, name);
		goto unlock;
	}
	if (err->errcode != nowdb_err_key_not_found) goto unlock;
	nowdb_err_release(err);

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

	SCOPENOTOPEN();

	err = findContext(scope, name, &ctx);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_store_close(&ctx->store);

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

	SCOPENOTOPEN();

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
	uint16_t sz;

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

	SCOPENOTOPEN();

	err = createIndex(scope, name, context, keys, sz);

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

/* -----------------------------------------------------------------------
 * Create new stored procedure/function
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_createProcedure(nowdb_scope_t  *scope,
                                        nowdb_proc_desc_t *pd) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2;

	SCOPENULL();

	if (pd == NULL) return nowdb_err_get(nowdb_err_invalid,
	              FALSE, OBJECT, "proc descriptor is NULL");

	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) return err;

	err = nowdb_procman_add(scope->pman, pd);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Drop stored procedure/function
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_dropProcedure(nowdb_scope_t *scope,
                                      char          *name) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2;

	SCOPENULL();

	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "name is NULL");

	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) return err;

	err = nowdb_procman_remove(scope->pman, name);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Get stored procedure/function
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_getProcedure(nowdb_scope_t   *scope,
                                     char             *name,
                                     nowdb_proc_desc_t **pd) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2;

	SCOPENULL();

	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "name is NULL");

	err = nowdb_lock_read(&scope->lock);
	if (err != NOWDB_OK) return err;

	err = nowdb_procman_get(scope->pman, name, pd);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_read(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Helper: add propids to props
 * -----------------------------------------------------------------------
 */
static nowdb_err_t addPropids(nowdb_scope_t  *scope,
                              ts_algo_list_t *props) {
	nowdb_err_t err=NOWDB_OK;
	ts_algo_list_node_t *runner;
	nowdb_model_prop_t *prop;

	for(runner=props->head; runner!=NULL; runner=runner->nxt) {
		prop = runner->cont;
		err = nowdb_text_insert(scope->text, prop->name,
		                                  &prop->propid);
		if (err != NOWDB_OK) break;
		/*
		fprintf(stderr, "add prop %s (=%lu) as %u\n",
		         prop->name, prop->propid, prop->pos);
		*/
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Create type within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_createType(nowdb_scope_t     *scope,
                                   char               *name,
                                   ts_algo_list_t    *props) {
	nowdb_err_t err2,err=NOWDB_OK;
	nowdb_context_t *ctx;

	SCOPENULL();

	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "name is NULL");

	/* currently, we lock the scope
	 * for reading, so it cannot be 
	 * closed while we are working */
	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) return err;

	SCOPENOTOPEN();

	// not context with that name exists
	err = findContext(scope, name, &ctx);
	if (err == NOWDB_OK) {
		err = nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT, name);
		goto unlock;
	}
	if (err->errcode != nowdb_err_key_not_found) goto unlock;
	nowdb_err_release(err);

	if (props != NULL) {
		err = addPropids(scope, props);
		if (err != NOWDB_OK) goto unlock;
	}
	err = nowdb_model_addType(scope->model, name, props);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Drop type within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_dropType(nowdb_scope_t     *scope,
                                 char               *name) {
	nowdb_err_t err2,err=NOWDB_OK;

	SCOPENULL();

	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "name is NULL");

	err = nowdb_lock_read(&scope->lock);
	if (err != NOWDB_OK) return err;

	SCOPENOTOPEN();

	err = nowdb_model_removeType(scope->model, name);
unlock:
	err2 = nowdb_unlock_read(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Create edge within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_createEdge(nowdb_scope_t  *scope,
                                   char           *name,
                                   char           *origin,
                                   char           *destin,
                                   uint32_t        label,
                                   uint32_t        weight,
                                   uint32_t        weight2) {
	nowdb_err_t err2,err=NOWDB_OK;
	nowdb_model_edge_t   *e=NULL;
	nowdb_model_vertex_t *v=NULL;
	nowdb_context_t *ctx;

	SCOPENULL();

	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "name is NULL");
	if (origin == NULL) return nowdb_err_get(nowdb_err_invalid,
	                      FALSE, OBJECT, "edge without origin");
	if (destin == NULL) return nowdb_err_get(nowdb_err_invalid,
	                      FALSE, OBJECT, "edge without destin");

	err = nowdb_lock_write(&scope->lock);
	if (err != NOWDB_OK) return err;

	SCOPENOTOPEN();

	// not context with that name exists
	err = findContext(scope, name, &ctx);
	if (err == NOWDB_OK) {
		err = nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT, name);
		goto unlock;
	}
	if (err->errcode != nowdb_err_key_not_found) goto unlock;
	nowdb_err_release(err);

	e = calloc(1,sizeof(nowdb_model_edge_t));
	if (e == NULL) {
		NOMEM("allocating edge");
		goto unlock;
	}

	e->name = strdup(name);
	if (e->name == NULL) {
		free(e);
		NOMEM("allocating edge name");
		goto unlock;
	}

	e->edge = NOWDB_MODEL_TEXT;
	e->label = label==NOWDB_TYP_TEXT?
	                  NOWDB_MODEL_TEXT:
	                  NOWDB_MODEL_NUM;
	e->weight = weight;
	e->weight2 = weight2;

	err = nowdb_model_getVertexByName(scope->model, origin, &v);
	if (err != NOWDB_OK) {
		free(e->name); free(e);
		goto unlock;
	}
	e->origin = v->roleid;

	err = nowdb_model_getVertexByName(scope->model, destin, &v);
	if (err != NOWDB_OK) {
		free(e->name); free(e);
		goto unlock;
	}
	e->destin = v->roleid;

	err = nowdb_text_insert(scope->text, name, &e->edgeid);
	if (err != NOWDB_OK) {
		free(e->name); free(e);
		goto unlock;
	}

	err = nowdb_model_addEdge(scope->model, e);
	if (err != NOWDB_OK) {
		free(e->name); free(e);
		goto unlock;
	}

unlock:
	err2 = nowdb_unlock_write(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Drop edge within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_dropEdge(nowdb_scope_t *scope,
                                 char          *name) {
	nowdb_err_t err2,err=NOWDB_OK;
	nowdb_model_edge_t *e;

	SCOPENULL();

	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "name is NULL");

	err = nowdb_lock_read(&scope->lock);
	if (err != NOWDB_OK) return err;

	SCOPENOTOPEN();

	err = nowdb_model_getEdgeByName(scope->model, name, &e);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_model_removeEdge(scope->model, e->edgeid);
	if (err != NOWDB_OK) goto unlock;
unlock:
	err2 = nowdb_unlock_read(&scope->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Register vertex
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_registerVertex(nowdb_scope_t *scope,
                                       nowdb_roleid_t  role,
                                       nowdb_key_t      vid) {
	beet_err_t ber;
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	char key[12];
	char found=0;

	err = nowdb_lock_write(&scope->vertices.lock);
	if (err != NOWDB_OK) return err;

	err = nowdb_plru12_get(scope->evache, role, vid, &found);
	if (err != NOWDB_OK) goto unlock;
	if (found) {
		err = nowdb_err_get(nowdb_err_dup_key,
		              FALSE, OBJECT, "vertex");
		goto unlock;
	}

	err = nowdb_plru12_get(scope->ivache, role, vid, &found);
	if (err != NOWDB_OK) goto unlock;
	if (found) {
		err = nowdb_err_get(nowdb_err_dup_key,
		              FALSE, OBJECT, "vertex");
		goto unlock;
	}

	memcpy(key, &role, 4); memcpy(key+4, &vid, 8);
	ber = beet_index_doesExist(scope->vindex->idx, key);
	if (ber == BEET_ERR_KEYNOF) {
		err = nowdb_plru12_addResident(scope->evache, role, vid);
		goto unlock;
	}
	if (ber != BEET_OK) {
		err = makeBeetError(ber); goto unlock;
	}
	err = nowdb_plru12_add(scope->ivache, role, vid);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT, "vertex");

unlock:
	err2 = nowdb_unlock_write(&scope->vertices.lock);
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
                               nowdb_store_t *store,
                               void          *data);
