/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Index Manager
 * ========================================================================
 */

#include <nowdb/index/man.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

static char *OBJECT = "idxman";

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

/* ------------------------------------------------------------------------
 * Macro: cast to descriptor
 * ------------------------------------------------------------------------
 */
#define DESC(x) \
	((nowdb_index_desc_t*)x)

/* ------------------------------------------------------------------------
 * Tree callbacks: compare by name
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t comparebyname(void *ignore, void *left, void *right) {
	int cmp = strcmp(DESC(left)->name,
	                 DESC(right)->name);
	if (cmp < 0) return ts_algo_cmp_less;
	if (cmp > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Helper: compare offset by offset
 * ------------------------------------------------------------------------
 */
static inline ts_algo_cmp_t comparekeys(nowdb_index_keys_t *left,
                                        nowdb_index_keys_t *right) 
{
	for(int i=0; i<left->sz; i++) {
		if (left->off[i] < right->off[i]) return ts_algo_cmp_less;
		if (left->off[i] > right->off[i]) return ts_algo_cmp_greater;
	}
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Tree callbacks: compare by key
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t comparebykey(void *ignore, void *left, void *right) {
	if (DESC(left)->ctx < DESC(right)->ctx) return ts_algo_cmp_less;
	if (DESC(left)->ctx > DESC(right)->ctx) return ts_algo_cmp_greater;
	if (DESC(left)->keys->sz < 
	    DESC(right)->keys->sz) return ts_algo_cmp_less;
	if (DESC(left)->keys->sz > 
	    DESC(right)->keys->sz) return ts_algo_cmp_greater;
	return comparekeys(DESC(left)->keys, DESC(right)->keys);
}

/* ------------------------------------------------------------------------
 * Tree callbacks for index desc: update (do nothing!)
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Tree callbacks for index desc: delete and destroy
 * ------------------------------------------------------------------------
 */
static void destroydesc(void *ignore, void **n) {
	if (*n != NULL) {
		if (DESC(*n)->idx != NULL) {
			nowdb_index_close(DESC(*n)->idx);
			free(DESC(*n)->idx); DESC(*n)->idx = NULL;
		}
		nowdb_index_desc_destroy((nowdb_index_desc_t*)(*n));
		free(*n); *n = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Tree callbacks for index desc: don't destroy twice!
 * ------------------------------------------------------------------------
 */
static void nodestroy(void *ignore, void **n) {}

/* ------------------------------------------------------------------------
 * Tree callbacks for index desc: filter context
 * ------------------------------------------------------------------------
 */
static ts_algo_bool_t ofctx(void *ignore, const void *ctx, const void *node) {
	if (DESC(node)->ctx == ctx) return TRUE;
	return FALSE;
}

/* ------------------------------------------------------------------------
 * Helper: create file if not exists
 * ------------------------------------------------------------------------
 */
static nowdb_err_t createFile(nowdb_index_man_t *man) {
	struct stat  st;
	FILE *f;

	if (stat(man->path, &st) == 0) return NOWDB_OK;

	f = fopen(man->path, "wb");
	if (f == NULL) return nowdb_err_get(nowdb_err_open,
	                          TRUE, OBJECT, man->path);
	if (fclose(f) != 0) return nowdb_err_get(nowdb_err_close,
                                        TRUE, OBJECT, man->path);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: open file
 * ------------------------------------------------------------------------
 */
static nowdb_err_t openFile(nowdb_index_man_t *man) {
	nowdb_err_t err;

	err = createFile(man);
	if (err != NOWDB_OK) return err;

	man->file = fopen(man->path, "rb+");
	if (man->file == NULL) return nowdb_err_get(nowdb_err_open,
	                                  TRUE, OBJECT, man->path);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load line
 * ------------------------------------------------------------------------
 */
static nowdb_err_t getOneLine(char     *buf,
                              uint32_t   sz,
                              uint32_t *off,
                              char   **obuf) {
	uint32_t i = *off;
	size_t   s;

	while(i < sz && buf[i] != '\n') i++;
	s = i-(*off);

	*obuf=buf+(*off);

	(*off) += s;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: registerIndex
 * ------------------------------------------------------------------------
 */
static nowdb_err_t registerIndex(nowdb_index_man_t  *iman,
                                 nowdb_index_desc_t *desc) {
	nowdb_err_t err;
	nowdb_index_desc_t *tmp;

	tmp = ts_algo_tree_find(iman->byname, desc);
	if (tmp != NULL) {
		return nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT,
		                                        "index name");
	}
	tmp = ts_algo_tree_find(iman->bykey, desc);
	if (tmp != NULL) {
		return nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT,
		                                        "index keys");
	}
	if (ts_algo_tree_insert(iman->byname, desc) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                    "byname.insert");
	}
	if (ts_algo_tree_insert(iman->bykey, desc) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                     "bykey.insert");
		ts_algo_tree_delete(iman->byname, desc);
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: make context path
 * ------------------------------------------------------------------------
 */
/*
static nowdb_err_t getCtxPath(nowdb_index_man_t *man, char *idxname,
                                                      char *ctxname,
                                                      char **path) {
	char *tmp;

	tmp = nowdb_path_append(man->ctxpath, ctxname);
	if (tmp == NULL) return nowdb_err_get(nowdb_err_no_mem,
	              FALSE, OBJECT, "allocating context path");
	*path = nowdb_path_append(tmp, "index"); free(tmp);
	if (*path == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "allocating context path");
	return NOWDB_OK;
}
*/

/* ------------------------------------------------------------------------
 * Helper: make vertex path
 * ------------------------------------------------------------------------
 */
/*
static nowdb_err_t getVertexPath(nowdb_index_man_t *man, char *idxname,
                                                         char **path) {
	*path = nowdb_path_append(man->vxpath, "index");
	if (*path == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "allocating vertex path");
	return NOWDB_OK;
}
*/

/* ------------------------------------------------------------------------
 * Helper: open index
 * ------------------------------------------------------------------------
 */
static nowdb_err_t openIndex(nowdb_index_man_t  *man,
                             nowdb_index_desc_t *desc) {
	nowdb_err_t err;
	char *path=NULL;
	char *tmp;

	if (desc->ctx == NULL) {
		// err = getVertexPath(man, desc->name, &path);
		path = strdup("vertex/index");
	} else {
		// err = getCtxPath(man, desc->name, desc->ctx->name, &path);
		tmp = nowdb_path_append("context", desc->ctx->name);
		if (tmp == NULL) {
			NOMEM("allocating index path");
			return err;
		}
		path = nowdb_path_append(tmp, "index"); free(tmp);
	}
	// if (err != NOWDB_OK) return err;
	if (path == NULL) {
		NOMEM("allocating index path");
		return err;
	}

	err = nowdb_index_open(man->base, path, man->handle, desc); free(path);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: line2desc
 * ------------------------------------------------------------------------
 */
static nowdb_err_t line2desc(nowdb_index_man_t   *man,
                             char *buf, uint16_t  sz,
                             nowdb_index_desc_t **desc) {
	uint32_t i,k;
	uint16_t s;
	char *inm, *cnm=NULL;
	nowdb_context_t tmp;
	nowdb_index_keys_t *keys;

	/* get name */
	for(i=0;i<sz && buf[i] != 0;i++) {
		/* fprintf(stderr, "%c ", buf[i]); */
	}
	/* fprintf(stderr, "\n"); */
	
	if (i>=sz-4 || i == 0) {
		return nowdb_err_get(nowdb_err_catalog, FALSE, OBJECT,
	                                  "no name in index catalog");
	}

	inm = malloc(i+1);
	if (inm == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "allocating index name");

	i++; k=i;
	memcpy(inm, buf, i);

	/* get context name */
	for(;i<sz && buf[i] != 0; i++) {}
	if (i>=sz-4) {
		free(inm);
		return nowdb_err_get(nowdb_err_catalog, FALSE, OBJECT,
	                                "no context in index catalog");
	}
	if (i-k>0) {
		cnm = malloc(i-k+2);
		if (cnm == NULL) {
			free(inm);
			return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
			                          "allocating context name");
		}
		memcpy(cnm, buf+k, i-k+1);
	}

	i++;

	/* get keys */
	keys = calloc(1,sizeof(nowdb_index_keys_t));
	if (keys == NULL) {
		free(inm);
		if (cnm != NULL) free(cnm);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                    "allocating index keys");
	}
	memcpy(&s, buf+i, sizeof(uint16_t)); i+=sizeof(uint16_t);

	if (s > 32) {
		free(inm); free(keys);
		if (cnm != NULL) free(cnm);
		return nowdb_err_get(nowdb_err_catalog, FALSE, OBJECT,
	                                  "no keys in index catalog");
	}
	keys->sz  = s;

	/* get offsets */
	keys->off = calloc(s, sizeof(uint16_t));
	if (keys->off == NULL) {
		free(inm); free(keys);
		if (cnm != NULL) free(cnm);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                             "allocating index key offsets");
	}
	for(k=0; i<sz && k<s; k++) {
		memcpy(keys->off+k, buf+i, sizeof(uint16_t));
		i+=sizeof(uint16_t);
	}
	if (k<s) {
		free(inm); free(keys->off); free(keys);
		if (cnm != NULL) free(cnm);
		return nowdb_err_get(nowdb_err_catalog, FALSE, OBJECT,
	                              "index key offsets incomplete");
	}
	*desc = calloc(1, sizeof(nowdb_index_desc_t));
	if (*desc == NULL) {
		free(inm); free(keys->off); free(keys);
		if (cnm != NULL) free(cnm);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                             "allocating index key offsets");
	}

	(*desc)->name = inm;
	(*desc)->keys = keys;
	(*desc)->idx  = NULL;
	(*desc)->ctx  = NULL;

	/* when we do this, the scope must be locked! */
	if (cnm != NULL) {
		tmp.name = cnm;
		(*desc)->ctx = ts_algo_tree_find(man->context, &tmp);
		if ((*desc)->ctx == NULL) {
			nowdb_index_desc_destroy(*desc);
			free(*desc); free(cnm);
			return nowdb_err_get(nowdb_err_catalog, FALSE, OBJECT,
			                  "unknown context in index catalog");
		}
		free(cnm);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load file
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadfile(FILE *file, char *path,
                            char **buf, uint32_t *sz) {
	struct stat st;

	rewind(file);

	if (stat(path, &st) != 0) return nowdb_err_get(nowdb_err_stat,
	                                          TRUE, OBJECT, path);
	*sz = (uint32_t)st.st_size;
	*buf = malloc(*sz);
	if (*buf == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                    FALSE, OBJECT, "allocating buffer");
	if (fread(*buf, 1, *sz, file) != *sz) {
		free(*buf); return nowdb_err_get(nowdb_err_read,
		                            TRUE, OBJECT, path);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load buf from file
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadCat(nowdb_index_man_t *man) {
	nowdb_index_desc_t *desc=NULL;
	char *line=NULL;
	nowdb_err_t err;
	char *tmp=NULL;
	uint32_t off=0;
	uint32_t   x=0;
	uint32_t  sz=0;

	err = openFile(man);
	if (err != NOWDB_OK) return err;

	err = loadfile(man->file, man->path, &tmp, &sz);
	if (err != NOWDB_OK) return err;
	
	while(off < sz) {
		err = getOneLine(tmp, sz, &off, &line);
		if (err != NOWDB_OK) break;

		err = line2desc(man, line, (uint16_t)(sz-x), &desc);
		if (err != NOWDB_OK) break;

		err = openIndex(man, desc);
		if (err != NOWDB_OK) {
			nowdb_index_desc_destroy(desc);
			free(desc); break;
		}

		err = registerIndex(man, desc);
		if (err != NOWDB_OK) {
			nowdb_index_desc_destroy(desc);
			free(desc); break;
		}
		off++; x=off;
	}
	free(tmp);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: write desc
 * ------------------------------------------------------------------------
 */
static nowdb_err_t writeDesc(nowdb_index_man_t  *man,
                             nowdb_index_desc_t *desc) {
	size_t s;
	char x=0;

	s = strlen(desc->name)+1;
	if (fwrite(desc->name, 1, s, man->file) != s) {
		return nowdb_err_get(nowdb_err_write,
			    TRUE, OBJECT, man->path);
	}
	if (desc->ctx == NULL) {
		if (fwrite(&x, 1, 1, man->file) != 1) {
			return nowdb_err_get(nowdb_err_write,
			            TRUE, OBJECT, man->path);
		}
	} else {
		s = strlen(desc->ctx->name)+1;
		if (fwrite(desc->ctx->name, 1, s, man->file) != s) {
			return nowdb_err_get(nowdb_err_write,
			            TRUE, OBJECT, man->path);
		}
	}
	s = desc->keys->sz;
	if (fwrite(&desc->keys->sz, 2, 1, man->file) != 1) {
		return nowdb_err_get(nowdb_err_write,
			     TRUE, OBJECT, man->path);
	}
	for(int i=0;i<s;i++) {
		if (fwrite(desc->keys->off+i, 2, 1, man->file) != 1) {
			return nowdb_err_get(nowdb_err_write,
			            TRUE, OBJECT, man->path);
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: write eol
 * ------------------------------------------------------------------------
 */
static nowdb_err_t writeEOL(nowdb_index_man_t *man) {
	char eol = '\n';

	if (fwrite(&eol, 1, 1, man->file) != 1) {
		return nowdb_err_get(nowdb_err_write,
		            TRUE, OBJECT, man->path);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: write cat
 * ------------------------------------------------------------------------
 */
static nowdb_err_t writeCat(nowdb_index_man_t *man) {
	nowdb_err_t err=NOWDB_OK;
	ts_algo_list_t *list;
	ts_algo_list_node_t *runner;

	/* consider backup! */
	if (ftruncate(fileno(man->file), 0) != 0)
		return nowdb_err_get(nowdb_err_trunc,
		            TRUE, OBJECT, man->path);
	if (man->byname->count == 0) return NOWDB_OK;

	list = ts_algo_tree_toList(man->byname);
	if (list == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                           TRUE, OBJECT, "tree.toList");
	rewind(man->file);
	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		err = writeDesc(man, runner->cont);
		if (err != NOWDB_OK) break;

		if (runner->nxt != NULL) {
			err = writeEOL(man);
			if (err != NOWDB_OK) break;
		}
	}
	ts_algo_list_destroy(list); free(list);
	if (err != NOWDB_OK) return err;
	if (fflush(man->file) != 0) return nowdb_err_get(nowdb_err_flush,
			                        TRUE, OBJECT, man->path);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Init index manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_init(nowdb_index_man_t   *iman,
                                 ts_algo_tree_t   *context,
                                              void *handle,
                                                char *base,
                                                char *path,
                                             char *ctxpath,
                                              char *vxpath) {
	nowdb_err_t err;

	if (iman == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index manager NULL");
	if (handle == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "handle NULL");
	if (context == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "context NULL");
	if (base == NULL) return nowdb_err_get(nowdb_err_invalid,
	                         FALSE, OBJECT, "base path NULL");
	if (path == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "path NULL");
	if (ctxpath == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "ctxpath NULL");
	if (vxpath == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "vxpath NULL");
	iman->base = NULL;
	iman->path = NULL;
	iman->file = NULL;
	iman->ctxpath = NULL;
	iman->vxpath = NULL;
	iman->handle = handle;
	iman->context = context;
	iman->byname = NULL;
	iman->bykey  = NULL;

	err = nowdb_rwlock_init(&iman->lock);
	if (err != NOWDB_OK) return err;

	iman->base = strdup(base);
	if (iman->base == NULL) {
		nowdb_rwlock_destroy(&iman->lock);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                              "allocating base path");
	}

	iman->path = strdup(path);
	if (iman->path == NULL) {
		nowdb_rwlock_destroy(&iman->lock);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                  "allocating path");
	}
	iman->ctxpath = strdup(ctxpath);
	if (iman->ctxpath == NULL) {
		nowdb_rwlock_destroy(&iman->lock);
		free(iman->path); iman->path = NULL;
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                           "allocating context path");
	}
	iman->vxpath = strdup(vxpath);
	if (iman->vxpath == NULL) {
		nowdb_rwlock_destroy(&iman->lock);
		free(iman->path); iman->path = NULL;
		free(iman->ctxpath); iman->ctxpath = NULL;
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                           "allocating vertex path");
	}

	/*
	fprintf(stderr, "paths: %s | %s | %s | %s\n",
	                iman->base, iman->path, iman->ctxpath, iman->vxpath);
	*/

	iman->byname = ts_algo_tree_new(&comparebyname,
                                        NULL, &noupdate,
	                                &destroydesc,
	                                &destroydesc);
	if (iman->byname == NULL) {
		nowdb_index_man_destroy(iman);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                       "byname.new");
	}
	iman->bykey = ts_algo_tree_new(&comparebykey,
                                       NULL, &noupdate,
	                               &nodestroy,
	                               &nodestroy);
	if (iman->bykey == NULL) {
		nowdb_index_man_destroy(iman);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                        "bykey.new");
	}

	err = loadCat(iman);
	if (err != NOWDB_OK) {
		nowdb_index_man_destroy(iman);
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy index manager
 * ------------------------------------------------------------------------
 */
void nowdb_index_man_destroy(nowdb_index_man_t *iman) {
	if (iman == NULL) return;
	if (iman->bykey != NULL) {
		ts_algo_tree_destroy(iman->bykey);
		free(iman->bykey); iman->bykey = NULL;
	}
	if (iman->byname != NULL) {
		ts_algo_tree_destroy(iman->byname);
		free(iman->byname); iman->byname = NULL;
	}
	if (iman->file != NULL) {
		fclose(iman->file); iman->file = NULL;
	}
	if (iman->path != NULL) {
		free(iman->path); iman->path = NULL;
	}
	if (iman->ctxpath != NULL) {
		free(iman->ctxpath); iman->ctxpath = NULL;
	}
	if (iman->vxpath != NULL) {
		free(iman->vxpath); iman->vxpath = NULL;
	}
	if (iman->base != NULL) {
		free(iman->base); iman->base = NULL;
	}
	nowdb_rwlock_destroy(&iman->lock);
}

/* ------------------------------------------------------------------------
 * Register an index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_register(nowdb_index_man_t  *iman,
                                     nowdb_index_desc_t *desc) {
	nowdb_err_t err = NOWDB_OK, err2;
	size_t s;

	if (iman == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index manager NULL");
	if (desc == NULL) return nowdb_err_get(nowdb_err_invalid,
	                  FALSE, OBJECT, "index descriptor NULL");

	s = strnlen(desc->name, 4097);
	if (s > 4096) return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                 "index name too long (max: 4096)");
	/* lock ! */
	err = nowdb_lock_write(&iman->lock);
	if (err != NOWDB_OK) return err;

	err = registerIndex(iman, desc);
	if (err != NOWDB_OK) goto unlock;

	err = writeCat(iman);

unlock:
	err2 = nowdb_unlock_write(&iman->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * unregister an index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_unregister(nowdb_index_man_t *iman,
                                       char              *name) {
	nowdb_err_t err = NOWDB_OK, err2;
	nowdb_index_desc_t *desc, tmp;

	if (iman == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index manager NULL");
	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "name NULL");

	err = nowdb_lock_write(&iman->lock);
	if (err != NOWDB_OK) return err;

	tmp.name = name;
	desc = ts_algo_tree_find(iman->byname, &tmp);
	if (desc == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                     FALSE, OBJECT, "name");
		goto unlock;
	}

	ts_algo_tree_delete(iman->bykey, desc);
	ts_algo_tree_delete(iman->byname, &tmp);

	err = writeCat(iman);

unlock:
	err2 = nowdb_unlock_write(&iman->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * get index by name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_getByName(nowdb_index_man_t   *iman,
                                      char                *name,
                                      nowdb_index_desc_t **desc) {
	nowdb_err_t err = NOWDB_OK, err2;
	nowdb_index_desc_t tmp;

	if (iman == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index manager NULL");
	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "name NULL");
	if (desc == NULL) return nowdb_err_get(nowdb_err_invalid,
	              FALSE, OBJECT, "index descriptor is NULL");

	err = nowdb_lock_read(&iman->lock);
	if (err != NOWDB_OK) return err;

	tmp.name = name;

	*desc = ts_algo_tree_find(iman->byname, &tmp);
	if (*desc == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                     FALSE, OBJECT, "name");
		goto unlock;
	}

	if ((*desc)->idx != NULL) err = nowdb_index_use((*desc)->idx);

unlock:
	err2 = nowdb_unlock_read(&iman->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * get index by key
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_getByKeys(nowdb_index_man_t  *iman,
                                      nowdb_context_t    *ctx,
                                      nowdb_index_keys_t *keys,
                                      nowdb_index_desc_t **desc) {
	nowdb_err_t err = NOWDB_OK, err2;
	nowdb_index_desc_t tmp;

	if (iman == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index manager NULL");
	if (keys == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "keys NULL");
	if (desc == NULL) return nowdb_err_get(nowdb_err_invalid,
	              FALSE, OBJECT, "index descriptor is NULL");

	err = nowdb_lock_read(&iman->lock);
	if (err != NOWDB_OK) return err;

	tmp.ctx = ctx;
	tmp.keys = keys;

	*desc = ts_algo_tree_find(iman->bykey, &tmp);
	if (*desc == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                     FALSE, OBJECT, "keys");
		goto unlock;
	}

	if ((*desc)->idx != NULL) err = nowdb_index_use((*desc)->idx);

unlock:
	err2 = nowdb_unlock_read(&iman->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Get all
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_getAll(nowdb_index_man_t *iman,
                                   ts_algo_list_t   **list) {
	nowdb_err_t err = NOWDB_OK, err2;

	if (iman == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index manager NULL");
	if (list == NULL) return nowdb_err_get(nowdb_err_invalid,
	                FALSE, OBJECT, "list parameter is NULL");

	err = nowdb_lock_read(&iman->lock);
	if (err != NOWDB_OK) return err;

	*list = NULL;
	if (iman->byname->count == 0) goto unlock;

	*list = ts_algo_tree_toList(iman->byname);
	if (*list == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                      "tree.toList");
		goto unlock;
	}

unlock:
	err2 = nowdb_unlock_read(&iman->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Get all by context
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_getAllOf(nowdb_index_man_t *iman,
                                     nowdb_context_t    *ctx,
                                     ts_algo_list_t    *list) {
	nowdb_err_t err = NOWDB_OK, err2;

	if (iman == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index manager NULL");
	if (list == NULL) return nowdb_err_get(nowdb_err_invalid,
	                FALSE, OBJECT, "list parameter is NULL");

	err = nowdb_lock_read(&iman->lock);
	if (err != NOWDB_OK) return err;

	if (iman->byname->count == 0) goto unlock;

	if (ts_algo_tree_filter(iman->byname, list,
	                               ctx, &ofctx) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                      "tree.filter");
		goto unlock;
	}

unlock:
	err2 = nowdb_unlock_read(&iman->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

