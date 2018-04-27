/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Store: a collection of files
 * ========================================================================
 *
 * ========================================================================
 */
#include <nowdb/store/store.h>
#include <nowdb/store/storewrk.h>
#include <tsalgo/types.h>

static char *OBJECT = "store";

static char nullrec[64] = {0,0,0,0,0,0,0,0,
                           0,0,0,0,0,0,0,0,
                           0,0,0,0,0,0,0,0,
                           0,0,0,0,0,0,0,0,
                           0,0,0,0,0,0,0,0,
                           0,0,0,0,0,0,0,0,
                           0,0,0,0,0,0,0,0,
                           0,0,0,0,0,0,0,0};

#define MAX_FILE_NAME 32

/* ------------------------------------------------------------------------
 * Allocate and initialise new store object
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_new(nowdb_store_t **store,
                            nowdb_path_t     base,
                            nowdb_version_t   ver,
                            uint32_t      recsize,
                            uint32_t     filesize)
{
	nowdb_err_t err = NOWDB_OK;
	if (store == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                    "pointer to store object is NULL");
	}
	*store = malloc(sizeof(nowdb_store_t));
	if (*store == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                          "allocating store object");
	}
	err = nowdb_store_init(*store, base, ver, recsize, filesize);
	if (err != NOWDB_OK) {
		free(*store); *store = NULL; return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Tree callbacks
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t compare(void *ignore, void *left, void *right) {
	if (((nowdb_file_t*)left)->id < 
	    ((nowdb_file_t*)right)->id) return ts_algo_cmp_less;
	if (((nowdb_file_t*)left)->id > 
	    ((nowdb_file_t*)right)->id) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static ts_algo_rc_t update(void *ignore, void *o, void *n) {
	NOWDB_IGNORE(nowdb_file_update(n, o));
	nowdb_file_destroy(n); free(n);
	return TS_ALGO_OK;
}

static void delete (void *ignore, void **n) {}

static void destroy(void *ignore, void **n) {
	if (*n != NULL) {
		nowdb_file_destroy((nowdb_file_t*)(*n));
		free(*n); *n = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Helper: Copy file descriptor
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t copyFile(nowdb_file_t  *source,
                                   nowdb_file_t **target) {
	nowdb_err_t err;
	*target = malloc(sizeof(nowdb_file_t));
	if (*target == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                        "allocating file desriptor");
	}
	err = nowdb_file_copy(source, *target);
	if (err != NOWDB_OK) {
		free(*target); *target = NULL; return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: Deep Copy file descriptor list
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t copyFileList(ts_algo_list_t *source,
                                       ts_algo_list_t *target,
                                       nowdb_time_t     start,
                                       nowdb_time_t       end) {
	ts_algo_list_node_t *runner;
	nowdb_file_t *file;
	nowdb_err_t    err;

	for(runner=source->head; runner!=NULL; runner=runner->nxt) {
		file = runner->cont;
		if (file->newest < start || 
		    file->oldest > end) continue;

		err = copyFile(runner->cont, &file);
		if (err != NOWDB_OK) return err;
		if (ts_algo_list_append(target, file) != TS_ALGO_OK) {
			free(file);
			return nowdb_err_get(nowdb_err_no_mem,  
				FALSE, OBJECT, "list append");
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: destroy list of files
 * ------------------------------------------------------------------------
 */
static inline void destroyFiles(ts_algo_list_t *files) {
	ts_algo_list_node_t *runner;
	runner = files->head;
	while(runner!=NULL) {
		ts_algo_list_node_t *tmp;
		tmp = runner->nxt;
		ts_algo_list_remove(files, runner);
		nowdb_file_destroy(runner->cont); free(runner->cont);
		free(runner); runner = tmp;
	}
}

/* ------------------------------------------------------------------------
 * Helper: destroy spares
 * ------------------------------------------------------------------------
 */
static inline void destroySpares(nowdb_store_t *store) {
	destroyFiles(&store->spares);
}

/* ------------------------------------------------------------------------
 * Helper: destroy waiting
 * ------------------------------------------------------------------------
 */
static inline void destroyWaiting(nowdb_store_t *store) {
	destroyFiles(&store->waiting);
}

/* ------------------------------------------------------------------------
 * Helper: destroy readers
 * ------------------------------------------------------------------------
 */
static inline void destroyReaders(nowdb_store_t *store) {
	ts_algo_tree_destroy(&store->readers);
}

/* ------------------------------------------------------------------------
 * Helper: destroy writer
 * ------------------------------------------------------------------------
 */
static inline void destroyWriter(nowdb_store_t *store) {
	if (store->writer == NULL) return;
	if (store->writer->state == nowdb_file_state_mapped) {
		NOWDB_IGNORE(nowdb_file_umap(store->writer));
	}
	if (store->writer->state == nowdb_file_state_open) {
		NOWDB_IGNORE(nowdb_file_close(store->writer));
	}
	nowdb_file_destroy(store->writer);
	free(store->writer); store->writer=NULL;
}

/* ------------------------------------------------------------------------
 * Helper: destroy all files
 * ------------------------------------------------------------------------
 */
static inline void destroyAllFiles(nowdb_store_t *store) {
	destroySpares(store);
	destroyWaiting(store);
	destroyWriter(store);
	destroyReaders(store);
}

/* ------------------------------------------------------------------------
 * Helper: initialise tree of readers
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t initreaders(nowdb_store_t *store) {
	ts_algo_rc_t rc = ts_algo_tree_init(&store->readers,
	                                    &compare, NULL,
	                                    &update, &delete,
	                                    &destroy);
	if (rc != 0) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                       "cannot initialise AVL tree");
	}
	return NOWDB_OK;
}


/* ------------------------------------------------------------------------
 * Helper: initialise all files
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t initAllFiles(nowdb_store_t *store) {
	ts_algo_list_init(&store->spares);
	ts_algo_list_init(&store->waiting);
	return initreaders(store);
}

/* ------------------------------------------------------------------------
 * Helper: find file in list of files
 * ------------------------------------------------------------------------
 */
static inline ts_algo_list_node_t *findInList(ts_algo_list_t *list,
                                              nowdb_file_t   *file) {
	ts_algo_list_node_t *runner;
	nowdb_file_t        *tmp;
	for(runner=list->head; runner!=NULL; runner=runner->nxt) {
		tmp = runner->cont;
		if (tmp->id == file->id) return runner;
	}
	return NULL;
}

/* ------------------------------------------------------------------------
 * Find file in waiting
 * ------------------------------------------------------------------------
 */
static inline nowdb_file_t *findWaiting(nowdb_store_t *store,
                                        nowdb_file_t   *file) {
	ts_algo_list_node_t *tmp;
	tmp = findInList(&store->waiting, file);
	if (tmp == NULL) return NULL;
	return tmp->cont;
}

/* ------------------------------------------------------------------------
 * Find file in spares
 * ------------------------------------------------------------------------
 */
static inline nowdb_file_t *findSpare(nowdb_store_t *store,
                                       nowdb_file_t  *file) {
	ts_algo_list_node_t *tmp;
	tmp = findInList(&store->spares, file);
	if (tmp == NULL) return NULL;
	return tmp->cont;
}

/* ------------------------------------------------------------------------
 * Find file in readers
 * ------------------------------------------------------------------------
 */
static inline nowdb_file_t *findReader(nowdb_store_t *store,
                                       nowdb_file_t   *file) {
	return ts_algo_tree_find(&store->readers, file);
}

/* ------------------------------------------------------------------------
 * Initialise already allocated store object
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_init(nowdb_store_t  *store,
                             nowdb_path_t     base,
                             nowdb_version_t   ver,
                             uint32_t      recsize,
                             uint32_t     filesize)
{
	nowdb_err_t err;
	size_t s;

	if (store == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                              "store object is NULL");
	}

	/* defaults */
	store->version = ver;
	store->recsize = recsize;
	store->filesize = filesize;
	store->starting = FALSE;
	store->path = NULL;
	store->catalog = NULL;
	store->writer = NULL;
	store->compare = NULL;
	store->comp = NOWDB_COMP_FLAT;
	store->cdict = NULL;
	store->ddict = NULL;
	store->cctx = NULL;
	store->dctx = NULL;
	store->nextid = 1;

	/* lists of files */
	err = initAllFiles(store);

	/* lock */ 
	err = nowdb_rwlock_init(&store->lock);
	if (err != NOWDB_OK) {
		nowdb_err_t err2 = nowdb_err_get(nowdb_err_store,
		                            FALSE, OBJECT, NULL);
		err2->cause = err;
		return err2;
	}

	/* check base */
	if (base == NULL) {
		nowdb_rwlock_destroy(&store->lock);
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                      "base is NULL");
	}
	s = strnlen(base, NOWDB_MAX_PATH-4);
	if (s > NOWDB_MAX_PATH - 3) {
		nowdb_rwlock_destroy(&store->lock);
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                     "path too long");
	}

	/* base path */
	store->path = malloc(s+1);
	if (store->path == NULL) {
		nowdb_rwlock_destroy(&store->lock);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                            "allocating store path");
	}
	strcpy(store->path, base);

	/* catalog */
	store->catalog = nowdb_path_append(store->path, "cat");
	if (store->catalog == NULL) {
		nowdb_rwlock_destroy(&store->lock);
		free(store->path);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                    "allocating store catalog path");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Configure sorting
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_configSort(nowdb_store_t     *store,
                                   nowdb_comprsc_t compare) {
	if (store == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "store object is NULL");
	store->compare = compare;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Configure compression
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_configCompression(nowdb_store_t *store,
                                          nowdb_comp_t   comp) {
	if (store == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "store object is NULL");
	store->comp = comp;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy store
 * ------------------------------------------------------------------------
 */
void nowdb_store_destroy(nowdb_store_t *store) {
	if (store == NULL) return;
	if (store->path != NULL) {
		free(store->path); store->path = NULL;
	}
	if (store->catalog != NULL) {
		free(store->catalog); store->catalog = NULL;
	}
	if (store->cdict != NULL) {
		ZSTD_freeCDict(store->cdict); store->cdict = NULL;
	}
	if (store->ddict != NULL) {
		ZSTD_freeDDict(store->ddict); store->ddict = NULL;
	}
	if (store->cctx != NULL) {
		ZSTD_freeCCtx(store->cctx); store->cctx = NULL;
	}
	if (store->dctx != NULL) {
		ZSTD_freeDCtx(store->dctx); store->dctx = NULL;
	}
	destroyAllFiles(store);
	nowdb_rwlock_destroy(&store->lock);
}

/* ------------------------------------------------------------------------
 * Helper: new id
 * NOTE: must only be called from protected context
 * ------------------------------------------------------------------------
 */
static inline nowdb_fileid_t getFileId(nowdb_store_t *store) {
	nowdb_fileid_t fid = store->nextid;
	store->nextid++;
	return fid;
}

/* ------------------------------------------------------------------------
 * Helper: make file name
 * NOTE: must only be called from protected context
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t makeFileName(nowdb_store_t *store, char *name) {
	nowdb_err_t err;
	nowdb_time_t  t;
	nowdb_path_t  p;

	for(;;) {
		err = nowdb_time_now(&t);
		if (err != NOWDB_OK) return err;
		sprintf(name, "%lu.db", t);
		p = nowdb_path_append(store->path, name);
		if (p == NULL) return nowdb_err_get(nowdb_err_no_mem,
		               FALSE, OBJECT, "allocating file path");
		if (!nowdb_path_exists(p, NOWDB_DIR_TYPE_ANY)) {
			free(p); return NOWDB_OK;
		}
		free(p);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: make file 
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t makeFile(nowdb_store_t *store,
                                   nowdb_file_t  **file,
                                   char           *name,
                                   nowdb_fileid_t   fid) {
	nowdb_path_t  p;
	nowdb_err_t err;
	p = nowdb_path_append(store->path, name);
	if (p == NULL) return nowdb_err_get(nowdb_err_no_mem,
	               FALSE, OBJECT, "allocating file path");
	err = nowdb_file_new(file, fid, p, store->filesize,0,
	                     NOWDB_IDX_PAGE, store->recsize, 
	                     NOWDB_FILE_WRITER | NOWDB_FILE_SPARE,
	                     NOWDB_COMP_FLAT, NOWDB_ENCP_NONE,
	                     1, NOWDB_TIME_DAWN, NOWDB_TIME_DUSK); 
	free(p); return err;
}

/* ------------------------------------------------------------------------
 * Helper: config compression in reader
 * ------------------------------------------------------------------------
 */
static inline void configReader(nowdb_store_t *store, nowdb_file_t *file) {
	if (file->comp == NOWDB_COMP_ZSTD) {
		file->ddict = store->ddict;
		file->cdict = store->cdict;
		file->cctx  = store->cctx;
		file->dctx  = store->dctx;
	}
}

/* ------------------------------------------------------------------------
 * create reader
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_createReader(nowdb_store_t *store,
                                     nowdb_file_t  **file) {
	nowdb_err_t    err=NOWDB_OK;
	nowdb_err_t    err2;
	nowdb_fileid_t fid;
	char fname[MAX_FILE_NAME];

	if (store == NULL) nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                          "store object is NULL");
	if (file == NULL) nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                          "file object is NULL");

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	err = makeFileName(store, fname);
	if (err != NOWDB_OK) goto unlock;
	
	fid = getFileId(store);

	err = makeFile(store, file, fname, fid);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_file_create(*file);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(*file); free(*file); *file = NULL;
		goto unlock;
	}

	err = nowdb_file_makeReader(*file);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(*file); free(*file); *file = NULL;
		goto unlock;
	}
	(*file)->comp = store->comp;
	configReader(store, *file);
	
unlock:
	err2 = nowdb_unlock_write(&store->lock);
	if (err != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: create file
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t createFile(nowdb_store_t *store) {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_fileid_t fid;
	char fname[MAX_FILE_NAME];

	err = makeFileName(store, fname);
	if (err != NOWDB_OK) return err;
	
	fid = getFileId(store);

	err = makeFile(store, &file, fname, fid);
	if (err != NOWDB_OK) return err;

	err = nowdb_file_create(file);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(file); free(file);
		return err;
	}
	if (ts_algo_list_append(&store->spares, file) != 0) {
		nowdb_file_destroy(file); free(file);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, 
		                                    "spares append");
	}
	return err;
}

static inline nowdb_err_t createSpares(nowdb_store_t *store) {
	while (store->spares.len < 3) {
		nowdb_err_t err = createFile(store);
		if (err != NOWDB_OK) return err;
	}
	return NOWDB_OK;
}

static inline nowdb_err_t getWriter(nowdb_store_t *store) {
	ts_algo_list_node_t  *node;
	nowdb_err_t err = NOWDB_OK;

	if (store->writer != NULL) return NOWDB_OK;
	if (store->spares.len < 1) err = createFile(store);
	if (err != NOWDB_OK) return err;
	node = store->spares.head;
	ts_algo_list_remove(&store->spares, node);
	store->writer = node->cont; free(node);
	NOWDB_IGNORE(nowdb_file_makeWriter(store->writer));
	return NOWDB_OK;
}

static inline nowdb_err_t makeSpare(nowdb_store_t *store,
                                    nowdb_file_t  *file) {
	if (store->spares.len > 3) {
		NOWDB_IGNORE(nowdb_file_close(file));
		NOWDB_IGNORE(nowdb_file_remove(file));
		nowdb_file_destroy(file); free(file);
		return NOWDB_OK;
	}
	if (ts_algo_list_append(&store->spares, file) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                   "readers toList");
	}
	return nowdb_file_makeSpare(file);
}

static inline nowdb_err_t makeWaiting(nowdb_store_t *store,
                                      nowdb_file_t  *file) {
	nowdb_err_t err = NOWDB_OK;

	if (ts_algo_list_append(&store->waiting, file) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                   "pending append");
	}
	err = nowdb_file_makeReader(store->writer);
	if (err != NOWDB_OK) return err;

	if (store->starting) return NOWDB_OK;
	return nowdb_store_sortNow(&store->sortwrk);
}

static inline nowdb_err_t swapWriter(nowdb_store_t *store) {
	nowdb_err_t err;
	
	err = nowdb_file_umap(store->writer);
	if (err != NOWDB_OK) return err;
	
	err = nowdb_file_close(store->writer);
	if (err != NOWDB_OK) return err;
	
	err = makeWaiting(store, store->writer);
	if (err != NOWDB_OK) return err;

	store->writer = NULL;
	err = getWriter(store);
	if (err != NOWDB_OK) return err;

	store->writer->pos = 0;
	return nowdb_file_map(store->writer);
}

static inline nowdb_err_t remapWriter(nowdb_store_t *store) {
	if (store->writer->pos >= store->writer->capacity) {
		return swapWriter(store);
	}
	return nowdb_file_move(store->writer);
}

static inline nowdb_err_t adjustWriter(nowdb_store_t *store) {
	nowdb_err_t err;
	uint32_t pos = 0;

	while (store->writer->pos < store->writer->capacity) {
		if (pos >= store->writer->bufsize) {
			if (store->writer->pos >= 
			    store->writer->capacity) break;
			err = nowdb_file_move(store->writer);
			if (err != NOWDB_OK) break;
			pos = 0;
		}
		if (memcmp(store->writer->mptr+pos,nullrec,
		               store->recsize) == 0) break;
		
		/*
		fprintf(stderr, "%u/%u/%u\n", store->writer->pos, pos,
		                              store->writer->bufsize);
		*/
		pos += store->recsize;
		store->writer->pos+=store->recsize;
	}
	if (err == NOWDB_OK) store->writer->size = store->writer->pos;
	return err;
}

static inline nowdb_err_t prepareWriter(nowdb_store_t *store) {
	nowdb_err_t err;

	if (store->writer == NULL) return nowdb_err_get(nowdb_err_panic,
	                                                FALSE, OBJECT,
	                                                "no writer");
	/* position to size */
	err = nowdb_file_mapAt(store->writer, store->writer->size);
	if (err != NOWDB_OK) return err;

	err = adjustWriter(store);
	if (err != NOWDB_OK) return err;

	if (store->writer->size == store->writer->capacity) {
		return swapWriter(store);
	}
	return NOWDB_OK;
}

static inline nowdb_err_t writeCatalogLine(char *buf, int *off, 
                                            nowdb_file_t *file) {
	nowdb_path_t nm;

	memcpy(buf+*off, &file->id, 4); *off += 4;
	memcpy(buf+*off, &file->order, 4); *off += 4;
	memcpy(buf+*off, &file->capacity, 4); *off += 4;
	memcpy(buf+*off, &file->size, 4); *off += 4;
	memcpy(buf+*off, &file->recordsize, 4); *off += 4;
	memcpy(buf+*off, &file->blocksize, 4); *off += 4;
	memcpy(buf+*off, &file->ctrl, 1); *off += 1;
	memcpy(buf+*off, &file->comp, 4); *off += 4;
	memcpy(buf+*off, &file->encp, 4); *off += 4;
	memcpy(buf+*off, &file->grain, 8); *off += 8;
	memcpy(buf+*off, &file->oldest, 8); *off += 8;
	memcpy(buf+*off, &file->newest, 8); *off += 8;

	nm = nowdb_path_filename(file->path);
	if (nm == NULL) return nowdb_err_get(nowdb_err_bad_path,
	                              FALSE, OBJECT, file->path);

	memcpy(buf+*off, nm, strlen(nm)+1); *off += strlen(nm)+1; free(nm);
	return NOWDB_OK;
}

static inline uint32_t measureCatalogSize(nowdb_store_t *store) {
	struct stat st;
	if (stat(store->catalog, &st) != 0) return 0;
	return st.st_size;
}

static inline nowdb_err_t readCatalogLine(nowdb_store_t *store,
                                          ts_algo_list_t *files,
                                          char *buf,
                                          int *off, int size,
                                          nowdb_version_t ver) {
	nowdb_err_t  err;
	nowdb_file_t tmp, *file;
	nowdb_path_t p;
	int i;

	memcpy(&tmp.id, buf+*off, 4); *off += 4;
	memcpy(&tmp.order, buf+*off, 4); *off += 4;
	memcpy(&tmp.capacity, buf+*off, 4); *off += 4;
	memcpy(&tmp.size, buf+*off, 4); *off += 4;
	memcpy(&tmp.recordsize, buf+*off, 4); *off += 4;
	memcpy(&tmp.blocksize, buf+*off, 4); *off += 4;
	memcpy(&tmp.ctrl, buf+*off, 1); *off += 1;
	memcpy(&tmp.comp, buf+*off, 4); *off += 4;
	memcpy(&tmp.encp, buf+*off, 4); *off += 4;
	memcpy(&tmp.grain, buf+*off, 8); *off += 8;
	memcpy(&tmp.oldest, buf+*off, 8); *off += 8;
	memcpy(&tmp.newest, buf+*off, 8); *off += 8;

	for(i=0;*off+i<size;i++) {
		if (buf[*off+i] == 0) break;
	}
	if (*off+i>=size) return nowdb_err_get(nowdb_err_catalog,
		                  FALSE, OBJECT, store->catalog);
	if (buf[*off+i] != 0) return nowdb_err_get(nowdb_err_catalog,
		                      FALSE, OBJECT, store->catalog);
	p = nowdb_path_append(store->path, buf+(*off)); *off+=i+1;
	if (p == NULL) return nowdb_err_get(nowdb_err_no_mem,
		       FALSE, OBJECT, "allocating store path");
	err = nowdb_file_new(&file, tmp.id, p,
	                            tmp.capacity,
	                            tmp.size,
	                            tmp.blocksize,
	                            tmp.recordsize,
	                            tmp.ctrl,
	                            tmp.comp,
                                    tmp.encp,
	                            tmp.grain,
	                            tmp.oldest,
	                            tmp.newest);
	free(p);
	if (err != NOWDB_OK) return err;
	file->size = tmp.size;
	if (ts_algo_list_append(files, file) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                      "list append");
	}
	return NOWDB_OK;
}
                                           
static inline nowdb_err_t openstore(nowdb_store_t *store, char *buf, int size) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_version_t ver;
	uint32_t magic;
	ts_algo_list_t files;
	ts_algo_list_node_t *runner;
	nowdb_file_t *file;
	int off = 0;

	memcpy(&magic, buf, 4); off+=4;
	if (magic != NOWDB_MAGIC) return nowdb_err_get(nowdb_err_magic,
	                                FALSE, OBJECT, store->catalog);
	memcpy(&ver, buf+off, 4); off+=4;

	ts_algo_list_init(&files);
	while(off < size) {
		err = readCatalogLine(store, &files, buf, &off, size, ver);
		if (err != NOWDB_OK) {
			destroyFiles(&files); return err;
		}
	}
	runner = files.head;
	while (runner!=NULL) {
		file = runner->cont;
		if (store->nextid <= file->id) {
			store->nextid = file->id + 1;
		}	
		/*
		fprintf(stderr, "%u: %s -- %u\n",
		        file->id, file->path, (uint32_t)file->ctrl);
		*/
		if (file->ctrl & NOWDB_FILE_SPARE) {
			if (ts_algo_list_append(
			    &store->spares, file) != TS_ALGO_OK)
			{
				err = nowdb_err_get(nowdb_err_no_mem,
				      FALSE, OBJECT, "spares append");
				break;
			}

		} else if (file->ctrl & NOWDB_FILE_WRITER) {
			if (store->writer != NULL) {
				fprintf(stderr, "too many writers\n");
				err = nowdb_err_get(nowdb_err_catalog,
				       FALSE, OBJECT, store->catalog);
				break;
			}
			store->writer = file;

		} else if ((file->ctrl & NOWDB_FILE_READER) &&
		           (file->ctrl & NOWDB_FILE_SORT)) {

			configReader(store, file);

			if (ts_algo_tree_insert(
			    &store->readers, file) != TS_ALGO_OK) 
			{
				err = nowdb_err_get(nowdb_err_no_mem,
				      FALSE, OBJECT, "readers insert");
				break;
			}

		} else if (file->ctrl & NOWDB_FILE_READER) {
			if (ts_algo_list_append(
			    &store->waiting, file) != TS_ALGO_OK)
			{
				err = nowdb_err_get(nowdb_err_no_mem,
				      FALSE, OBJECT, "pending append");
				break;
			}

		} else {
			fprintf(stderr,
			"don't know what to do with %s\n", file->path); 
			nowdb_file_destroy(file); free(file);
		}
		ts_algo_list_node_t *tmp = runner->nxt;
		ts_algo_list_remove(&files, runner);
		free(runner); runner = tmp;
	}
	destroyFiles(&files);

	/* check for errors */
	if (err != NOWDB_OK) {
		destroyAllFiles(store); return err;
	}
	
	/* if no spares, create */
	err = createSpares(store);
	if (err != NOWDB_OK) {
		destroyAllFiles(store); return err;
	}
	
	/* if no writer, get a spare */
	if (store->writer == NULL) err = getWriter(store);
	if (err != NOWDB_OK) {
		destroyAllFiles(store); return err;
	}
	return NOWDB_OK;
}

static inline nowdb_err_t readCatalogFile(nowdb_store_t *store,
                                           char *buf, int size) {
	ssize_t x;
	FILE *cat;

	cat = fopen(store->catalog, "r");
	if (cat == NULL) return nowdb_err_get(nowdb_err_open, TRUE, OBJECT,
	                                                   store->catalog);
	x = fread(buf, 1, size, cat);
	if (x != size) {
		fclose(cat);
		return nowdb_err_get(nowdb_err_read, TRUE, OBJECT,
		                                  store->catalog);
	}
	if (fclose(cat) != 0) return nowdb_err_get(nowdb_err_close,
	                             TRUE, OBJECT, store->catalog);
	return NOWDB_OK;
}

static inline nowdb_err_t readCatalog(nowdb_store_t *store) {
	nowdb_err_t err;
	char *buf;
	uint32_t sz = measureCatalogSize(store);
	if (sz == 0) return nowdb_err_get(nowdb_err_catalog, FALSE, OBJECT,
	                                                   store->catalog);
	buf = malloc(sz);
	if (buf == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                   FALSE, OBJECT, "allocating buffer");
	err = readCatalogFile(store, buf, sz);
	if (err != NULL) {
		free(buf); return err;
	}
	err = openstore(store, buf, sz);
	if (err != NULL) {
		free(buf); return err;
	}
	free(buf);

	err = prepareWriter(store);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

static inline uint32_t computeCatalogSize(nowdb_store_t *store) {
	/* once:
	 * MAGIC + version
	 *
	 * per line:
	 * ---------
	 * id                4
	 * order,            4
	 * capactiy,         4 
	 * size,             4
	 * recsize,          4
	 * blocksize,        4
	 * ctrl,             1
	 * compression,      4 
	 * encryption,       4
	 * grain,            8
	 * oldest,           8
	 * newest,           8
	 * file name        32
	 */
	uint32_t once = 8;
	uint32_t perline = 92;
	uint32_t nfiles = 1;

	nfiles += store->spares.len;
	nfiles += store->waiting.len;
	nfiles += store->readers.count;

	return once + nfiles * perline + 1;
}

static inline nowdb_err_t writeCatalogFile(nowdb_store_t *store,
                                            char *buf, int size) {
	nowdb_err_t  err;
	nowdb_path_t   p=NULL;
	nowdb_bool_t bkp;
	ssize_t x;
	FILE *cat;

	bkp = nowdb_path_exists(store->catalog, NOWDB_DIR_TYPE_ANY);

	if (bkp) {
		p = nowdb_path_append(store->path, "cat.bkp");
		if (p == NULL) return nowdb_err_get(nowdb_err_no_mem,
		               FALSE, OBJECT, "allocating backup path");
		err = nowdb_path_move(store->catalog, p);
		if (err != NOWDB_OK) {
			free(p); return err;
		}
	}
	cat = fopen(store->catalog, "w");
	if (cat == NULL) {
		if (bkp) {
			NOWDB_IGNORE(nowdb_path_move(p, store->catalog));
			free(p);
		}
		return nowdb_err_get(nowdb_err_open, TRUE, OBJECT,
		                                   store->catalog);
	}
	x = fwrite(buf, 1, size, cat);
	if (x != size) {
		fprintf(stderr, "%d - %lu\n", size, x);
		fclose(cat);
		if (bkp) {
			NOWDB_IGNORE(nowdb_path_move(p, store->catalog));
			free(p);
		}
		return nowdb_err_get(nowdb_err_write, TRUE, OBJECT,
		                                   store->catalog);
	}
	if (fclose(cat) != 0) {
		if (bkp) {
			NOWDB_IGNORE(nowdb_path_move(p, store->catalog));
			free(p);
		}
		return nowdb_err_get(nowdb_err_write, TRUE, OBJECT,
		                                   store->catalog);
	}
	if (bkp) {
		nowdb_path_remove(p); free(p);
	}
	return NOWDB_OK;
}

static inline nowdb_err_t storeCatalog(nowdb_store_t *store) {
	nowdb_err_t err;
	uint32_t magic = NOWDB_MAGIC;
	char *buf = NULL;
	int off=8;
	ts_algo_list_node_t *runner;
	ts_algo_list_t *readers;
	uint32_t sz = computeCatalogSize(store);
	buf = malloc(sz);
	if (buf == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                   FALSE, OBJECT, "allocating buffer");

	memcpy(buf, &magic, 4);
	memcpy(buf+4, &store->version, 4);

	/* writer */
	if (store->writer != NULL) {
		err = writeCatalogLine(buf, &off, store->writer);
		if (err != NOWDB_OK) {
			free(buf); return err;
		}
	}
	/* spares */
	for(runner=store->spares.head; runner!=NULL; runner=runner->nxt) {
		err = writeCatalogLine(buf, &off, runner->cont);
		if (err != NOWDB_OK) {
			free(buf); return err;
		}
	}
	/* waiting */
	for(runner=store->waiting.head; runner!=NULL; runner=runner->nxt) {
		err = writeCatalogLine(buf, &off, runner->cont);
		if (err != NOWDB_OK) {
			free(buf); return err;
		}
	}
	/* readers */
	if (store->readers.count > 0) {
		readers = ts_algo_tree_toList(&store->readers);
		if (readers == NULL) {
			free(buf); return nowdb_err_get(nowdb_err_no_mem,
			                FALSE, OBJECT, "readers toList");
		}
		for(runner=readers->head; runner!=NULL; runner=runner->nxt) {
			err = writeCatalogLine(buf, &off, runner->cont);
			if (err != NOWDB_OK) {
				ts_algo_list_destroy(readers); free(readers);
				free(buf); return err;
			}
		}
		ts_algo_list_destroy(readers); free(readers);
	}
	err = writeCatalogFile(store, buf, off);
	free(buf); return err;
}

static inline nowdb_err_t startWorkers(nowdb_store_t *store) {
	nowdb_err_t err = NOWDB_OK;

	err = nowdb_store_startSync(&store->syncwrk, store, NULL);
	if (err != NOWDB_OK) return err;

	err = nowdb_store_startSorter(&store->sortwrk, store, NULL);
	if (err != NOWDB_OK) {
		NOWDB_IGNORE(nowdb_store_stopSync(&store->syncwrk));
		return err;
	}
	return NOWDB_OK;
}

static inline nowdb_err_t stopWorkers(nowdb_store_t *store) {
	nowdb_err_t err = NOWDB_OK;

	err = nowdb_store_stopSorter(&store->sortwrk);
	if (err != NOWDB_OK) return err;

	err = nowdb_store_stopSync(&store->syncwrk);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Open store
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_open(nowdb_store_t *store) {
	nowdb_err_t err  = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;

	/* lock */
	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	store->starting = TRUE;

	/* compression */
	if (store->comp == NOWDB_COMP_ZSTD) {
		err = nowdb_store_loadZSTDDict(store);
		if (err != NOWDB_OK) goto unlock;
	}

	/* read catalog */
	err = readCatalog(store);
	if (err != NOWDB_OK) {
		destroyAllFiles(store); goto unlock;
	}

	/* store catalog */
	err = storeCatalog(store);
	if (err != NOWDB_OK) {
		destroyAllFiles(store); goto unlock;
	}

	/* start workers */
	err = startWorkers(store);
	if (err != NOWDB_OK) {
		destroyAllFiles(store); goto unlock;
	}
unlock:
	store->starting = FALSE;
	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Close store
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_close(nowdb_store_t *store) {
	nowdb_err_t err  = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;

	/* stop workers
	 * DONT LOCK the store when stopping workers! */
	err = stopWorkers(store);
	if (err != NOWDB_OK) return err;

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	/* write catalog */
	err = storeCatalog(store);
	if (err != NOWDB_OK) goto unlock;

	destroyAllFiles(store);
	err = initAllFiles(store);

unlock:
	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Create store physically on disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_create(nowdb_store_t *store) {
	nowdb_err_t err  = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;

	if (store == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                              "store object is NULL");
	}

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	if (nowdb_path_exists(store->path, NOWDB_DIR_TYPE_ANY)) {
		err = nowdb_err_get(nowdb_err_create, FALSE, OBJECT,
		                                        store->path);
		goto unlock;
	}

	err = nowdb_dir_create(store->path);
	if (err != NOWDB_OK) goto unlock;

	err = createSpares(store);
	if (err != NOWDB_OK) goto unlock;

	err = getWriter(store);
	if (err != NOWDB_OK) {
		destroySpares(store); goto unlock;
	}

	/*
	fprintf(stderr, "writer: %s -- %u\n",
		         store->writer->path, store->writer->ctrl);
	*/

	err = storeCatalog(store);
unlock:
	destroySpares(store);
	destroyWriter(store);
	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Remove store physically from disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_drop(nowdb_store_t *store) {
	ts_algo_list_t dir;
	ts_algo_list_node_t *runner;
	nowdb_dir_ent_t *e;
	nowdb_err_t err =NOWDB_OK;
	nowdb_err_t err2=NOWDB_OK;

	if (store == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                              "store object is NULL");
	}

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	ts_algo_list_init(&dir);
	err = nowdb_dir_content(store->path, NOWDB_DIR_TYPE_FILE, &dir);
	if (err != NOWDB_OK) goto unlock;

	for (runner=dir.head; runner!=NULL; runner=runner->nxt) {
		e = runner->cont;
		err = nowdb_path_remove(e->path);
		if (err != NOWDB_OK) {
			goto unlock;
		}
	}
	err = nowdb_path_remove(store->path);

unlock:
	nowdb_dir_content_destroy(&dir);
	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Insert one record
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_insert(nowdb_store_t *store,
                               void          *data) {
	nowdb_err_t err  = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	uint32_t pos;

	if (store == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                              "store object is NULL");
	}
	if (store->writer == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                 "store is not open");
	}
	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	pos = store->writer->pos%store->writer->bufsize;
	memcpy(store->writer->mptr+pos, data, store->recsize);
	if (!store->writer->dirty) store->writer->dirty = TRUE;

	store->writer->size += store->recsize;
	pos+=store->recsize; store->writer->pos+=store->recsize;
	if (pos == store->writer->bufsize) {
		err = remapWriter(store);
		if (err != NOWDB_OK) {
			fprintf(stderr, "remap error!\n");
			goto unlock;
		}
	}

	/* insert into indices ? */

unlock:
	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Insert n records
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_insertBulk(nowdb_store_t *store,
                                   void           *data,
                                   uint32_t       count);

/* ------------------------------------------------------------------------
 * Get all readers for period start - end
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_getFiles(nowdb_store_t *store,
                                 ts_algo_list_t *list,
                                 nowdb_time_t   start,
                                 nowdb_time_t    end) {
	ts_algo_list_t      *tmp;
	nowdb_file_t       *file;
	nowdb_err_t  err=NOWDB_OK;
	nowdb_err_t err2=NOWDB_OK;

	if (store == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "store object is NULL");
	if (store == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "list pointer is NULL");

	err = nowdb_lock_read(&store->lock);
	if (err != NOWDB_OK) return err;
	
	/* readers */
	if (store->readers.count > 0) {
		tmp = ts_algo_tree_toList(&store->readers);
		if (tmp == NULL) {
			err = nowdb_err_get(nowdb_err_no_mem,
			     FALSE, OBJECT, "readers toList");
			goto unlock;
		}
		err = copyFileList(tmp, list, start, end);
		ts_algo_list_destroy(tmp); free(tmp);
		if (err != NOWDB_OK) goto unlock;
	}
	
	/* pending */
	if (store->waiting.len > 0) {
		err = copyFileList(&store->waiting, list, start, end);
		if (err != NOWDB_OK) goto unlock;
		
	}
	/* writer */
	err = copyFile(store->writer, &file);
	if (err != NOWDB_OK) goto unlock;
	err = nowdb_file_makeReader(file);
	if (err != NOWDB_OK) goto unlock;
	if (ts_algo_list_append(list, file) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                     "list append");
		goto unlock;
	}
		
unlock:
	err2 = nowdb_unlock_read(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Find file in spares
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_findSpare(nowdb_store_t *store,
                                  nowdb_file_t  *file,
                                  nowdb_bool_t  *found) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	if (store != NULL) nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                          "store object is NULL");
	if (file != NULL) nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                          "file object is NULL");
	if (found != NULL) nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                       "found parameter is NULL");

	*found = FALSE;

	err = nowdb_lock_read(&store->lock);
	if (err != NOWDB_OK) return err;

	if (findSpare(store, file) != NULL) *found = TRUE;

	err2 = nowdb_unlock_read(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Find file in waiting
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_findWaiting(nowdb_store_t *store,
                                    nowdb_file_t  *file,
                                    nowdb_bool_t  *found) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	if (store != NULL) nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                          "store object is NULL");
	if (file != NULL) nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                          "file object is NULL");
	if (found != NULL) nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                       "found parameter is NULL");

	*found = FALSE;

	err = nowdb_lock_read(&store->lock);
	if (err != NOWDB_OK) return err;

	if (findWaiting(store, file) != NULL) *found = TRUE;

	err2 = nowdb_unlock_read(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Find file in readers
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_findReader(nowdb_store_t *store,
                                   nowdb_file_t  *file,
                                   nowdb_bool_t  *found) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	if (store != NULL) nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                          "store object is NULL");
	if (file != NULL) nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                          "file object is NULL");
	if (found != NULL) nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                       "found parameter is NULL");

	*found = FALSE;

	err = nowdb_lock_read(&store->lock);
	if (err != NOWDB_OK) return err;

	if (findReader(store, file) != NULL) *found = TRUE;

	err2 = nowdb_unlock_read(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Find reader with capacity to store more
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_getFreeReader(nowdb_store_t *store,
                                      nowdb_file_t  **file) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	ts_algo_list_node_t   *runner;
	ts_algo_list_t      *tmp=NULL;
	nowdb_file_t  *candidate=NULL;

	if (store == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                                 OBJECT, "store object is NULL");
	if (file == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                      OBJECT, "pointer to file object is NULL");

	err = nowdb_lock_read(&store->lock);
	if (err != NOWDB_OK) return err;

	/* nothing available */
	if (store->readers.count == 0) goto unlock;

	/* this is not optimal! */
	tmp = ts_algo_tree_toList(&store->readers);
	if (tmp == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                  "readers.toList");
		goto unlock;
	}
	for(runner=tmp->head; runner!=NULL; runner=runner->nxt) {
		candidate = runner->cont;
		if (candidate->size < NOWDB_FILE_MAXSIZE) {
			*file = malloc(sizeof(nowdb_file_t));
			if (file == NULL) {
				err = nowdb_err_get(nowdb_err_no_mem,
				                       FALSE, OBJECT,
				       "allocating file descriptor");
				break;
			}
			err = nowdb_file_copy(candidate, *file);
			if (err != NOWDB_OK) {
				free(*file); *file = NULL; break;
			}
			break;
		}
	}

unlock:
	if (tmp != NULL) {
		ts_algo_list_destroy(tmp); free(tmp);
	}
	err2 = nowdb_unlock_read(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Get waiting
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_getWaiting(nowdb_store_t *store,
                                   nowdb_file_t  **file) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	ts_algo_list_node_t *tmp;

	if (store == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                                 OBJECT, "store object is NULL");
	if (file == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                      OBJECT, "pointer to file object is NULL");

	err = nowdb_lock_read(&store->lock);
	if (err != NOWDB_OK) return err;

	tmp = store->waiting.head;
	if (tmp == NULL) goto unlock;

	*file = malloc(sizeof(nowdb_file_t));
	if (*file == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                      "allocating file descriptor");
		goto unlock;
	}
	err = nowdb_file_copy(tmp->cont, *file);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_read(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Promote
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_promote(nowdb_store_t  *store,
                                nowdb_file_t *waiting,
                                nowdb_file_t  *reader) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	ts_algo_list_node_t *tmp;

	if (store == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                                 OBJECT, "store object is NULL");
	if (waiting == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                                        OBJECT, "waiting is NULL");
	if (reader == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                                        OBJECT, "reader is NULL");

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	tmp = findInList(&store->waiting, waiting);
	if (tmp == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found, FALSE, OBJECT,
	                                              "waiting not found");
		goto unlock;
	}
	if (ts_algo_tree_insert(&store->readers,reader) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                          "readers insert");
		goto unlock;
	}
	ts_algo_list_remove(&store->waiting, tmp);
	nowdb_file_destroy(tmp->cont); free(tmp->cont); free(tmp);
unlock:
	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Donate empty file to spares
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_donate(nowdb_store_t *store, nowdb_file_t *file) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	if (store == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                                 OBJECT, "store object is NULL");
	if (file == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                      OBJECT, "pointer to file object is NULL");

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	err = makeSpare(store, file);

	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Destroy files and list
 * ------------------------------------------------------------------------
 */
void nowdb_store_destroyFiles(ts_algo_list_t *files) {
	destroyFiles(files);
}

/* ------------------------------------------------------------------------
 * Add a file
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_addFile(nowdb_store_t *store,
                                nowdb_file_t  *file);

/* ------------------------------------------------------------------------
 * Remove a file
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_removeFile(nowdb_store_t *store,
                                   nowdb_file_t  *file);

/* ------------------------------------------------------------------------
 * Drop all files containing only data before that timestamp
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_dropFiles(nowdb_store_t *store,
                                  nowdb_time_t   stamp);

/* ------------------------------------------------------------------------
 * Pretty-print the catalog
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_showCatalog(nowdb_store_t *store) {
	ts_algo_list_node_t *runner;
	ts_algo_list_t files;
	nowdb_version_t ver;
	uint32_t magic;
	nowdb_err_t err;
	char *buf;
	int off= 0;
	int sz = measureCatalogSize(store);
	if (sz == 0) return nowdb_err_get(nowdb_err_catalog, FALSE, OBJECT,
	                                                   store->catalog);
	buf = malloc(sz);
	if (buf == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                   FALSE, OBJECT, "allocating buffer");
	err = readCatalogFile(store, buf, sz);
	if (err != NULL) {
		free(buf); return err;
	}

	memcpy(&magic, buf, 4); off+=4;
	if (magic != NOWDB_MAGIC) return nowdb_err_get(nowdb_err_magic,
	                                FALSE, OBJECT, store->catalog);
	memcpy(&ver, buf+off, 4); off+=4;

	ts_algo_list_init(&files);
	while(off < sz) {
		err = readCatalogLine(store, &files, buf, &off, sz, ver);
		if (err != NOWDB_OK) {
			destroyFiles(&files); return err;
		}
	}
	free(buf);

	off = 1;
	fprintf(stdout, "Version: %u\n", ver);

	for(runner=files.head; runner!=NULL; runner=runner->nxt) {
		nowdb_file_t *file = runner->cont;
		fprintf(stdout, "\n%05u. entry\n", off);
		fprintf(stdout, "============\n");
		fprintf(stdout, "Id         : %u\n", file->id);
		fprintf(stdout, "Path       : %s\n", file->path);
		fprintf(stdout, "Capacity   : %u\n", file->capacity);
		fprintf(stdout, "Size       : %u\n", file->size);
		fprintf(stdout, "Blocksize  : %u\n", file->blocksize);
		fprintf(stdout, "Recordsize : %u\n", file->recordsize);
		if (file->comp == NOWDB_COMP_FLAT) {
			fprintf(stdout, "Compressed : NO\n");
		} else if (file->comp == NOWDB_COMP_ZSTD) {
			fprintf(stdout, "Compressed : ZSTD\n");
		} else {
			fprintf(stdout, "Compressed : ?\n");
		}
		fprintf(stdout, "Encryption : %u\n", file->encp);
		fprintf(stdout, "Grain      : %ld\n", file->grain);
		fprintf(stdout, "Oldest     : %ld\n", file->oldest);
		fprintf(stdout, "Newest     : %ld\n", file->newest);
		if (file->ctrl & NOWDB_FILE_SPARE) {
			fprintf(stdout, "Role       : SPARE\n");
		} else if (file->ctrl & NOWDB_FILE_WRITER) {
			fprintf(stdout, "Role       : WRITER\n");
		} else if (file->ctrl & NOWDB_FILE_READER) {
			fprintf(stdout, "Role       : READER\n");
		}
		if (file->ctrl & NOWDB_FILE_SORT) {
			fprintf(stdout, "Sorted     : YES\n");
		} else {
			fprintf(stdout, "Sorted     : NO\n");
		}
		off++;
	}
	destroyFiles(&files);
	return NOWDB_OK;
}

