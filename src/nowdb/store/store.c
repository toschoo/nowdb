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

extern char nowdb_nullrec[1024];

#define MAX_FILE_NAME 32

#define MAX_SPARES 9
#define MIN_SPARES 3

#define STORENULL() \
	if (store == NULL) { \
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, \
		                              "store object is NULL"); \
	}

#define LISTNULL() \
	if (list == NULL) { \
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, \
		                               "list object is NULL"); \
	}

#define FILENULL() \
	if (file == NULL) { \
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, \
		                               "file object is NULL"); \
	}

/* ------------------------------------------------------------------------
 * Sorter message
 * ------------------------------------------------------------------------
 */
static nowdb_wrk_message_t sortmsg = {11,NULL,NULL};

/* ------------------------------------------------------------------------
 * Allocate and initialise new store object
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_new(nowdb_store_t **store,
                            nowdb_path_t     base,
                            nowdb_plru8r_t   *lru,
                            nowdb_version_t   ver,
                            nowdb_content_t  cont,
                            nowdb_storage_t *strg,
                            uint32_t      recsize,
                            char               ts)
{
	nowdb_err_t err = NOWDB_OK;

	STORENULL();

	*store = malloc(sizeof(nowdb_store_t));
	if (*store == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                          "allocating store object");
	}
	err = nowdb_store_init(*store, base, lru, ver,
                              cont, strg, recsize, ts);
	if (err != NOWDB_OK) {
		free(*store); *store = NULL; return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Tree callbacks for readers: compare
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t compare(void *ignore, void *left, void *right) {
	if (((nowdb_file_t*)left)->id < 
	    ((nowdb_file_t*)right)->id) return ts_algo_cmp_less;
	if (((nowdb_file_t*)left)->id > 
	    ((nowdb_file_t*)right)->id) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Tree callbacks for readers: update:
 * - the old values are replaced by new values
 * - the new file descriptor is freed
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t update(void *ignore, void *o, void *n) {
	NOWDB_IGNORE(nowdb_file_update(n, o));
	nowdb_file_destroy(n); free(n);
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Tree callbacks for readers: delete (we do not delete files...?)
 * ------------------------------------------------------------------------
 */
static void delete(void *ignore, void **n) {}

/* ------------------------------------------------------------------------
 * Tree callbacks for readers: destroy
 * ------------------------------------------------------------------------
 */
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
                                       nowdb_bool_t    openit,
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
		if (openit) {
			err = nowdb_file_open(file);
			if (err != NOWDB_OK) return err;
		}
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
                             nowdb_plru8r_t   *lru,
                             nowdb_version_t   ver,
                             nowdb_content_t  cont,
                             nowdb_storage_t *strg,
                             uint32_t      recsize,
                             char               ts)
{
	nowdb_err_t err;
	size_t s;

	STORENULL();

	/* defaults */
	store->version = ver;
	store->recsize = recsize;
	store->filesize = strg->filesize;
	store->largesize = strg->largesize;
	store->starting = FALSE;
	store->state = NOWDB_STORE_CLOSED;
	store->path = NULL;
	store->catalog = NULL;
	store->writer = NULL;
	store->compare = NULL;
	store->iman = NULL;
	store->lru  = lru;
	store->context = NULL;
	store->comp = NOWDB_COMP_FLAT;
	store->ctx  = NULL;
	store->nextid = 1;
	store->ts = ts;
	store->cont = cont;
	store->storage = strg;

	// prepare sort message
	memcpy(&store->srtmsg, &sortmsg, sizeof(nowdb_wrk_message_t));
	store->srtmsg.stcont = store;

	store->setsize = nowdb_pagectrlSize(recsize);

	/* lists of files */
	err = initAllFiles(store);
	if (err != NOWDB_OK) return err;

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
	s = strnlen(base, NOWDB_MAX_PATH-3);
	if (s >= NOWDB_MAX_PATH - 4) {
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
	strncpy(store->path, base, s); store->path[s] = 0;

	/* catalog */
	store->catalog = nowdb_path_append(store->path, "catalog");
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
	STORENULL();
	store->compare = compare;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Configure worker
 * ------------------------------------------------------------------------
 */
/*
nowdb_err_t nowdb_store_configWorkers(nowdb_store_t *store,
                                      uint32_t    tasknum) {
	STORENULL();
	if (tasknum > 32) return nowdb_err_get(nowdb_err_invalid,
	             FALSE, OBJECT, "tasknum too big (max: 32)");

	store->tasknum = tasknum;
	return NOWDB_OK;
}
*/

/* ------------------------------------------------------------------------
 * Configure indexing
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_configIndexing(nowdb_store_t *store,
                                       void           *iman,
                                       void        *context) {
	STORENULL();
	store->iman = iman;
	store->context = context;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Configure compression
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_configCompression(nowdb_store_t *store,
                                          nowdb_comp_t   comp) {
	nowdb_err_t err;

	STORENULL();

	store->comp = comp;
	if (comp == NOWDB_COMP_FLAT) return NOWDB_OK;
	
	err = nowdb_compctx_new(&store->ctx, 32, 128);
	if (err != NOWDB_OK) return err;

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
	if (store->ctx != NULL) {
		nowdb_compctx_destroy(store->ctx);
		free(store->ctx); store->ctx = NULL;
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
	int rc = 0;
	nowdb_time_t  t;
	nowdb_path_t  p;

	for(;;) {
		rc = nowdb_time_now(&t);
		if (rc != 0) return nowdb_err_get(rc, FALSE,
		                OBJECT, "getting timestamp");
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
	nowdb_bitmap8_t ctrl = NOWDB_FILE_WRITER | NOWDB_FILE_SPARE;

	if (store->ts) ctrl |= NOWDB_FILE_TS;

	p = nowdb_path_append(store->path, name);
	if (p == NULL) return nowdb_err_get(nowdb_err_no_mem,
	               FALSE, OBJECT, "allocating file path");
	err = nowdb_file_new(file, fid, p, store->filesize,0,
	                     NOWDB_IDX_PAGE, store->recsize,
	                     store->cont, ctrl,
	                     NOWDB_COMP_FLAT, NOWDB_ENCP_NONE,
	                     1, NOWDB_TIME_DAWN, NOWDB_TIME_DUSK); 
	free(p); return err;
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

	STORENULL();
	FILENULL();

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	err = makeFileName(store, fname);
	if (err != NOWDB_OK) goto unlock;
	
	fid = getFileId(store);

	err = makeFile(store, file, fname, fid);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_file_makeReader(*file);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(*file); free(*file); *file = NULL;
		goto unlock;
	}
	(*file)->comp = store->comp;
	
unlock:
	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: create file
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t createFile(nowdb_store_t *store, nowdb_fileid_t reuse) {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_fileid_t fid;
	char fname[MAX_FILE_NAME];

	err = makeFileName(store, fname);
	if (err != NOWDB_OK) return err;

	fid = reuse>0?reuse:getFileId(store);

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

/* ------------------------------------------------------------------------
 * Helper: create spares
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t createSpares(nowdb_store_t *store) {
	while (store->spares.len < MIN_SPARES) {
		nowdb_err_t err = createFile(store,0);
		if (err != NOWDB_OK) return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: get a new writer
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getWriter(nowdb_store_t *store) {
	ts_algo_list_node_t  *node;
	nowdb_err_t err = NOWDB_OK;

	if (store->writer != NULL) return NOWDB_OK;
	if (store->spares.len < 1) err = createFile(store,0);
	if (err != NOWDB_OK) return err;
	node = store->spares.head;
	ts_algo_list_remove(&store->spares, node);
	store->writer = node->cont; free(node);
	NOWDB_IGNORE(nowdb_file_makeWriter(store->writer));
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: add a file to spares (or remove it)
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t makeSpare(nowdb_store_t *store,
                                    nowdb_file_t  *file) {
	if (store->spares.len > MAX_SPARES) {
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

/* ------------------------------------------------------------------------
 * Helper: make a new spare
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t newSpare(nowdb_store_t *store,
                                   nowdb_file_t  *file) {
	nowdb_fileid_t fid = file->id;

	NOWDB_IGNORE(nowdb_file_close(file));
	NOWDB_IGNORE(nowdb_file_remove(file));
	nowdb_file_destroy(file); free(file);
	if (store->spares.len < MIN_SPARES) {
		return createFile(store, fid);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: make file waiting
 * ------------------------------------------------------------------------
 */
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
	return nowdb_store_sortNow(store->storage, store);
}

/* ------------------------------------------------------------------------
 * Helper: swap writer
 * ------------------------------------------------------------------------
 */
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

/* ------------------------------------------------------------------------
 * Helper: map next position in writer
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t remapWriter(nowdb_store_t *store) {
	if (store->writer->pos >= store->writer->capacity) {
		return swapWriter(store);
	}
	return nowdb_file_move(store->writer);
}

/* ------------------------------------------------------------------------
 * Helper: adjust writer position to real position on open,
 *         i.e. adjust to last written position instead of 'size'
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t adjustWriter(nowdb_store_t *store) {
	nowdb_err_t err=NOWDB_OK;
	uint32_t pos = 0;
	uint32_t realsz = (NOWDB_IDX_PAGE/store->recsize)*store->recsize;
	uint32_t remsz = NOWDB_IDX_PAGE - realsz;

	while (store->writer->pos < store->writer->capacity) {
		if (pos >= store->writer->bufsize) {
			if (store->writer->pos >= 
			    store->writer->capacity) break;
			err = nowdb_file_move(store->writer);
			if (err != NOWDB_OK) break;
			pos = 0;
		}
		if (memcmp(store->writer->mptr+pos,nowdb_nullrec,
		                     store->recsize) == 0) break;
		
		/*
		fprintf(stderr, "%u/%u/%u\n", store->writer->pos, pos,
		                              store->writer->bufsize);
		*/
		pos += store->recsize;
		store->writer->pos+=store->recsize;

		if (pos%NOWDB_IDX_PAGE >= realsz) {
			pos+=remsz;
			store->writer->pos+=remsz;
		}
	}
	if (err == NOWDB_OK) store->writer->size = store->writer->pos;
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: get writer ready for being written
 * ------------------------------------------------------------------------
 */
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

/* ------------------------------------------------------------------------
 * Helper: write store data to catalog
 * ------------------------------------------------------------------------
 */
static inline void writeCatalogHeader(nowdb_store_t *store,
                                      char *buf, int  *off)
{
	memcpy(buf+*off, &store->count, 8); *off+=8;
	memcpy(buf+*off, &store->max  , 8); *off+=8;
	memcpy(buf+*off, &store->min  , 8); *off+=8;
}

/* ------------------------------------------------------------------------
 * Helper: read store data into store
 * ------------------------------------------------------------------------
 */
static inline void readCatalogHeader(nowdb_store_t *store,
                                      char *buf, int *off) 
{
	memcpy(&store->count, buf+*off, 8); *off+=8;
	memcpy(&store->max  , buf+*off, 8); *off+=8;
	memcpy(&store->min  , buf+*off, 8); *off+=8;
}

/* ------------------------------------------------------------------------
 * Helper: write one file into catalog
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t writeCatalogLine(char *buf, int *off, 
                                            nowdb_file_t *file) {
	nowdb_path_t nm;

	memcpy(buf+*off, &file->id, 4); *off += 4;
	memcpy(buf+*off, &file->order, 4); *off += 4;
	memcpy(buf+*off, &file->capacity, 4); *off += 4;
	memcpy(buf+*off, &file->size, 4); *off += 4;
	memcpy(buf+*off, &file->recordsize, 4); *off += 4;
	memcpy(buf+*off, &file->blocksize, 4); *off += 4;
	memcpy(buf+*off, &file->cont, 1); *off += 1;
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

/* ------------------------------------------------------------------------
 * Helper: get catalog size from stat
 * ------------------------------------------------------------------------
 */
static inline uint32_t measureCatalogSize(nowdb_store_t *store) {
	return nowdb_path_filesize(store->catalog);
}

/* ------------------------------------------------------------------------
 * Helper: read one file from catalog
 * ------------------------------------------------------------------------
 */
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
	memcpy(&tmp.cont, buf+*off, 1); *off+=1;
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
	                            tmp.cont,
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

/* ------------------------------------------------------------------------
 * Helper: read catalog and create files
 * ------------------------------------------------------------------------
 */
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

	readCatalogHeader(store, buf, &off);

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

/* ------------------------------------------------------------------------
 * Helper: read catalog into buffer
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t readCatalogFile(nowdb_store_t *store,
                                           char *buf, int size) {
	return nowdb_readFile(store->catalog, buf, size);
}

/* ------------------------------------------------------------------------
 * Helper: read catalog, get files, prepare writer
 * ------------------------------------------------------------------------
 */
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

/* ------------------------------------------------------------------------
 * Helper: compute catalog size
 * ------------------------------------------------------------------------
 */
static inline uint32_t computeCatalogSize(nowdb_store_t *store) {
	/* once:
	 * MAGIC + version
         * count + max + min (8+8+8)
	 *
	 * per line:
	 * ---------
	 * id                4
	 * order,            4
	 * capactiy,         4 
	 * size,             4
	 * recsize,          4
	 * blocksize,        4
	 * cont,             1
	 * ctrl,             1
	 * compression,      4 
	 * encryption,       4
	 * grain,            8
	 * oldest,           8
	 * newest,           8
	 * file name        32
	 */
	uint32_t once = 32;
	uint32_t perline = 93;
	uint32_t nfiles = 1;

	nfiles += store->spares.len;
	nfiles += store->waiting.len;
	nfiles += store->readers.count;

	return once + nfiles * perline + 1;
}

/* ------------------------------------------------------------------------
 * Helper: write buffer to catalog
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t writeCatalogFile(nowdb_store_t *store,
                                            char *buf, int size) {
	return nowdb_writeFileWithBkp(store->path, store->catalog, buf, size);
}

/* ------------------------------------------------------------------------
 * Helper: compute catalog size and alloc buf
 *         write files to buffer and write buffers to disk
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t storeCatalog(nowdb_store_t *store) {
	nowdb_err_t err;
	uint32_t magic = NOWDB_MAGIC;
	char *buf = NULL;
	ts_algo_list_node_t *runner;
	ts_algo_list_t *readers;
	uint32_t sz;

	sz = computeCatalogSize(store);
	buf = malloc(sz);
	if (buf == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                   FALSE, OBJECT, "allocating buffer");

	int off=8;
	memcpy(buf, &magic, 4);
	memcpy(buf+4, &store->version, 4);

	writeCatalogHeader(store, buf, &off);

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
	if (store->comp == NOWDB_COMP_ZSTD &&
	    store->ctx != NULL) {
		/* if we can load it now, fine.
		 * Otherwise, we do it later */
		NOWDB_IGNORE(nowdb_compctx_loadDict(store->ctx,
		                                    store->path));
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

	store->state = NOWDB_STORE_OPEN;

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

	/* check if store is open */
	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;
	
	if (store->state != NOWDB_STORE_OPEN) {
		return nowdb_unlock_write(&store->lock);
	}

	/* close store is NOT THREADSAFE */
	err = nowdb_unlock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	/* write catalog */
	err = storeCatalog(store);
	if (err != NOWDB_OK) goto unlock;

	destroyAllFiles(store);

	err = initAllFiles(store);

	store->state = NOWDB_STORE_CLOSED;

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
	char *p;

	STORENULL();

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	if (nowdb_path_exists(store->path, NOWDB_DIR_TYPE_ANY)) {
		err = nowdb_err_get(nowdb_err_create, FALSE, OBJECT,
		                                        store->path);
		goto unlock;
	}

	err = nowdb_dir_create(store->path);
	if (err != NOWDB_OK) goto unlock;

	p = nowdb_path_append(store->path, "index");
	if (p == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                  "allocating path");
		goto unlock;
	}

	err = nowdb_dir_create(p); free(p);
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

	STORENULL();

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	ts_algo_list_init(&dir);
	err = nowdb_dir_content(store->path, NOWDB_DIR_TYPE_FILE |
	                                     NOWDB_DIR_TYPE_DIR, &dir);
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

#define REMAINDER (NOWDB_IDX_PAGE - (pos%NOWDB_IDX_PAGE))

/* ------------------------------------------------------------------------
 * Insert one record
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_insert(nowdb_store_t *store,
                               void          *data) {
	nowdb_err_t err  = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	uint32_t pos;

	STORENULL();
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
	store->count++;

	if (REMAINDER < store->recsize) {
		uint32_t d = REMAINDER;
		pos+=d;
		store->writer->size += d;
		store->writer->pos += d;
	}
	if (pos >= store->writer->bufsize) {
		err = remapWriter(store);
		if (err != NOWDB_OK) {
			fprintf(stderr, "remap error!\n");
			goto unlock;
		}
	}

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
 * Helper: set decompression settings to all files in list
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t setDecomp(nowdb_store_t *store,
                                    ts_algo_list_t *list) {
	ts_algo_list_node_t *runner;
	nowdb_file_t          *file;
	nowdb_err_t    err=NOWDB_OK;
	ZSTD_DDict    *dict;
	ZSTD_DCtx     *ctx;

	if (store->comp == NOWDB_COMP_FLAT) return NOWDB_OK;
	if (list->len == 0) return NOWDB_OK;

	err = nowdb_compctx_getDCtx(store->ctx, &ctx);
	if (err != NOWDB_OK) return err;

	err = nowdb_compctx_getDDict(store->ctx, &dict);
	if (err != NOWDB_OK) {
		NOWDB_IGNORE(nowdb_compctx_releaseDCtx(store->ctx, ctx));
		return err;
	}
	for(runner=list->head; runner!=NULL; runner=runner->nxt) {
		file = runner->cont;
		file->ddict = dict;
		file->dctx  = ctx;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * To be with the period means
 * ------------------------------------------------------------------------
 */
static nowdb_bool_t inPeriod(void *rsc,
                       const void *pattern,
                       const void *node) {
	return (((nowdb_file_t*)node)->oldest <=
	        ((nowdb_file_t*)pattern)->newest &&
	        ((nowdb_file_t*)node)->newest >=
	        ((nowdb_file_t*)pattern)->oldest);
}

/* ------------------------------------------------------------------------
 * Helper: get all readers for period start - end
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getReaders(nowdb_store_t *store,
                                     ts_algo_list_t *list,
                                     nowdb_time_t   start,
                                     nowdb_time_t     end) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_t tmp;
	nowdb_file_t   pattern;

	if (store->readers.count == 0) return NOWDB_OK;

	pattern.oldest = start;
	pattern.newest = end;

	ts_algo_list_init(&tmp);
	if (ts_algo_tree_filter(&store->readers,
	              &tmp, &pattern, &inPeriod) != TS_ALGO_OK)
	{
		err = nowdb_err_get(nowdb_err_no_mem,
		     FALSE, OBJECT, "readers toList");
		ts_algo_list_destroy(&tmp);
		return err;
	}
	err = copyFileList(&tmp, list, FALSE, start, end);
	ts_algo_list_destroy(&tmp);
	return err;
}

/* ------------------------------------------------------------------------
 * Get all readers for period start - end
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_getReaders(nowdb_store_t *store,
                                   ts_algo_list_t *list,
                                   nowdb_time_t   start,
                                   nowdb_time_t    end) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	STORENULL();
	LISTNULL();

	err = nowdb_lock_read(&store->lock);
	if (err != NOWDB_OK) return err;
	err = getReaders(store, list, start, end);
	err2 = nowdb_unlock_read(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return setDecomp(store, list);
}

/* ------------------------------------------------------------------------
 * Helper: get all pending files
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getAllWaiting(nowdb_store_t *store,
                                        ts_algo_list_t *list) {
	nowdb_file_t *file;
	nowdb_err_t    err;

	if (store->waiting.len > 0) {
		err = copyFileList(&store->waiting, list, TRUE,
		                               NOWDB_TIME_DAWN,
		                               NOWDB_TIME_DUSK);
		if (err != NOWDB_OK) return err;
	}

	/* writer */
	err = copyFile(store->writer, &file);
	if (err != NOWDB_OK) return err;

	err = nowdb_file_makeReader(file);
	if (err != NOWDB_OK) return err;

	err = nowdb_file_open(file);
	if (err != NOWDB_OK) return err;

	if (ts_algo_list_append(list, file) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                     "list append");
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: get all files for period start - end
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getFiles(nowdb_store_t *store,
                                   ts_algo_list_t *list,
                                   nowdb_time_t   start,
                                   nowdb_time_t     end) {
	nowdb_err_t    err;
	
	/* readers */
	err = getReaders(store, list, start, end);
	if (err != NOWDB_OK) return err;

	/* set decompression settings to readers */
	err = setDecomp(store, list);
	if (err != NOWDB_OK) return err;

	/* pending and writer */
	err = getAllWaiting(store, list);
	if (err != NOWDB_OK) return err;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get all files for period start - end
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_getFiles(nowdb_store_t *store,
                                 ts_algo_list_t *list,
                                 nowdb_time_t   start,
                                 nowdb_time_t    end) {
	nowdb_err_t  err=NOWDB_OK;
	nowdb_err_t err2=NOWDB_OK;

	STORENULL();
	LISTNULL();

	err = nowdb_lock_read(&store->lock);
	if (err != NOWDB_OK) return err;

	err = getFiles(store, list, start, end);

	err2 = nowdb_unlock_read(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Get n copies of all files for period start - end
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_getNFiles(nowdb_store_t  *store,
                                  uint32_t       copies,
                                  ts_algo_list_t *lists,
                                  nowdb_time_t    start,
                                  nowdb_time_t      end) {
	nowdb_err_t  err=NOWDB_OK;
	nowdb_err_t err2=NOWDB_OK;

	STORENULL();
	if (lists == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "list pointer is NULL");

	err = nowdb_lock_read(&store->lock);
	if (err != NOWDB_OK) return err;

	for(uint32_t i=0;i<copies;i++) {
		err = getFiles(store, lists+i, start, end);
		if (err != NOWDB_OK) break;
	}
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_read(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Get all pending (pending only)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_getAllWaiting(nowdb_store_t  *store,
                                      ts_algo_list_t *list) {
	nowdb_err_t  err=NOWDB_OK;
	nowdb_err_t err2=NOWDB_OK;

	STORENULL();
	LISTNULL();

	err = nowdb_lock_read(&store->lock);
	if (err != NOWDB_OK) return err;

	err = getAllWaiting(store, list);

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

	STORENULL();
	FILENULL();

	if (found == NULL) return nowdb_err_get(nowdb_err_invalid,
	                FALSE, OBJECT, "found parameter is NULL");
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

	STORENULL();
	FILENULL();

	if (found == NULL) return nowdb_err_get(nowdb_err_invalid,
	                 FALSE, OBJECT, "found parameter is NULL");
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

	STORENULL();
	FILENULL();

	if (found == NULL) return nowdb_err_get(nowdb_err_invalid,
	                 FALSE, OBJECT, "found parameter is NULL");

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
 * A reader is free if ...
 * ------------------------------------------------------------------------
 */
static ts_algo_bool_t isFree(void *rsc,
                       const void *pattern,
                       const void *file) {
	return (!((nowdb_file_t*)file)->used &&
	         ((nowdb_file_t*)file)->size < NOWDB_FILE_MAXSIZE);
}

/* ------------------------------------------------------------------------
 * Find reader with capacity to store more
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_getFreeReader(nowdb_store_t *store,
                                      nowdb_file_t  **file) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_file_t  *tmp=NULL;

	STORENULL();
	FILENULL();

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	/* nothing available */
	if (store->readers.count == 0) goto unlock;

	/* search the tree */
	tmp = ts_algo_tree_search(&store->readers, NULL, &isFree);
	if (tmp != NULL) {
		*file = malloc(sizeof(nowdb_file_t));
		if (*file == NULL) {
			err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
				              "allocating file descriptor");
			goto unlock;
		}
		err = nowdb_file_copy(tmp, *file);
		if (err != NOWDB_OK) {
			free(*file); *file = NULL; goto unlock;
		}
		tmp->used = TRUE;
	}
unlock:
	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Release reader
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_releaseReader(nowdb_store_t *store,
                                      nowdb_file_t  *file) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_file_t  *tmp=NULL;

	STORENULL();
	FILENULL();

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	/* nothing available */
	if (store->readers.count == 0) goto unlock;

	tmp = ts_algo_tree_find(&store->readers, file);
	if (tmp == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found, FALSE, OBJECT,
	                                               "reader not found");
		goto unlock;
	}
	tmp->used = FALSE;

unlock:
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
	nowdb_file_t *f;

	STORENULL();
	FILENULL();

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	for(tmp=store->waiting.head; tmp!=NULL; tmp=tmp->nxt) {
		f = tmp->cont; if (!f->used) break;
	}
	if (tmp == NULL) goto unlock;

	*file = malloc(sizeof(nowdb_file_t));
	if (*file == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                      "allocating file descriptor");
		goto unlock;
	}
	err = nowdb_file_copy(tmp->cont, *file);
	if (err != NOWDB_OK) goto unlock;
	f->used = TRUE;

unlock:
	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Release waiting
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_releaseWaiting(nowdb_store_t *store,
                                       nowdb_file_t  *file) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	ts_algo_list_node_t *tmp;
	nowdb_file_t *f;

	STORENULL();
	FILENULL();

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	tmp = findInList(&store->waiting, file);
	if (tmp == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found, FALSE, OBJECT,
	                                              "waiting not found");
		goto unlock;
	}
	f = tmp->cont; f->used = FALSE;
unlock:
	err2 = nowdb_unlock_write(&store->lock);
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

	STORENULL();
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

	STORENULL();
	FILENULL();

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	file->used = FALSE;
	err = makeSpare(store, file);

	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Release file: not pending any more
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_releasePending(nowdb_store_t *store,
                                       nowdb_file_t *file) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	STORENULL();
	FILENULL();

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	file->used = FALSE;
	err = newSpare(store, file);

	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Release decompression contexts from files
 * ------------------------------------------------------------------------
 */
static inline void releaseDecomp(nowdb_store_t  *store,
                                 ts_algo_list_t *files) {
	nowdb_err_t err;
	ts_algo_list_node_t *runner;
	nowdb_file_t        *file;

	if (files->head == NULL) return;
	file = files->head->cont;
	if (file->dctx != NULL) {
		err = nowdb_compctx_releaseDCtx(store->ctx,
			                        file->dctx);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
		}
	}
	for(runner=files->head; runner!=NULL; runner=runner->nxt) {
		file = runner->cont;
		if (file->dctx != NULL) {
			file->dctx = NULL;
			file->ddict = NULL;
		}
	}
}

/* ------------------------------------------------------------------------
 * Destroy files and list
 * ------------------------------------------------------------------------
 */
void nowdb_store_destroyFiles(nowdb_store_t  *store,
                              ts_algo_list_t *files) {
	releaseDecomp(store, files);
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
 * Count all records in store
 * ------------------------------------------------------------------------
 */
uint64_t nowdb_store_count(nowdb_store_t *store) {
	return store->count;
}

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

	readCatalogHeader(store, buf, &off);

	ts_algo_list_init(&files);
	while(off < sz) {
		err = readCatalogLine(store, &files, buf, &off, sz, ver);
		if (err != NOWDB_OK) {
			destroyFiles(&files); return err;
		}
	}
	free(buf);

	off = 1;
	fprintf(stdout, "Count  : %lu\n", store->count);
	fprintf(stdout, "Max    : %lu\n", store->max);
	fprintf(stdout, "Min    : %lu\n", store->min);

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
