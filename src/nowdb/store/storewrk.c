/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Workers for stores
 * ========================================================================
 */
#include <nowdb/store/storewrk.h>
#include <nowdb/store/store.h>
#include <nowdb/store/comp.h>
#include <nowdb/io/file.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include <zstd.h>
#include <zstd/zdict.h>

#define SYNCPERIOD    500000000l
#define SORTPERIOD    500000000l
#define SYNCTIMEOUT 10000000000l
#define SORTTIMEOUT 60000000000l

static nowdb_err_t syncjob(nowdb_worker_t      *wrk,
                           uint32_t              id,
                           nowdb_wrk_message_t *msg);

static nowdb_err_t sortjob(nowdb_worker_t      *wrk,
                           uint32_t              id,
                           nowdb_wrk_message_t *msg);

static void nodrain(void **ignore) {}

static nowdb_wrk_message_t sortmsg = {11, NULL};

nowdb_err_t nowdb_store_startSync(nowdb_worker_t *wrk,
                                  void         *store,
                                  nowdb_queue_t *errq) 
{
	if (wrk == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                             "store", "worker object is NULL");
	if (store == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                                "store", "store object is NULL");

	return nowdb_worker_init(wrk, "sync", 1, SYNCPERIOD, &syncjob,
	                                       errq, &nodrain, store);
}

nowdb_err_t nowdb_store_stopSync(nowdb_worker_t *wrk) {
	return nowdb_worker_stop(wrk, SYNCTIMEOUT);
}

nowdb_err_t nowdb_store_startSorter(nowdb_worker_t *wrk,
                                    void        *pstore,
                                    nowdb_queue_t *errq) {
	nowdb_store_t *store = pstore;
	if (wrk == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                             "store", "worker object is NULL");
	if (store == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                                "store", "store object is NULL");
	return nowdb_worker_init(wrk, "sorter",
	                         store->tasknum,
	                         SORTPERIOD, &sortjob,
	                         errq, &nodrain, store);
}

nowdb_err_t nowdb_store_stopSorter(nowdb_worker_t *wrk) {
	return nowdb_worker_stop(wrk, SORTTIMEOUT);
}

nowdb_err_t nowdb_store_sortNow(nowdb_worker_t *wrk) {
	return nowdb_worker_do(wrk, &sortmsg);
}

static nowdb_err_t syncjob(nowdb_worker_t      *wrk,
                           uint32_t              id,
                           nowdb_wrk_message_t *msg) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_store_t *store = wrk->rsc;
	
	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	if (store->writer->dirty) {
		err = nowdb_file_sync(store->writer);
		store->writer->dirty = FALSE;
	}

	err2 = nowdb_unlock_write(&store->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

nowdb_cmp_t nowdb_store_edge_compare(const void *left,
                                     const void *right,
                                     void      *ignore)
{
	if (((nowdb_edge_t*)left)->origin <  
	    ((nowdb_edge_t*)right)->origin) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->origin >
	    ((nowdb_edge_t*)right)->origin) return NOWDB_SORT_GREATER;

	if (((nowdb_edge_t*)left)->edge   <
	    ((nowdb_edge_t*)right)->edge) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->edge   >
	    ((nowdb_edge_t*)right)->edge) return NOWDB_SORT_GREATER;

	if (((nowdb_edge_t*)left)->destin <
	    ((nowdb_edge_t*)right)->destin) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->destin >
	    ((nowdb_edge_t*)right)->destin) return NOWDB_SORT_GREATER;

	if (((nowdb_edge_t*)left)->label  <
	    ((nowdb_edge_t*)right)->label) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->label >
	    ((nowdb_edge_t*)right)->label) return NOWDB_SORT_GREATER;

	if (((nowdb_edge_t*)left)->timestamp <
	    ((nowdb_edge_t*)right)->timestamp) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->timestamp >
	    ((nowdb_edge_t*)right)->timestamp) return NOWDB_SORT_GREATER;

	return NOWDB_SORT_EQUAL;
}

nowdb_cmp_t nowdb_store_vertex_compare(const void *left,
                                       const void *right,
                                       void      *ignore)
{
	if (((nowdb_vertex_t*)left)->role <
	    ((nowdb_vertex_t*)right)->role) return NOWDB_SORT_LESS;
	if (((nowdb_vertex_t*)left)->role >
	    ((nowdb_vertex_t*)right)->role) return NOWDB_SORT_GREATER;

	if (((nowdb_vertex_t*)left)->vertex <
	    ((nowdb_vertex_t*)right)->vertex) return NOWDB_SORT_LESS;
	if (((nowdb_vertex_t*)left)->vertex >
	    ((nowdb_vertex_t*)right)->vertex) return NOWDB_SORT_GREATER;

	if (((nowdb_vertex_t*)left)->property <
	    ((nowdb_vertex_t*)right)->property) return NOWDB_SORT_LESS;
	if (((nowdb_vertex_t*)left)->property >
	    ((nowdb_vertex_t*)right)->property) return NOWDB_SORT_GREATER;

	return NOWDB_SORT_EQUAL;
}

static inline nowdb_err_t findMinMax(char *buf, nowdb_file_t *file) {
	nowdb_time_t max = NOWDB_TIME_DAWN;
	nowdb_time_t min = NOWDB_TIME_DUSK;
	nowdb_edge_t *e;
	for (int i=0; i<file->size; i+=file->recordsize) {
		e = (nowdb_edge_t*)(buf+i);
		if (e->timestamp < min) min = e->timestamp;
		if (e->timestamp > max) max = e->timestamp;
	}
	file->oldest = min;
	file->newest = max;
	return NOWDB_OK;
}

static inline void setMinMax(nowdb_file_t *src, nowdb_file_t *trg) {
	if (trg->oldest == NOWDB_TIME_DAWN || src->oldest < trg->oldest)
		trg->oldest = src->oldest;

	if (trg->newest == NOWDB_TIME_DUSK || src->newest > trg->newest)
		trg->newest = src->newest;
}

/*
#define DICTNAME "zdict"
nowdb_err_t nowdb_store_loadZSTDDict(nowdb_store_t *store) {
	char *buf;
	nowdb_err_t err;
	nowdb_path_t p;
	struct stat st;
	ssize_t x;
	FILE *d;

	p = nowdb_path_append(store->path, DICTNAME);
	if (p == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                   FALSE, "store", "append to path");
	if (stat(p, &st) != 0) {
		free(p); return NOWDB_OK;
	}
	buf = malloc(st.st_size);
	if (buf == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, "store", p);
		free(p); return err;
	}
	d = fopen(p, "rb");
	if (d == NULL) {
		err = nowdb_err_get(nowdb_err_open, TRUE, "store", p);
		free(buf); free(p); return err;
	}
	x = fread(buf, 1, st.st_size, d);
	if (x != st.st_size) {
		err = nowdb_err_get(nowdb_err_read, TRUE, "store", p);
		fclose(d); free(buf); free(p); return err;
	}
	fclose(d); free(p);

	store->cdict = ZSTD_createCDict(buf, st.st_size, NOWDB_ZSTD_LEVEL);
	if (store->cdict == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
		                      "cannot load ZSTD dictionary");
		free(buf); return err;
	}
	store->ddict = ZSTD_createDDict(buf, st.st_size);
	if (store->cdict == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
		                      "cannot load ZSTD dictionary");
		free(buf); return err;
	}
	free(buf);

	return NOWDB_OK;
}

#define DICTSIZE 102400
static inline nowdb_err_t trainZSTDDict(nowdb_store_wrapper_t *wrap,
                                        char *buf, uint32_t size) {
	nowdb_path_t  p;
	nowdb_err_t err;
	size_t dsz;
	size_t *samplesz;
	size_t nm;
	char *dictbuf;
	FILE *d;
	ssize_t x;

	dictbuf = malloc(DICTSIZE);
	if (dictbuf == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
		                                 "allocating buffer");
	}
	nm = size/NOWDB_IDX_PAGE;
	if (nm == 0) {
		err = nowdb_err_get(nowdb_err_invalid, FALSE, "store",
		                          "training buffer too small");
		free(dictbuf); return err;
	}
	samplesz = malloc(nm*sizeof(size_t));
	if (samplesz == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
		                                "allocating buffer"); 
		free(dictbuf); return err;
	}
	for(int i=0;i<nm;i++) {
		samplesz[i] = NOWDB_IDX_PAGE;
	}
	dsz = ZDICT_trainFromBuffer(dictbuf, DICTSIZE, buf, samplesz, nm);
	if (ZDICT_isError(dsz)) {
		err = nowdb_err_get(nowdb_err_compdict, FALSE, "store",
		                       (char*)ZDICT_getErrorName(dsz));
		free(dictbuf); free(samplesz); return err;
	}
	p = nowdb_path_append(wrap->store->path, DICTNAME);
	if (p == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
		                                "appending to path");
		free(dictbuf); free(samplesz); return err;
	}
	d = fopen(p, "wb");
	if (d == NULL) {
		err = nowdb_err_get(nowdb_err_open, TRUE, "store", p);
		free(p); free(dictbuf); free(samplesz); return err;
	}
	x = fwrite(dictbuf, 1, dsz, d);
	if (x != dsz) {
		err = nowdb_err_get(nowdb_err_write, TRUE, "store", p);
		fclose(d); free(p); free(dictbuf); free(samplesz);
		return err;
	}
	fclose(d); free(p);
	free(dictbuf); free(samplesz);
	return NOWDB_OK;
}
*/

static inline nowdb_err_t getZSTDResources(nowdb_store_t     *store,
                                           char *buf, uint32_t size) {
	nowdb_err_t err2, err = NOWDB_OK;

	err = nowdb_lock(&store->ctx->lock);
	if (err != NOWDB_OK) return err;

	if (store->ctx->cdict != NULL) {
		return nowdb_unlock(&store->ctx->lock);
	}

	err = nowdb_compctx_loadDict(store->ctx, store->path);
	if (err != NOWDB_OK) {
		if (err->errcode != nowdb_err_stat) goto unlock;
		nowdb_err_release(err);
	}
	if (store->ctx->cdict != NULL) goto unlock;

	err = nowdb_compctx_trainDict(store->ctx,
	                              store->path,
	                              buf,  size);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_compctx_loadDict(store->ctx, store->path);
	if (err != NOWDB_OK) goto unlock;

	if (store->ctx->cdict == NULL) {
		err = nowdb_err_get(nowdb_err_compdict, FALSE, "store",
		                          "no compression dictionary");
		goto unlock;
	}
unlock:
	err2 = nowdb_unlock(&store->ctx->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: config compression in reader
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t configReader(nowdb_store_t *store, nowdb_file_t *file) {
	nowdb_err_t err;
	if (file->comp == NOWDB_COMP_ZSTD) {
		err = nowdb_compctx_getCDict(store->ctx, &file->cdict);
		if (err != NOWDB_OK) return err;
		err = nowdb_compctx_getCCtx(store->ctx, &file->cctx);
		if (err != NOWDB_OK) return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: config compression in reader
 * ------------------------------------------------------------------------
 */
static inline void releaseReader(nowdb_store_t *store, nowdb_file_t *file) {
	nowdb_err_t err;
	if (file->comp == NOWDB_COMP_ZSTD) {
		err = nowdb_compctx_releaseCCtx(store->ctx, file->cctx);
		if (err != NOWDB_OK) {
			nowdb_err_print(err); nowdb_err_release(err);
		}
	}
	file->cctx  = NULL;
	file->cdict = NULL;
	nowdb_store_releaseReader(store, file);
}

static inline nowdb_err_t getReader(nowdb_store_t *store,
                                    nowdb_file_t  **file) {
	nowdb_err_t err;

	err = nowdb_store_getFreeReader(store, file);
	if (err != NOWDB_OK) return err;
	if (*file != NULL) return NOWDB_OK;
	err = nowdb_store_createReader(store, file);
	if (err != NOWDB_OK) return err;
	(*file)->capacity = store->largesize;
	configReader(store, *file);
	return nowdb_file_create(*file);
}

static inline nowdb_err_t getContent(nowdb_worker_t *wrk,
                                     nowdb_file_t  *file,
                                     char           *buf) {
	FILE *stream;
	ssize_t    x;

	stream = fopen(file->path, "r");
	if (stream == NULL) return nowdb_err_get(nowdb_err_open, TRUE,
	                                       wrk->name, file->path);
	x = fread(buf, 1, file->size, stream);
	if (x != file->size) {
		fclose(stream);
		return nowdb_err_get(nowdb_err_read, TRUE,
	                           wrk->name, file->path);
	}
	if (fclose(stream) != 0) {
		return nowdb_err_get(nowdb_err_close, TRUE,
	                            wrk->name, file->path);
	}
	return NOWDB_OK;
}

static inline nowdb_err_t putContent(char *buf, uint32_t size,
                                     nowdb_file_t       *file) {
	nowdb_err_t err;

	err = nowdb_file_open(file);
	if (err != NOWDB_OK) return err;

	err = nowdb_file_position(file, file->size);
	if (err != NOWDB_OK) return err;

	for(uint32_t i=0; i<size; i+=file->bufsize) {
		err = nowdb_file_writeBuf(file, buf+i, file->bufsize);
		if (err != NOWDB_OK) {
			NOWDB_IGNORE(nowdb_file_close(file));
			return err;
		}
	}
	return nowdb_file_close(file);
}

static inline nowdb_err_t compsort(nowdb_worker_t  *wrk,
                                   uint32_t          id,
                                   nowdb_store_t *store) {
	nowdb_err_t err;
	nowdb_file_t *src=NULL;
	nowdb_file_t *reader=NULL;
	char *buf=NULL;

	// fprintf(stderr, "SORTING\n");

	/* get waiting file 
	 * we have to free it in case of error!
	 */
	err = nowdb_store_getWaiting(store, &src);
	if (err != NOWDB_OK) return err;
	if (src == NULL) return NOWDB_OK;

	/* get content from source */
	buf = malloc(src->size);
	if (buf == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, wrk->name,
		                            "allocating large buffer");
		nowdb_store_releaseWaiting(store, src);
		nowdb_file_destroy(src); free(src); return err;
	}
	err = getContent(wrk, src, buf);
	if (err != NOWDB_OK) {
		nowdb_store_releaseWaiting(store, src);
		nowdb_file_destroy(src); free(src);
		free(buf); return err;
	}

	/* find min/max if this is edges */
	if (store->recsize == 64) { /* not too convincing */
		err = findMinMax(buf, src);
		if (err != NOWDB_OK) {
			nowdb_store_releaseWaiting(store, src);
			nowdb_file_destroy(src); free(src);
			free(buf); return err;
		}
	}

	/* sort buf -- if compare is not NULL! */
	if (store->compare != NULL) {
		nowdb_mem_sort(buf, src->size/store->recsize,
		                    store->recsize,
		                    store->compare, NULL);
	}

	/* prepare compression */
	if (store->comp == NOWDB_COMP_ZSTD) {
		err = getZSTDResources(store, buf, src->size);
		if (err != NOWDB_OK) {
			nowdb_store_releaseWaiting(store, src);
			nowdb_file_destroy(src); free(src);
			free(buf); return err;
		}
	}

	/* get (or create) reader with space */
	err = getReader(store, &reader);
	if (err != NOWDB_OK) {
		nowdb_store_releaseWaiting(store, src);
		nowdb_file_destroy(src); free(src);
		free(buf); return err;
	}

	/* set file sorted */
	if (store->compare != NULL) {
		reader->ctrl |= NOWDB_FILE_SORT;
	}

	/* set and reset min/max */
	setMinMax(src, reader);
	src->oldest = NOWDB_TIME_DAWN;
	src->newest = NOWDB_TIME_DUSK;

	/* write to reader (potentially compressing) */
	err = putContent(buf, src->size, reader);
	if (err != NOWDB_OK) {
		releaseReader(store, reader);
		nowdb_file_destroy(reader); free(reader); free(buf);
		nowdb_store_releaseWaiting(store, src);
		nowdb_file_destroy(src); free(src); return err;
	}

	/* insert into index */
	/* TODO              */

	/* we don't need it anymore */
	free(buf);

	/* release compression context */
	err = nowdb_compctx_releaseCCtx(store->ctx, reader->cctx);
	if (err != NOWDB_OK) {
		nowdb_err_print(err); nowdb_err_release(err);
	}

	/* promote to reader */
	err = nowdb_store_promote(store, src, reader);
	if (err != NOWDB_OK) {
		releaseReader(store, reader);
		nowdb_file_destroy(reader); free(reader);
		nowdb_store_releaseWaiting(store, src);
		nowdb_file_destroy(src); free(src); return err;
	}

	/* erase waiting... */
	err = nowdb_file_erase(src);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(src); free(src); return err;
	}

	/* ...and donate it */
	err = nowdb_store_donate(store, src);
	if (err != NOWDB_OK) return err;

	// fprintf(stderr, "DONE SORTING\n");
	return NOWDB_OK;
}

static nowdb_err_t sortjob(nowdb_worker_t      *wrk,
                           uint32_t              id,
                           nowdb_wrk_message_t *msg) {
	return compsort(wrk, id, wrk->rsc);
}
