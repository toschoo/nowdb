/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Workers for stores
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <nowdb/store/storewrk.h>
#include <nowdb/store/store.h>
#include <nowdb/store/storage.h>
#include <nowdb/store/comp.h>
#include <nowdb/store/indexer.h>
#include <nowdb/index/man.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include <tsalgo/list.h>

#include <zstd.h>
#include <zstd/zdict.h>

/* ------------------------------------------------------------------------
 * Sync Worker Period and Timeout
 * ------------------------------------------------------------------------
 */
#define SYNCPERIOD    500000000l
#define SYNCTIMEOUT 10000000000l

/* ------------------------------------------------------------------------
 * Sorter Period and Timeout
 * ------------------------------------------------------------------------
 */
#define SORTPERIOD     500000000l
#define SORTTIMEOUT 300000000000l

#define NOMEM(x) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, "store", x);

/* ------------------------------------------------------------------------
 * Syncjob predeclaration
 * ------------------------------------------------------------------------
 */
static nowdb_err_t syncjob(nowdb_worker_t      *wrk,
                           uint32_t              id,
                           nowdb_wrk_message_t *msg);

/* ------------------------------------------------------------------------
 * Sorter  predeclaration
 * ------------------------------------------------------------------------
 */
static nowdb_err_t sortjob(nowdb_worker_t      *wrk,
                           uint32_t              id,
                           nowdb_wrk_message_t *msg);

/* ------------------------------------------------------------------------
 * All messages are static, no drain required for queues
 * ------------------------------------------------------------------------
 */
static void nodrain(void **ignore) {}

/* ------------------------------------------------------------------------
 * Start Sync Worker
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_startSync(nowdb_worker_t *wrk,
                                  void          *strg,
                                  nowdb_queue_t *errq) 
{
	if (wrk == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                             "store", "worker object is NULL");
	if (strg == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                              "store", "storage object is NULL");

	return nowdb_worker_init(wrk, "sync", 1, SYNCPERIOD, &syncjob,
	                                         errq, &nodrain, strg);
}

/* ------------------------------------------------------------------------
 * Stop Sync Worker
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_stopSync(nowdb_worker_t *wrk) {
	return nowdb_worker_stop(wrk, SYNCTIMEOUT);
}

/* ------------------------------------------------------------------------
 * Start Sorter
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_startSorter(nowdb_worker_t *wrk,
                                    void         *pstrg,
                                    nowdb_queue_t *errq) {
	nowdb_storage_t *strg = pstrg;
	if (wrk == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                             "store", "worker object is NULL");
	if (strg == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                           "storeage", "storage object is NULL");
	return nowdb_worker_init(wrk, "sorter",
	                         strg->tasknum,
	                         SORTPERIOD, &sortjob,
	                         errq, &nodrain, strg);
}

/* ------------------------------------------------------------------------
 * Stop Sorter
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_stopSorter(nowdb_worker_t *wrk) {
	return nowdb_worker_stop(wrk, SORTTIMEOUT);
}

/* ------------------------------------------------------------------------
 * Do your job, sorter!
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_sortNow(nowdb_storage_t *strg, void *store) {
	if (!strg->started) return NOWDB_OK;
	return nowdb_worker_do(&strg->sortwrk,
	     &((nowdb_store_t*)store)->srtmsg);
}

/* ------------------------------------------------------------------------
 * Syncjob
 * ------------------------------------------------------------------------
 */
static nowdb_err_t syncjob(nowdb_worker_t      *wrk,
                           uint32_t              id,
                           nowdb_wrk_message_t *msg) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	nowdb_err_t err3 = NOWDB_OK;
	nowdb_storage_t *strg = wrk->rsc;
	nowdb_store_t *store;
	ts_algo_list_node_t *runner;

	err = nowdb_lock(&strg->lock);
	if (err != NOWDB_OK) return err;

	for(runner=strg->stores.head; runner!=NULL; runner=runner->nxt) {
		store = runner->cont;

		err = nowdb_lock_write(&store->lock);
		if (err != NOWDB_OK) break;

		if (store->state == NOWDB_STORE_OPEN &&
		    store->writer != NULL            &&
		    store->writer->dirty)
		{
			err2 = nowdb_file_sync(store->writer);
			store->writer->dirty = FALSE;
		}
		err = nowdb_unlock_write(&store->lock);
		if (err != NOWDB_OK) {
			err->cause = err2; break;
		}
		if (err2 != NOWDB_OK) {
			err=err2; break;
		}
	}

	err3 = nowdb_unlock(&strg->lock);
	if (err3 != NOWDB_OK) {
		err3->cause = err; return err3;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Find minmax (only with timestamp)
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t findMinMax(char *buf, nowdb_file_t *file) {
	nowdb_time_t max = NOWDB_TIME_DAWN;
	nowdb_time_t min = NOWDB_TIME_DUSK;

	for (int i=0; i<file->size; i+=file->recordsize) {
	        if (*(nowdb_time_t*)(buf+i+NOWDB_OFF_STAMP) < min)
	        	memcpy(&min, buf+i+NOWDB_OFF_STAMP,
			              sizeof(nowdb_time_t));
		
	        if (*(nowdb_time_t*)(buf+i+NOWDB_OFF_STAMP) > max)
	        	memcpy(&max, buf+i+NOWDB_OFF_STAMP,
			              sizeof(nowdb_time_t));
	}
	file->oldest = min;
	file->newest = max;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Find set min and max (only with timestamp)
 * ------------------------------------------------------------------------
 */
static inline void setMinMax(nowdb_file_t *src, nowdb_file_t *trg) {
	if (trg->oldest == NOWDB_TIME_DAWN || src->oldest < trg->oldest)
		trg->oldest = src->oldest;

	if (trg->newest == NOWDB_TIME_DUSK || src->newest > trg->newest)
		trg->newest = src->newest;
}

/* ------------------------------------------------------------------------
 * Get dictionary; create it if it does not exist.
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getZSTDResources(nowdb_store_t     *store,
                                           char *buf, uint32_t size) {
	nowdb_err_t err2, err = NOWDB_OK;

	/* we lock to avoid several threads
	 * creating several dictionaries in parallel */
	err = nowdb_lock(&store->ctx->lock);
	if (err != NOWDB_OK) return err;

	/* if it exists, there is nothing to do */
	if (store->ctx->cdict != NULL) {
		return nowdb_unlock(&store->ctx->lock);
	}

	err = nowdb_compctx_loadDict(store->ctx, store->path);
	if (err != NOWDB_OK) {
		/* stat error: dictionary does not exist */
		if (err->errcode != nowdb_err_stat) goto unlock;
		nowdb_err_release(err);
	}
	/* we could load it */
	if (store->ctx->cdict != NULL) goto unlock;

	/* train and create dictionary */
	err = nowdb_compctx_trainDict(store->ctx,
	                              store->path,
	                              buf,  size);
	if (err != NOWDB_OK) goto unlock;

	/* now, we should be able to load it */
	err = nowdb_compctx_loadDict(store->ctx, store->path);
	if (err != NOWDB_OK) goto unlock;

	/* otherwise, something is wrong */
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
	if (store->comp == NOWDB_COMP_ZSTD) {
		file->comp = NOWDB_COMP_ZSTD;
		err = nowdb_compctx_getCDict(store->ctx, &file->cdict);
		if (err != NOWDB_OK) return err;
		err = nowdb_compctx_getCCtx(store->ctx, &file->cctx);
		if (err != NOWDB_OK) return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: release reader obtained from store (in case of error)
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
	NOWDB_IGNORE(nowdb_store_releaseReader(store, file));
}

/* ------------------------------------------------------------------------
 * Helper: get free reader from store or create new one
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getReader(nowdb_store_t *store,
                                    nowdb_file_t  **file) {
	nowdb_err_t err;

	err = nowdb_store_getFreeReader(store, file);
	if (err != NOWDB_OK) return err;
	if (*file != NULL) {
		configReader(store, *file);
		return NOWDB_OK;
	}
	err = nowdb_store_createReader(store, file);
	if (err != NOWDB_OK) return err;
	(*file)->capacity = store->largesize;
	configReader(store, *file);
	return nowdb_file_create(*file);
}

/* ------------------------------------------------------------------------
 * Helper: read whole content of waiting file into buffer
 * ------------------------------------------------------------------------
 */
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

/* ------------------------------------------------------------------------
 * Helper: destroy indexer
 * ------------------------------------------------------------------------
 */
static inline void destroyIndexer(nowdb_indexer_t *xer,
                                  uint32_t           n) {
	if (xer == NULL) return;
	for(int i=0; i<n; i++) {
		nowdb_indexer_destroy(xer+i);
	}
	free(xer);
}

/* ------------------------------------------------------------------------
 * Helper: get indexer
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getIndexer(nowdb_store_t  *store,
                                     nowdb_indexer_t **xer,
                                     uint32_t           *n) {
	ts_algo_list_t idxes;
	ts_algo_list_node_t *runner;
	nowdb_err_t      err;

	*xer = NULL;
	ts_algo_list_init(&idxes);
	err = nowdb_index_man_getAllOf(store->iman, store->context, &idxes);
	if (err != NOWDB_OK) return err;

	if (idxes.len == 0) {
		ts_algo_list_destroy(&idxes);
		return NOWDB_OK;
	}

	*xer = calloc(idxes.len, sizeof(nowdb_indexer_t));
	if (*xer == NULL) {
		ts_algo_list_destroy(&idxes);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
		                               "allocating indexers");
	}
	*n = 0;
	for(runner=idxes.head; runner!=NULL; runner=runner->nxt) {
		nowdb_index_desc_t *desc= runner->cont;
		err = nowdb_indexer_init((*xer)+(*n),
		                         desc->idx,
		                         store->cont);
		if (err != NOWDB_OK) break;
		(*n)++;
	}
	ts_algo_list_destroy(&idxes);
	if (err != NOWDB_OK) {
		destroyIndexer(*xer, *n); free(*xer); *xer = NULL;
		return err;
	}
	return NOWDB_OK;
}

static inline nowdb_pageid_t mkpageid(nowdb_file_t *file, uint32_t off) {
	nowdb_pageid_t pge = 0;

	pge = file->id;
	pge <<= 32;
	pge += file->size + off;

	return pge;
}

/* ------------------------------------------------------------------------
 * Helper: write buffer to target file (reader)
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t putContent(nowdb_store_t     *store,
                                     char *buf, uint32_t size,
                                     nowdb_file_t       *file) {
	nowdb_err_t err;
	nowdb_indexer_t *xer=NULL;
	nowdb_pageid_t pge;
	uint32_t n=0;

	if (store->iman != NULL) {
		err = getIndexer(store, &xer, &n);
		if (err != NOWDB_OK) return err;
	}

	err = nowdb_file_open(file);
	if (err != NOWDB_OK) {
		if (store->iman != NULL) destroyIndexer(xer, n);
		return err;
	}

	err = nowdb_file_position(file, file->size);
	if (err != NOWDB_OK) {
		if (store->iman != NULL) destroyIndexer(xer, n);
		return err;
	}
	for(uint32_t i=0; i<size; i+=file->bufsize) {

		pge = mkpageid(file, 0);

		/* write to file... */
		err = nowdb_file_writeBuf(file, buf+i, file->bufsize);
		if (err != NOWDB_OK) {
			if (store->iman != NULL) destroyIndexer(xer, n);
			NOWDB_IGNORE(nowdb_file_close(file));
			return err;
		}

		/* write to index... */
		if (store->iman != NULL) {
			err = nowdb_indexer_index(xer, n, pge,
			                          store,
			                          store->recsize, 
			                          file->bufsize,
			                          buf+i);
			if (err != NOWDB_OK) {
				destroyIndexer(xer, n);
				NOWDB_IGNORE(nowdb_file_close(file));
				return err;
			}
		}
	}
	if (store->iman != NULL) destroyIndexer(xer, n);
	return nowdb_file_close(file);
}

/* ------------------------------------------------------------------------
 * Sorter: sort and compress
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t compsort(nowdb_worker_t  *wrk,
                                   uint32_t          id,
                                   nowdb_store_t *store) {
	nowdb_err_t err;
	nowdb_file_t *src=NULL;
	nowdb_file_t *reader=NULL;
	char *buf=NULL;

	/*
	fprintf(stderr, "[%lu] %s.%u SORTING\n",
	         pthread_self(), wrk->name, id);
	*/

	/* get waiting file 
	 * we have to free it in case of error! */
	err = nowdb_store_getWaiting(store, &src);
	if (err != NOWDB_OK) return err;
	if (src == NULL) return NOWDB_OK;

	/*
	fprintf(stderr, "[%lu] %s.%u WAITING\n",
	         pthread_self(), wrk->name, id);
	*/

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

	/* find min/max (if this is timestamp) */
	if (store->ts) {
		err = findMinMax(buf, src);
		if (err != NOWDB_OK) {
			nowdb_store_releaseWaiting(store, src);
			nowdb_file_destroy(src); free(src);
			free(buf); return err;
		}
	}

	/* sort buf -- if compare is not NULL! */
	if (store->compare != NULL) {
		if (nowdb_mem_merge(buf, src->size, NOWDB_IDX_PAGE,
		                    store->recsize,
		                    store->compare, NULL) != 0)
		{
			NOMEM("mergesort");
			nowdb_store_releaseWaiting(store, src);
			nowdb_file_destroy(src); free(src);
			free(buf); return err;
		}
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

	/* get (or create) reader with space left */
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
	if (store->ts) { /* not too convincing */
		setMinMax(src, reader);
		src->oldest = NOWDB_TIME_DAWN;
		src->newest = NOWDB_TIME_DUSK;
	}

	/* write to reader (potentially compressing) */
	err = putContent(store, buf, src->size, reader);
	if (err != NOWDB_OK) {
		releaseReader(store, reader);
		nowdb_file_destroy(reader); free(reader); free(buf);
		NOWDB_IGNORE(nowdb_store_releaseWaiting(store, src));
		nowdb_file_destroy(src); free(src); return err;
	}

	/* we don't need it anymore */
	free(buf);

	/* release compression context */
	if (reader->cctx != NULL) {
		err = nowdb_compctx_releaseCCtx(store->ctx, reader->cctx);
		if (err != NOWDB_OK) {
			/* not really anything to do on error */
			nowdb_err_print(err); nowdb_err_release(err);
		}
		reader->cctx = NULL;
	}

	/* promote to reader */
	err = nowdb_store_promote(store, src, reader);
	if (err != NOWDB_OK) {
		NOWDB_IGNORE(nowdb_store_releaseReader(store, reader));
		nowdb_file_destroy(reader); free(reader);
		NOWDB_IGNORE(nowdb_store_releaseWaiting(store, src));
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

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Sorter: just a wrapper around 'compsort'
 * ------------------------------------------------------------------------
 */
static nowdb_err_t sortjob(nowdb_worker_t      *wrk,
                           uint32_t              id,
                           nowdb_wrk_message_t *msg) {
	if (msg == NULL) return NOWDB_OK;
	return compsort(wrk, id, msg->stcont);
}
