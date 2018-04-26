/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Workers for stores
 * ========================================================================
 */
#include <nowdb/store/storewrk.h>
#include <nowdb/store/store.h>
#include <nowdb/io/file.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define SYNCPERIOD    500000000l
#define SORTPERIOD   5000000000l
#define SYNCTIMEOUT 10000000000l
#define SORTTIMEOUT 10000000000l

static nowdb_err_t syncjob(nowdb_worker_t      *wrk,
                           nowdb_wrk_message_t *msg);

static nowdb_err_t sortjob(nowdb_worker_t      *wrk,
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

	return nowdb_worker_init(wrk, "sync", SYNCPERIOD, &syncjob,
	                                    errq, &nodrain, store);
}

nowdb_err_t nowdb_store_stopSync(nowdb_worker_t *wrk) {
	return nowdb_worker_stop(wrk, SYNCTIMEOUT);
}

nowdb_err_t nowdb_store_startSorter(nowdb_worker_t *wrk,
                                    void         *store,
                                    nowdb_queue_t *errq) {
	if (wrk == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                             "store", "worker object is NULL");
	if (store == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                                "store", "store object is NULL");

	return nowdb_worker_init(wrk, "sorter", SORTPERIOD, &sortjob,
	                                      errq, &nodrain, store);
}

nowdb_err_t nowdb_store_stopSorter(nowdb_worker_t *wrk) {
	return nowdb_worker_stop(wrk, SORTTIMEOUT);
}

nowdb_err_t nowdb_store_sortNow(nowdb_worker_t *wrk) {
	return nowdb_worker_do(wrk, &sortmsg);
}

static nowdb_err_t syncjob(nowdb_worker_t      *wrk,
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

static inline nowdb_err_t findMinMax(nowdb_file_t *file) {
	return NOWDB_OK;
}

static inline void setMinMax(nowdb_file_t *src, nowdb_file_t *trg) {
	if (trg->oldest == NOWDB_TIME_DAWN || src->oldest < trg->oldest)
		trg->oldest = src->oldest;

	if (trg->newest == NOWDB_TIME_DUSK || src->newest > trg->newest)
		trg->newest = src->newest;
}

static inline nowdb_err_t getReader(nowdb_store_t *store,
                                    nowdb_file_t  **file) {
	nowdb_err_t err;

	err = nowdb_store_getFreeReader(store, file);
	if (err != NOWDB_OK) return err;
	if (*file != NULL) return NOWDB_OK;
	return nowdb_store_createReader(store, file);
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
                                   nowdb_store_t *store) {
	nowdb_err_t err;
	nowdb_file_t *src=NULL;
	nowdb_file_t *reader=NULL;
	char *buf=NULL;

	// fprintf(stderr, "SORTING\n");

	// get waiting
	err = nowdb_store_getWaiting(store, &src);
	if (err != NOWDB_OK) return err;
	if (src == NULL) return NOWDB_OK;

	// find max/min
	err = findMinMax(src);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(src); free(src); return err;
	}

	// get (or create) reader with space
	err = getReader(store, &reader);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(src); free(src); return err;
	}

	buf = malloc(src->size);
	if (buf == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, wrk->name,
		                            "allocating large buffer");
		nowdb_file_destroy(reader); free(reader);
		nowdb_file_destroy(src); free(src); return err;
	}
	err = getContent(wrk, src, buf);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(reader); free(reader); free(buf);
		nowdb_file_destroy(src); free(src); return err;
	}

	// sort buf -- if compare is not NULL!
	if (store->compare != NULL) {
		nowdb_mem_sort(buf, src->size/store->recsize,
		                    store->recsize,
		                    store->compare, NULL);
		reader->ctrl |= NOWDB_FILE_SORT;
		setMinMax(src, reader);
	}

	// write to reader (potentially compressing)
	err = putContent(buf, src->size, reader);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(reader); free(reader); free(buf);
		nowdb_file_destroy(src); free(src); return err;
	}

	// insert into index

	// we don't need it anymore
	free(buf);

	// promote to reader 
	err = nowdb_store_promote(store, src, reader);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(reader); free(reader);
		nowdb_file_destroy(src); free(src); return err;
	}

	// erase waiting... 
	err = nowdb_file_erase(src);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(src); free(src); return err;
	}

	// ...and donate it
	err = nowdb_store_donate(store, src);
	if (err != NOWDB_OK) return err;

	// fprintf(stderr, "DONE SORTING\n");
	return NOWDB_OK;
}

static nowdb_err_t sortjob(nowdb_worker_t      *wrk,
                           nowdb_wrk_message_t *msg) {
	return compsort(wrk, wrk->rsc);
}
