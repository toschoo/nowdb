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
#define SYNCTIMEOUT  5000000000l
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

static nowdb_err_t sortjob(nowdb_worker_t      *wrk,
                           nowdb_wrk_message_t *msg) {
	return NOWDB_OK;
}
