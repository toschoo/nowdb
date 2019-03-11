/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Workers for stores
 * ========================================================================
 */
#ifndef nowdb_store_wrk_decl
#define nowdb_store_wrk_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/worker.h>
#include <nowdb/store/storage.h>

/* ------------------------------------------------------------------------
 * Start sync worker
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_startSync(nowdb_worker_t *wrk,
                                  void       *storage,
                                  nowdb_queue_t *errq);

/* ------------------------------------------------------------------------
 * Stop sync worker
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_stopSync(nowdb_worker_t *wrk);

/* ------------------------------------------------------------------------
 * Start Sorter
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_startSorter(nowdb_worker_t *wrk,
                                    void       *storage,
                                    nowdb_queue_t *errq);

/* ------------------------------------------------------------------------
 * Stop Sorter
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_stopSorter(nowdb_worker_t *wrk);

/* ------------------------------------------------------------------------
 * Sorter, sort now!
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_sortNow(nowdb_storage_t *strg, void *store);

#endif

