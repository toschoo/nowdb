/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Workers for stores
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_store_wrk_decl
#define nowdb_store_wrk_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/worker.h>

nowdb_err_t nowdb_store_startSync(nowdb_worker_t *wrk,
                                  void         *store,
                                  nowdb_queue_t *errq);

nowdb_err_t nowdb_store_stopSync(nowdb_worker_t *wrk);

nowdb_err_t nowdb_store_startSorter(nowdb_worker_t *wrk,
                                    void         *store,
                                    nowdb_queue_t *errq);

nowdb_err_t nowdb_store_stopSorter(nowdb_worker_t *wrk);

#endif

