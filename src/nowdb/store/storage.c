/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Storage: storage parameters and store workers
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/io/file.h>
#include <nowdb/task/lock.h>
#include <nowdb/task/worker.h>
#include <nowdb/store/storage.h>
#include <nowdb/store/storewrk.h>

static char *OBJECT = "storage";

/* ------------------------------------------------------------------------
 * Macros
 * ------------------------------------------------------------------------
 */
#define STORAGENULL() \
	if (strg == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                        FALSE, OBJECT, "storage is NULL");

#define NOMEM(x) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, x);

#define INVALID(x) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, x);

nowdb_err_t nowdb_storage_new(nowdb_storage_t **strg,
                              char             *name,
                              nowdb_storage_config_t *cfg) {
	nowdb_err_t err;

	STORAGENULL();

	*strg = calloc(1,sizeof(nowdb_storage_t));
	if (*strg == NULL) {
		NOMEM("allocating storage");
		return err;
	}
	err = nowdb_storage_init(*strg, name, cfg);
	if (err != NOWDB_OK) {
		free(*strg); *strg = NULL;
		return err;
	}
	return NOWDB_OK;
}

nowdb_err_t nowdb_storage_init(nowdb_storage_t *strg,
                               char            *name,
                               nowdb_storage_config_t *cfg) {
	nowdb_err_t err;

	STORAGENULL();

	ts_algo_list_init(&strg->stores);

	err = nowdb_lock_init(&strg->lock);
	if (err != NOWDB_OK) return err;

	strg->name = strdup(name);
	if (strg->name == NULL) {
		NOMEM("allocating storage name");
		return err;
	}

	strg->filesize = cfg->filesize;
	strg->largesize = cfg->largesize;
	strg->tasknum = cfg->sorters;
	strg->sort = cfg->sort;
	strg->comp = cfg->comp;
	strg->encp = cfg->encp;

	return NOWDB_OK;
}

void nowdb_storage_destroy(nowdb_storage_t *strg) {
	if (strg->name != NULL) {
		free(strg->name); strg->name = NULL;
	}
	ts_algo_list_destroy(&strg->stores);
	nowdb_lock_destroy(&strg->lock);
	free(strg);
}

nowdb_err_t nowdb_storage_addStore(nowdb_storage_t *strg,
                                   void           *store) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	STORAGENULL();

	err = nowdb_lock(&strg->lock);
	if (err != NOWDB_OK) return err;

	if (ts_algo_list_append(&strg->stores, store)
	                               != TS_ALGO_OK) {
		NOMEM("list.append");
	}

	err2 = nowdb_unlock(&strg->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: start workers
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t startWorkers(nowdb_storage_t *strg) {
	nowdb_err_t err = NOWDB_OK;

	err = nowdb_store_startSync(&strg->syncwrk, strg, NULL);
	if (err != NOWDB_OK) return err;

	err = nowdb_store_startSorter(&strg->sortwrk, strg, NULL);
	if (err != NOWDB_OK) {
		NOWDB_IGNORE(nowdb_store_stopSync(&strg->syncwrk));
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: stop workers
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t stopWorkers(nowdb_storage_t *strg) {
	nowdb_err_t err = NOWDB_OK;

	err = nowdb_store_stopSorter(&strg->sortwrk);
	if (err != NOWDB_OK) return err;

	err = nowdb_store_stopSync(&strg->syncwrk);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

nowdb_err_t nowdb_storage_start(nowdb_storage_t *strg) {
	return startWorkers(strg);
}

nowdb_err_t nowdb_storage_stop(nowdb_storage_t *strg) {
	return stopWorkers(strg);
}

