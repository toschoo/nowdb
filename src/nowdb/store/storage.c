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

/* ------------------------------------------------------------------------
 * Allocate new storage object
 * ------------------------------------------------------------------------
 */
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

/* ------------------------------------------------------------------------
 * Initialise already allocated storage object
 * ------------------------------------------------------------------------
 */
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
	strg->started = 0;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy storage object
 * ------------------------------------------------------------------------
 */
void nowdb_storage_destroy(nowdb_storage_t *strg) {
	if (strg->name != NULL) {
		free(strg->name); strg->name = NULL;
	}
	ts_algo_list_destroy(&strg->stores);
	nowdb_lock_destroy(&strg->lock);
}

/* ------------------------------------------------------------------------
 * Add store to storage
 * ------------------------------------------------------------------------
 */
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

/* -----------------------------------------------------------------------
 * Remove a store from the storage
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_storage_removeStore(nowdb_storage_t *strg,
                                      void           *store) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_node_t *runner;
	nowdb_err_t err2;

	STORAGENULL();

	err = nowdb_lock(&strg->lock);
	if (err != NOWDB_OK) return err;

	for(runner=strg->stores.head; runner!=NULL; runner=runner->nxt) {
		if (runner->cont == store) {
			ts_algo_list_remove(&strg->stores, runner);
			free(runner); break;
		}
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

/* ------------------------------------------------------------------------
 * Start workers
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_storage_start(nowdb_storage_t *strg) {
	nowdb_err_t err;
	err = startWorkers(strg);
	if (err != NOWDB_OK) return err;
	strg->started = 1;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Stop workers
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_storage_stop(nowdb_storage_t *strg) {
	if (strg->started == 0) return NOWDB_OK;
	strg->started = 0;
	return stopWorkers(strg);
}

/* -----------------------------------------------------------------------
 * Predefine configurations
 * -----------------------------------------------------------------------
 */
void nowdb_storage_config(nowdb_storage_config_t *cfg,
                          nowdb_bitmap64_t    options) {

	if (cfg == NULL) return;

	cfg->sort = 1;
	cfg->encp = NOWDB_ENCP_NONE;

	if (options & NOWDB_CONFIG_SIZE_TINY) {

		cfg->filesize  = NOWDB_MEGA;
		cfg->largesize = NOWDB_MEGA;
		cfg->sorters   = 1;
		cfg->comp      = NOWDB_COMP_FLAT;

	} else if (options & NOWDB_CONFIG_SIZE_SMALL) {

		cfg->filesize  = NOWDB_MEGA;
		cfg->largesize = 8 * NOWDB_MEGA;
		cfg->sorters   = 2;
		cfg->comp      = NOWDB_COMP_ZSTD;

	} else if (options & NOWDB_CONFIG_SIZE_MEDIUM) {

		cfg->filesize  = 8 * NOWDB_MEGA;
		cfg->largesize = 64 * NOWDB_MEGA;
		cfg->sorters   = 4;
		cfg->comp      = NOWDB_COMP_ZSTD;

	} else if (options & NOWDB_CONFIG_SIZE_BIG) {

		cfg->filesize  = 8 * NOWDB_MEGA;
		cfg->largesize = 128 * NOWDB_MEGA;
		cfg->sorters   = 6;
		cfg->comp      = NOWDB_COMP_ZSTD;

	} else if (options & NOWDB_CONFIG_SIZE_LARGE) {

		cfg->filesize  = 8 * NOWDB_MEGA;
		cfg->largesize = 256 * NOWDB_MEGA;
		cfg->sorters   = 8;
		cfg->comp      = NOWDB_COMP_ZSTD;

	} else if (options & NOWDB_CONFIG_SIZE_HUGE) {

		cfg->filesize  = 8 * NOWDB_MEGA;
		cfg->largesize = 1024 * NOWDB_MEGA;
		cfg->sorters   = 10;
		cfg->comp      = NOWDB_COMP_ZSTD;
	}
	if (options & NOWDB_CONFIG_INSERT_MODERATE) {

		if (cfg->sorters < 2) cfg->sorters = 2;

	} else if (options & NOWDB_CONFIG_INSERT_CONSTANT) {

		cfg->sorters += 4;

	} else if (options & NOWDB_CONFIG_INSERT_INSANE) {

		cfg->sorters *= 2;
		if (cfg->sorters > 32) cfg->sorters = 32;
		if (cfg->filesize  < 8) cfg->filesize  = 8;
		cfg->largesize = 1024;
	}
	if (options & NOWDB_CONFIG_DISK_HDD) {
		if (cfg->largesize < 128 * NOWDB_MEGA) {
			cfg->largesize = 128 * NOWDB_MEGA;
		}
	}
	if (options & NOWDB_CONFIG_NOCOMP) {
		cfg->comp = NOWDB_COMP_FLAT;
	}
	if (options & NOWDB_CONFIG_NOSORT) {
		cfg->sort = 0;
	}
}

