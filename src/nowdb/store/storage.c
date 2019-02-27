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
                              uint32_t            id,
                              char             *name,
                              nowdb_storage_config_t *cfg) {
	nowdb_err_t err;

	STORAGENULL();

	*strg = calloc(1,sizeof(nowdb_storage_t));
	if (*strg == NULL) {
		NOMEM("allocating storage");
		return err;
	}
	err = nowdb_storage_init(*strg, id, name, cfg);
	if (err != NOWDB_OK) {
		free(*strg); *strg = NULL;
		return err;
	}
	return NOWDB_OK;
}

nowdb_err_t nowdb_storage_init(nowdb_storage_t *strg,
                               uint32_t           id,
                               char            *name,
                               nowdb_storage_config_t *cfg) {
	nowdb_err_t err;

	STORAGENULL();

	ts_algo_list_init(&strg->stores);

	strg->id = id;
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
	free(strg);
}

nowdb_err_t nowdb_storage_addStore(nowdb_storage_t *strg,
                                   void           *store) {
	nowdb_err_t err;

	STORAGENULL();

	if (ts_algo_list_append(&strg->stores, store)
	                               != TS_ALGO_OK) {
		NOMEM("list.append");
		return err;
	}
	return NOWDB_OK;
}
