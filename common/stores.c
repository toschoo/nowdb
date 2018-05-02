#include <nowdb/store/store.h>

nowdb_store_t *mkStoreBlock(nowdb_path_t path, uint32_t block, uint32_t large) {
	nowdb_store_t *store;
	nowdb_err_t err;
	err = nowdb_store_new(&store, path, 1, 64, block, large);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return store;
}

nowdb_store_t *mkStore(nowdb_path_t path) {
	return mkStoreBlock(path, NOWDB_MEGA, NOWDB_MEGA);
}

nowdb_bool_t createStore(nowdb_store_t *store) {
	nowdb_err_t     err;
	err = nowdb_store_create(store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	return TRUE;
}

nowdb_bool_t dropStore(nowdb_store_t *store) {
	nowdb_err_t     err;
	err = nowdb_store_drop(store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	return TRUE;
}

nowdb_bool_t openStore(nowdb_store_t *store) {
	nowdb_err_t     err;
	err = nowdb_store_open(store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	return TRUE;
}

nowdb_bool_t closeStore(nowdb_store_t *store) {
	nowdb_err_t     err;
	err = nowdb_store_close(store);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot close store\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	return TRUE;
}

nowdb_store_t *xBootstrap(nowdb_path_t       path,
                          nowdb_comprsc_t compare,
                          nowdb_comp_t   compress,
                          uint32_t        tasknum,
                          uint32_t          block,
                          uint32_t          large) {
	nowdb_err_t      err;
	nowdb_store_t *store;
	store = mkStore(path);
	if (store == NULL) return NULL;
	if (openStore(store)) {
		if (!closeStore(store)) goto failure;
		if (!dropStore(store)) goto failure;
	}
	nowdb_store_destroy(store); free(store);
	store = mkStoreBlock(path, block, large);
	if (store == NULL) return NULL;
	err = nowdb_store_configSort(store, compare);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot configure store.Sort\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		goto failure;
	}
	err = nowdb_store_configCompression(store, compress);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot configure store.Compression\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		goto failure;
	}
	err = nowdb_store_configWorkers(store, tasknum);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot configure store.Workers\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		goto failure;
	}
	if (!createStore(store)) goto failure;
	if (!openStore(store)) goto failure;
	return store;
failure:
	closeStore(store);
	nowdb_store_destroy(store);
	return NULL;
}

nowdb_store_t *bootstrap(nowdb_path_t path) {
	return xBootstrap(path, NULL, NOWDB_COMP_FLAT, 1,
	                         NOWDB_MEGA, NOWDB_MEGA);
}
