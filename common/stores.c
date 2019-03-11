#include <nowdb/store/store.h>

nowdb_store_t *mkStoreBlock(nowdb_path_t path, nowdb_comp_t comp,
                            uint32_t recsz, uint32_t block, uint32_t large) {
	nowdb_storage_t *storage;
	nowdb_storage_config_t cfg;
	nowdb_store_t *store;
	nowdb_err_t err;

	cfg.filesize = block;
	cfg.largesize = large;
	cfg.sorters = 2;
	cfg.sort = 1;
	cfg.comp = comp;
	cfg.encp = 0;

	err = nowdb_storage_new(&storage, "default", &cfg);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	err = nowdb_store_new(&store, path, NULL, 1,
	                     NOWDB_CONT_EDGE, storage, recsz, 1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_storage_destroy(storage); free(storage);
		return NULL;
	}
	return store;
}

nowdb_store_t *mkStore(nowdb_path_t path) {
	return mkStoreBlock(path, 1, 64, NOWDB_MEGA, NOWDB_MEGA);
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

void destroyStore(nowdb_store_t *store) {
	if (store == NULL) return;
	if (store->storage != NULL) {
		NOWDB_IGNORE(nowdb_storage_stop(store->storage));
		nowdb_storage_destroy(store->storage);
		free(store->storage); store->storage = NULL;
	}
	nowdb_store_destroy(store);
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

nowdb_bool_t startStorage(nowdb_store_t *store) {
	nowdb_err_t     err;
	if (store->storage != NULL) {
		err = nowdb_storage_start(store->storage);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
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
	if (store->storage != NULL) {
		err = nowdb_storage_stop(store->storage);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
	}
	return TRUE;
}

nowdb_store_t *xBootstrap(nowdb_path_t       path,
                          nowdb_comprsc_t compare,
                          nowdb_comp_t   compress,
                          uint32_t        tasknum,
                          uint32_t        recsize,
                          uint32_t          block,
                          uint32_t          large) {
	nowdb_err_t      err;
	nowdb_store_t *store=NULL;
	store = mkStore(path);
	if (store == NULL) return NULL;
	if (openStore(store)) {
		if (!closeStore(store)) goto failure;
		if (!dropStore(store)) goto failure;
	}
	destroyStore(store); free(store);
	store = mkStoreBlock(path, compress, recsize, block, large);
	if (store == NULL) return NULL;
	if (store->storage != NULL) {
		if (!startStorage(store)) goto failure;
	}
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
	if (!createStore(store)) goto failure;
	if (!openStore(store)) goto failure;
	return store;
failure:
	if (store != NULL) {
		closeStore(store);
		destroyStore(store);
		free(store);
	}
	return NULL;
}

nowdb_store_t *bootstrap(nowdb_path_t path, uint32_t recsz) {
	return xBootstrap(path, NULL, NOWDB_COMP_FLAT, 1,
	                   recsz,NOWDB_MEGA, NOWDB_MEGA);
}
