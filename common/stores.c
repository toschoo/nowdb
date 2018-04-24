#include <nowdb/store/store.h>

nowdb_store_t *mkStore(nowdb_path_t path) {
	nowdb_store_t *store;
	nowdb_err_t err;
	err = nowdb_store_new(&store, path, 1, 64, NOWDB_MEGA);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return store;
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

nowdb_store_t *bootstrap(nowdb_path_t path) {
	nowdb_store_t *store;
	store = mkStore(path);
	if (store == NULL) return NULL;
	if (openStore(store)) {
		if (!closeStore(store)) goto failure;
		if (!dropStore(store)) goto failure;
	}
	nowdb_store_destroy(store); free(store);
	store = mkStore(path);
	if (store == NULL) return NULL;
	if (!createStore(store)) goto failure;
	if (!openStore(store)) goto failure;
	return store;

failure:
	closeStore(store);
	nowdb_store_destroy(store);
	return NULL;
	
}

