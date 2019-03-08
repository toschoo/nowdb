/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Show catalog
 * ========================================================================
 */
#include <nowdb/store/store.h>

#include <stdio.h>
#include <stdlib.h>

void helptext(char *progname) {
	fprintf(stderr, "%s <path-to-file>\n", progname);
}

nowdb_storage_t *createStorage() {
	nowdb_storage_config_t cfg;
	nowdb_storage_t *strg;
	nowdb_err_t       err;
	nowdb_bitmap64_t   os;

	os = NOWDB_CONFIG_SIZE_TINY       |
	     NOWDB_CONFIG_INSERT_MODERATE |
	     NOWDB_CONFIG_DISK_HDD;
	nowdb_storage_config(&cfg, os);

	err = nowdb_storage_new(&strg, "default", &cfg);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create default storage\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return strg;
}


int main(int argc, char **argv) {
	nowdb_storage_t *strg;
	nowdb_store_t store;
	nowdb_err_t   err;
	nowdb_path_t  path;
	size_t        s;

	if (argc < 2) {
		helptext(argv[0]);
		return EXIT_FAILURE;
	}
	path = argv[1];
	s = strnlen(path, NOWDB_MAX_PATH+1);
	if (s >= NOWDB_MAX_PATH) {
		fprintf(stderr, "path too long (max: 4096)\n");
		return EXIT_FAILURE;
	}
	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init error manager\n");
		return EXIT_FAILURE;
	}
	strg = createStorage();
	if (strg == NULL) {
		fprintf(stderr, "cannot create storage\n");
		return EXIT_FAILURE;
	}
	// we need to get the true recsize!
	err = nowdb_store_init(&store, path, NULL, 0,
	                   NOWDB_CONT_EDGE,strg,64,1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_storage_destroy(strg); free(strg);
		return EXIT_FAILURE;
	}
	err = nowdb_store_showCatalog(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_storage_destroy(strg); free(strg);
		return EXIT_FAILURE;
	}
	nowdb_store_destroy(&store);
	nowdb_storage_destroy(strg); free(strg);
	nowdb_err_destroy();
	return EXIT_SUCCESS;
}
