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

int main(int argc, char **argv) {
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
	err = nowdb_store_init(&store, path, 0, 64, NOWDB_MEGA);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return EXIT_FAILURE;
	}
	err = nowdb_store_showCatalog(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return EXIT_FAILURE;
	}
	nowdb_store_destroy(&store);
	nowdb_err_destroy();
	return EXIT_SUCCESS;
}
