/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for index manager
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/index/index.h>
#include <nowdb/index/catalog.h>
#include <nowdb/index/man.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

int initMan(nowdb_index_man_t *iman,
            nowdb_index_cat_t *icat) {
	nowdb_err_t err;

	err = nowdb_index_man_init(iman, icat);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init iman\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int testOpenClose(nowdb_index_man_t *iman) {
	if (initMan(iman, NULL) != 0) return -1;
	nowdb_index_man_destroy(iman);
	return 0;
}


int main() {
	int rc = EXIT_SUCCESS;
	nowdb_index_man_t man;
	int haveMan = 0;

	if (testOpenClose(&man) != 0) {
		fprintf(stderr, "testOpenClose failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (initMan(&man, NULL) != 0) {
		fprintf(stderr, "initMan failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	haveMan = 1;

cleanup:
	if (haveMan) {
		nowdb_index_man_destroy(&man);
	}
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
