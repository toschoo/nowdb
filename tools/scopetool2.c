/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tool to interact with nowdb using SQL
 * ========================================================================
 */
#include <nowdb/nowdb.h>

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

int main() {
	nowdb_err_t err;
	nowdb_t nowdb;
	nowdb_session_t ses;
	int rc = EXIT_SUCCESS;

	err = nowdb_library_init(&nowdb, 1);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init library\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return EXIT_FAILURE;
	}
	err = nowdb_getSession(nowdb, &ses, 0, 1, 2);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot get session\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	nowdb_library_close(nowdb);
	return rc;
}



