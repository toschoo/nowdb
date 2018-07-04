/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for text
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/io/dir.h>
#include <nowdb/text/text.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define PATH "rsc/text10"

int preparePath() {
	nowdb_err_t err;
	struct stat  st;

	if (stat(PATH, &st) == 0) {
		err = nowdb_path_rRemove(PATH);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return -1;
		}
	}
	if (mkdir(PATH, NOWDB_DIR_MODE) != 0) {
		perror("cannot create path");
		return -1;
	}
	return 0;
}

nowdb_text_t *mkText(char *path) {
	nowdb_err_t err;
	nowdb_text_t *txt;

	txt = calloc(1, sizeof(nowdb_text_t));
	if (txt == NULL) {
		perror("out-of-mem\n");
		return NULL;
	}
	err = nowdb_text_init(txt, path);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);	
		return NULL;
	}
	return txt;
}

int createText(nowdb_text_t *txt) {
	nowdb_err_t err;

	err = nowdb_text_create(txt);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int dropText(nowdb_text_t *txt) {
	nowdb_err_t err;

	err = nowdb_text_drop(txt);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int openText(nowdb_text_t *txt) {
	nowdb_err_t err;

	err = nowdb_text_open(txt);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int closeText(nowdb_text_t *txt) {
	nowdb_err_t err;

	err = nowdb_text_close(txt);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_text_t *txt= NULL;

	if (!nowdb_init()) {
		fprintf(stderr, "FAILED: cannot init\n");
		return EXIT_FAILURE;
	}
	if (preparePath() != 0) {
		fprintf(stderr, "FAILED: prepare path\n");
		return EXIT_FAILURE;
	}
	txt = mkText(PATH);
	if (txt == NULL) {
		fprintf(stderr, "mkText failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (createText(txt) != 0) {
		fprintf(stderr, "createText failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (dropText(txt) != 0) {
		fprintf(stderr, "dropText failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (preparePath() != 0) {
		fprintf(stderr, "FAILED: prepare path\n");
		return EXIT_FAILURE;
	}
	if (createText(txt) != 0) {
		fprintf(stderr, "createText failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (openText(txt) != 0) {
		fprintf(stderr, "openText failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (closeText(txt) != 0) {
		fprintf(stderr, "closeText failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (openText(txt) != 0) {
		fprintf(stderr, "openText failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	if (txt != NULL) {
		nowdb_text_destroy(txt); free(txt);
	}
	nowdb_close();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}

