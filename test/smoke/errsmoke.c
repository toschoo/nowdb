#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <time.h>

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>

nowdb_bool_t simpletest() {
	nowdb_err_t rc = NOWDB_OK;
	nowdb_err_t rc2 = NOWDB_OK;
	nowdb_err_t tmp = NOWDB_OK;

	/* no error */
	nowdb_err_print(NOWDB_OK);

	/* out-of-mem */
	nowdb_err_print(NOWDB_NO_MEM);

	/* simple error */
	rc = nowdb_err_get(nowdb_err_busy, TRUE, "test", NULL);
	if (NOWDB_SUCCESS(rc)) return FALSE;
	nowdb_err_print(rc);

	tmp = rc;
	nowdb_err_release(rc);

	/* create file open error */
	FILE *f = fopen("doesnotexist", "r");
	if (f != NULL) {
		fprintf(stderr, "oops! file does exist!\n");
	}
	rc = nowdb_err_get(nowdb_err_open, TRUE, "test", NULL);
	if (NOWDB_SUCCESS(rc)) return FALSE;
	if (rc != tmp) {
		fprintf(stderr, "error address differs: %p -- %p\n", tmp, rc);
		return FALSE;
	}
	nowdb_err_print(rc);
	nowdb_err_release(rc);

	/* create file open error with info */
	rc = nowdb_err_get(nowdb_err_open, TRUE, "test", "doesnotexist");
	if (NOWDB_SUCCESS(rc)) return FALSE;
	if (rc != tmp) {
		fprintf(stderr, "error address differs: %p -- %p\n", tmp, rc);
		return FALSE;
	}
	nowdb_err_print(rc);
	nowdb_err_release(rc);

	/* create file open error as route cause of another error */
	rc = nowdb_err_get(nowdb_err_open, FALSE, "scope", "main");
	if (NOWDB_SUCCESS(rc)) return FALSE;
	if (rc != tmp) {
		fprintf(stderr, "error address differs: %p -- %p\n", tmp, rc);
		return FALSE;
	}
	
	rc2 = nowdb_err_get(nowdb_err_open, TRUE, "file", "doesnotexist");
	if (NOWDB_SUCCESS(rc2)) return FALSE;
	if (rc2 == tmp) return FALSE;
	rc->cause = rc2;
	nowdb_err_print(rc);
	nowdb_err_release(rc);

	/* once more */
	rc = nowdb_err_get(nowdb_err_busy, FALSE, "test", "once more");
	if (NOWDB_SUCCESS(rc)) return FALSE;
	if (rc != tmp) return FALSE;
	nowdb_err_print(rc);
	nowdb_err_release(rc);

	return TRUE;
}

/* fd tests */
nowdb_bool_t fdtest() {
	nowdb_err_t rc = NOWDB_OK;
	nowdb_err_t rc2 = NOWDB_OK;

	/* no error */
	nowdb_err_send(NOWDB_OK, 2);

	/* out-of-mem */
	nowdb_err_send(NOWDB_NO_MEM, 2);

	/* create file close error */
	close(99);
	rc = nowdb_err_get(nowdb_err_close, TRUE, "test", "99");
	if (NOWDB_SUCCESS(rc)) return FALSE;

	rc2 = nowdb_err_get(nowdb_err_close, FALSE, "scope", "main scope");
	if (NOWDB_SUCCESS(rc2)) return FALSE;
	rc2->cause = rc;

	nowdb_err_send(rc2,2);
	nowdb_err_release(rc2);

	return TRUE;
}

/* create 1000 errors */
nowdb_bool_t oneKiloPlease() {
	nowdb_err_t x = NULL;

	for(int i=0; i<1000;i++) {
		nowdb_errcode_t c = 0;
		while(c<1) {
			c = rand()%44;
		}
		nowdb_bool_t os = rand()%2;
		nowdb_err_t e = nowdb_err_get(c, os, "test", NULL);
		if (NOWDB_SUCCESS(e)) return FALSE;
		if (x == e) return FALSE;
		nowdb_err_print(e);
		x = e;
	}
	return TRUE;
}

int main() {
	srand(time(NULL)^(uint64_t)printf);
	if (!nowdb_err_init()) {
		fprintf(stderr, "init failed\n");
		goto failure;
	}
	if (!simpletest()) {
		fprintf(stderr, "simple test failed\n");
		goto failure;
	}
	if (!fdtest()) {
		fprintf(stderr, "simple test failed\n");
		goto failure;
	}
	if (!oneKiloPlease()) {
		fprintf(stderr, "kilo test failed\n");
		goto failure;
	}

	nowdb_err_destroy();
	fprintf(stderr, "PASSED\n");
	return EXIT_SUCCESS;

failure:
	nowdb_err_destroy();
	fprintf(stderr, "FAILED\n");
	return EXIT_FAILURE;
}
