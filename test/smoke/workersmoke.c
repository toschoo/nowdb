/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for workers
 * ========================================================================
 */
#include <nowdb/types/error.h>
#include <nowdb/types/time.h>
#include <nowdb/task/lock.h>
#include <nowdb/task/worker.h>

#include <common/bench.h>
#include <common/math.h>

#include <tsalgo/list.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

#define PERIOD 10000000
#define TMO  1000000000

nowdb_err_t hellojob(nowdb_worker_t      *wrk,
                     nowdb_wrk_message_t *msg) {
	if (msg == NULL) {
		return nowdb_err_get(nowdb_err_invalid,
		  FALSE, wrk->name, "message is NULL");
	}
	if (msg->cont != NULL) {
		fprintf(stderr, "%s\n", (char*)msg->cont);
	} else {
		fprintf(stderr, "hello world\n");
	}
	return NOWDB_OK;
}

void drainer(void **message) {
	nowdb_wrk_message_t *msg;
	if (message == NULL) return;
	if (*message == NULL) return;
	msg = *message;
	if (msg->cont != NULL) {
		free(msg->cont); msg->cont = NULL;
	}
	free(*message); *message = NULL;
}

nowdb_bool_t testCreateStopDestroy() {
	nowdb_worker_t wrk;
	nowdb_err_t    err;

	err = nowdb_worker_init(&wrk, "test", PERIOD,
	                        &hellojob, NULL,
	                        &drainer, NULL);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_task_sleep(PERIOD);
	err = nowdb_worker_stop(&wrk, TMO);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_worker_destroy(&wrk);
	return TRUE;
}

nowdb_bool_t testHelloJob() {
	nowdb_wrk_message_t *msg;
	nowdb_worker_t wrk;
	nowdb_err_t    err;
	struct timespec t1, t2;

	err = nowdb_worker_init(&wrk, "test", PERIOD,
	                        &hellojob, NULL,
	                        &drainer, NULL);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	msg = malloc(sizeof(nowdb_wrk_message_t));
	if (msg == NULL) {
		fprintf(stderr, "no mem\n");
		return FALSE;
	}
	msg->type = 1;
	msg->cont = NULL;
	err = nowdb_worker_do(&wrk, msg);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(msg);
		return FALSE;
	}
	nowdb_task_sleep(PERIOD);
	timestamp(&t1);
	err = nowdb_worker_stop(&wrk, TMO);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	timestamp(&t2);
	fprintf(stderr, "waited: %luus\n", minus(&t2, &t1)/1000);
	nowdb_worker_destroy(&wrk);
	return TRUE;
}

nowdb_bool_t testPrintCont() {
	nowdb_wrk_message_t *msg;
	nowdb_worker_t wrk;
	nowdb_err_t    err;

	err = nowdb_worker_init(&wrk, "test", PERIOD,
	                        &hellojob, NULL,
	                        &drainer, NULL);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	msg = malloc(sizeof(nowdb_wrk_message_t));
	if (msg == NULL) {
		fprintf(stderr, "no mem\n");
		return FALSE;
	}
	msg->cont = malloc(32);
	if (msg->cont == NULL) {
		fprintf(stderr, "no mem\n");
		free(msg); return FALSE;
	}
	msg->type = 1;
	strcpy(msg->cont, "this is not a love song");
	err = nowdb_worker_do(&wrk, msg);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(msg->cont); free(msg);
		return FALSE;
	}
	nowdb_task_sleep(PERIOD);
	err = nowdb_worker_stop(&wrk, TMO);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_worker_destroy(&wrk);
	return TRUE;
}

int main() {
	int rc = EXIT_SUCCESS;

	if (!nowdb_err_init()) {
		fprintf(stderr, "err init failed\n");
		return EXIT_FAILURE;
	}
	if (!testCreateStopDestroy()) {
		fprintf(stderr, "testCreateStopDestroy failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testHelloJob()) {
		fprintf(stderr, "testHelloJob failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testPrintCont()) {
		fprintf(stderr, "testPrintCont failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}

