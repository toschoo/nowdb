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


#define DELAY   1000000
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
	if (msg->type == NOWDB_WRK_STOP) return;
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
	nowdb_task_sleep(DELAY);
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
		NOWDB_IGNORE(nowdb_worker_stop(&wrk, TMO));
		return FALSE;
	}
	msg->type = 1;
	msg->cont = NULL;
	err = nowdb_worker_do(&wrk, msg);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_worker_stop(&wrk, TMO));
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
	nowdb_task_sleep(DELAY);
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
	return TRUE;
}

void qdrain(void **message) {
	if (message == NULL) return;
	if (*message == NULL) return;
	free(*message); *message = NULL;
}

typedef struct {
	nowdb_queue_t *q;
	uint32_t    last;
} fibo_t;

nowdb_err_t job(nowdb_worker_t      *wrk,
                nowdb_wrk_message_t *msg) {
	fibo_t *fibo = wrk->rsc;
	uint32_t f1, f2;
	uint32_t *f;

	f = malloc(sizeof(uint32_t));
	if (f == NULL) {
		return nowdb_err_get(nowdb_err_no_mem,
		    FALSE, wrk->name, "allocating f");
	}
	f1 = fibo->last;
	f2 = *((uint32_t*)msg->cont);
	*f = f1 + f2;
	fibo->last = f2;
	return nowdb_queue_enqueue(fibo->q, f);
}

nowdb_bool_t test2tasks() {
	uint32_t *fibs;
	nowdb_err_t err;
	nowdb_worker_t wrk;
	nowdb_queue_t  q;
	nowdb_wrk_message_t *msg;
	fibo_t fibo;
	uint32_t *f, *rcv=NULL;
	uint32_t f1 = 1;

	fibs = malloc(sizeof(uint32_t) * 20);
	if (fibs == NULL) {
		fprintf(stderr, "no mem\n");
		return FALSE;
	}
	fibonacci(fibs, 20);

	f = malloc(sizeof(uint32_t));
	if (f == NULL) {
		fprintf(stderr, "no mem\n");
		free(fibs);
		return FALSE;
	}
	*f = f1;

	err = nowdb_queue_init(&q, 0, DELAY, &qdrain);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(fibs); free(f);
		return FALSE;
	}

	fibo.q = &q; fibo.last = 1;
	err = nowdb_worker_init(&wrk, "fibo", -1,
	                        &job, NULL, &drainer, &fibo);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(fibs); free(f);
		nowdb_queue_destroy(&q);
		return FALSE;
	}
	fprintf(stderr, "1 1 ");
	for(int i=2;i<20;i++) {
		msg = malloc(sizeof(nowdb_wrk_message_t));
		if (msg == NULL) {
			fprintf(stderr, "no mem");
			free(fibs);
			NOWDB_IGNORE(nowdb_worker_stop(&wrk, TMO));
			NOWDB_IGNORE(nowdb_queue_shutdown(&q));
			nowdb_queue_destroy(&q);
			return FALSE;
		}
		msg->type = 10;
		msg->cont = f;
		err = nowdb_worker_do(&wrk,msg);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			free(fibs);
			NOWDB_IGNORE(nowdb_worker_stop(&wrk, TMO));
			NOWDB_IGNORE(nowdb_queue_shutdown(&q));
			nowdb_queue_destroy(&q);
			return FALSE;
		}
		err = nowdb_queue_dequeue(&q, TMO, (void**)&rcv);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			free(fibs);
			NOWDB_IGNORE(nowdb_worker_stop(&wrk, TMO));
			NOWDB_IGNORE(nowdb_queue_shutdown(&q));
			nowdb_queue_destroy(&q);
			return FALSE;
		}
		f = malloc(sizeof(uint32_t));
		if (f == NULL) {
			fprintf(stderr, "no mem\n");
			free(fibs);
			NOWDB_IGNORE(nowdb_worker_stop(&wrk, TMO));
			NOWDB_IGNORE(nowdb_queue_shutdown(&q));
			nowdb_queue_destroy(&q);
			return FALSE;
		}
		*f = *rcv; free(rcv); f1 = *f;
		fprintf(stderr, "%u ", f1);
		if (f1 != fibs[i]) {
			fprintf(stderr, "wrong number: %u ", f1);
			free(fibs); free(f);
			NOWDB_IGNORE(nowdb_worker_stop(&wrk, TMO));
			NOWDB_IGNORE(nowdb_queue_shutdown(&q));
			nowdb_queue_destroy(&q);
			return FALSE;
		}
	}
	fprintf(stderr, "\n");
	free(f); // the last one
	err = nowdb_worker_stop(&wrk, TMO);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(fibs);
		NOWDB_IGNORE(nowdb_queue_shutdown(&q));
		return FALSE;
	}
	err = nowdb_queue_shutdown(&q);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(fibs);
		return FALSE;
	}
	nowdb_queue_destroy(&q);
	free(fibs);
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
	if (!test2tasks()) {
		fprintf(stderr, "test2tasks failed\n");
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

