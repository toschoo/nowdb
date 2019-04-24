/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for queues
 * ========================================================================
 */
#include <nowdb/types/error.h>
#include <nowdb/types/time.h>
#include <nowdb/task/lock.h>
#include <nowdb/task/task.h>
#include <nowdb/task/queue.h>

#include <common/bench.h>
#include <common/math.h>

#include <tsalgo/list.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <signal.h>

#define DELAY 1000000

void drainer(void **message) {
	if (*message != NULL) {
		free(*message); *message = NULL;
	}
}

void nop(void **ignore) {}

nowdb_bool_t testInitDestroy() {
	nowdb_queue_t q;
	nowdb_err_t err;

	err = nowdb_queue_init(&q, 8, DELAY, &drainer);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_queue_destroy(&q);
	return TRUE;
}

nowdb_bool_t testEnqueueNoBlock() {
	nowdb_queue_t q;
	nowdb_err_t err;
	int  *msg, *rcv;

	err = nowdb_queue_init(&q, 8, DELAY, &drainer);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	msg = malloc(sizeof(int));
	if (msg == NULL) {
		fprintf(stderr, "no mem\n");
		nowdb_queue_destroy(&q);
		return FALSE;
	}
	*msg = 42;
	err = nowdb_queue_enqueue(&q, msg);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q); free(msg);
		return FALSE;
	}
	err = nowdb_queue_dequeue(&q, -1, (void**)&rcv);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q); free(msg);
		return FALSE;
	}
	if (rcv == NULL) {
		fprintf(stderr, "no message received\n");
		nowdb_queue_destroy(&q); free(msg);
		return FALSE;
	}
	if (*rcv != *msg) {
		fprintf(stderr, "message differs: %d\n", *rcv);
		nowdb_queue_destroy(&q); free(msg);
		return FALSE;
	}
	free(msg); nowdb_queue_destroy(&q);
	return TRUE;
}

nowdb_bool_t testEnqueueClosed() {
	nowdb_queue_t q;
	nowdb_err_t err;
	int  *msg, *rcv;

	err = nowdb_queue_init(&q, 8, DELAY, &drainer);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_queue_close(&q);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q);
		return FALSE;
	}
	msg = malloc(sizeof(int));
	if (msg == NULL) {
		fprintf(stderr, "no mem\n");
		nowdb_queue_destroy(&q);
		return FALSE;
	}
	*msg = 42;
	err = nowdb_queue_enqueue(&q, msg);
	if (err == NOWDB_OK) {
		fprintf(stderr, "enqueue into a closed queue\n");
		free(msg); nowdb_queue_destroy(&q);
		return FALSE;
	}

	nowdb_err_print(err);
	nowdb_err_release(err);

	err = nowdb_queue_dequeue(&q, -1, (void**)&rcv);
	if (err == NOWDB_OK) {
		fprintf(stderr, "dequeue from empty closed queue\n");
		free(msg); nowdb_queue_destroy(&q);
		return FALSE;
	}

	nowdb_err_print(err);
	nowdb_err_release(err);

	err = nowdb_queue_open(&q);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q); free(msg);
		return FALSE;
	}
	err = nowdb_queue_enqueue(&q, msg);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q); free(msg);
		return FALSE;
	}
	err = nowdb_queue_dequeue(&q, -1, (void**)&rcv);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q); free(msg);
		return FALSE;
	}
	if (rcv == NULL) {
		fprintf(stderr, "no message received\n");
		nowdb_queue_destroy(&q); free(msg);
		return FALSE;
	}
	if (*rcv != *msg) {
		fprintf(stderr, "message differs: %d\n", *rcv);
		nowdb_queue_destroy(&q); free(msg);
		return FALSE;
	}
	nowdb_queue_destroy(&q); free(msg);
	return TRUE;
}

nowdb_bool_t testEnqueuePrio() {
	nowdb_queue_t q;
	nowdb_err_t err;
	int  *msg1, *msg2;
	int  *prio, *rcv;

	err = nowdb_queue_init(&q, 8, DELAY, &drainer);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	msg1 = malloc(sizeof(int));
	if (msg1 == NULL) {
		fprintf(stderr, "no mem\n");
		nowdb_queue_destroy(&q);
		return FALSE;
	}
	*msg1 = 42;
	err = nowdb_queue_enqueue(&q, msg1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q); free(msg1);
		return FALSE;
	}
	msg2 = malloc(sizeof(int));
	if (msg2 == NULL) {
		fprintf(stderr, "no mem\n");
		nowdb_queue_destroy(&q); free(msg1);
		return FALSE;
	}
	*msg2 = 13;
	err = nowdb_queue_enqueue(&q, msg2);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q);
		free(msg1); free(msg2);
		return FALSE;
	}
	err = nowdb_queue_dequeue(&q, -1, (void**)&rcv);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q);
		free(msg1); free(msg2);
		return FALSE;
	}
	if (rcv == NULL) {
		fprintf(stderr, "nothing received\n");
		nowdb_queue_destroy(&q);
		free(msg1); free(msg2);
		return FALSE;
	}
	if (*rcv != *msg1) {
		fprintf(stderr, "wrong message (1): %d\n", *rcv);
		nowdb_queue_destroy(&q);
		free(msg1); free(msg2);
		return FALSE;
	}
	free(msg1);

	prio = malloc(sizeof(int));
	if (prio== NULL) {
		fprintf(stderr, "no mem\n");
		nowdb_queue_destroy(&q);
		nowdb_queue_destroy(&q); free(msg2);
		return FALSE;
	}
	*prio = -1;
	err = nowdb_queue_enqueuePrio(&q, prio);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q);
		free(msg2); free(prio);
		return FALSE;
	}
	err = nowdb_queue_dequeue(&q, -1, (void**)&rcv);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q);
		free(msg2); free(prio);
		return FALSE;
	}
	if (rcv == NULL) {
		fprintf(stderr, "nothing received\n");
		nowdb_queue_destroy(&q);
		free(msg2); free(prio);
		return FALSE;
	}
	if (*rcv != *prio) {
		fprintf(stderr, "wrong message (2): %d\n", *rcv);
		nowdb_queue_destroy(&q);
		free(msg2); free(prio);
		return FALSE;
	}
	err = nowdb_queue_dequeue(&q, -1, (void**)&rcv);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q);
		free(msg2); free(prio);
		return FALSE;
	}
	if (rcv == NULL) {
		fprintf(stderr, "nothing received\n");
		nowdb_queue_destroy(&q);
		free(msg2); free(prio);
		return FALSE;
	}
	if (*rcv != *msg2) {
		fprintf(stderr, "wrong message (3): %d\n", *rcv);
		nowdb_queue_destroy(&q);
		free(msg2); free(prio);
		return FALSE;
	}
	nowdb_queue_destroy(&q);
	free(msg2); free(prio);
	return TRUE;
}

nowdb_bool_t testShutdown() {
	nowdb_queue_t q;
	nowdb_err_t err;

	err = nowdb_queue_init(&q, 8, DELAY, &drainer);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	for (int i=0;i<8;i++) {
		int *msg = malloc(sizeof(int));
		if (msg == NULL) {
			fprintf(stderr, "no mem\n");
			NOWDB_IGNORE(nowdb_queue_shutdown(&q));
			nowdb_queue_destroy(&q);
			return FALSE;
		}
		*msg = i;
		err = nowdb_queue_enqueue(&q, msg);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			NOWDB_IGNORE(nowdb_queue_shutdown(&q));
			nowdb_queue_destroy(&q);
			return FALSE;
		}
	}
	err = nowdb_queue_shutdown(&q);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q);
		return FALSE;
	}
	nowdb_queue_destroy(&q);
	return TRUE;
}

typedef struct {
	nowdb_queue_t *q1;
	nowdb_queue_t *q2;
	nowdb_lock_t  *lock;
} twoqueues_t;

void *job(void *args) {
	nowdb_err_t err;
	uint32_t *rcv;
	uint32_t *f;
	uint32_t  p=1;
	twoqueues_t *qs = (twoqueues_t*)args;
	nowdb_queue_t *q1 = qs->q1;
	nowdb_queue_t *q2 = qs->q2;

	err = nowdb_lock(qs->lock);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	for(;;) {
		// fprintf(stderr, "dequeuing from %p\n", q1);
		err = nowdb_queue_dequeue(q1, -1, (void**)&rcv);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			NOWDB_IGNORE(nowdb_unlock(qs->lock));
			return NULL;
		}
		if (*rcv == 0xffffffff) {
			free(rcv); break;
		}
		f = malloc(sizeof(uint32_t));
		if (f == NULL) {
			fprintf(stderr, "no mem\n");
			free(rcv);
			NOWDB_IGNORE(nowdb_unlock(qs->lock));
			return NULL;
		}
		*f = *rcv + p; p = *rcv; free(rcv);
		// fprintf(stderr, "enqueuing %u into %p\n", *f, q2);
		err = nowdb_queue_enqueue(q2,f);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			free(f);
			NOWDB_IGNORE(nowdb_unlock(qs->lock));
			return NULL;
		}
	}
	NOWDB_IGNORE(nowdb_unlock(qs->lock));
	return NULL;
}

nowdb_bool_t test2tasks() {
	twoqueues_t qs;
	uint32_t *fibs;
	uint32_t *f;
	uint32_t *rcv;
	nowdb_queue_t q1, q2;
	nowdb_err_t err;
	nowdb_task_t  t;
	nowdb_lock_t  l;

	fibs = malloc(sizeof(uint32_t)*20);
	if (fibs == NULL) {
		fprintf(stderr, "no mem\n");
		return FALSE;
	}
	fibonacci(fibs, 20);
	for(int i=0;i<20;i++) {
		fprintf(stderr, "%u ", fibs[i]);
	}
	fprintf(stderr, "\n");

	err = nowdb_queue_init(&q1, 0, DELAY, &drainer);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(fibs);
		return FALSE;
	}
	err = nowdb_queue_init(&q2, 0, DELAY, &drainer);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(fibs);
		return FALSE;
	}

	err = nowdb_lock_init(&l);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(fibs);
		return FALSE;
	}
	qs.q1 = &q1; qs.q2 = &q2; qs.lock = &l;
	err = nowdb_task_create(&t, job, &qs);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_lock_destroy(&l);
		free(fibs);
		return FALSE;
	}

	f = malloc(sizeof(uint32_t));
	if (f == NULL) {
		fprintf(stderr, "no mem\n");
		free(fibs);
		NOWDB_IGNORE(nowdb_queue_shutdown(&q1));
		NOWDB_IGNORE(nowdb_queue_shutdown(&q2));
		nowdb_lock_destroy(&l);
		return FALSE;
	}
	*f = 1;
	// fprintf(stderr, "enqueuing into %p\n", &q1);
	err = nowdb_queue_enqueue(&q1,f);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_queue_shutdown(&q1));
		NOWDB_IGNORE(nowdb_queue_shutdown(&q2));
		nowdb_lock_destroy(&l);
		free(fibs);
		return FALSE;
	}
	fprintf(stderr, "1 1 ");
	for(int i=0;i<15;i++) {
		// fprintf(stderr, "dequeuing from %p\n", &q2);
		err = nowdb_queue_dequeue(&q2, -1, (void**)&rcv);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			NOWDB_IGNORE(nowdb_queue_shutdown(&q1));
			NOWDB_IGNORE(nowdb_queue_shutdown(&q2));
			nowdb_lock_destroy(&l);
			free(fibs);
			return FALSE;
		}
		f = malloc(sizeof(int));
		if (f == NULL) {
			fprintf(stderr, "no mem\n");
			free(fibs); free(rcv);
			NOWDB_IGNORE(nowdb_queue_shutdown(&q1));
			NOWDB_IGNORE(nowdb_queue_shutdown(&q2));
			nowdb_lock_destroy(&l);
			return FALSE;
		}
		fprintf(stderr, "%u ", *rcv);
		*f = *rcv; free(rcv);
		if (*f != fibs[i+2]) {
			fprintf(stderr, "\nwrong number: %u\n", *f);
			free(fibs);
			NOWDB_IGNORE(nowdb_queue_shutdown(&q1));
			NOWDB_IGNORE(nowdb_queue_shutdown(&q2));
			nowdb_lock_destroy(&l);
			return FALSE;
		}
		// fprintf(stderr, "enqueuing into %p\n", &q1);
		err = nowdb_queue_enqueue(&q1,f);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			NOWDB_IGNORE(nowdb_queue_shutdown(&q1));
			NOWDB_IGNORE(nowdb_queue_shutdown(&q2));
			nowdb_lock_destroy(&l);
			free(fibs);
			return FALSE;
		}
	}
	fprintf(stderr, "\n");
	f = malloc(sizeof(uint32_t));
	if (f == NULL) {
		fprintf(stderr, "no mem\n");
		free(fibs);
		NOWDB_IGNORE(nowdb_queue_shutdown(&q1));
		NOWDB_IGNORE(nowdb_queue_shutdown(&q2));
		nowdb_lock_destroy(&l);
		return FALSE;
	}
	*f = 0xffffffff;
	err = nowdb_queue_enqueuePrio(&q1,f);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_queue_shutdown(&q1));
		NOWDB_IGNORE(nowdb_queue_shutdown(&q2));
		nowdb_lock_destroy(&l);
		free(fibs);
		return FALSE;
	}
	err = nowdb_lock(&l);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_queue_shutdown(&q1));
		NOWDB_IGNORE(nowdb_queue_shutdown(&q2));
		nowdb_lock_destroy(&l);
		free(fibs);
		return FALSE;
	}
	NOWDB_IGNORE(nowdb_unlock(&l));
	NOWDB_IGNORE(nowdb_queue_shutdown(&q1));
	NOWDB_IGNORE(nowdb_queue_shutdown(&q2));
	nowdb_lock_destroy(&l);
	nowdb_task_destroy(t);
	free(fibs);
	return TRUE;
}

int main() {
	int rc = EXIT_SUCCESS;

	sigset_t six;
	sigemptyset(&six);
	sigaddset(&six, SIGUSR1);
	pthread_sigmask(SIG_SETMASK, &six, NULL);

	if (!nowdb_err_init()) {
		fprintf(stderr, "nowdb_err_init failed\n");
		return EXIT_FAILURE;
	}
	if (!testInitDestroy()) {
		fprintf(stderr, "testInitDestroy failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testEnqueueNoBlock()) {
		fprintf(stderr, "testEnqueueNoBlock failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testEnqueueClosed()) {
		fprintf(stderr, "testEnqueueClosed failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testEnqueuePrio()) {
		fprintf(stderr, "testEnqueuePrio failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testShutdown()) {
		fprintf(stderr, "testShutdown failed\n");
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

