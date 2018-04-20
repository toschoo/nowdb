/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for tasks
 * ========================================================================
 */
#include <nowdb/types/error.h>
#include <nowdb/types/time.h>
#include <nowdb/task/lock.h>
#include <nowdb/task/task.h>

#include <common/bench.h>

#include <tsalgo/list.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

nowdb_bool_t testSleep() {
	nowdb_err_t err;
	nowdb_time_t delay;
	struct timespec t1, t2;
	uint64_t d;

	delay = 1;
	for(int i=0;i<9;i++) {
		delay *= 10;

		fprintf(stderr, "delay: %ldns\n", delay);

		timestamp(&t1);
		err = nowdb_task_sleep(delay);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		timestamp(&t2);
		d = minus(&t2, &t1);
		if (d < delay) {
			fprintf(stderr, "couldn't sleep: %lu, %ld\n",
			                                   d, delay);
			return FALSE;
		}
		if (d > 10*delay && delay > 100000) {
			fprintf(stderr, "overslept: %lu, %ld\n",
			                              d, delay);
			return FALSE;
		}
	}
	return TRUE;
}

nowdb_lock_t   lock;
ts_algo_list_t inlist;
ts_algo_list_t outlist;
nowdb_err_t    globalErr;

void *job1(void *ignore) {
	int f1=1, tmp;
	ts_algo_list_node_t *node;
	nowdb_bool_t z;
	int *f = malloc(sizeof(int));
	if (f == NULL) {
		globalErr = nowdb_err_get(nowdb_err_no_mem,
			              FALSE, "job1", NULL);
		return NULL;
	}
	*f = 1;

	globalErr = nowdb_lock(&lock);
	if (globalErr != NOWDB_OK) return NULL;
	if (ts_algo_list_insert(&inlist, f) != TS_ALGO_OK) {
		globalErr = nowdb_err_get(nowdb_err_no_mem,
			              FALSE, "job1", NULL);
		NOWDB_IGNORE(nowdb_unlock(&lock));
		return NULL;
	}
	globalErr = nowdb_unlock(&lock);
	if (globalErr != NOWDB_OK) return NULL;
	fprintf(stderr, "%d ", f1);
	for(;;) {
		globalErr = nowdb_lock(&lock);
		if (globalErr != NOWDB_OK) return NULL;
		z = TRUE;
		if (outlist.len > 0) {
			node = outlist.head;
			ts_algo_list_remove(&outlist, node);
			z = FALSE;
		}
		globalErr = nowdb_unlock(&lock);
		if (globalErr != NOWDB_OK) return NULL;

		if (z) {
			nowdb_task_sleep(1000000); continue;
		}

		f = node->cont;
		tmp = *f;
		*f = f1+tmp;
		f1 = tmp;
		free(node);
		fprintf(stderr, "%d ", f1);
		if (f1 > 1000) break;

		globalErr = nowdb_lock(&lock);
		if (globalErr != NOWDB_OK) return NULL;
		if (ts_algo_list_append(&inlist, f) != TS_ALGO_OK) {
			globalErr = nowdb_err_get(nowdb_err_no_mem,
			                      FALSE, "job1", NULL);
			free(node->cont); free(node);
			NOWDB_IGNORE(nowdb_unlock(&lock));
			return NULL;
		}
		globalErr = nowdb_unlock(&lock);
		if (globalErr != NOWDB_OK) return NULL;
	}
	fprintf(stderr, "\n");
	if (f != NULL) free(f);
	return NULL;
}

void *job2(void *ignore) {
	ts_algo_list_node_t *node;
	int f1 = 1, tmp;
	nowdb_bool_t z;
	int *f = malloc(sizeof(int));
	if (f == NULL) {
		globalErr = nowdb_err_get(nowdb_err_no_mem,
			              FALSE, "job2", NULL);
		return NULL;
	}
	*f = 1;

	globalErr = nowdb_lock(&lock);
	if (globalErr != NOWDB_OK) return NULL;
	if (ts_algo_list_insert(&outlist, f) != TS_ALGO_OK) {
		globalErr = nowdb_err_get(nowdb_err_no_mem,
			              FALSE, "job2", NULL);
		NOWDB_IGNORE(nowdb_unlock(&lock));
		return NULL;
	}
	globalErr = nowdb_unlock(&lock);
	if (globalErr != NOWDB_OK) return NULL;
	for(;;) {
		globalErr = nowdb_lock(&lock);
		if (globalErr != NOWDB_OK) return NULL;
		z = TRUE;
		if (inlist.len > 0) {
			node = inlist.head;
			ts_algo_list_remove(&inlist, node);
			z = FALSE;
		}
		globalErr = nowdb_unlock(&lock);
		if (globalErr != NOWDB_OK) return NULL;

		if (z) {
			nowdb_task_sleep(1000000); continue;
		}

		f = node->cont;
		tmp = *f;
		*f = tmp + f1;
		f1 = tmp;
		free(node);
		if (f1 > 1000) break;

		globalErr = nowdb_lock(&lock);
		if (globalErr != NOWDB_OK) return NULL;
		if (ts_algo_list_append(&outlist, f) != TS_ALGO_OK) {
			globalErr = nowdb_err_get(nowdb_err_no_mem,
			                      FALSE, "job2", NULL);
			NOWDB_IGNORE(nowdb_unlock(&lock));
			return NULL;
		}
		globalErr = nowdb_unlock(&lock);
		if (globalErr != NOWDB_OK) return NULL;
	}
	if (f != NULL) free(f);
	return NULL;
}

nowdb_task_t test2task() {
	nowdb_task_t t1, t2;
	nowdb_err_t err;

	ts_algo_list_init(&inlist);
	ts_algo_list_init(&outlist);
	err = nowdb_lock_init(&lock);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_task_create(&t1, job1, NULL);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_task_create(&t2, job2, NULL);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_task_sleep(1000000000); // <- use a lock instead
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	ts_algo_list_destroy(&inlist);
	ts_algo_list_destroy(&outlist);
	nowdb_task_destroy(t1);
	nowdb_task_destroy(t2);

	if (globalErr == NOWDB_OK) return TRUE;
	nowdb_err_print(globalErr);
	nowdb_err_release(globalErr);
	return FALSE;
}

int main() {
	int rc = EXIT_SUCCESS;

	if (!testSleep()) {
		fprintf(stderr, "testSleep failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!test2task()) {
		fprintf(stderr, "test2task failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}

