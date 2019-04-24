/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Queue: inter-thread communication
 * ========================================================================
 */
#include <nowdb/task/queue.h>
#include <nowdb/task/task.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <signal.h>

static char *OBJECT = "queue";

#define NOMEM(m) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, m);

/* ------------------------------------------------------------------------
 * Initialise the queue
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_init(nowdb_queue_t *q, int max,
                             nowdb_time_t        delay,
                             nowdb_queue_drain_t drain) {
	nowdb_err_t err;

	if (q == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                             "queue object is NULL");
	if (drain == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "drain object is NULL");
	if (delay < 0) return nowdb_err_get(nowdb_err_invalid,
	                     FALSE, OBJECT, "negative delay");
	q->max = max;
	q->delay = delay;
	q->closed = TRUE;
	q->drain  = drain;

	ts_algo_list_init(&q->readers);

	sigemptyset(&q->six);
	sigaddset(&q->six, SIGUSR1);

	ts_algo_list_init(&q->list);

	err = nowdb_lock_init(&q->lock);
	if (err != NOWDB_OK) return err;

	q->closed = FALSE;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy the queue
 * ------------------------------------------------------------------------
 */
void nowdb_queue_destroy(nowdb_queue_t *q) {
	if (q == NULL) return;
	ts_algo_list_destroy(&q->list);
	nowdb_lock_destroy(&q->lock);
	ts_algo_list_destroy(&q->readers);
}

/* ------------------------------------------------------------------------
 * Helper: drain the queue
 * ------------------------------------------------------------------------
 */
static inline void drainme(nowdb_queue_t *q) {
	ts_algo_list_node_t *runner, *tmp;

	runner=q->list.head;
	while(runner!=NULL) {
		ts_algo_list_remove(&q->list, runner);
		q->drain(&runner->cont); runner->cont = NULL;
		tmp = runner->nxt; free(runner); runner=tmp;
	}
}

/* ------------------------------------------------------------------------
 * Shutdown
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_shutdown(nowdb_queue_t *q) {
	nowdb_err_t err;

	if (q == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                             "queue object is NULL");
	err = nowdb_lock(&q->lock);
	if (err != NOWDB_OK) return err;

	q->closed = TRUE;
	drainme(q);

	return nowdb_unlock(&q->lock);
}

/* ------------------------------------------------------------------------
 * Removes all messages from the queue
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_drain(nowdb_queue_t *q) {
	nowdb_err_t err;

	if (q == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                             "queue object is NULL");
	err = nowdb_lock(&q->lock);
	if (err != NOWDB_OK) return err;

	drainme(q);

	return nowdb_unlock(&q->lock);
}

/* ------------------------------------------------------------------------
 * Open the queue after it was closed.
 * The function has no effect, when the queue was already open.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_open(nowdb_queue_t *q) {
	nowdb_err_t err;

	if (q == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                             "queue object is NULL");
	err = nowdb_lock(&q->lock);
	if (err != NOWDB_OK) return err;

	if (q->closed) q->closed = FALSE;

	return nowdb_unlock(&q->lock);
}

/* ------------------------------------------------------------------------
 * Closes the queue for 'enqueue'
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_close(nowdb_queue_t *q) {
	nowdb_err_t err;

	if (q == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                             "queue object is NULL");
	err = nowdb_lock(&q->lock);
	if (err != NOWDB_OK) return err;

	if (!q->closed) q->closed = TRUE;

	return nowdb_unlock(&q->lock);
}

static inline void removereader(nowdb_queue_t *q) {
	ts_algo_list_node_t *run;
	nowdb_task_t me = pthread_self();

	for(run=q->readers.head; run!=NULL; run=run->nxt) {
		if (pthread_equal(me, (nowdb_task_t)run->cont)) break;
	}
	if (run == NULL) return;
	ts_algo_list_remove(&q->readers, run); free(run);
}

static inline void signalreaders(nowdb_queue_t *q) {
	ts_algo_list_node_t *run;
	for(run=q->readers.head; run!=NULL; run=run->nxt) {
		if (pthread_kill((nowdb_task_t)run->cont, 0) == 0) {
			pthread_kill((nowdb_task_t)run->cont, SIGUSR1);
		}
	}
}

/* ------------------------------------------------------------------------
 * Helper: wait until the condition 'what' is fulfilled;
 *         returns with the lock held on success.
 * ------------------------------------------------------------------------
 */
#define FULL  0
#define EMPTY 1
#define NPERSEC 1000000000ll
static inline nowdb_err_t block(nowdb_queue_t *q,
                                nowdb_time_t tmo,
                                int         what) {
	nowdb_time_t s = tmo;
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	char first=1;

	for(;;) {
		err = nowdb_lock(&q->lock);
		if (err != NOWDB_OK) return err;

		if (first && what == EMPTY) {
			first=0;
			if (ts_algo_list_append(&q->readers,
			      (void*)pthread_self()) != TS_ALGO_OK) {
				NOMEM("appending to readers");
				goto unlock;
			}
		}
		
		if (what == FULL) {
			if (q->max <= 0 ||
			    q->max >  q->list.len) return NOWDB_OK;
		} else {
			if (q->list.len > 0) return NOWDB_OK;
			if (q->closed) {
				removereader(q);
				err = nowdb_err_get(nowdb_err_no_rsc,
				   FALSE, OBJECT, "queue is closed");
			}
		}
		if (err != NOWDB_OK) goto unlock;
		if (tmo == 0 || (tmo > 0 && s <= 0)) {
			if (what == FULL) removereader(q);
			err = nowdb_err_get(nowdb_err_timeout,
			                  FALSE, OBJECT, NULL);
		}
unlock:
		err2 = nowdb_unlock(&q->lock);
		if (err2 != NOWDB_OK) {
			err2->cause = err; return err2;
		}
		if (err != NOWDB_OK) return err;
		if (what == FULL) {
			err = nowdb_task_sleep(q->delay);
			if (err != NOWDB_OK) return err;
			if (tmo > 0) s-=q->delay;
		} else {
			struct timespec tv;
			tv.tv_sec = 0;
			if (q->delay > NPERSEC) {
				tv.tv_sec = q->delay/NPERSEC;
				tv.tv_nsec = q->delay - tv.tv_sec * NPERSEC;
			} else {
				tv.tv_nsec = q->delay;
			}
			int x = sigtimedwait(&q->six, NULL, &tv);
			if (x == EAGAIN) s=0; // timeout
			else if (x < 0) {
				err = nowdb_err_get(nowdb_err_sigwait,
				                   TRUE, OBJECT, NULL);
			}
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Writes 'message' to the end of the queue (last in / last out)
 * If the queue is already full (contains more than max messages),
 * the calling thread blocks.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_enqueue(nowdb_queue_t *q, void *message) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	if (q == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                             "queue object is NULL");
	err = block(q, -1, FULL);
	if (err != NOWDB_OK) return err;
	if (q->closed) {
		err = nowdb_err_get(nowdb_err_no_rsc, FALSE, OBJECT,
		                                 "queue is closed");
		goto unlock;
	}
	// fprintf(stderr, "ENQ: %p\n", message);
	if (ts_algo_list_append(&q->list, message) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                     "list append");
	}
	if (q->readers.head != NULL) {
		signalreaders(q);
	}
unlock:
	err2 = nowdb_unlock(&q->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Writes 'message' to the head of the queue (last in / first out)
 * The message is written even when the queue is full.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_enqueuePrio(nowdb_queue_t *q, void *message) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	if (q == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                             "queue object is NULL");
	err = nowdb_lock(&q->lock);
	if (err != NOWDB_OK) return err;
	// fprintf(stderr, "ENQ: %p\n", message);
	if (ts_algo_list_insert(&q->list, message) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                     "list append");
	}
	if (q->readers.head != NULL) {
		signalreaders(q);
	}
	err2 = nowdb_unlock(&q->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Removes the 'message' at the head of the queue
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_dequeue(nowdb_queue_t *q, 
                                nowdb_time_t tmo,
                                void   **message) {
	ts_algo_list_node_t  *node;
	nowdb_err_t err = NOWDB_OK;

	if (q == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                             "queue object is NULL");
	if (message == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "message is NULL");
	err = block(q, tmo, EMPTY);
	if (err != NOWDB_OK) return err;

	node = q->list.head;
	if (node == NULL) {
		*message = NULL;
		return nowdb_unlock(&q->lock);
	}
	*message = node->cont;
	ts_algo_list_remove(&q->list, node);
	free(node);

	// fprintf(stderr, "DEQ: %p\n", message);

	removereader(q);
	return nowdb_unlock(&q->lock);
}
