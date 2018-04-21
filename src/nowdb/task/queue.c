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

static char *OBJECT = "queue";

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

/* ------------------------------------------------------------------------
 * Helper: wait until the condition 'what' is fulfilled;
 *         returns with the lock held on success.
 * ------------------------------------------------------------------------
 */
#define FULL  0
#define EMPTY 1
static inline nowdb_err_t block(nowdb_queue_t *q, int what) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	for(;;) {
		err = nowdb_lock(&q->lock);
		if (err != NOWDB_OK) return err;
		if (what == FULL) {
			if (q->max <= 0 ||
			    q->max >  q->list.len) return NOWDB_OK;
		} else {
			if (q->list.len > 0) return NOWDB_OK;
			if (q->closed) {
				err = nowdb_err_get(nowdb_err_no_rsc,
				   FALSE, OBJECT, "queue is closed");
			}
		}
		err2 = nowdb_unlock(&q->lock);
		if (err2 != NOWDB_OK) {
			err2->cause = err; return err2;
		}
		if (err != NOWDB_OK) return err;
		err = nowdb_task_sleep(q->delay);
		if (err != NOWDB_OK) return err;
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
	err = block(q, FULL);
	if (err != NOWDB_OK) return err;
	if (q->closed) {
		err = nowdb_err_get(nowdb_err_no_rsc, FALSE, OBJECT,
		                                 "queue is closed");
		goto unlock;
	}
	if (ts_algo_list_append(&q->list, message) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                     "list append");
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
	if (ts_algo_list_insert(&q->list, message) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                     "list append");
	}
	err2 = nowdb_unlock(&q->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Removes the 'message' at the head of the queue
 * If there are no messages in the queue,
 * the calling thread blocks.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_dequeue(nowdb_queue_t *q, void **message) {
	ts_algo_list_node_t  *node;
	nowdb_err_t err = NOWDB_OK;

	if (q == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                             "queue object is NULL");
	if (message == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "message is NULL");
	err = block(q, EMPTY);
	if (err != NOWDB_OK) return err;

	node = q->list.head;
	*message = node->cont;
	ts_algo_list_remove(&q->list, node);
	free(node);

	return nowdb_unlock(&q->lock);
}
