/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Worker: worker thread waiting for jobs through a queue
 * ========================================================================
 */
#include <nowdb/task/worker.h>
#include <stdio.h>

/* ------------------------------------------------------------------------
 * DELAY is used for the queue (which will check for messages
 *       every 'DELAY' nanoseconds, i.e. every 50ms).
 * MINOR is used to checking the states on 'stop' (i.e. every 10ms).
 * ------------------------------------------------------------------------
 */
#define DELAY 25000000
#define MINOR 10000000

static nowdb_wrk_message_t stopmsg = {0,NULL};

/* ------------------------------------------------------------------------
 * Report errors
 * ------------------------------------------------------------------------
 */
static inline void reportError(nowdb_worker_t *wrk,
                               nowdb_err_t   error) {
	nowdb_err_t err;
	if (wrk->errqueue != NULL) {
		err = nowdb_queue_enqueue(wrk->errqueue, error);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			nowdb_err_print(error);
			nowdb_err_release(error);
			return;
		}
	}
	nowdb_err_print(error);
	nowdb_err_release(error);
}

/* ------------------------------------------------------------------------
 * Life of a worker
 * ------------------------------------------------------------------------
 */
static void *wrkentry(void *p) {
	nowdb_worker_t  *wrk = p;
	nowdb_wrk_message_t *msg=NULL;
	nowdb_err_t     err;
	nowdb_job_t job;

	job = wrk->job;

	err = nowdb_lock(&wrk->lock);
	if (err != NOWDB_OK) {
		reportError(wrk, err); return NULL;
	}
	wrk->state = NOWDB_WRK_RUNNING;

	err = nowdb_unlock(&wrk->lock);
	if (err != NOWDB_OK) {
		reportError(wrk, err); return NULL;
	}
	for(;;) {
		err = nowdb_queue_dequeue(&wrk->jobqueue,
		                           wrk->period,   /* wait for   */
		                           (void**)&msg); /* one period */
		if (err != NOWDB_OK) {
			/* on timeout: perform the job without message */
			if (err->errcode == nowdb_err_timeout) {
				nowdb_err_release(err);
				err = job(wrk, NULL);
				if (err != NOWDB_OK) reportError(wrk, err);
				nowdb_err_release(err);
			} else {
				reportError(wrk, err);
				nowdb_err_release(err);
			}
			continue;
		}

		/* aperiodic event */
		if (msg == NULL) continue;
		if (msg->type == NOWDB_WRK_STOP) {
			/* free(msg); */ break;
		}
		err = job(wrk, msg);
		if (err != NOWDB_OK) reportError(wrk, err);

		/* cleanup */
		wrk->jobqueue.drain((void**)&msg); msg = NULL;
	}
	/* announce that I'm stopping */
	err = nowdb_lock(&wrk->lock);
	if (err != NOWDB_OK) reportError(wrk, err); /* what to do,    */
	wrk->state = NOWDB_WRK_STOPPED;             /* when we cannot */
	err = nowdb_unlock(&wrk->lock);             /* lock???        */
	if (err != NOWDB_OK) reportError(wrk, err);
	return NULL;
}

/* ------------------------------------------------------------------------
 * Allocate and initialise worker
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_worker_new(nowdb_worker_t      **wrk,
                             char                *name,
                             nowdb_time_t       period,
                             nowdb_job_t           job,
                             nowdb_queue_t   *errqueue,
	                     nowdb_queue_drain_t drain,
                             void                *rsc) {
	nowdb_err_t err;
	if (wrk == NULL) return nowdb_err_get(nowdb_err_invalid, FALSE,
	                 "worker", "pointer to worker object is NULL");
	*wrk = malloc(sizeof(nowdb_worker_t));
	if (*wrk == NULL) return nowdb_err_get(nowdb_err_no_mem, FALSE,
	                         "worker", "worker object allocation");
	err = nowdb_worker_init(*wrk, name, period, job,
	                          errqueue, drain, rsc);
	if (err != NOWDB_OK) {
		free(*wrk); *wrk = NULL;
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: wait for stop
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t waitFor(nowdb_worker_t *wrk,
                                  char          state,
                                  nowdb_time_t    tmo) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_time_t t = tmo;

	for(;;) {
		err = nowdb_lock(&wrk->lock);
		if (err != NOWDB_OK) return err;
		
		if (wrk->state == state) {
			return nowdb_unlock(&wrk->lock);
		}
		err = nowdb_unlock(&wrk->lock);
		if (err != NOWDB_OK) return err;

		if (tmo == 0) return nowdb_err_get(nowdb_err_timeout,
		                  FALSE, wrk->name, "with timout=0");
		if (tmo > 0 && t <= 0) return nowdb_err_get(nowdb_err_timeout,
		                          FALSE, wrk->name, "with timeout>0");
		err = nowdb_task_sleep(MINOR);
		if (err != NOWDB_OK) return err;
		t -= MINOR;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Initialise worker
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_worker_init(nowdb_worker_t       *wrk,
                              char                *name,
                              nowdb_time_t       period,
                              nowdb_job_t           job,
                              nowdb_queue_t   *errqueue,
                              nowdb_queue_drain_t drain,
                              void                 *rsc) {
	nowdb_err_t err;
	if (wrk == NULL) return nowdb_err_get(nowdb_err_invalid,
	              FALSE, "worker", "worker object is NULL");
	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                        FALSE, "worker", "name is NULL");

	strncpy(wrk->name, name, 8); wrk->name[7] = 0;
	wrk->errqueue = errqueue;
	wrk->job = job;
	wrk->rsc = rsc;
	wrk->period = period>0?period:-1;
	wrk->state = NOWDB_WRK_STOPPED;

	err = nowdb_lock_init(&wrk->lock);
	if (err != NOWDB_OK) return err;

	err = nowdb_queue_init(&wrk->jobqueue, 0, DELAY, drain);
	if (err != NOWDB_OK) return err;

	err = nowdb_lock(&wrk->lock);
	if (err != NOWDB_OK) return err;

	err = nowdb_task_create(&wrk->task, wrkentry, wrk);
	if (err != NOWDB_OK) {
		nowdb_queue_destroy(&wrk->jobqueue);
		return err;
	}
	err = nowdb_unlock(&wrk->lock);
	if (err != NOWDB_OK) return err;

	/* wait for running */
	return waitFor(wrk, NOWDB_WRK_RUNNING, DELAY);
}

/* ------------------------------------------------------------------------
 * Helper: destroy worker
 * ------------------------------------------------------------------------
 */
static inline void destroy(nowdb_worker_t *wrk) {
	if (wrk == NULL) return;
	nowdb_queue_destroy(&wrk->jobqueue);
	nowdb_lock_destroy(&wrk->lock);
	nowdb_task_destroy(wrk->task);
}

/* ------------------------------------------------------------------------
 * Do the job
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_worker_do(nowdb_worker_t      *wrk,
                            nowdb_wrk_message_t *msg) {
	nowdb_err_t err;
	if (wrk == NULL) return nowdb_err_get(nowdb_err_invalid,
	              FALSE, "worker", "worker object is NULL");
	err = nowdb_queue_enqueue(&wrk->jobqueue, msg);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Stop the worker
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_worker_stop(nowdb_worker_t *wrk,
                              nowdb_time_t    tmo) {
	nowdb_err_t err;

	if (wrk == NULL) return nowdb_err_get(nowdb_err_invalid,
	              FALSE, "worker", "worker object is NULL");

	/* if the worker is already stopped,
	 * there is nothing to do */
	err = nowdb_lock(&wrk->lock);
	if (err != NOWDB_OK) return err;

	if (wrk->state == NOWDB_WRK_STOPPED) {
		return nowdb_unlock(&wrk->lock);
	}
	err = nowdb_unlock(&wrk->lock);
	if (err != NOWDB_OK) return err;

	/* send stop message */
	err = nowdb_queue_enqueuePrio(&wrk->jobqueue, &stopmsg);
	if (err != NOWDB_OK) return err;

	/* wait for stop */
	err = waitFor(wrk, NOWDB_WRK_STOPPED, tmo);
	if (err != NOWDB_OK) return err;

	/* wait for thread to terminate */
	err = nowdb_task_join(wrk->task);
	if (err != NOWDB_OK) return err;

	/* stop queue */
	err = nowdb_queue_shutdown(&wrk->jobqueue);
	if (err != NOWDB_OK) return err;

	/* destroy everything */
	destroy(wrk); return NOWDB_OK;
}
