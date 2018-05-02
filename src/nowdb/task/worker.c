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
 * MINOR is used to check the states on 'stop' (i.e. every 10ms).
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
		}
		return;
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
	uint32_t idx;

	job = wrk->job;

	/* lock to obtain and set running */
	err = nowdb_lock(&wrk->lock);
	if (err != NOWDB_OK) {
		reportError(wrk, err); return NULL;
	}

	/* this is my id */
	idx = wrk->running;

	/* this is how many tasks are running */
	wrk->running++;

	/* unlock worker */
	err = nowdb_unlock(&wrk->lock);
	if (err != NOWDB_OK) {
		reportError(wrk, err); return NULL;
	}

	/* now we run forever (or until we receive a stop message */
	for(;;) {
		/* dequeue */
		err = nowdb_queue_dequeue(&wrk->jobqueue,
		                           wrk->period,   /* wait for   */
		                           (void**)&msg); /* one period */
		if (err != NOWDB_OK) {

			/* on timeout: perform the job without message */
			if (err->errcode == nowdb_err_timeout) {
				nowdb_err_release(err);
				err = job(wrk, idx, NULL);
				if (err != NOWDB_OK) reportError(wrk, err);
			} else {
				reportError(wrk, err);
			}
			continue;
		}

		/* aperiodic event.
		 * empty message: ignore */
		if (msg == NULL) continue;

		/* stop message: end loop */
		if (msg->type == NOWDB_WRK_STOP) break;

		/* do your job */
		err = job(wrk, idx, msg);
		if (err != NOWDB_OK) reportError(wrk, err);

		/* cleanup */
		wrk->jobqueue.drain((void**)&msg); msg = NULL;
	}
	/* announce that I'm stopping */
	err = nowdb_lock(&wrk->lock);
	if (err != NOWDB_OK) reportError(wrk, err); /* what to do,    */
	wrk->running--;                             /* when we cannot */
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
                             uint32_t             pool,
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
	err = nowdb_worker_init(*wrk, name, pool, period, job,
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
                                  uint32_t   expected,
                                  nowdb_time_t    tmo) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_time_t t = tmo;

	for(;;) {
		err = nowdb_lock(&wrk->lock);
		if (err != NOWDB_OK) return err;
		
		if (wrk->running == expected) {
			return nowdb_unlock(&wrk->lock);
		}
		/*
		fprintf(stderr, "%s waiting for %u, having %u\n",
		        wrk->name, expected, wrk->running);
		*/
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
                              uint32_t             pool,
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

	wrk->tasks = NULL;
	strncpy(wrk->name, name, 8); wrk->name[7] = 0;
	wrk->errqueue = errqueue;
	wrk->pool = pool;
	wrk->job = job;
	wrk->rsc = rsc;
	wrk->period = period>0?period:-1;
	wrk->running = 0;

	err = nowdb_lock_init(&wrk->lock);
	if (err != NOWDB_OK) return err;

	err = nowdb_queue_init(&wrk->jobqueue, 0, DELAY, drain);
	if (err != NOWDB_OK) {
		nowdb_lock_destroy(&wrk->lock);
		return err;
	}

	wrk->tasks = malloc(sizeof(nowdb_task_t)*wrk->pool);
	if (wrk->tasks == NULL) {
		nowdb_lock_destroy(&wrk->lock);
		nowdb_queue_destroy(&wrk->jobqueue);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, wrk->name,
		                                    "allocating tasks");
	}

	err = nowdb_lock(&wrk->lock);
	if (err != NOWDB_OK) {
		nowdb_lock_destroy(&wrk->lock);
		nowdb_queue_destroy(&wrk->jobqueue);
		free(wrk->tasks);
		return err;
	}

	/* from here on it's a mess. Hard to stop tasks
	 * with a non-initialised worker */
	for(uint32_t i=0;i<wrk->pool;i++) {
		err = nowdb_task_create(wrk->tasks+i, wrkentry, wrk);
		if (err != NOWDB_OK) return err;
	}
	err = nowdb_unlock(&wrk->lock);
	if (err != NOWDB_OK) return err;

	/* wait for running */
	return waitFor(wrk, wrk->pool, DELAY);
}

/* ------------------------------------------------------------------------
 * Helper: destroy worker
 * ------------------------------------------------------------------------
 */
static inline void destroy(nowdb_worker_t *wrk) {
	if (wrk == NULL) return;

	nowdb_queue_destroy(&wrk->jobqueue);
	nowdb_lock_destroy(&wrk->lock);

	if (wrk->tasks != NULL) {
		for(uint32_t i=0;i<wrk->pool;i++) {
			nowdb_task_destroy(wrk->tasks[i]);
		}
		free(wrk->tasks); wrk->tasks = NULL;
	}
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

	/* should we forbid sending empty messages ? */

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

	/* lock to check if we are already stopped */
	err = nowdb_lock(&wrk->lock);
	if (err != NOWDB_OK) return err;

	/* if the worker is already stopped,
	 * there is nothing to do */
	if (wrk->running == 0) {
		return nowdb_unlock(&wrk->lock);
	}

	/* unlock */
	err = nowdb_unlock(&wrk->lock);
	if (err != NOWDB_OK) return err;

	/* send stop message */
	for(uint32_t i=0;i<wrk->pool;i++) {
		err = nowdb_queue_enqueuePrio(&wrk->jobqueue, &stopmsg);
		if (err != NOWDB_OK) return err;
	}

	/* wait for stop */
	err = waitFor(wrk, 0, tmo);
	if (err != NOWDB_OK) return err;

	/* wait for threads to terminate */
	for(uint32_t i=0;i<wrk->pool;i++) {
		err = nowdb_task_join(wrk->tasks[i]);
		if (err != NOWDB_OK) return err;
	}

	/* stop queue */
	err = nowdb_queue_shutdown(&wrk->jobqueue);
	if (err != NOWDB_OK) return err;

	/* destroy everything */
	destroy(wrk); return NOWDB_OK;
}
