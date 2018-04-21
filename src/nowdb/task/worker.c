/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Worker: worker thread waiting for jobs through a queue
 * ========================================================================
 */
#include <nowdb/task/worker.h>
#include <stdio.h>

#define DELAY 50000000
#define MINOR 10000000

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

static void *wrkentry(void *p) {
	nowdb_worker_t  *wrk = p;
	nowdb_wrk_message_t *msg=NULL;
	nowdb_err_t     err;
	nowdb_job_t job;

	job = wrk->job;

	for(;;) {
		err = nowdb_queue_dequeue(&wrk->jobqueue,
		                           wrk->period,
		                           (void**)&msg);
		if (err != NOWDB_OK) {
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
		if (msg == NULL) continue;
		if (msg->type == NOWDB_WRK_STOP) {
			free(msg); break;
		}
		err = job(wrk, msg);
		if (err != NOWDB_OK) reportError(wrk, err);
		wrk->jobqueue.drain((void**)&msg); msg = NULL;
	}
	err = nowdb_lock(&wrk->lock);
	if (err != NOWDB_OK) reportError(wrk, err);
	wrk->state = NOWDB_WRK_STOPPED;
	err = nowdb_unlock(&wrk->lock);
	return NULL;
}

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

	err = nowdb_task_create(&wrk->task, wrkentry, wrk);
	if (err != NOWDB_OK) {
		nowdb_queue_destroy(&wrk->jobqueue);
		return err;
	}
	wrk->state = NOWDB_WRK_RUNNING;
	return NOWDB_OK;
}

void nowdb_worker_destroy(nowdb_worker_t *wrk) {
	if (wrk == NULL) return;
	nowdb_task_destroy(wrk->task);
	nowdb_queue_destroy(&wrk->jobqueue);
	nowdb_lock_destroy(&wrk->lock);
}

static inline nowdb_err_t waitForStop(nowdb_worker_t *wrk,
                                      nowdb_time_t    tmo) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_time_t t = tmo;

	for(;;) {
		err = nowdb_lock(&wrk->lock);
		if (err != NOWDB_OK) return err;
		
		if (wrk->state == NOWDB_WRK_STOPPED) {
			return nowdb_unlock(&wrk->lock);
		}
		err = nowdb_unlock(&wrk->lock);
		if (err != NOWDB_OK) return err;

		if (tmo == 0) return nowdb_err_get(nowdb_err_timeout,
		                             FALSE, wrk->name, NULL);
		if (tmo > 0 && t <= 0) return nowdb_err_get(nowdb_err_timeout,
		                                      FALSE, wrk->name, NULL);
		err = nowdb_task_sleep(MINOR);
		if (err != NOWDB_OK) return err;
		t -= MINOR;
	}
	return NOWDB_OK;
}

nowdb_err_t nowdb_worker_do(nowdb_worker_t      *wrk,
                            nowdb_wrk_message_t *msg) {
	nowdb_err_t err;
	if (wrk == NULL) return nowdb_err_get(nowdb_err_invalid,
	              FALSE, "worker", "worker object is NULL");
	err = nowdb_queue_enqueue(&wrk->jobqueue, msg);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

nowdb_err_t nowdb_worker_stop(nowdb_worker_t *wrk,
                              nowdb_time_t    tmo) {
	nowdb_wrk_message_t *msg;
	nowdb_err_t err, err2;

	if (wrk == NULL) return nowdb_err_get(nowdb_err_invalid,
	              FALSE, "worker", "worker object is NULL");

	err = nowdb_lock(&wrk->lock);
	if (err != NOWDB_OK) return err;

	if (wrk->state == NOWDB_WRK_STOPPED) {
		return nowdb_unlock(&wrk->lock);
	}

	msg = malloc(sizeof(nowdb_wrk_message_t));
	if (msg == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, wrk->name,
		                                   "allocate message");
		goto unlock;
	}

	msg->type = NOWDB_WRK_STOP;
	msg->cont = NULL;

	err = nowdb_queue_enqueuePrio(&wrk->jobqueue, msg);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock(&wrk->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	if (err != NOWDB_OK) return err;
	err = waitForStop(wrk,tmo);
	if (err != NOWDB_OK) return err;
	return nowdb_queue_shutdown(&wrk->jobqueue);
}

