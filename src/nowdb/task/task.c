/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Task: wrapper for simple thread services
 * ========================================================================
 */
#include <nowdb/types/error.h>
#include <nowdb/types/time.h>
#include <nowdb/task/task.h>

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <errno.h>

static char *OBJECT = "task";

/* ------------------------------------------------------------------------
 * Create a task
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_task_create(nowdb_task_t       *task,
                              nowdb_task_entry_t entry,
                              void               *args) 
{
	int x = pthread_create(task, NULL, entry, args);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_thread, x, OBJECT,
		                           "cannot create pthread");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy task after it has terminated
 * ------------------------------------------------------------------------
 */
void nowdb_task_destroy(nowdb_task_t task) {
	/* since we join before,
	 * this is not necessary! */
	/* pthread_detach(task); */
}

/* ------------------------------------------------------------------------
 * Join task
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_task_join(nowdb_task_t task) {
	int x = pthread_join(task, NULL);
	if (x != 0) return nowdb_err_getRC(nowdb_err_busy, x, OBJECT,
	                                       "cannot join pthread");
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * My task
 * ------------------------------------------------------------------------
 */
nowdb_task_t nowdb_task_myself() {
	return pthread_self();
}

/* ------------------------------------------------------------------------
 * Sleep for n nanoseconds
 * ------------------------------------------------------------------------
 */
#define NPERSEC 1000000000
nowdb_err_t nowdb_task_sleep(nowdb_time_t delay) {
	struct timespec ts, rem;

	if (delay < 0) return nowdb_err_get(nowdb_err_invalid,
	                      FALSE, OBJECT, "negative delay");
	if (delay == 0) return NOWDB_OK;

	ts.tv_sec = delay / NPERSEC;
	ts.tv_nsec = delay - ts.tv_sec * NPERSEC;

	rem.tv_sec = 0;
	rem.tv_nsec = 0;

	while (nanosleep(&ts, &rem) != 0) {
		if (errno != EINTR) {
			return nowdb_err_get(nowdb_err_thread, TRUE, OBJECT,
			                                "nanosleep failed");
		}
		if (rem.tv_sec > 0 || rem.tv_nsec > 0) {
			ts.tv_sec = rem.tv_sec;
			ts.tv_nsec = rem.tv_nsec;
			rem.tv_sec = 0;
			rem.tv_nsec = 0;
			continue;
		}
		break;
	}
	return NOWDB_OK;
}
