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
	if (pthread_create(task, NULL, entry, args) != 0) {
		return nowdb_err_get(nowdb_err_thread, TRUE, OBJECT,
		                           "cannot create pthread");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy task after it has terminated
 * ------------------------------------------------------------------------
 */
void nowdb_task_destroy(nowdb_task_t task) {
	pthread_detach(task);
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
