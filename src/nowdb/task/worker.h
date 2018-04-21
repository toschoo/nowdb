/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Worker: worker thread waiting for jobs through a queue
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_worker_decl
#define nowdb_worker_decl

#include <nowdb/types/error.h>
#include <nowdb/types/time.h>
#include <nowdb/task/lock.h>
#include <nowdb/task/task.h>
#include <nowdb/task/queue.h>

typedef struct {
	uint32_t       type;
	void          *cont;
} nowdb_wrk_message_t;

#define NOWDB_WRK_STOP 0

typedef struct {
	nowdb_lock_t         lock; /* exclusive lock              */
	char              name[8]; /* worker name                 */
	nowdb_time_t       period; /* periodic workers            */
	nowdb_task_t         task; /* task running the worker     */
	void                 *job; /* user-defined callback       */
	nowdb_queue_t    jobqueue; /* queue where jobs arrive     */
	nowdb_queue_t   *errqueue; /* queue where to write errors */
	void                 *rsc; /* additional resources        */
	char                state; /* running or stopped          */
} nowdb_worker_t;

#define NOWDB_WRK_STOPPED 0
#define NOWDB_WRK_RUNNING 1

typedef nowdb_err_t (*nowdb_job_t)(nowdb_worker_t      *wrk,
                                   nowdb_wrk_message_t *msg);

nowdb_err_t nowdb_worker_init(nowdb_worker_t       *wrk,
                              char                *name,
                              nowdb_time_t       period,
                              nowdb_job_t           job,
                              nowdb_queue_t   *errqueue,
	                      nowdb_queue_drain_t drain,
                              void                *rsc);

void nowdb_worker_destroy(nowdb_worker_t *wrk);

nowdb_err_t nowdb_worker_do(nowdb_worker_t      *wrk,
                            nowdb_wrk_message_t *msg);

nowdb_err_t nowdb_worker_stop(nowdb_worker_t *wrk,
                              nowdb_time_t    tmo);
#endif
