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

/* ------------------------------------------------------------------------
 * Message to send to the worker
 * -------
 * 'type': can be freely selected by the user.
 *         the values 0-9, however, are reserved
 *         for internal use.
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t       type; // message type
	void        *stcont; // static content (never freed)
	void          *cont; // dynamic content
} nowdb_wrk_message_t;

#define NOWDB_WRK_STOP  0
#define NOWDB_WRK_USER 10

/* ------------------------------------------------------------------------
 * Worker
 * ------
 * Workers block on a queue until a message is found or until a timeout,
 * called 'period' here, expires.
 *
 * Workers may be periodic or aperiodic.
 * Periodic workers perform their job every 'period' nanoseconds.
 *          They also perform their job, when they receive a message.
 *          (This means that periodic workers must be able to perform
 *           their job without a message!)
 * Aperiodic workers by contrast perform their job
 *           only when a message is sent.
 *
 * Workers consist of one or more tasks. 'pool' defines how many tasks
 * this specific worker will have. The tasks of a worker read the queue
 * concurrently. The application has to make sure that shared resources
 * are protected appropriately.
 *
 * 'rsc' is an additional resource that can be used
 * by the user-defined job.
 *
 * 'errqueue' may be NULL. In that case, errors are announced on
 * stderr. Otherwise, errors are sent to that queue (which shall
 * be initialised and opened beforehand). 
 * If an error occurs while writing to the error queue,
 * errors (including that error) are announced on stderr.
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_lock_t         lock; /* exclusive lock                 */
	char              name[8]; /* worker name                    */
        uint32_t             pool; /* how many tasks are in the pool */
	nowdb_time_t       period; /* periodic workers               */
	nowdb_task_t       *tasks; /* tasks running the worker       */
	void                 *job; /* user-defined callback          */
	nowdb_queue_t    jobqueue; /* queue where jobs arrive        */
	nowdb_queue_t   *errqueue; /* queue where to write errors    */
	void                 *rsc; /* additional resources           */
	uint32_t          running; /* how many tasks are running     */
} nowdb_worker_t;

/* ------------------------------------------------------------------------
 * Job to be performed by a worker
 * ------------------------------------------------------------------------
 */
typedef nowdb_err_t (*nowdb_job_t)(nowdb_worker_t      *wrk,  /* the worker */
                                   uint32_t              id,  /* task id    */
                                   nowdb_wrk_message_t *msg); /* message    */

/* ------------------------------------------------------------------------
 * Allocate and initialise a worker
 * --------     ----------
 * NOTE: the drain callback is passed to the jobqueue.
 *       all messages are destroyed using drain
 *       (except: internal messages!!!)
 *       If you don't want the worker to manage the memory
 *       you should pass a 'drain' that does nothing.
 *       Otherwise, you need to destroy your content and
 *       the user data (cont) itself.                ---
 *       The drain method shall ignore messages with type < 10!
 *
 * NOTE: The worker is immediately available,
 *       when the function has returned.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_worker_new(nowdb_worker_t      **wrk,
                             char                *name,
                             uint32_t             pool,
                             nowdb_time_t       period,
                             nowdb_job_t           job,
                             nowdb_queue_t   *errqueue,
	                     nowdb_queue_drain_t drain,
                             void                *rsc);

/* ------------------------------------------------------------------------
 * Initialise an already allocated worker.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_worker_init(nowdb_worker_t       *wrk,
                              char                *name,
                              uint32_t             pool,
                              nowdb_time_t       period,
                              nowdb_job_t           job,
                              nowdb_queue_t   *errqueue,
	                      nowdb_queue_drain_t drain,
                              void                *rsc);

/* ------------------------------------------------------------------------
 * Stops and destroys all resources in use by the worker
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_worker_stop(nowdb_worker_t *wrk,
                              nowdb_time_t    tmo);

/* ------------------------------------------------------------------------
 * Do your job.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_worker_do(nowdb_worker_t      *wrk,
                            nowdb_wrk_message_t *msg);
#endif
