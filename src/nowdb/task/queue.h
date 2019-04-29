/* ========================================================================
 * (c) Tobias Schoofs, 2018 -- 2019
 * ========================================================================
 * Queue: inter-thread communication
 * ========================================================================
 */
#ifndef nowdb_queue_decl
#define nowdb_queue_decl

#include <nowdb/types/error.h>
#include <nowdb/types/time.h>
#include <nowdb/task/lock.h>

#include <tsalgo/list.h>
#include <stdint.h>
#include <pthread.h>
#include <signal.h>

/* ------------------------------------------------------------------------
 * Queue is infinite
 * ------------------------------------------------------------------------
 */
#define NOWDB_QUEUE_INF 0

/* ------------------------------------------------------------------------
 * User defined callback to destroy messages before destroying the queue
 * ------------------------------------------------------------------------
 */
typedef void (*nowdb_queue_drain_t)(void **message);

/* ------------------------------------------------------------------------
 * Queue
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_lock_t         lock;  // Exclusive lock
	nowdb_bool_t       closed;  // closed for enqueue
	ts_algo_list_t       list;  // the queue content
	int                   max;  // max messages
	nowdb_time_t        delay;  // delay between checking the queue
	nowdb_queue_drain_t drain;  // callback to free messages
	sigset_t              six;  // blocking and waiting for
	ts_algo_list_t    readers;  // currently reading
} nowdb_queue_t;

/* ------------------------------------------------------------------------
 * Initialise the queue
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_init(nowdb_queue_t *q, int max,
                             nowdb_time_t        delay,
                             nowdb_queue_drain_t drain);

/* ------------------------------------------------------------------------
 * Destroy the queue
 * ------------------------------------------------------------------------
 */
void nowdb_queue_destroy(nowdb_queue_t *q);

/* ------------------------------------------------------------------------
 * Shutdown
 * --------
 * close and drain the queue
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_shutdown(nowdb_queue_t *q);

/* ------------------------------------------------------------------------
 * Open the queue after it was closed.
 * The function has no effect, when the queue was already open.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_open(nowdb_queue_t *q);

/* ------------------------------------------------------------------------
 * Closes the queue for 'enqueue'
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_close(nowdb_queue_t *q);

/* ------------------------------------------------------------------------
 * Writes 'message' to the end of the queue (last in / last out)
 * If the queue is already full (contains more than max messages),
 * the calling thread blocks and uses the delay parameter, which
 * was passed in on initialising, to schedule look-ups.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_enqueue(nowdb_queue_t *q, void *message);

/* ------------------------------------------------------------------------
 * Writes 'message' to the head of the queue (last in / first out)
 * The message is written even when the queue is full.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_enqueuePrio(nowdb_queue_t *q, void *message);

/* ------------------------------------------------------------------------
 * Removes the 'message' at the head of the queue
 * If there are no messages in the queue,
 * the calling thread blocks according to tmo:
 * tmo < 0: blocks forever
 * tmo = 0: returns immediately
 * tmo > 0: blocks tmo nanoseconds
 * dequeue does not use the delay parameter,
 * but uses the timout directly to perform a sigtimedwait,
 * waiting for the signal SIGUSR1. This signal shall therefore be
 * blocked by threads reading from a queue.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_dequeue(nowdb_queue_t *q, 
                                nowdb_time_t tmo,
                                void  **message);

/* ------------------------------------------------------------------------
 * Removes all messages from the queue
 * calling 'drain' on each message.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_queue_drain(nowdb_queue_t *q);

#endif
