/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Lock: mutual exclusion and read/write locks
 * ========================================================================
 */
#include <nowdb/task/lock.h>
#include <stdio.h>
#include <errno.h>

static char *OBJECT = "lock";

nowdb_err_t nowdb_lock_init(nowdb_lock_t *lock) {
	if (lock == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                              "lock is NULL");
	}
	int x = pthread_mutex_init(lock, NULL);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_lock, x, OBJECT,
		                     "pthread_mutex_init failed");
	}
	return NOWDB_OK;
}

nowdb_err_t nowdb_rwlock_init(nowdb_rwlock_t *lock) {
	if (lock == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                              "lock is NULL");
	}
	int x = pthread_rwlock_init(lock, NULL);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_lock, x, OBJECT,
		                    "pthread_rwlock_init failed");
	}
	return NOWDB_OK;
}

void nowdb_lock_destroy(nowdb_lock_t *lock) {
	if (lock == NULL) return;
	pthread_mutex_destroy(lock);
}

void nowdb_rwlock_destroy(nowdb_rwlock_t *lock) {
	if (lock == NULL) return;
	pthread_rwlock_destroy(lock);
}

nowdb_err_t nowdb_lock(nowdb_lock_t *lock) {
	if (lock == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                              "lock is NULL");
	}
	int x = pthread_mutex_lock(lock);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_lock, x, OBJECT,
		                     "pthread_mutex_lock failed");
	}
	return NOWDB_OK;
}

nowdb_err_t nowdb_unlock(nowdb_lock_t *lock) {
	if (lock == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                              "lock is NULL");
	}
	int x = pthread_mutex_unlock(lock);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_lock, x, OBJECT,
		                   "pthread_mutex_unlock failed");
	}
	return NOWDB_OK;
}

nowdb_err_t nowdb_lock_read(nowdb_rwlock_t *lock) {
	if (lock == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                              "lock is NULL");
	}
	int x = pthread_rwlock_rdlock(lock);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_lock, x, OBJECT,
		                  "pthread_rwlock_rdlock failed");
	}
	return NOWDB_OK;
}

nowdb_err_t nowdb_unlock_read(nowdb_rwlock_t *lock) {
	if (lock == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                              "lock is NULL");
	}
	int x = pthread_rwlock_unlock(lock);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_lock, x, OBJECT,
		                  "pthread_rwlock_unlock failed");
	}
	return NOWDB_OK;
}

nowdb_err_t nowdb_lock_write(nowdb_rwlock_t *lock) {
	if (lock == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                              "lock is NULL");
	}
	int x = pthread_rwlock_wrlock(lock);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_lock, x, OBJECT,
		                  "pthread_wrlock_wrlock failed");
	}
	return NOWDB_OK;
}

nowdb_err_t nowdb_unlock_write(nowdb_rwlock_t *lock) {
	if (lock == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                              "lock is NULL");
	}
	int x = pthread_rwlock_unlock(lock);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_lock, x, OBJECT,
		                  "pthread_wrlock_unlock failed");
	}
	return NOWDB_OK;
}

