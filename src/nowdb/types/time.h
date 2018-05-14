/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Time provides time and date data types and services
 * ========================================================================
 */
#ifndef nowdb_time_decl
#define nowdb_time_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>

#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <limits.h>

/* ------------------------------------------------------------------------
 * The earliest and latest possible time points.
 * The concrete meaning depends on epoch and units.
 * For the UNIX epoch with nanosecond unit, 
 *         DAWN is in the year 1677
 *         DUSK is in the year 2262
 * ------------------------------------------------------------------------
 */
#define NOWDB_TIME_DAWN (nowdb_time_t)(LONG_MIN)
#define NOWDB_TIME_DUSK (nowdb_time_t)(LONG_MAX)

/* ------------------------------------------------------------------------
 * System time according to POSIX.
 * ------------------------------------------------------------------------
 */
typedef struct timespec nowdb_system_time_t;

/* ------------------------------------------------------------------------
 * Set the Epoch
 * -------------
 * By default, nowdb uses the UNIX epoch (01/01/1970).
 * A different epoch may be defined relative to this epoch.
 * An epoch is defined as offset from the UNIX epoch.
 * If we wanted an epoch one second after the UNIX epoch, 
 * we would call:
 * nowdb_time_setEpoch(-1000000000);
 *
 * For the epoch that starts one second before the UNIX epoch:
 * nowdb_time_setEpoch(1000000000);
 * 
 * When converting from system time, the epoch difference is
 * added to the UNIX epoch. Therefore, with an epoch that starts later,
 * the difference must be subtracted.
 *
 * NOTE: The epoch to be set must be computed from the UNIX epoch, 
 *       that means that the epoch must be set back to the UNIX epoch,
 *       via nowdb_time_setEpoch(0), before it can be set again.
 *       Obviously, the unit (see below) must be set before
 *       the epoch is set.
 * ------------------------------------------------------------------------
 */
void nowdb_time_setEpoch(int64_t epoch);

/* ------------------------------------------------------------------------
 * Get the epoch.
 * Returns 0 for the UNIX epoch.
 * ------------------------------------------------------------------------
 */
int64_t nowdb_time_getEpoch();

/* ------------------------------------------------------------------------
 * Set the time unit
 * -----------------
 * The value is set as units per second.
 * By default one second has 1000000000 units;
 * the default unit, hence, is a nanosecond.
 * To set the unit to one second, call
 *     nowdb_time_setPerSec(1);
 * To set the unit to one millisecond, call
 *     nowdb_time_setPerSec(1000);
 * ------------------------------------------------------------------------
 */
void nowdb_time_setPerSec(int64_t unit);

/* ------------------------------------------------------------------------
 * Get the time unit
 * ------------------------------------------------------------------------
 */
int64_t nowdb_time_getPerSec();

/* ------------------------------------------------------------------------
 * Convert UNIX system time to nowdb time
 * ------------------------------------------------------------------------
 */
void nowdb_time_fromSystem(const nowdb_system_time_t *tm,
                                 nowdb_time_t      *time);

/* ------------------------------------------------------------------------
 * Convert nowdb time to UNIX system time
 * --------------------------------------
 * Errors:
 * - Time out of range
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_time_toSystem(nowdb_time_t       time,
                                nowdb_system_time_t *tm);

/* ------------------------------------------------------------------------
 * Standard periods according to unit
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_nano();
nowdb_time_t nowdb_time_micro();
nowdb_time_t nowdb_time_milli();
nowdb_time_t nowdb_time_sec();
nowdb_time_t nowdb_time_min();
nowdb_time_t nowdb_time_hour();
nowdb_time_t nowdb_time_day();
nowdb_time_t nowdb_time_week();

/* ------------------------------------------------------------------------
 * Get current system time as nowdb time
 * Errors:
 * - OS Error
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_time_now(nowdb_time_t *time);

/* ------------------------------------------------------------------------
 * Benchmarking: get monotonic timestamp
 * ------------------------------------------------------------------------
 */
void nowdb_timestamp(struct timespec *t);

/* ------------------------------------------------------------------------
 * Benchmarking: subtract two timespecs
 * ------------------------------------------------------------------------
 */
uint64_t nowdb_time_minus(struct timespec *t1,
                          struct timespec *t2);
#endif

