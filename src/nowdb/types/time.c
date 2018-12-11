/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Time provides time and date data types and services
 * ========================================================================
 */
#include <nowdb/types/time.h>
#include <limits.h>
#include <stdio.h>

/* ------------------------------------------------------------------------
 * Thats me
 * ------------------------------------------------------------------------
 */
#define OBJECT "time"

/* ------------------------------------------------------------------------
 * Important constants
 * ------------------------------------------------------------------------
 */
static int64_t max_unix_sec = sizeof(time_t) == 4?INT_MAX:LONG_MAX;
static int64_t min_unix_sec = sizeof(time_t) == 4?INT_MIN:LONG_MIN;

#define NANOPERSEC 1000000000l

/* ------------------------------------------------------------------------
 * Unit
 * ------------------------------------------------------------------------
 */
static int64_t persec = 1000000000l;

/* ------------------------------------------------------------------------
 * The epoch
 * ------------------------------------------------------------------------
 */
static int64_t nowdb_epoch = 0;

/* ------------------------------------------------------------------------
 * Set the epoch
 * ------------------------------------------------------------------------
 */
void nowdb_time_setEpoch(int64_t epoch) {
	nowdb_epoch = epoch;
}

/* ------------------------------------------------------------------------
 * Get the epoch
 * ------------------------------------------------------------------------
 */
int64_t nowdb_time_getEpoch() {
	return nowdb_epoch;
}

/* ------------------------------------------------------------------------
 * Set units per second
 * ------------------------------------------------------------------------
 */
void nowdb_time_setPerSec(int64_t unit) {
	persec = unit;
}

/* ------------------------------------------------------------------------
 * Get units per second
 * ------------------------------------------------------------------------
 */
int64_t nowdb_time_getPerSec() {
	return persec;
}

/* ------------------------------------------------------------------------
 * Helper to treat negative values uniformly, in particular
 * if the nanoseconds are negative,
 * the seconds are decremented by 1
 * and nanoseconds are set to one second + nanoseconds, i.e.
 * second - abs(nanosecond).
 * ------------------------------------------------------------------------
 */
static inline void normalize(nowdb_system_time_t *tm) {
	if (tm->tv_nsec < 0) {
		tm->tv_sec -= 1;
		tm->tv_nsec = NANOPERSEC + tm->tv_nsec;
	}
}

/* ------------------------------------------------------------------------
 * Convert UNIX system time to nowdb time (helper)
 * ------------------------------------------------------------------------
 */
static inline void fromSystem(const nowdb_system_time_t *tm,
                                    nowdb_time_t      *time) {
	nowdb_system_time_t tmp;
	memcpy(&tmp, tm, sizeof(nowdb_system_time_t));
	normalize(&tmp);
	*time = tmp.tv_sec;
	*time *= persec;
	*time += tmp.tv_nsec/(NANOPERSEC/persec);
	*time += nowdb_epoch;
}

/* ------------------------------------------------------------------------
 * Convert UNIX system time to nowdb time
 * ------------------------------------------------------------------------
 */
void nowdb_time_fromSystem(const nowdb_system_time_t *tm,
                                 nowdb_time_t      *time) {
	fromSystem(tm,time);
}

/* ------------------------------------------------------------------------
 * Convert nowdb time to UNIX system time (helper)
 * ------------------------------------------------------------------------
 */
static inline int toSystem(nowdb_time_t       time,
                           nowdb_system_time_t *tm) {
	nowdb_time_t t = time - nowdb_epoch;
	nowdb_time_t tmp = t / persec;
	if (tmp > max_unix_sec || tmp < min_unix_sec) {
		return (int)nowdb_err_too_big;
	}
	tm->tv_sec = (time_t)tmp;
	tm->tv_nsec = (long)(t - tmp*persec) * (NANOPERSEC/persec);
	normalize(tm);
	return 0;
}

/* ------------------------------------------------------------------------
 * Convert nowdb time to UNIX system time
 * ------------------------------------------------------------------------
 */
int nowdb_time_toSystem(nowdb_time_t       time,
                        nowdb_system_time_t *tm) {
	return toSystem(time,tm);
}

/* ------------------------------------------------------------------------
 * Get current system time as nowdb time
 * ------------------------------------------------------------------------
 */
int nowdb_time_now(nowdb_time_t *time) {
	nowdb_system_time_t tm;

	if (clock_gettime(CLOCK_REALTIME, &tm) != 0) {
		return (int)nowdb_err_time;
	}
	nowdb_time_fromSystem(&tm, time);
	return 0;
}

/* ------------------------------------------------------------------------
 * Get time from string
 * ------------------------------------------------------------------------
 */
int nowdb_time_fromString(const char *buf,
                          const char *frm,
                          nowdb_time_t *t) {
	struct tm tm;
	char  *nsecs, *hlp;
	struct timespec tv;

	memset(&tm, 0, sizeof(struct tm));
	memset(&tv, 0, sizeof(struct timespec));

	nsecs = strptime(buf, frm, &tm);
	if (nsecs == NULL || (*nsecs != '.' && *nsecs != 0)) {
		return (int)nowdb_err_time;
	}

	tv.tv_sec = timegm(&tm);

	if (nsecs != NULL && *nsecs != 0) {
		nsecs++;

		tv.tv_nsec = strtol(nsecs, &hlp, 10);
		if (hlp == NULL || *hlp != 0) {
			return (int)nowdb_err_time;
		}
		switch(strlen(nsecs)) {
		case 1: tv.tv_nsec *= 100000000; break;
		case 2: tv.tv_nsec *= 10000000; break;
		case 3: tv.tv_nsec *= 1000000; break;
		case 4: tv.tv_nsec *= 100000; break;
		case 5: tv.tv_nsec *= 10000; break;
		case 6: tv.tv_nsec *= 1000; break;
		case 7: tv.tv_nsec *= 100; break;
		case 8: tv.tv_nsec *= 10; break;
		}
	}
	fromSystem(&tv, t);
	return 0;
}

/* ------------------------------------------------------------------------
 * nowdb time to broken down time
 * ------------------------------------------------------------------------
 */
int nowdb_time_break(nowdb_time_t t,
                     struct tm  *tm) {
	int err;
	struct timespec tv;

	memset(&tv, 0, sizeof(struct timespec));

	err = toSystem(t, &tv);
	if (err != 0) return err;

	if (gmtime_r(&tv.tv_sec, tm) == NULL) {
		return (int)nowdb_err_time;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * Write time to string
 * ------------------------------------------------------------------------
 */
int nowdb_time_toString(nowdb_time_t  t,
                        const char *frm,
                              char *buf,
                             size_t max) {
	int err;
	struct tm tm;
	struct timespec tv;
	size_t sz;

	memset(&tv, 0, sizeof(struct timespec));

	err = toSystem(t, &tv);
	if (err != 0) return err;

	if (gmtime_r(&tv.tv_sec, &tm) == NULL) {
		return (int)nowdb_err_time;
	}
	sz = strftime(buf, max, frm, &tm);
	if (sz == 0) {
		return (int)nowdb_err_too_big;
	}
	if (tv.tv_nsec > 0 && sz+11<max) {
		sprintf(buf+sz, ".%09ld", tv.tv_nsec);
	} else if (tv.tv_nsec > 0) {
		return (int)nowdb_err_too_big;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * Nanoseconds according to unit
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_nano() {
	return persec/NANOPERSEC;
}

/* ------------------------------------------------------------------------
 * Nanoseconds according to unit
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_micro() {
	return (1000*persec)/NANOPERSEC;
}

/* ------------------------------------------------------------------------
 * Milliseconds according to unit
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_milli() {
	return (1000000*persec)/NANOPERSEC;
}

/* ------------------------------------------------------------------------
 * Seconds according to unit
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_sec() {
	return persec;
}

/* ------------------------------------------------------------------------
 * Minutes according to unit
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_min() {
	return (60*nowdb_time_sec());
}

/* ------------------------------------------------------------------------
 * Hours according to unit
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_hour() {
	return (3600*nowdb_time_sec());
}

/* ------------------------------------------------------------------------
 * Days according to unit
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_day() {
	return (24*3600*nowdb_time_sec());
}

/* ------------------------------------------------------------------------
 * Weeks according to unit
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_week() {
	return (7*24*3600*nowdb_time_sec());
}

/* ------------------------------------------------------------------------
 * Benchmarking: get monotonic timestamp
 * ------------------------------------------------------------------------
 */
void nowdb_timestamp(struct timespec *t) {
	clock_gettime(CLOCK_MONOTONIC, t);
}

/* ------------------------------------------------------------------------
 * Benchmarking: subtract two timespecs
 * ------------------------------------------------------------------------
 */
#define NPERSEC 1000000000
uint64_t nowdb_time_minus(struct timespec *t1,
                          struct timespec *t2) {
	uint64_t d;
	if (t2->tv_nsec > t1->tv_nsec) {
		t1->tv_nsec += NPERSEC;
		t1->tv_sec  -= 1;
	}
	d = NPERSEC * (t1->tv_sec - t2->tv_sec);
	d += t1->tv_nsec - t2->tv_nsec;
	return d;
}
