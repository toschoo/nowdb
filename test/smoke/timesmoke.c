/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for time
 * ========================================================================
 */
#include <nowdb/types/time.h>
#include <time.h>
#include <stdio.h>
#include <limits.h>

/* -------------------------------------------------------------------------
 * Eric S. Raymond's implementation of
 * broken to seconds since Unix epoch
 * for platforms where timegm is not available.
 * -------------------------------------------------------------------------
 */
time_t raymond_timegm(struct tm * t)
{
	long year;
	time_t result;
#define MONTHSPERYEAR   12      /* months per calendar year */
	static const int cumdays[MONTHSPERYEAR] =
           { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };

	year = 1900 + t->tm_year + t->tm_mon / MONTHSPERYEAR;
	result = (year - 1970) * 365 + cumdays[t->tm_mon % MONTHSPERYEAR];
	result += (year - 1968) / 4;
	result -= (year - 1900) / 100;
	result += (year - 1600) / 400;
    	if ((year % 4) == 0     && 
	    ((year % 100) != 0  ||
	     (year % 400) == 0) &&
	    (t->tm_mon % MONTHSPERYEAR) < 2)
		result--;
	result += t->tm_mday - 1;
	result *= 24;
	result += t->tm_hour;
	result *= 60;
	result += t->tm_min;
	result *= 60;
	result += t->tm_sec;
	if (t->tm_isdst == 1) result -= 3600;
	return (result);
}

/* -------------------------------------------------------------------------
 * broken to nowdb_time_t
 * -------------------------------------------------------------------------
 */
nowdb_time_t frombroken(struct tm *broken) {
	nowdb_time_t t;
	time_t tt = timegm(broken);
	nowdb_system_time_t st;
	st.tv_sec = tt;
	st.tv_nsec = 0;
	nowdb_time_fromSystem(&st, &t);
	return t;
}

/* -------------------------------------------------------------------------
 * print nowdb_time_t using gmtime_r
 * -------------------------------------------------------------------------
 */
char *stringtime(nowdb_time_t t) {
	char *nice;
	struct tm broken;
	nowdb_err_t err;
	nowdb_system_time_t st;

	err = nowdb_time_toSystem(t, &st);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}

	gmtime_r(&st.tv_sec,&broken);

	nice = malloc(64);
	if (nice == NULL) return NULL;

	sprintf(nice, "%02u/%02u/%u %02u:%02u:%02u.%03lu",
			broken.tm_mon+1,
			broken.tm_mday,
			broken.tm_year+1900,
			broken.tm_hour,
			broken.tm_min,
			broken.tm_sec,
			st.tv_nsec/1000000);
	return nice;
}

/* -------------------------------------------------------------------------
 * just a simple experiment
 * -------------------------------------------------------------------------
 */
void findmaxunixdate() {
	uint64_t tmp=0;
	char *str;

	for (uint64_t k=1;;k<<=1) {
		str = stringtime((nowdb_time_t)k);
		if (str == NULL) break;
		tmp = k; free(str);
	}
	for (int64_t k=tmp;;k*=1.5) {
		str = stringtime(k);
		if (str == NULL) break;
		tmp = k; free(str);
	}
	for (int64_t k=tmp;;k*=1.1) {
		str = stringtime(k);
		if (str == NULL) break;
		tmp = k; free(str);
	}
	for (int64_t k=tmp;;k*=1.01) {
		str = stringtime(k);
		if (str == NULL) break;
		tmp = k; free(str);
	}
	for (int64_t k=tmp;;k*=1.001) {
		str = stringtime(k);
		if (str == NULL) break;
		tmp = k; free(str);
	}
	str = stringtime(tmp);
	if (str == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return;
	}
	fprintf(stderr, "MAX: %s (=%lu)\n", str, tmp);
	free(str); return;
}

/* -------------------------------------------------------------------------
 * Test NOWDB_TIME_DAWN and DUSK
 * -------------------------------------------------------------------------
 */
nowdb_bool_t checkMinMax() {

	fprintf(stderr, "MIN: %ld\n", LONG_MIN);
	fprintf(stderr, "MAX: %ld\n", LONG_MAX);

	char *str = stringtime(NOWDB_TIME_DAWN);
	if (str == NULL) {
		fprintf(stderr, "cannot convert dawn to system\n");
		return FALSE;
	}
	fprintf(stderr, "DAWN: %s\n", str); free(str);
	str = stringtime(NOWDB_TIME_DUSK);
	if (str == NULL) {
		fprintf(stderr, "cannot convert dusk to system\n");
		return FALSE;
	}
	fprintf(stderr, "DUSK: %s\n", str); free(str);

	if ((nowdb_time_t)(INT_MIN) < NOWDB_TIME_DAWN) {
		fprintf(stderr, "%d less than dawn\n", INT_MIN);
		return FALSE;
	}
	if ((nowdb_time_t)(-12) < NOWDB_TIME_DAWN) {
		fprintf(stderr, "-12 is less than dawn\n");
		return FALSE;
	}
	if ((nowdb_time_t)(-1) < NOWDB_TIME_DAWN) {
		fprintf(stderr, "0 is less than dawn\n");
		return FALSE;
	}
	if ((nowdb_time_t)(0) < NOWDB_TIME_DAWN) {
		fprintf(stderr, "0 is less than dawn\n");
		return FALSE;
	}
	if ((nowdb_time_t)1 < NOWDB_TIME_DAWN) {
		fprintf(stderr, "1 is less than dawn\n");
		return FALSE;
	}
	if ((nowdb_time_t)(12) < NOWDB_TIME_DAWN) {
		fprintf(stderr, "12 is less than dawn\n");
		return FALSE;
	}
	if ((nowdb_time_t)(INT_MAX) < NOWDB_TIME_DAWN) {
		fprintf(stderr, "%d less than dawn\n", INT_MIN);
		return FALSE;
	}
	if ((nowdb_time_t)(LONG_MIN) != NOWDB_TIME_DAWN) {
		fprintf(stderr, "%ld is not dawn\n", LONG_MIN);
		return FALSE;
	}
	if ((nowdb_time_t)(INT_MIN) > NOWDB_TIME_DUSK) {
		fprintf(stderr, "%d less than dawn\n", INT_MIN);
		return FALSE;
	}
	if ((nowdb_time_t)(-12) > NOWDB_TIME_DUSK) {
		fprintf(stderr, "-12 is less than dawn\n");
		return FALSE;
	}
	if ((nowdb_time_t)(-1) > NOWDB_TIME_DUSK) {
		fprintf(stderr, "0 is less than dawn\n");
		return FALSE;
	}
	if ((nowdb_time_t)(0) > NOWDB_TIME_DUSK) {
		fprintf(stderr, "0 is less than dawn\n");
		return FALSE;
	}
	if ((nowdb_time_t)1 > NOWDB_TIME_DUSK) {
		fprintf(stderr, "1 is less than dawn\n");
		return FALSE;
	}
	if ((nowdb_time_t)(12) > NOWDB_TIME_DUSK) {
		fprintf(stderr, "12 is less than dawn\n");
		return FALSE;
	}
	if ((nowdb_time_t)(INT_MAX) > NOWDB_TIME_DUSK) {
		fprintf(stderr, "%d less than dawn\n", INT_MIN);
		return FALSE;
	}
	if ((nowdb_time_t)(LONG_MAX) != NOWDB_TIME_DUSK) {
		fprintf(stderr, "%ld is not dusk\n", LONG_MAX);
		return FALSE;
	}
	return TRUE;
}

/* -------------------------------------------------------------------------
 * Test periods
 * -------------------------------------------------------------------------
 */
#define NPERSEC 1000000000
nowdb_bool_t checkPeriods() {

	fprintf(stderr, "check periods\n");

	/* 1. Default setting */
	nowdb_time_setPerSec(NPERSEC);
	if (nowdb_time_getPerSec() != NPERSEC) {
		fprintf(stderr, "persec != NPERSEC\n");
		return FALSE;
	}
	if (nowdb_time_nano() != 1) {
		fprintf(stderr, "nano is not 1\n");
		return FALSE;
	}
	if (nowdb_time_micro() != 1000) {
		fprintf(stderr, "micro is not 1000\n");
		return FALSE;
	}
	if (nowdb_time_milli() != 1000000) {
		fprintf(stderr, "milli is not 1000000\n");
		return FALSE;
	}
	if (nowdb_time_sec() != 1000000000) {
		fprintf(stderr, "sec is not 1000000000\n");
		return FALSE;
	}
	if (nowdb_time_min() != 60000000000) {
		fprintf(stderr, "minute is wrong\n");
		return FALSE;
	}
	if (nowdb_time_hour() != 3600000000000l) {
		fprintf(stderr, "hour is wrong\n");
		return FALSE;
	}
	if (nowdb_time_day() != 24*3600000000000l) {
		fprintf(stderr, "day is wrong\n");
		return FALSE;
	}
	if (nowdb_time_week() != 7*24*3600000000000l) {
		fprintf(stderr, "week is wrong\n");
		return FALSE;
	}
	/* 2. persec = micro */
	nowdb_time_setPerSec(1000000l);
	if (nowdb_time_getPerSec() != 1000000) {
		fprintf(stderr, "persec is wrong\n");
		return FALSE;
	}
	if (nowdb_time_nano() != 0) {
		fprintf(stderr, "nano is wrong\n");
		return FALSE;
	}
	if (nowdb_time_micro() != 1) {
		fprintf(stderr, "micro is wrong: %ld\n", nowdb_time_micro());
		return FALSE;
	}
	if (nowdb_time_milli() != 1000) {
		fprintf(stderr, "milli is wrong\n");
		return FALSE;
	}
	if (nowdb_time_sec() != 1000000) {
		fprintf(stderr, "sec is wrong\n");
		return FALSE;
	}
	if (nowdb_time_min() != 60000000) {
		fprintf(stderr, "minute is wrong\n");
		return FALSE;
	}
	if (nowdb_time_hour() != 3600000000l) {
		fprintf(stderr, "hour is wrong\n");
		return FALSE;
	}
	if (nowdb_time_day() != 24*3600000000l) {
		fprintf(stderr, "day is wrong\n");
		return FALSE;
	}
	if (nowdb_time_week() != 7*24*3600000000l) {
		fprintf(stderr, "week is wrong\n");
		return FALSE;
	}
	/* 3. persec = milli */
	nowdb_time_setPerSec(1000);
	if (nowdb_time_getPerSec() != 1000) {
		fprintf(stderr, "persec is wrong\n");
		return FALSE;
	}
	if (nowdb_time_nano() != 0) {
		fprintf(stderr, "nano is wrong\n");
		return FALSE;
	}
	if (nowdb_time_micro() != 0) {
		fprintf(stderr, "micro is wrong: %ld\n", nowdb_time_micro());
		return FALSE;
	}
	if (nowdb_time_milli() != 1) {
		fprintf(stderr, "milli is wrong\n");
		return FALSE;
	}
	if (nowdb_time_sec() != 1000) {
		fprintf(stderr, "sec is wrong\n");
		return FALSE;
	}
	if (nowdb_time_min() != 60000) {
		fprintf(stderr, "minute is wrong\n");
		return FALSE;
	}
	if (nowdb_time_hour() != 3600000l) {
		fprintf(stderr, "hour is wrong\n");
		return FALSE;
	}
	if (nowdb_time_day() != 24*3600000l) {
		fprintf(stderr, "day is wrong\n");
		return FALSE;
	}
	if (nowdb_time_week() != 7*24*3600000l) {
		fprintf(stderr, "week is wrong\n");
		return FALSE;
	}
	/* 4. persec = sec */
	nowdb_time_setPerSec(1);
	if (nowdb_time_getPerSec() != 1) {
		fprintf(stderr, "persec is wrong\n");
		return FALSE;
	}
	if (nowdb_time_nano() != 0) {
		fprintf(stderr, "nano is wrong\n");
		return FALSE;
	}
	if (nowdb_time_micro() != 0) {
		fprintf(stderr, "micro is wrong: %ld\n", nowdb_time_micro());
		return FALSE;
	}
	if (nowdb_time_milli() != 0) {
		fprintf(stderr, "milli is wrong\n");
		return FALSE;
	}
	if (nowdb_time_sec() != 1) {
		fprintf(stderr, "sec is wrong\n");
		return FALSE;
	}
	if (nowdb_time_min() != 60) {
		fprintf(stderr, "minute is wrong\n");
		return FALSE;
	}
	if (nowdb_time_hour() != 3600) {
		fprintf(stderr, "hour is wrong\n");
		return FALSE;
	}
	if (nowdb_time_day() != 24*3600l) {
		fprintf(stderr, "day is wrong\n");
		return FALSE;
	}
	if (nowdb_time_week() != 7*24*3600l) {
		fprintf(stderr, "week is wrong\n");
		return FALSE;
	}
	return TRUE;
}

/* -------------------------------------------------------------------------
 * Compute my personal epoch from broken
 * -------------------------------------------------------------------------
 */
nowdb_time_t myepoch() {
	struct tm broken;
	broken.tm_mon   = 4;
	broken.tm_mday  = 18;
	broken.tm_year  = 71;
	broken.tm_hour  = 0;
	broken.tm_min   = 0;
	broken.tm_sec   = 0;
	broken.tm_isdst = 0;
	broken.tm_gmtoff= 0;
	return frombroken(&broken);
}

/* -------------------------------------------------------------------------
 * Simple test using the standard unix epoch
 * Passes if convering 'now' to system and back is identical.
 * -------------------------------------------------------------------------
 */
nowdb_bool_t unixepochtest() {
	nowdb_err_t err;
	nowdb_time_t time1, time2;
	nowdb_system_time_t tm;

	fprintf(stderr, "UNIX Epoch Test\n");

	err = nowdb_time_now(&time1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	char *str = stringtime(time1);
	if (str == NULL) {
		fprintf(stderr, "no stringtime\n");
		return FALSE;
	}
	fprintf(stderr, "TEST: %s (%ld)\n", str, time1); free(str);

	err = nowdb_time_toSystem(time1, &tm);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_time_fromSystem(&tm, &time2);
	if (time1 != time2) {
		fprintf(stderr, "times differ\n");
		return FALSE;
	}
	return TRUE;
}

/* -------------------------------------------------------------------------
 * Simple test using the my personal epoch
 * Passes if convering 'now' to system and back is identical.
 * -------------------------------------------------------------------------
 */
nowdb_bool_t myepochtest() {
	nowdb_time_t mepoch;
	nowdb_err_t err;
	nowdb_time_t time1, time2;
	nowdb_system_time_t tm;

	fprintf(stderr, "My Epoch Test\n");

	nowdb_time_setPerSec(NPERSEC);
	nowdb_time_setEpoch(0);
 	mepoch = myepoch();

	char *str = stringtime(mepoch);
	if (str == NULL) return 0;
	fprintf(stderr, "EPOCH: %s\n", str); free(str);

	nowdb_time_setEpoch(-mepoch);

	err = nowdb_time_now(&time1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	str = stringtime(time1);
	if (str == NULL) {
		fprintf(stderr, "no stringtime\n");
		return FALSE;
	}
	fprintf(stderr, "TEST: %s (%ld)\n", str, time1); free(str);

	err = nowdb_time_toSystem(time1, &tm);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_time_fromSystem(&tm, &time2);
	if (time1 != time2) {
		fprintf(stderr, "times differ\n");
		return FALSE;
	}
	return TRUE;
}

/* -------------------------------------------------------------------------
 * Simple test using the standard UNIX epoch and milliscond as unit.
 * Passes if convering 'now' to system and back is identical.
 * -------------------------------------------------------------------------
 */
nowdb_bool_t myunittest() {
	nowdb_time_t mepoch = 0;
	nowdb_err_t err;
	nowdb_time_t time1, time2;
	nowdb_system_time_t tm;

	fprintf(stderr, "My Unit Test\n");

	nowdb_time_setPerSec(NPERSEC);
	nowdb_time_setEpoch(0);

	char *str = stringtime(mepoch);
	if (str == NULL) return 0;
	fprintf(stderr, "EPOCH: %s\n", str); free(str);

	nowdb_time_setPerSec(1000);
	nowdb_time_setEpoch(0);

	err = nowdb_time_now(&time1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	str = stringtime(time1);
	if (str == NULL) {
		fprintf(stderr, "no stringtime\n");
		return FALSE;
	}
	fprintf(stderr, "TEST: %s (%ld)\n", str, time1); free(str);

	err = nowdb_time_toSystem(time1, &tm);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_time_fromSystem(&tm, &time2);
	if (time1 != time2) {
		fprintf(stderr, "times differ: %ld -- %lu \n", time1, time2);
		return FALSE;
	}
	return TRUE;
}

/* -------------------------------------------------------------------------
 * Simple test using my personal epoch and milliscond as unit.
 * Passes if convering 'now' to system and back is identical.
 * -------------------------------------------------------------------------
 */
nowdb_bool_t myepochunittest() {
	nowdb_time_t mepoch = 0;
	nowdb_err_t err;
	nowdb_time_t time1, time2;
	nowdb_system_time_t tm;

	fprintf(stderr, "My Epoch & Unit Test\n");

	nowdb_time_setPerSec(NPERSEC);
	nowdb_time_setPerSec(1000);

	mepoch = myepoch();

	char *str = stringtime(mepoch);
	if (str == NULL) return 0;
	fprintf(stderr, "EPOCH: %s\n", str); free(str);

	nowdb_time_setEpoch(-mepoch);

	err = nowdb_time_now(&time1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	str = stringtime(time1);
	if (str == NULL) {
		fprintf(stderr, "no stringtime\n");
		return FALSE;
	}
	fprintf(stderr, "TEST: %s (%ld)\n", str, time1); free(str);

	err = nowdb_time_toSystem(time1, &tm);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_time_fromSystem(&tm, &time2);
	if (time1 != time2) {
		fprintf(stderr, "times differ: %ld -- %lu \n", time1, time2);
		return FALSE;
	}
	return TRUE;
}

/* -------------------------------------------------------------------------
 * Compare twoo broken down times
 * -------------------------------------------------------------------------
 */
nowdb_bool_t comparebroken(struct tm *broken1,
                           struct tm *broken2) {
	if (broken1->tm_year!= broken2->tm_year) {
		fprintf(stderr, "years differ\n");
		return FALSE;
	}
	if (broken1->tm_mon != broken2->tm_mon) {
		fprintf(stderr, "months differ\n");
		return FALSE;
	}
	if (broken1->tm_mday!= broken2->tm_mday) {
		fprintf(stderr, "days differ\n");
		return FALSE;
	}
	if (broken1->tm_hour != broken2->tm_hour) {
		fprintf(stderr, "hours differ\n");
		return FALSE;
	}
	if (broken1->tm_min  != broken2->tm_min) {
		fprintf(stderr, "minutes differ\n");
		return FALSE;
	}
	if (broken1->tm_sec  != broken2->tm_sec) {
		fprintf(stderr, "seconds differ\n");
		return FALSE;
	}
	return TRUE;
}

/* -------------------------------------------------------------------------
 * Reveives a broken, converts it to and from system.
 * Passes if the result equals the input.
 * -------------------------------------------------------------------------
 */
nowdb_bool_t testbroken(struct tm *broken1) {
	nowdb_err_t err;
	nowdb_time_t time1, time2;
	nowdb_system_time_t st;
	struct tm broken2;

	time1 = frombroken(broken1);
	char *str = stringtime(time1);
	if (str == NULL) {
		fprintf(stderr, "no stringtime\n");
		return FALSE;
	}
	fprintf(stderr, "TEST : %s (%ld)\n", str, time1); free(str);

	err = nowdb_time_toSystem(time1, &st);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_time_fromSystem(&st, &time2);
	if (time1 != time2) {
		fprintf(stderr, "times differ: %ld -- %lu \n", time1, time2);
		return FALSE;
	}
	gmtime_r(&st.tv_sec,&broken2);
	if (!comparebroken(broken1, &broken2)) return FALSE;
	return TRUE;
}

/* -------------------------------------------------------------------------
 * Manual tests using broken with different epochs and units.
 * -------------------------------------------------------------------------
 */
nowdb_bool_t manualtests() {
	nowdb_time_t mepoch = 0;
	struct tm broken;

	fprintf(stderr, "Manual Tests\n");

	nowdb_time_setPerSec(NPERSEC);
	nowdb_time_setEpoch(0);
	mepoch = nowdb_time_getEpoch();

	char *str = stringtime(mepoch);
	if (str == NULL) return 0;
	fprintf(stderr, "EPOCH: %s\n", str); free(str);

	broken.tm_mon   = 4;
	broken.tm_mday  = 18;
	broken.tm_year  = 71;
	broken.tm_hour  = 0;
	broken.tm_min   = 0;
	broken.tm_sec   = 0;
	broken.tm_isdst = 0;
	broken.tm_gmtoff= 0;
	if (!testbroken(&broken)) return FALSE;

	nowdb_time_setPerSec(NPERSEC);
	mepoch = myepoch();
	nowdb_time_setEpoch(-mepoch);
	if (!testbroken(&broken)) return FALSE;

	nowdb_time_setPerSec(1000000);
	nowdb_time_setEpoch(0);
	if (!testbroken(&broken)) return FALSE;

	mepoch = myepoch();
	nowdb_time_setEpoch(-mepoch);
	if (!testbroken(&broken)) return FALSE;

	nowdb_time_setPerSec(1000);
	nowdb_time_setEpoch(0);
	if (!testbroken(&broken)) return FALSE;

	nowdb_time_setPerSec(1000);
	mepoch = myepoch();
	nowdb_time_setEpoch(-mepoch);
	if (!testbroken(&broken)) return FALSE;

	broken.tm_mon   = 0;
	broken.tm_mday  = 1;
	broken.tm_year  = 70;
	broken.tm_hour  = 0;
	broken.tm_min   = 0;
	broken.tm_sec   = 0;
	broken.tm_isdst = 0;
	broken.tm_gmtoff= 0;

	nowdb_time_setPerSec(NPERSEC);
	nowdb_time_setEpoch(0);
	if (!testbroken(&broken)) return FALSE;

	nowdb_time_setPerSec(NPERSEC);
	mepoch = myepoch();
	nowdb_time_setEpoch(-mepoch);
	if (!testbroken(&broken)) return FALSE;

	nowdb_time_setPerSec(1000000);
	nowdb_time_setEpoch(0);
	if (!testbroken(&broken)) return FALSE;

	mepoch = myepoch();
	nowdb_time_setEpoch(-mepoch);
	if (!testbroken(&broken)) return FALSE;

	nowdb_time_setPerSec(1000);
	nowdb_time_setEpoch(0);
	if (!testbroken(&broken)) return FALSE;

	nowdb_time_setPerSec(1000);
	mepoch = myepoch();
	nowdb_time_setEpoch(-mepoch);
	if (!testbroken(&broken)) return FALSE;

	return TRUE;
}

/* -------------------------------------------------------------------------
 * Random tests:
 * generates random brokens and tests them
 * with different epochs and units.
 * -------------------------------------------------------------------------
 */
nowdb_bool_t randombroken() {
	nowdb_time_t mepoch = 0;
	struct tm broken;

	fprintf(stderr, "Random Tests\n");

	int MONDAYS[12] = {
		32, 29, 32, 31, 32, 31, 32, 32, 31, 32, 31, 32};

	for(int i=0;i<100;i++) {
		nowdb_time_setPerSec(NPERSEC);
		nowdb_time_setEpoch(0);

		broken.tm_mon   = rand()%12;
		broken.tm_mday  = MONDAYS[broken.tm_mon];
		broken.tm_year  = rand()%200;
		broken.tm_hour  = rand()%24;
		broken.tm_min   = rand()%60;
		broken.tm_sec   = rand()%60;
		broken.tm_isdst = 0;
		broken.tm_gmtoff= 0;
		if (!testbroken(&broken)) return FALSE;

		nowdb_time_setPerSec(NPERSEC);
		mepoch = myepoch();
		nowdb_time_setEpoch(-mepoch);
		if (!testbroken(&broken)) return FALSE;

		nowdb_time_setPerSec(1000000);
		nowdb_time_setEpoch(0);
		if (!testbroken(&broken)) return FALSE;
	
		mepoch = myepoch();
		nowdb_time_setEpoch(-mepoch);
		if (!testbroken(&broken)) return FALSE;

		nowdb_time_setPerSec(1000);
		nowdb_time_setEpoch(0);
		if (!testbroken(&broken)) return FALSE;

		nowdb_time_setPerSec(1000);
		mepoch = myepoch();
		nowdb_time_setEpoch(-mepoch);
		if (!testbroken(&broken)) return FALSE;

		nowdb_time_setPerSec(1);
		nowdb_time_setEpoch(0);
		if (!testbroken(&broken)) return FALSE;

		nowdb_time_setPerSec(1);
		mepoch = myepoch();
		nowdb_time_setEpoch(-mepoch);
		if (!testbroken(&broken)) return FALSE;
	}
	return TRUE;
}

/* -------------------------------------------------------------------------
 * Random tests:
 * generates random nowdb_time_t converts them forth 
 * and back and compares.
 * -------------------------------------------------------------------------
 */
nowdb_bool_t randomtimeunix() {
	nowdb_err_t err;
	nowdb_time_t t1, t2=0;
	nowdb_system_time_t st;
	char *str;

	nowdb_time_setEpoch(0);
	nowdb_time_setPerSec(NPERSEC);

	for(int i=0;i<100;i++) {
		err = nowdb_time_now(&t1);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}

		int64_t d = rand()%50000;
		int64_t h = rand()%24;
		int64_t m = rand()%60;
		int64_t s = rand()%60;
		int64_t t = rand()%1000;
		int64_t u = rand()%1000000;
		int64_t n = rand()%NPERSEC;

		d *= nowdb_time_day();
		int b = rand()%2;
		if (b) t2 = t1 + d; else t2 = t1 - d;

		h *= nowdb_time_hour();
		b = rand()%2;
		t2 += b ? h : -h;

		m *= nowdb_time_min();
		b = rand()%2;
		t2 += b ? m : -m;

		s *= nowdb_time_sec();
		b = rand()%2;
		t2 += b ? s : -s;

		t *= nowdb_time_milli();
		b = rand()%2;
		t2 += b ? t : -t;

		u *= nowdb_time_micro();
		b = rand()%2;
		t2 += b ? u : -u;

		n *= nowdb_time_nano();
		b = rand()%2;
		t2 += b ? n : -n;

		str = stringtime(t2);
		if (str == NULL) {
			fprintf(stderr, "cannot get stringtime with %ld\n", t1);
			return FALSE;
		}
		fprintf(stderr, "%s (%ld)\n", str, t2); free(str);

		err = nowdb_time_toSystem(t2, &st);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		nowdb_time_fromSystem(&st, &t1);
		if (t1 != t2) {
			fprintf(stderr,
			        "representations differ %ld - %ld\n", t1, t2);
			str = stringtime(t1);
			if (str == NULL) {
				fprintf(stderr,
				"cannot get stringtime with %ld\n", t1);
				return FALSE;
			}
			fprintf(stderr, "%s\n", str); free(str);
			return FALSE;
		}
	}
	return TRUE;
}

/* -------------------------------------------------------------------------
 * Random tests:
 * generates random nowdb_time_t converts them forth 
 * and back with different units and units and compares.
 * -------------------------------------------------------------------------
 */
nowdb_bool_t randomtime() {
	nowdb_time_t mepoch;
	nowdb_err_t err;
	nowdb_time_t t1, t2=0;
	nowdb_system_time_t st;
	nowdb_time_setEpoch(0);

	for(nowdb_time_t unit = NPERSEC; unit>0; unit/=1000) {

		nowdb_time_setPerSec(unit);

		fprintf(stderr, "UNIT: %ld\n", nowdb_time_getPerSec());

		nowdb_time_setEpoch(0);
	 	mepoch = myepoch();
		char *str = stringtime(mepoch);
		if (str == NULL) return FALSE;
		fprintf(stderr, "EPOCH: %s (%ld)\n", str, mepoch); free(str);

		nowdb_time_setEpoch(-mepoch);

		str = stringtime(0);
		if (str == NULL) return FALSE;
		fprintf(stderr, "ZERO : %s (%ld)\n", str, mepoch); free(str);

		for(int i=0;i<100;i++) {
			err = nowdb_time_now(&t1);
			if (err != NOWDB_OK) {
				nowdb_err_print(err);
				nowdb_err_release(err);
				return FALSE;
			}
			
			int64_t d = rand()%50000;
			int64_t h = rand()%24;
			int64_t m = rand()%60;
			int64_t s = rand()%60;
			int64_t t = rand()%1000;
			int64_t u = rand()%1000000;
			int64_t n = rand()%NPERSEC;

			d *= nowdb_time_day();
			int b = rand()%2;
			if (b) t2 = t1 + d; else t2 = t1 - d;

			h *= nowdb_time_hour();
			b = rand()%2;
			t2 += b ? h : -h;

			m *= nowdb_time_min();
			b = rand()%2;
			t2 += b ? m : -m;

			s *= nowdb_time_sec();
			b = rand()%2;
			t2 += b ? s : -s;

			t *= nowdb_time_milli();
			b = rand()%2;
			t2 += b ? t : -t;

			u *= nowdb_time_micro();
			b = rand()%2;
			t2 += b ? u : -u;

			n *= nowdb_time_nano();
			b = rand()%2;
			t2 += b ? n : -n;

			str = stringtime(t2);
			if (str == NULL) {
				fprintf(stderr,
				"cannot get stringtime with %ld\n", t1);
				return FALSE;
			}
			fprintf(stderr, "%s (%ld)\n", str, t2); free(str);
	
			err = nowdb_time_toSystem(t2, &st);
			if (err != NOWDB_OK) {
				nowdb_err_print(err);
				nowdb_err_release(err);
				return FALSE;
			}
			nowdb_time_fromSystem(&st, &t1);
			if (t1 != t2) {
				fprintf(stderr,
				        "representations differ %ld - %ld\n",
					t1, t2);
				str = stringtime(t1);
				if (str == NULL) {
					fprintf(stderr,
					"cannot get stringtime with %ld\n",
					 t1);
					return FALSE;
				}
				fprintf(stderr, "%s\n", str); free(str);
				return FALSE;
			}
		}
	}
	return TRUE;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_err_init();
	srand(time(NULL) ^ (uint64_t)printf);

	if (!checkMinMax()) {
		fprintf(stderr, "min/max failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!checkPeriods()) {
		fprintf(stderr, "periods failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!unixepochtest()) {
		fprintf(stderr, "tests with unix epoch failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!myepochtest()) {
		fprintf(stderr, "tests with my epoch failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!myunittest()) {
		fprintf(stderr, "tests with my unit failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!myepochunittest()) {
		fprintf(stderr, "tests with my epoch & unit failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!manualtests()) {
		fprintf(stderr, "manual tests failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!randombroken()) {
		fprintf(stderr, "random broken tests failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!randomtimeunix()) {
		fprintf(stderr, "tests with random time (unix) failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!randomtime()) {
		fprintf(stderr, "tests with random time failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}

cleanup:
	nowdb_err_destroy();
	if (rc != EXIT_SUCCESS) {
		fprintf(stderr, "FAILED\n");
	} else {
		fprintf(stderr, "PASSED\n");
	}
	return rc;
}

