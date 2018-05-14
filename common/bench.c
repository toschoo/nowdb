#include <nowdb/types/time.h>
#include <common/bench.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>

void timestamp(struct timespec *tp) {
	return nowdb_timestamp(tp);
}

uint64_t minus (struct timespec *t1,
                struct timespec *t2) {
	return nowdb_time_minus(t1, t2);
}

uint64_t percentile(uint64_t *buf, int size, uint32_t p) {
	if (p > 100) return 0;
	int n = p*size/100;
	return buf[n];
}

uint64_t median(uint64_t *buf, int size) {
	if (size == 1) return buf[0];
	if (size%2 != 0) {
		return ((buf[(size-1)/2]+buf[(size+1)/2])/2);
	} else {
		return buf[(size-1)/2];
	}
}

int compare(const void *left, const void *right) {
	return (*(uint64_t*)left - *(uint64_t*)right);
}

void sort(void *buf, int size) {
	qsort(buf, size, sizeof(uint64_t), &compare);
}

