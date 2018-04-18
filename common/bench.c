#include <common/bench.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>

void timestamp(struct timespec *tp) {
	clock_gettime(CLOCK_MONOTONIC, tp);
}

#define NPERSEC 1000000000

uint64_t minus(struct timespec *t1,
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

void sort(char *buf, int size) {
	qsort(buf, size, sizeof(uint64_t), &compare);
}

