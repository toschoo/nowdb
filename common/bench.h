/* ------------------------------------------------------------------------------
 * Generic benchmarking functions
 * (c) Tobias Schoofs, 2016
 * ------------------------------------------------------------------------------
 */
#include <time.h>
#include <stdint.h>

void timestamp(struct timespec *tp);

uint64_t minus(struct timespec *t1,
               struct timespec *t2);

uint64_t percentile(uint64_t *buf, int size, uint32_t p);

uint64_t median(uint64_t *buf, int size);

int compare(const void *left, const void *right);

void sort(void *buf, int size);


