/* =======================================================================
 * Some math puzzles to be used in tests and benchmarks
 * (c) Tobias Schoofs, 2018
 * =======================================================================
 */
#include <time.h>
#include <stdint.h>
#include <math.h>

/* ------------------------------------------------------------------------
 * computes the first 'num' fibonacci numbers and stores them in buf
 * ------------------------------------------------------------------------
 */
void fibonacci(uint32_t *buf, int num) {
	buf[0] = 1;
	buf[1] = 1;

	for(int i=2; i<num; i++) {
		buf[i] = buf[i-1] + buf[i-2];
	}
}

