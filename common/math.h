/* ========================================================================
 * Some math puzzles to be used in tests and benchmarks
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 */
#ifndef mymath_decl
#define mymath_decl

#include <time.h>
#include <stdint.h>
#include <math.h>

/* ------------------------------------------------------------------------
 * computes the first 'num' fibonacci numbers and stores them in buf
 * ------------------------------------------------------------------------
 */
void fibonacci(uint32_t *buf, int num);

#endif

