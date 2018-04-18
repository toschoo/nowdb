/* ------------------------------------------------------------------------------
 * A simple command line parser
 * (c) Tobias Schoofs, 1999 -- 2018
 * ------------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdint.h>

#define TS_ALGO_ERR_INVALID -1
#define TS_ALGO_ERR_NO_MEM  -2
#define TS_ALGO_ERR_PARSE   -3
#define TS_ALGO_ERR_NAN     -4
#define TS_ALGO_ERR_PANIC   -99

int ts_algo_args_findBool(int argc, char **argv, int offset, char *name,
                          int instead, int *err);

uint64_t ts_algo_args_findUint(int argc, char **argv, int offset, char *name,
                               uint64_t instead, int *err);

int64_t ts_algo_args_findInt(int argc, char **argv, int offset, char *name,
                             int64_t instead, int *err);

double ts_algo_args_findFloat(int argc, char **argv, int offset, char *name,
                              double instead, int *err);

char *ts_algo_args_findString(int argc, char **argv, int offset, char *name,
                              char *instead, int *err);

char *ts_algo_args_describeErr(int err);

