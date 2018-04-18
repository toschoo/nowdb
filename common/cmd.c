/* ------------------------------------------------------------------------------
 * A simple command line parser
 * (c) Tobias Schoofs, 1999 -- 2018
 * ------------------------------------------------------------------------------
 */
#include <common/cmd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int isnatural(char *str) {
	for(int i=0;str[i]!=0;i++) {
		if (str[i] < '0' || str[i] > '9') return 0;
	}
	return 1;
}

int ts_algo_args_findBool(int argc, char **argv, int offset, char *name,
                                                  int instead, int *err) {
	*err = 0;
	for(int i=offset; i<argc; i++) {
		if (argv[i][0] == '-' && strcmp(argv[i]+1, name) == 0) {
			if (i+1 == argc) {
				*err = TS_ALGO_ERR_PARSE;
				return instead;
			}
			if (strcmp(argv[i+1], "0") == 0) return 0;
			if (strcmp(argv[i+1], "1") == 0) return 1;
			if (strcmp(argv[i+1], "f") == 0) return 0;
			if (strcmp(argv[i+1], "t") == 0) return 1;
			if (strcmp(argv[i+1], "false") == 0) return 0;
			if (strcmp(argv[i+1], "FALSE") == 0) return 0;
			if (strcmp(argv[i+1], "true") == 0) return 1;
			if (strcmp(argv[i+1], "TRUE") == 0) return 1;
			*err = TS_ALGO_ERR_NAN;
			return instead;
		}
	}
	return instead;
}

uint64_t ts_algo_args_findUint(int argc, char **argv, int offset, char *name,
                                                  uint64_t instead, int *err)
{
	*err = 0;
	for(int i=offset; i<argc; i++) {
		if (argv[i][0] == '-' && strcmp(argv[i]+1, name) == 0) {
			if (i+1 == argc) {
				*err = TS_ALGO_ERR_PARSE;
				return instead;
			}
			if (!isnatural(argv[i+1])) {
				*err = TS_ALGO_ERR_NAN;
				return instead;
			}
			return atol(argv[i+1]);
		}
	}
	return instead;
}

int64_t ts_algo_args_findInt(int argc, char **argv, int offset, char *name,
                             int64_t instead, int *err);

double ts_algo_args_findFloat(int argc, char **argv, int offset, char *name,
                              double instead, int *err);

char *ts_algo_args_findString(int argc, char **argv, int offset, char *name,
                                                    char *instead, int *err)
{
	*err = 0;
	for(int i=offset; i<argc; i++) {
		if (argv[i][0] == '-' && strcmp(argv[i]+1, name) == 0) {
			if (i+1 == argc) {
				*err = TS_ALGO_ERR_PARSE;
				return instead;
			}
			return argv[i+1];
		}
	}
	return instead;
}

char *ts_algo_args_describeErr(int err) {
	switch(err) {
	case -1: return "invalid parameter";
	case -2: return "out of memory";
	case -3: return "missing value";
	case -4: return "not a number";
	default: return "unknown";
	}
}

