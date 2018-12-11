/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * server-specific common implementation
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/errman.h>

#include <beet/config.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/resource.h>

void *nowdb_global_handle = NULL;

void *nowdb_lib() {
	return nowdb_global_handle;
}

static int setMaxFiles() {
	struct rlimit rl;
	if (getrlimit(RLIMIT_NOFILE,&rl) != 0) {
		fprintf(stderr, "cannot get max files limit: %d\n", errno);
		return -1;
	}

	rl.rlim_cur = rl.rlim_max;

	if (setrlimit(RLIMIT_NOFILE,&rl) != 0) {
		fprintf(stderr,"max files open: %d\n", errno);
		return -1;
	}
	return 0;
}

nowdb_bool_t nowdb_init() {
	if (!nowdb_errman_init()) return FALSE;
	nowdb_global_handle = beet_lib_init(NULL);
	if (nowdb_global_handle == NULL) return FALSE;
	if (setMaxFiles() != 0) return FALSE;
	return TRUE;
}

void nowdb_close() {
	if (nowdb_global_handle != NULL) beet_lib_close(nowdb_global_handle);
	nowdb_errman_destroy();
}
