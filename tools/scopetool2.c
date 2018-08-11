/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tool to interact with nowdb using SQL
 * ========================================================================
 */
#include <nowdb/nowdb.h>

#include <common/cmd.h>

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>

/* -----------------------------------------------------------------------
 * get a little help for my friends
 * -----------------------------------------------------------------------
 */
void helptxt(char *progname) {
	fprintf(stderr, "%s <path-to-base> [options]\n", progname);
	fprintf(stderr, "all options are in the format -opt value\n");
	fprintf(stderr, "global_timing: timing information\n");
}

/* -----------------------------------------------------------------------
 * options
 * -------
 * -----------------------------------------------------------------------
 */
int global_count = 0;
char global_timing = 0;

/* -----------------------------------------------------------------------
 * get options
 * -----------------------------------------------------------------------
 */
int parsecmd(int argc, char **argv) {
	int err = 0;

	global_count = ts_algo_args_findUint(
	            argc, argv, 2, "count", 1000, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}

	global_timing = ts_algo_args_findBool(
	            argc, argv, 2, "timing", 0, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	return 0;
}

int main(int argc, char **argv) {
	nowdb_err_t err;
	nowdb_t nowdb;
	nowdb_session_t ses;
	char *path;
	int rc = EXIT_SUCCESS;
	sigset_t s;
	int sig, x;

	if (argc < 2) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	path = argv[1];
	if (strnlen(path, 4097) > 4096) {
		fprintf(stderr, "path too long (max: 4096)\n");
		return EXIT_FAILURE;
	}
	if (path[0] == '-') {
		fprintf(stderr, "invalid path\n");
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	if (parsecmd(argc, argv) != 0) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}

	err = nowdb_library_init(&nowdb, path, 5);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init library\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return EXIT_FAILURE;
	}
	err = nowdb_getSession(nowdb, &ses, 0, 1, 2);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot get session\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE; goto cleanup;
	}
	err = nowdb_session_run(ses);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot run session\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE; goto cleanup;
	}

	sigemptyset(&s);
        sigaddset(&s, SIGUSR1);
        sigaddset(&s, SIGINT);
        sigaddset(&s, SIGABRT);
	x = sigwait(&s, &sig);
	if (x != 0) {
		err = nowdb_err_getRC(nowdb_err_sigwait,
		                       x, "main", NULL);
	}

cleanup:
	err = nowdb_library_shutdown(nowdb);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot shutdown library\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE;
	}
	nowdb_library_close(nowdb);
	return rc;
}
