/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Wait for store to sort all waiting files or
 * until a SIGINT or SIGABRT is received
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/store/store.h>
#include <nowdb/store/storewrk.h>
#include <nowdb/scope/scope.h>

#include <common/cmd.h>

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>

#define DELAY 100000000

volatile sig_atomic_t keeprunning = 1;

void sighandler(int sig) {
	keeprunning = 0;
}

void siginit() {
	signal(SIGINT, &sighandler);
	signal(SIGABRT, &sighandler);
}

void helptext(char *progname) {
	fprintf(stderr, "%s <path-to-scope>\n", progname);
	fprintf(stderr, "keeps the store open until ");
	fprintf(stderr, "all files are sorted or\n");
	fprintf(stderr, "SIGINT or SIGABRT is received.\n");
}

char *global_context = NULL;

/* -----------------------------------------------------------------------
 * get options
 * -----------------------------------------------------------------------
 */
int parsecmd(int argc, char **argv) {
	int err = 0;

	global_context = ts_algo_args_findString(
	            argc, argv, 2, "context", NULL, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	if (global_context == NULL) {
		fprintf(stderr, "no context given\n");
		helptext(argv[0]);
		return -1;
	}
	return 0;
}

int main(int argc, char **argv) {
	int rc = EXIT_SUCCESS;
	nowdb_scope_t scope;
	nowdb_context_t *ctx;
	nowdb_err_t   err;
	nowdb_path_t  path;
	size_t        s;
	int           len=0;	

	if (argc < 2) {
		helptext(argv[0]);
		return EXIT_FAILURE;
	}
	path = argv[1];
	s = strnlen(path, NOWDB_MAX_PATH+1);
	if (s >= NOWDB_MAX_PATH) {
		fprintf(stderr, "path too long (max: 4096)\n");
		return EXIT_FAILURE;
	}

	if (parsecmd(argc, argv) != 0) return EXIT_FAILURE;

	siginit();

	if (!nowdb_init()) {
		fprintf(stderr, "cannot init error manager\n");
		return EXIT_FAILURE;
	}
	err = nowdb_scope_init(&scope, path, 1);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return EXIT_FAILURE;
	}
	err = nowdb_scope_open(&scope);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot open scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_scope_destroy(&scope);
		return EXIT_FAILURE;
	}
	fprintf(stdout, "%08d", len);
	while(keeprunning) {

		err = nowdb_scope_getContext(&scope, global_context, &ctx);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot get context '%s'\n",
			                            global_context);
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = EXIT_FAILURE; break;
		}

		err = nowdb_lock_read(&ctx->store.lock);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot lock store\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = EXIT_FAILURE; break;
		}
		len = ctx->store.waiting.len;
		err = nowdb_unlock_read(&ctx->store.lock);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot unlock store\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = EXIT_FAILURE; break;
		}
		if (len == 0) break;
		fprintf(stdout, "\b\b\b\b\b\b\b\b");
		fprintf(stdout, "%08d", len);
		fflush(stdout);
		for(int i=1;i<len;i++) {
			err = nowdb_store_sortNow(&ctx->store.sortwrk);
			if (err != NOWDB_OK) {
				fprintf(stderr,
				"\ncannot send sort message\n");
				nowdb_err_print(err);
				nowdb_err_release(err);
				rc = EXIT_FAILURE; break;
			}
		}
		err = nowdb_task_sleep(DELAY);
		if (err != NOWDB_OK) {
			fprintf(stderr, "\ncannot sleep\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = EXIT_FAILURE; break;
		}
	}
	fprintf(stdout, "\n");
	err = nowdb_scope_close(&scope);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot close store\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return EXIT_FAILURE;
	}
	nowdb_scope_destroy(&scope);
	nowdb_close();
	return rc;
}
