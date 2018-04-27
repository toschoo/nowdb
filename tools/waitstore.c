/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Wait for store to sort all waiting files or
 * until a SIGINT or SIGABRT is received
 * ========================================================================
 */
#include <nowdb/store/store.h>
#include <nowdb/store/storewrk.h>

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
	fprintf(stderr, "%s <path-to-file>\n", progname);
	fprintf(stderr, "keeps the store open until ");
	fprintf(stderr, "all files are sorted or\n");
	fprintf(stderr, "SIGINT or SIGABRT is received.\n");
}

int main(int argc, char **argv) {
	int rc = EXIT_SUCCESS;
	nowdb_store_t store;
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

	siginit();

	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init error manager\n");
		return EXIT_FAILURE;
	}
	err = nowdb_store_init(&store, path, 0, 64, NOWDB_MEGA);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init store\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return EXIT_FAILURE;
	}
	err = nowdb_store_configSort(&store, &nowdb_store_edge_compare);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot config store (sort)\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_store_destroy(&store);
		return EXIT_FAILURE;
	}
	/* parameter! */
	err = nowdb_store_configCompression(&store, NOWDB_COMP_ZSTD);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot config store (compression)\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_store_destroy(&store);
		return EXIT_FAILURE;
	}
	err = nowdb_store_open(&store);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot open store\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_store_destroy(&store);
		return EXIT_FAILURE;
	}
	fprintf(stdout, "%08d", len);
	while(keeprunning) {
		err = nowdb_lock_read(&store.lock);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot lock store\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = EXIT_FAILURE; break;
		}
		len = store.waiting.len;
		err = nowdb_unlock_read(&store.lock);
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
			err = nowdb_store_sortNow(&store.sortwrk);
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
	err = nowdb_store_close(&store);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot close store\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return EXIT_FAILURE;
	}
	nowdb_store_destroy(&store);
	nowdb_err_destroy();
	return rc;
}
