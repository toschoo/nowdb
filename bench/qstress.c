/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Benchmarking stress of queues with a given delay
 * ========================================================================
 */
#include <nowdb/task/queue.h>
#include <common/progress.h>
#include <common/cmd.h>
#include <common/bench.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>

void drainer(void **ignore){}

nowdb_time_t delay = 1000;

int parsecmd(int argc, char **argv) {
	int err = 0;

	delay = (nowdb_time_t)ts_algo_args_findUint(
	      argc, argv, 1, "delay", 1000, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	if (delay < 0) {
		fprintf(stderr, "negative delay not allowed\n");
		return -1;
	}
	return 0;
}

void helptxt(char *progname) {
	fprintf(stderr, "%s [-delay n]\n", progname);
}

int main(int argc, char **argv) {
	nowdb_queue_t q;
	nowdb_err_t err;
	char *msg;

	if (parsecmd(argc, argv) != 0) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}

	fprintf(stderr, "delay: %lu\n", delay);

	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init\n");
		return EXIT_FAILURE;
	}
	err = nowdb_queue_init(&q, 0, delay, &drainer);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_err_destroy();
		return EXIT_FAILURE;
	}
	nowdb_queue_dequeue(&q, (void**)&msg);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_queue_destroy(&q);
		nowdb_err_destroy();
		return EXIT_FAILURE;
	}
	nowdb_queue_destroy(&q);
	nowdb_err_destroy();
	return EXIT_SUCCESS;
}

