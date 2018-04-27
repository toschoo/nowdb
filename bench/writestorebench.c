/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Benchmarking plain file read
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <common/progress.h>
#include <common/cmd.h>
#include <common/bench.h>
#include <common/stores.h>

/*
void makeEdgePattern(nowdb_edge_t *e) {
	e->origin   = 1;
	e->destin   = 1;
	e->edge     = 1;
	e->label    = 0;
	e->weight2  = 0;
	e->wtype[0] = NOWDB_TYP_UINT;
	e->wtype[1] = NOWDB_TYP_NOTHING;
}

nowdb_bool_t insertEdges(nowdb_store_t *store, uint32_t count) {
	nowdb_err_t err;
	nowdb_edge_t e;

	makeEdgePattern(&e);
	err = nowdb_time_now(&e.timestamp);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	for(uint32_t i=0; i<count; i++) {
		e.weight = (uint64_t)i;
		err = nowdb_store_insert(store, &e);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
	}
	return TRUE;
}
*/

nowdb_bool_t insertEdges(nowdb_store_t *store, uint32_t count) {
	nowdb_err_t err;
	nowdb_edge_t e;

	for(uint32_t i=0; i<count; i++) {

		memset(&e,0,64);

		do e.origin = rand()%100; while(e.origin == 0);
		do e.destin = rand()%100; while(e.destin == 0);
		do e.edge   = rand()%10; while(e.edge == 0);
		do e.label  = rand()%10; while(e.label == 0);
		err = nowdb_time_now(&e.timestamp);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		e.weight = (uint64_t)i;
		e.weight2  = 0;
		e.wtype[0] = NOWDB_TYP_UINT;
		e.wtype[1] = NOWDB_TYP_NOTHING;

		err = nowdb_store_insert(store, &e);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
	}
	return TRUE;
}

uint32_t global_count = 1000;
uint32_t global_block = 1;
int      global_sort  = 0;
char    *global_comp  = NULL;

int parsecmd(int argc, char **argv) {
	int err = 0;

	global_count = (uint32_t)ts_algo_args_findUint(
	            argc, argv, 2, "count", 1000, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	global_block = (uint32_t)ts_algo_args_findUint(
	            argc, argv, 2, "block", 1, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	global_sort = ts_algo_args_findBool(
	            argc, argv, 2, "sort", 0, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	global_comp = ts_algo_args_findString(
	            argc, argv, 2, "comp", "flat", &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	return 0;
}

void helptxt(char *progname) {
	fprintf(stderr, "%s <path-to-file> [options]\n", progname);
	fprintf(stderr, "all options are in the format -opt value\n");
	fprintf(stderr, "[-count n] [-sort b] [-comp x] [-block n]\n");
	fprintf(stderr, "-count n: number of edges to insert\n");
	fprintf(stderr, "-sort  b: indicates whether or not to sort\n");
	fprintf(stderr,
		"     possible values for b: 0/1, true/false, t/f\n");
	fprintf(stderr, "-comp  a: compression algorithm, either\n");
	fprintf(stderr, "          'flat' or\n");
	fprintf(stderr, "          'zstd'\n");
	fprintf(stderr, "-block n: blocksize in megabyte\n");
}

int main(int argc, char **argv) {
	nowdb_comprsc_t compare;
	nowdb_comp_t    comp;
	int rc = EXIT_SUCCESS;
	nowdb_store_t *store = NULL;
	nowdb_path_t path;
	struct timespec t1, t2;

	if (argc < 2) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	path = argv[1];
	if (strnlen(path, 4097) == 4097) {
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
	fprintf(stderr, "count: %u\n", global_count);

	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}
	if (global_sort) {
		compare = &nowdb_store_edge_compare;
	} else {
		compare = NULL;
	}
	if (global_comp == NULL ||
	    strcmp(global_comp, "flat") == 0) {
		comp = NOWDB_COMP_FLAT;
	} else if (strcmp(global_comp, "zstd") == 0) {
		comp = NOWDB_COMP_ZSTD;
	} else {
		fprintf(stderr, "unknown compression: '%s'\n", global_comp);
		return EXIT_FAILURE;
	}
	store = xBootstrap(path, compare, comp, NOWDB_MEGA*global_block);
	if (store == NULL) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	timestamp(&t1);
	if (!insertEdges(store, global_count)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	timestamp(&t2);
	
	fprintf(stdout, "Running time: %luus\n", minus(&t2, &t1)/1000);
	nowdb_task_sleep(1000000000);

cleanup:
	if (store != NULL) {
		if (!closeStore(store)) {
			fprintf(stderr, "cannot close store\n");
		}
		nowdb_store_destroy(store);
		free(store);
	}
	nowdb_err_destroy();
	return rc;
}

