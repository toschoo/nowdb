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
	/*
	fprintf(stderr, "inserted %u (last: %lu)\n",
	                           count, e.weight);
	*/
	return TRUE;
}

uint32_t global_count = 1000;
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
	fprintf(stderr, "%s <path-to-file> ", progname);
	fprintf(stderr, "[-count n] [-sort b] [-comp x]\n");
	fprintf(stderr, "count is the number of edges to insert\n");
	fprintf(stderr, "sort indicates whether or not to sort\n");
	fprintf(stderr,
		"     possible values for bool: 0/1, true/false, t/f\n");
	fprintf(stderr, "compression algorithm, either\n");
	fprintf(stderr, "            'flat' or\n");
	fprintf(stderr, "            'zstd'\n");
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
	store = xBootstrap(path, compare, comp);
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

