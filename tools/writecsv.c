/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Tool to write random data in the canonical csv format to stdout
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/types/time.h>
#include <common/cmd.h>
#include <common/bench.h>

#include <stdlib.h>
#include <stdio.h>

/* -----------------------------------------------------------------------
 * get a little help for my friends
 * -----------------------------------------------------------------------
 */
void helptxt(char *progname) {
	fprintf(stderr, "%s [options]\n", progname);
	fprintf(stderr, "all options are in the format -opt value\n");
	fprintf(stderr, "-count n: number of edges/actors to insert\n");
	fprintf(stderr, "-vertex b: write vertices instead of edges\n");
	fprintf(stderr, "           (b: 1/0, t/f, true/false)\n");
}

/* -----------------------------------------------------------------------
 * write buffer to file
 * -----------------------------------------------------------------------
 */
void edges(FILE *csv, nowdb_edge_t *buf, uint32_t size) {
	for(uint32_t i = 0; i<size; i++) {
		fprintf(csv, "%lu;%lu;%lu;%lu;%ld;%lu;%lu;%u;%u\n",
		             buf[i].edge,
		             buf[i].origin,
		             buf[i].destin,
		             buf[i].label,
		             buf[i].timestamp,
		             buf[i].weight,
		             buf[i].weight2,
		             buf[i].wtype[0],
		             buf[i].wtype[1]);
	}
}

/* -----------------------------------------------------------------------
 * For large values of 'count', this will insert around 100K 'keys'
 * where key means, distinct values for origin, destin and edge
 * -----------------------------------------------------------------------
 */
nowdb_bool_t writeEdges(FILE *csv, uint64_t count) {
	nowdb_err_t err;
	nowdb_time_t tp;
	nowdb_edge_t buf[1024];
	int j = 0;

	memset(&buf,0,64*1024);

	for(uint32_t i=0; i<count; i++) {

		do buf[j].origin = rand()%100; while(buf[j].origin == 0);
		do buf[j].destin = rand()%100; while(buf[j].destin == 0);
		do buf[j].edge   = rand()%10; while(buf[j].edge == 0);
		buf[j].weight = (uint64_t)i;
		buf[j].wtype[0] = NOWDB_TYP_UINT;
		if (i%10 == 0) {
			err = nowdb_time_now(&tp);
			if (err != NOWDB_OK) {
				fprintf(stderr, "insert error\n");
				nowdb_err_print(err);
				nowdb_err_release(err);
				return FALSE;
			}
		}
		buf[j].timestamp = tp;
		j++;
		if (j == 1024) {
			edges(csv, buf, j); j=0;
		}
	}
	if (j>0 && j<1024) edges(csv, buf, j);

	return TRUE;
}

/* -----------------------------------------------------------------------
 * options
 * -------
 * global_count: how many edges we will insert
 * global_vertex: vertex or edge?
 * -----------------------------------------------------------------------
 */
uint64_t global_count = 1000;
int global_vertex = 0;

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
	global_vertex = ts_algo_args_findBool(
	            argc, argv, 2, "vertex", 0, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * MAIN
 * -----------------------------------------------------------------------
 */
int main(int argc, char **argv) {
	int rc = EXIT_SUCCESS;
	struct timespec t1, t2;
	uint64_t d=0;

	if (parsecmd(argc, argv) != 0) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	fprintf(stderr, "count: %lu\n", global_count);

	timestamp(&t1);
	if (!writeEdges(stdout, global_count)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	timestamp(&t2);
	d = minus(&t2, &t1)/1000;
	fprintf(stderr, "Running time: %luus\n", d);
cleanup:
	return rc;
}
