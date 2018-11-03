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
 * write buffer of edges to file
 * -----------------------------------------------------------------------
 */
void edges(FILE *csv, nowdb_edge_t *buf, uint32_t size) {
	char stamp[32];
	for(uint32_t i = 0; i<size; i++) {
		if (nowdb_time_toString(buf[i].timestamp,
		                        NOWDB_TIME_FORMAT,
		                        stamp, 32) != 0) {
			fprintf(stderr, "cannot convert timestamp\n");
			break;
		}
		fprintf(csv, "measure;%lu;%lu;%lu;%s;%lu;%lu\n",
		             buf[i].origin,
		             buf[i].destin,
		             buf[i].label,
		                    stamp,
		             buf[i].weight,
		             buf[i].weight2);
	}
}

/* -----------------------------------------------------------------------
 * Location vertex
 * ---------------
 * create type loc (
 *    id uint primary key,
 *    name text,
 *    lon  float,
 *    lat  float
 * )
 * -----------------------------------------------------------------------
 */
typedef struct {
	uint32_t   id;
	char name[32];
	char  lon[16];
	char  lat[16];
} loc_t;

/* -----------------------------------------------------------------------
 * write buffer of vertices to file
 * -----------------------------------------------------------------------
 */
void vertices(FILE *csv, loc_t *buf, uint32_t size) {
	fprintf(csv, "id;name;lon;lat\n");
	for(int32_t i=0; i<size; i++) {
		fprintf(csv, "%u;%s;%s;%s\n",
		             buf[i].id,
		             buf[i].name,
		             buf[i].lon,
		             buf[i].lat);
	}
}

/* -----------------------------------------------------------------------
 * For large values of 'count', this will insert around 100K 'keys'
 * where key means, distinct values for origin, destin and edge
 * -----------------------------------------------------------------------
 */
nowdb_bool_t writeEdges(FILE *csv, uint64_t count) {
	nowdb_time_t tp;
	nowdb_edge_t buf[1024];
	int j = 0;
	int rc;

	memset(&buf,0,64*1024);

	for(uint32_t i=0; i<count; i++) {

		do buf[j].origin = rand()%100; while(buf[j].origin == 0);
		do buf[j].destin = rand()%100; while(buf[j].destin == 0);
		buf[j].label  = rand()%100;
		buf[j].weight = (uint64_t)i;
		buf[j].wtype[0] = NOWDB_TYP_UINT;
		if (i%10 == 0) {
			rc = nowdb_time_now(&tp);
			if (rc != 0) {
				fprintf(stderr, "insert error: %d\n", rc);
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
 * Write a random string into str
 * -----------------------------------------------------------------------
 */
void randomString(char *str, uint32_t mx) {
	uint32_t sz;

	do {sz = rand()%mx;} while(sz == 0);
	str[sz] = 0;
	for(int i=0; i<sz; i++) {
		str[i] = rand()%26;
		str[i] += 64;
	}
}

/* -----------------------------------------------------------------------
 * Creates vertices of type 'location'
 * -----------------------------------------------------------------------
 */
nowdb_bool_t writeVertices(FILE *csv, uint64_t count) {
	loc_t buf[1024];
	int j = 0;
	int n, r;

	memset(&buf,0,32*1024);

	for(uint32_t i=0; i<count; i++) {

		buf[j].id = i;

		randomString(buf[j].name, 4);

 		n = rand()%181;
		if (rand()%2 == 0) n*=-1;
		r = rand()%10;
		sprintf(buf[j].lon, "%d.%d", n, r);

		n = rand()%91; if (rand()%2 == 0) n*=-1;
		r = rand()%10;
		sprintf(buf[j].lat, "%d.%d", n, r);
	
		j++;
		if (j == 1024) {
			vertices(csv, buf, j); j=0;
		}
	}
	if (j>0 && j<1024) vertices(csv, buf, j);
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
	            argc, argv, 1, "count", 1000, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	global_vertex = ts_algo_args_findBool(
	            argc, argv, 1, "vertex", 0, &err);
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
	if (global_vertex) {
		if (!writeVertices(stdout, global_count)) {
			rc = EXIT_FAILURE; goto cleanup;
		}
	} else {
		if (!writeEdges(stdout, global_count)) {
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	timestamp(&t2);
	d = minus(&t2, &t1)/1000;
	fprintf(stderr, "Running time: %luus\n", d);
cleanup:
	return rc;
}
