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

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/stat.h>

nowdb_err_t getFile(nowdb_file_t **file, nowdb_path_t path) {
	nowdb_err_t err;
	struct stat  st;

	if (stat(path, &st) != 0) {
		return nowdb_err_get(nowdb_err_open, TRUE, "bench", path);
	}
	err = nowdb_file_new(file, 0, path, st.st_size, 8192, 64,
	                      NOWDB_FILE_READER, NOWDB_COMP_FLAT,
	                               NOWDB_ENCP_NONE, 1, 0, 0);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

nowdb_err_t readfile(nowdb_path_t path, uint32_t *found) {
	nowdb_file_t *file;
	nowdb_err_t    err = NOWDB_OK;
	nowdb_edge_t *e;
	int z = 0;

	err = getFile(&file, path);
	if (err != NOWDB_OK) return err;

	file->size = file->capacity;

	err = nowdb_file_open(file);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(file); free(file);
		return err;
	}
	for(int i=0; i<file->size; i+=file->bufsize) {
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			nowdb_file_close(file);
			nowdb_file_destroy(file);
			free(file); return err;
		}
		for(int k=0; k<file->bufsize; k+=64) {
			e = (nowdb_edge_t*)(file->bptr+k);
			if (e->weight == 42) z++;
		}
	}
	*found += z;
	err = nowdb_file_close(file);
	if (err != NOWDB_OK) {
		nowdb_file_destroy(file); free(file);
		return err;
	}
	nowdb_file_destroy(file); free(file);
	return err;
}

nowdb_err_t iterate(int it, nowdb_path_t path) {
	nowdb_err_t err = NOWDB_OK;
	struct timespec t1, t2;
	uint64_t s=0, mx=0, mn=0;
	uint64_t *d;
	uint32_t found = 0;
	progress_t p;

	d = calloc(it, sizeof(uint64_t));
	if (d == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, TRUE, "bench", NULL);
	}
	init_progress(&p, stdout, it);
	for(int i=0;i<it;i++) {
		timestamp(&t1);
		err = readfile(path, &found);
		if (err != NOWDB_OK) {
			free(d); return err;
		}
		timestamp(&t2);
		update_progress(&p, i);
		d[i] = minus(&t2, &t1);
		s += d[i];
		if (mx == 0 || d[i] > mx) mx = d[i];
		if (mn == 0 || d[i] < mn) mn = d[i];
	}
	close_progress(&p); fprintf(stdout, "\n");
	qsort(d, it, sizeof(uint64_t), &compare);
	fprintf(stderr, "found : %u\n", found/it);
	fprintf(stderr, "avg   : %010luus\n", (s/it)/1000);
	fprintf(stderr, "median: %010luus\n", median(d, it)/1000);
	fprintf(stderr, "max   : %010luus\n", mx/1000);
	fprintf(stderr, "min   : %010luus\n", mn/1000);
	fprintf(stderr, "99    : %010luus\n", percentile(d, it, 99)/1000);
	fprintf(stderr, "50    : %010luus\n", percentile(d, it, 50)/1000);
	free(d);
	return err;
}

uint32_t it = 10;

int parsecmd(int argc, char **argv) {
	int err = 0;

	it = (uint32_t)ts_algo_args_findUint(argc, argv, 2, "it", 10, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	if (it == 0xffffffff) {
		fprintf(stderr, "too many iterations: %d\n", it);
		return -1;
	}
	return 0;
}

void helptxt(char *progname) {
	fprintf(stderr, "%s <path-to-file> [-it n]\n", progname);
}

int main(int argc, char **argv) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_path_t path;

	if (argc < 2) {
		helptxt(argv[0]); return EXIT_FAILURE;
	}
	path = argv[1];
	int s = strnlen(path, NOWDB_MAX_PATH);
	if (s == NOWDB_MAX_PATH || s == 0) {
		fprintf(stderr, "path too long (max: 4096)\n");
		return EXIT_FAILURE;
	}
	if (parsecmd(argc, argv) != 0) return EXIT_FAILURE;

	nowdb_err_init();
	err = iterate(it, path);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_err_destroy();
		return EXIT_FAILURE;
	}
	nowdb_err_destroy();
	return EXIT_SUCCESS;
}


