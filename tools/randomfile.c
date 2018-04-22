/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for files
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <common/cmd.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

nowdb_bool_t writeData(nowdb_file_t *file) {
	if (file->mptr == NULL) return FALSE;
	nowdb_edge_t e;

	e.origin = 1;
	e.destin = 1;
	e.label  = 0;
	e.wtype[0] = NOWDB_TYP_UINT;
	e.weight2 = 1;
	e.wtype[1] = 0;
	e.timestamp = 0;

	int cnt = 0;
	for(int i=0; i<file->bufsize; i+=64) {
		if (cnt%16==0) {
			e.weight = 42;
		} else {
			e.weight = 1;
		}
		memcpy(file->mptr+i, &e, 64);
		cnt++;
	}
	return TRUE;
}

nowdb_bool_t checkData(nowdb_file_t *file) {
	if (file->mptr == NULL) return FALSE;
	nowdb_edge_t *e;
	int k = 0;

	for(int i=0; i<file->bufsize; i+=64) {
		e = (nowdb_edge_t*)(file->mptr+i);
		if (e->weight == 42) k++;
	}
	if (k != (file->bufsize/file->recordsize)/16) return FALSE;
	return TRUE;
}

nowdb_file_t *makeFile(nowdb_path_t path, uint32_t size) {
	nowdb_file_t *file;
	nowdb_err_t   err;

	err = nowdb_file_new(&file, 0, path, size, 0, 8192, 64,
	                    NOWDB_FILE_WRITER, NOWDB_COMP_FLAT,
	                             NOWDB_ENCP_NONE, 1, 0, 0);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	err = nowdb_file_create(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return NULL;
	}
	return file;
}

void helptext(char *progname) {
	fprintf(stderr, "%s <path-to-file> <-s n>\n", progname);
	fprintf(stderr, "n: size-of-file in megabyte\n");
}

uint32_t mega = 8;

int parsecmd(int argc, char **argv) {
	int err = 0;

	mega = (uint32_t)ts_algo_args_findUint(argc, argv, 2, "s", 8, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %s\n",
		            ts_algo_args_describeErr(err));
		return -1;
	}
	if (mega > 1048576) {
		fprintf(stderr, "file too big: %d\n", mega);
		return -1;
	}
	if (mega == 0) {
		fprintf(stderr, "file too small: %d\n", mega);
		return -1;
	}
	return 0;
}

int main(int argc, char **argv) {
	nowdb_file_t *file;
	nowdb_err_t   err;
	nowdb_path_t  path;
	size_t        s;

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
	mega *= NOWDB_MEGA;

	file = makeFile(path, mega);
	if (file == NULL) return EXIT_FAILURE;

	err = nowdb_file_map(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_file_close(file));
		nowdb_file_destroy(file); free(file);
		return EXIT_FAILURE;
	}
	if (!writeData(file)) {
		NOWDB_IGNORE(nowdb_file_close(file));
		nowdb_file_destroy(file); free(file);
		return EXIT_FAILURE;
	}
	if (!checkData(file)) {
		NOWDB_IGNORE(nowdb_file_close(file));
		nowdb_file_destroy(file); free(file);
		return EXIT_FAILURE;
	}
	err = nowdb_file_umap(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_file_close(file));
		nowdb_file_destroy(file); free(file);
		return EXIT_FAILURE;
	}
	err = nowdb_file_close(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return EXIT_FAILURE;
	}
	nowdb_file_destroy(file); free(file);
	return EXIT_SUCCESS;
}
