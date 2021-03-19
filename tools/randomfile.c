/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Write some random data to a file
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <common/cmd.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#define EDGE_OFF  24
#define LABEL_OFF 32
#define WEIGHT_OFF 40

#define LOOP_INIT(stm, atts) \
	uint32_t recsz = nowdb_edge_recSize(stm, atts); \
	uint32_t bufsz = (NOWDB_IDX_PAGE/recsz)*recsz; \
	uint32_t remsz = NOWDB_IDX_PAGE - bufsz;

#define LOOP_HEAD \
	for(int i=0; i<NOWDB_IDX_PAGE;) { \
		if (i%NOWDB_IDX_PAGE >= bufsz) { \
			i+=remsz; continue; \
		}

#define LOOP_END \
		i+=recsz; \
	}

void setRandomValue(char *e, uint32_t off) {
	uint64_t x;
	do x = rand()%100; while(x == 0);
	memcpy(e+off, &x, 8);
}

void setValue(char *e, uint32_t off, uint64_t v) {
	memcpy(e+off, &v, 8);
}

int edgecompare(char *one, char *two, uint32_t off) {
	if (*(uint64_t*)(one+off) < *(uint64_t*)(two+off)) return -1;
	if (*(uint64_t*)(one+off) > *(uint64_t*)(two+off)) return  1;
	return 0;
}

void makeEdgePattern(char *e) {
	setValue(e, NOWDB_OFF_ORIGIN, 0xa);
	setValue(e, NOWDB_OFF_DESTIN, 0xb);
	nowdb_time_now((nowdb_time_t*)(e+NOWDB_OFF_STAMP));
	setValue(e, WEIGHT_OFF+8, 63);
	setValue(e, EDGE_OFF, 0xc);
	setValue(e, LABEL_OFF, 0xd);
	setValue(e, WEIGHT_OFF, 0);
}

nowdb_bool_t writeData(nowdb_file_t *file) {
	if (file->mptr == NULL) return FALSE;
	uint64_t w;
	int cnt = 0;
	LOOP_INIT(1,3);

	char *e = calloc(1,recsz);
	if (e == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return FALSE;
	}
	makeEdgePattern(e);
	for(int z=0; z<file->bufsize;) {
		if (z%NOWDB_IDX_PAGE >= bufsz) {
			z+=remsz; continue;
		}
		if (cnt%16==0) {
			w = 42;
		} else {
			w = 1;
		}
		memcpy(e+WEIGHT_OFF, &w, 8);
		memcpy(file->mptr+z, e, recsz);
		cnt++; 
		z+=recsz;
	}
	free(e);
	return TRUE;
}

nowdb_bool_t checkData(nowdb_file_t *file) {
	if (file->mptr == NULL) return FALSE;
	uint64_t w=42;
	int k = 0;

	LOOP_INIT(1,3);

	for(int i=0; i<file->bufsize;) {
		if (i%NOWDB_IDX_PAGE >= bufsz) {
			i+=remsz; continue;
		}
		if (memcmp(file->mptr+i+WEIGHT_OFF, &w, 8) == 0) k++;
		i+=recsz;
	}
	int pgs = file->bufsize/NOWDB_IDX_PAGE;
	int rpp = NOWDB_IDX_PAGE/file->recordsize;
	int rpf = pgs*rpp;
	if (k != rpf/16) return FALSE;
	return TRUE;
}

nowdb_file_t *makeFile(nowdb_path_t path, uint32_t size) {
	nowdb_file_t *file;
	nowdb_err_t   err;

	err = nowdb_file_new(&file, 0, path, size, 0, 8192, 64,
	                                       NOWDB_CONT_EDGE,
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

uint32_t mega = 1;

int parsecmd(int argc, char **argv) {
	int err = 0;

	mega = (uint32_t)ts_algo_args_findUint(argc, argv, 2, "s", 1, &err);
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
