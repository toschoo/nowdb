/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for files
 * ========================================================================
 */
#include <nowdb/io/file.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

typedef struct {
	uint64_t origin;
	uint64_t destin;
	uint64_t stamp; 
	double   value;
	uint64_t amount;
} myedge_t;

#define PAGESIZE 8192
#define FILESIZE NOWDB_MEGA
#define RECSIZE sizeof(myedge_t)

int modulus;

nowdb_bool_t writeData(nowdb_file_t *file) {
	int k=0;
	if (file->mptr == NULL) return FALSE;
	myedge_t e;

	e.origin = 1;
	e.destin = 1;
	e.value = 0.1;
	e.stamp = 0;

	int cnt = 0;
	for(int i=0; i<FILESIZE; i+=sizeof(myedge_t)) {
		if (PAGESIZE - (i%PAGESIZE) < RECSIZE) {
			/*
			fprintf(stderr, "jumping at %d (%d), %d (%d)\n",
			                 i,  (i%PAGESIZE), cnt, cnt);
			*/
			i+=PAGESIZE - (i%PAGESIZE);
			i-=RECSIZE;
			continue;
		}
		if (cnt%modulus==0) {
			k++;
			e.amount = 42;
		} else {
			e.amount = 1;
		}
		memcpy(file->mptr+i, &e, sizeof(myedge_t));
		cnt++;
	}
	fprintf(stderr, "have %d with %d 42\n", cnt, k);
	return TRUE;
}

#define EXPECTING() \
	((FILESIZE/PAGESIZE)*(PAGESIZE/RECSIZE))/modulus

#define JUMP(x) \
	if (PAGESIZE - (x%PAGESIZE) < RECSIZE) { \
		x+=PAGESIZE - (x%PAGESIZE); \
		continue; \
	}

#define MOVE(x) \
	x+=RECSIZE

nowdb_bool_t checkData(nowdb_file_t *file) {
	if (file->mptr == NULL) return FALSE;
	myedge_t *e;
	int k = 0;
	int mx = (file->bufsize/file->recordsize)*file->recordsize;

	for(int i=0; i<mx;) {
		JUMP(i);
		e = (myedge_t*)(file->mptr+i);
		if (e->amount == 42) k++;
		MOVE(i);
	}
	fprintf(stderr, "k is %d (expecting: %lu)\n", k, EXPECTING());
	if (k != EXPECTING()) return FALSE;
	return TRUE;
}

nowdb_file_t *testMakeFile(nowdb_path_t path) {
	nowdb_file_t *file;
	nowdb_err_t   err;

	err = nowdb_file_new(&file, 0, path, NOWDB_MEGA, 0, PAGESIZE, sizeof(myedge_t),
	                      NOWDB_CONT_EDGE,
	                      NOWDB_FILE_READER, NOWDB_COMP_FLAT,
	                      NOWDB_ENCP_NONE, 1, 0, 0);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return file;
}

nowdb_bool_t testNewDestroy() {
	nowdb_file_t *file;

	file = testMakeFile("rsc/test.db");
	if (file == NULL) return FALSE;
	nowdb_file_destroy(file); free(file);
	return TRUE;
}

nowdb_bool_t testNullPath() {
	nowdb_file_t *file;
	file = testMakeFile(NULL);
	if (file == NULL) return TRUE;
	nowdb_file_destroy(file); free(file);
	return FALSE;
}

nowdb_bool_t testMap() {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_bool_t rc = TRUE;

	file = testMakeFile("rsc/test.db");
	if (file == NULL) return FALSE;

	err = nowdb_file_makeWriter(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	err = nowdb_file_map(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	err = nowdb_file_umap(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	nowdb_file_destroy(file); free(file);
	return rc;
}

nowdb_bool_t testOpen() {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_bool_t rc = TRUE;

	file = testMakeFile("rsc/test.db");
	if (file == NULL) return FALSE;

	err = nowdb_file_open(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	err = nowdb_file_close(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	nowdb_file_destroy(file); free(file);
	return rc;
}

nowdb_bool_t testCreateRemove() {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_bool_t rc = TRUE;

	file = testMakeFile("rsc/test.db");
	if (file == NULL) return FALSE;

	err = nowdb_file_create(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	err = nowdb_file_remove(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	nowdb_file_destroy(file); free(file);
	return rc;
}

nowdb_bool_t createFile(nowdb_path_t path) {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_bool_t rc = TRUE;

	file = testMakeFile(path);
	if (file == NULL) return FALSE;

	err = nowdb_file_create(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	nowdb_file_destroy(file); free(file);
	return rc;
}

void removeFile() {
	nowdb_file_t *file;

	file = testMakeFile("rsc/test.db");
	if (file == NULL) return;
	NOWDB_IGNORE(nowdb_file_remove(file));
	nowdb_file_destroy(file); free(file);
}

nowdb_bool_t writeFile() {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_bool_t rc = TRUE;

	file = testMakeFile("rsc/test.db");
	if (file == NULL) return FALSE;

	err = nowdb_file_makeWriter(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}

	err = nowdb_file_map(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	if (!writeData(file)) {
		fprintf(stderr, "cannot write data\n");
		rc = FALSE;
	}
	if (!checkData(file)) {
		fprintf(stderr, "check failed\n");
		rc = FALSE;
	}
	err = nowdb_file_umap(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	nowdb_file_destroy(file); free(file);
	return rc;
}

nowdb_bool_t testReadFile() {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_bool_t rc = TRUE;
	myedge_t *e;
	int z = 0;

	file = testMakeFile("rsc/test.db");
	if (file == NULL) return FALSE;

	file->size = file->capacity;

	err = nowdb_file_makeReader(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}

	err = nowdb_file_open(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	fprintf(stderr, "testing with bufsize %u\n", file->bufsize);
	for(int i=0; i<file->size; i+=file->bufsize) {
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = FALSE; break;
		}
		for(int k=0; k<file->bufsize;) {
			JUMP(k);
			e = (myedge_t*)(file->bptr+k);
			if (e->amount == 42) z++;
			MOVE(k);
		}
	}
	if (z != EXPECTING()) {
		fprintf(stderr, "not correct: %d (expected: %lu)\n",
		                                    z, EXPECTING());
		rc = FALSE;
	}
	fprintf(stderr, "found: %d\n", z);
	err = nowdb_file_close(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	nowdb_file_destroy(file); free(file);
	return rc;
}

nowdb_bool_t testReadRewind() {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_bool_t rc = TRUE;
	myedge_t *e;
	int z = 0;

	file = testMakeFile("rsc/test.db");
	if (file == NULL) return FALSE;

	file->size = file->capacity;

	err = nowdb_file_makeReader(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}

	err = nowdb_file_open(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	for(int i=0; i<file->size; i+=file->bufsize) {
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = FALSE; break;
		}
		for(int k=0; k<file->bufsize;) {
			JUMP(k);
			e = (myedge_t*)(file->bptr+k);
			if (e->amount == 42) z++;
			MOVE(k);
		}
	}
	if (z != EXPECTING()) {
		fprintf(stderr, "not correct: %d\n", z);
		rc = FALSE;
	}
	fprintf(stderr, "found one: %d\n", z);
	err = nowdb_file_rewind(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	z=0;
	for(int i=0; i<file->size; i+=file->bufsize) {
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = FALSE; break;
		}
		for(int k=0; k<file->bufsize;) {
			JUMP(k);
			e = (myedge_t*)(file->bptr+k);
			if (e->amount == 42) z++;
			MOVE(k);
		}
	}
	if (z != EXPECTING()) {
		fprintf(stderr, "not correct: %d\n", z);
		rc = FALSE;
	}
	fprintf(stderr, "found two: %d\n", z);
	err = nowdb_file_close(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	nowdb_file_destroy(file); free(file);
	return rc;
}

nowdb_bool_t testCompressSimple(uint32_t *sz) {
	nowdb_file_t *file;
	nowdb_file_t *cfile;
	nowdb_err_t    err;
	nowdb_bool_t rc = TRUE;

	file = testMakeFile("rsc/test.db");
	if (file == NULL) return FALSE;

	file->size = file->capacity;

	err = nowdb_file_makeReader(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}

	cfile = testMakeFile("rsc/test.dbz");
	if (cfile == NULL) return FALSE;

	cfile->comp = NOWDB_COMP_ZSTD;

	err = nowdb_file_open(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		nowdb_file_destroy(cfile); free(cfile);
		return FALSE;
	}
	err = nowdb_file_open(cfile);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		nowdb_file_destroy(cfile); free(cfile);
		return FALSE;
	}

	fprintf(stderr, "size (before): %u\n", cfile->size);
	for(int i=0; i<file->size; i+=file->bufsize) {
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = FALSE; break;
		}
		err = nowdb_file_writeBuf(cfile, file->bptr, file->bufsize);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = FALSE; break;
		}
	}
	err = nowdb_file_close(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	err = nowdb_file_close(cfile);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	fprintf(stderr, "size: %u\n", cfile->size);
	*sz = cfile->size;
	nowdb_file_destroy(file); free(file);
	nowdb_file_destroy(cfile); free(cfile);
	return rc;
}

nowdb_bool_t testReadCompressed(uint32_t sz) {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_bool_t rc = TRUE;
	myedge_t *e;
	int z = 0;

	file = testMakeFile("rsc/test.dbz");
	if (file == NULL) return FALSE;

	file->size = sz;
	file->comp = NOWDB_COMP_ZSTD;

	err = nowdb_file_makeReader(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	err = nowdb_file_open(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	for(;;) { // int i=0; i<file->size; i+=) {
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			if (err->errcode != nowdb_err_eof) rc = FALSE;
			nowdb_err_print(err);
			nowdb_err_release(err);
			break;
		}
		for(int k=0; k<file->bufsize;) {
			JUMP(k);
			e = (myedge_t*)(file->bptr+k);
			if (e->amount == 42) z++;
			MOVE(k);
		}
		/*
		fprintf(stderr, "round %d OK\n", i/file->bufsize);
		*/
	}
	if (z != EXPECTING()) {
		fprintf(stderr, "not correct: %d (%lu)\n", z, EXPECTING());
		rc = FALSE;
	}
	fprintf(stderr, "found: %d\n", z);
	err = nowdb_file_close(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	nowdb_file_destroy(file); free(file);
	return rc;
}

nowdb_bool_t testReadCompRewind(uint32_t sz) {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_bool_t rc = TRUE;
	myedge_t *e;
	int z = 0;

	file = testMakeFile("rsc/test.dbz");
	if (file == NULL) return FALSE;

	/* we do not know filesize! */
	file->size = sz;
	file->comp = NOWDB_COMP_ZSTD;

	err = nowdb_file_makeReader(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	err = nowdb_file_open(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	for(;;) {
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			if (err->errcode != nowdb_err_eof) rc = FALSE;
			nowdb_err_print(err);
			nowdb_err_release(err);
			break;
		}
		for(int k=0; k<file->bufsize;) {
			JUMP(k);
			e = (myedge_t*)(file->bptr+k);
			if (e->amount == 42) z++;
			MOVE(k);
		}
		/*
		fprintf(stderr, "round %d OK\n", i/file->bufsize);
		*/
	}
	if (z != EXPECTING()) {
		fprintf(stderr, "not correct: %d\n", z);
		rc = FALSE;
	}
	fprintf(stderr, "found one: %d\n", z);
	err = nowdb_file_rewind(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	z=0;
	for(;;) {
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			if (err->errcode != nowdb_err_eof) rc = FALSE;
			nowdb_err_print(err);
			nowdb_err_release(err);
			break;
		}
		for(int k=0; k<file->bufsize;) {
			JUMP(k);
			e = (myedge_t*)(file->bptr+k);
			if (e->amount == 42) z++;
			MOVE(k);
		}
		/*
		fprintf(stderr, "round %d OK\n", i/file->bufsize);
		*/
	}
	if (z != EXPECTING()) {
		fprintf(stderr, "not correct: %d\n", z);
		rc = FALSE;
	}
	fprintf(stderr, "found two: %d\n", z);
	err = nowdb_file_close(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_file_destroy(file); free(file);
		return FALSE;
	}
	nowdb_file_destroy(file); free(file);
	return rc;
}

int main() {
	uint32_t sz;
	int rc = EXIT_SUCCESS;

	srand(time(NULL)+(uint64_t)&printf);

	modulus = rand()%10;
	modulus+=10;
	int p = PAGESIZE/RECSIZE;
	int t = FILESIZE/PAGESIZE;
	while ((t*p)%modulus!=0) modulus++;
	fprintf(stderr, "modulus: %d (%d)\n", modulus, t*p);
	fprintf(stderr, "in %d, there are %d à %d\n",
	       FILESIZE, FILESIZE/PAGESIZE, PAGESIZE);
	fprintf(stderr, "in one of %d, there are %lu à %lu\n",
	       PAGESIZE, PAGESIZE/RECSIZE, RECSIZE);
	fprintf(stderr, "there are, hence, %lu in %d\n",
	       (FILESIZE/PAGESIZE)*(PAGESIZE/RECSIZE), FILESIZE);

	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init error handling\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!testNewDestroy()) {
		fprintf(stderr, "new/destroy failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!testNullPath()) {
		fprintf(stderr, "null path failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testMap()) {
		fprintf(stderr, "map without file passed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testOpen()) {
		fprintf(stderr, "open without file passed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!testCreateRemove()) {
		fprintf(stderr, "create/remove failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!createFile("rsc/test.db")) {
		fprintf(stderr, "create failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!testMap()) {
		fprintf(stderr, "map file failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!testOpen()) {
		fprintf(stderr, "open file failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	fprintf(stderr, "writing file\n");
	if (!writeFile()) {
		fprintf(stderr, "write file failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	fprintf(stderr, "OK\n");
	if (!testReadFile()) {
		fprintf(stderr, "read file failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!testReadRewind()) {
		fprintf(stderr, "read+rewind file failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!createFile("rsc/test.dbz")) {
		fprintf(stderr, "create failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!testCompressSimple(&sz)) {
		fprintf(stderr, "compress file failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!testReadCompressed(sz)) {
		fprintf(stderr, "read compressed file failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!testReadCompRewind(sz)) {
		fprintf(stderr, "read+rewind compressed file failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
cleanup:
	removeFile();
	nowdb_err_destroy();
	if (rc == EXIT_FAILURE) {
		fprintf(stderr, "FAILED\n");
	} else {
		fprintf(stderr, "PASSED\n");
	}
	return rc;
}
