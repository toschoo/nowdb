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

nowdb_file_t *testMakeFile(nowdb_path_t path) {
	nowdb_file_t *file;
	nowdb_err_t   err;

	err = nowdb_file_new(&file, 0, path, NOWDB_MEGA, 0, 8192, 64,
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
	nowdb_edge_t *e;
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
		for(int k=0; k<file->bufsize; k+=64) {
			e = (nowdb_edge_t*)(file->bptr+k);
			if (e->weight == 42) z++;
		}
	}
	if (z != (file->size / 64) / 16) {
		fprintf(stderr, "not correct: %d\n", z);
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
	nowdb_edge_t *e;
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
		for(int k=0; k<file->bufsize; k+=64) {
			e = (nowdb_edge_t*)(file->bptr+k);
			if (e->weight == 42) z++;
		}
	}
	if (z != (file->size / 64) / 16) {
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
		for(int k=0; k<file->bufsize; k+=64) {
			e = (nowdb_edge_t*)(file->bptr+k);
			if (e->weight == 42) z++;
		}
	}
	if (z != (file->size / 64) / 16) {
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

nowdb_bool_t testCompressSimple() {
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
	nowdb_file_destroy(file); free(file);
	nowdb_file_destroy(cfile); free(cfile);
	return rc;
}

nowdb_bool_t testReadCompressed() {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_bool_t rc = TRUE;
	nowdb_edge_t *e;
	int z = 0;

	file = testMakeFile("rsc/test.dbz");
	if (file == NULL) return FALSE;

	/* we do not know filesize! */
	file->size = file->capacity;
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
	for(int i=0; i<file->size; i+=file->bufsize) {
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			if (err->errcode != nowdb_err_eof) rc = FALSE;
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = FALSE; break;
		}
		for(int k=0; k<file->bufsize; k+=64) {
			e = (nowdb_edge_t*)(file->bptr+k);
			if (e->weight == 42) z++;
		}
		/*
		fprintf(stderr, "round %d OK\n", i/file->bufsize);
		*/
	}
	if (z != (file->size / 64) / 16) {
		fprintf(stderr, "not correct: %d\n", z);
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

nowdb_bool_t testReadCompRewind() {
	nowdb_file_t *file;
	nowdb_err_t    err;
	nowdb_bool_t rc = TRUE;
	nowdb_edge_t *e;
	int z = 0;

	file = testMakeFile("rsc/test.dbz");
	if (file == NULL) return FALSE;

	/* we do not know filesize! */
	file->size = file->capacity;
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
	for(int i=0; i<file->size; i+=file->bufsize) {
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			if (err->errcode != nowdb_err_eof) rc = FALSE;
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = FALSE; break;
		}
		for(int k=0; k<file->bufsize; k+=64) {
			e = (nowdb_edge_t*)(file->bptr+k);
			if (e->weight == 42) z++;
		}
		/*
		fprintf(stderr, "round %d OK\n", i/file->bufsize);
		*/
	}
	if (z != (file->size / 64) / 16) {
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
			if (err->errcode != nowdb_err_eof) rc = FALSE;
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = FALSE; break;
		}
		for(int k=0; k<file->bufsize; k+=64) {
			e = (nowdb_edge_t*)(file->bptr+k);
			if (e->weight == 42) z++;
		}
		/*
		fprintf(stderr, "round %d OK\n", i/file->bufsize);
		*/
	}
	if (z != (file->size / 64) / 16) {
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
	int rc = EXIT_SUCCESS;

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
	if (!writeFile()) {
		fprintf(stderr, "write file failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
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
	if (!testCompressSimple()) {
		fprintf(stderr, "compress file failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!testReadCompressed()) {
		fprintf(stderr, "read compressed file failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!testReadCompRewind()) {
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

