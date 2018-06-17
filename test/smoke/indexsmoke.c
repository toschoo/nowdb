/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for index
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/index/index.h>

#include <beet/index.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/stat.h>

#define BASEPATH "rsc"
#define IDXPATH "rsc/idx10"
#define IDXNAME "idx10"
#define CTXNAME "CTX_TEST"

nowdb_index_keys_t *makekeys(uint32_t ksz)  {
	nowdb_index_keys_t *k;
	int x;

	k = malloc(sizeof(nowdb_index_keys_t));
	if (k == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return NULL;
	}
	k->sz = ksz;
	k->off = calloc(ksz,sizeof(uint32_t));
	if (k->off == NULL) {
		fprintf(stderr, "out-of-mem\n");
		free(k); return NULL;
	}
	for(uint32_t i=0;i<ksz;i++) {
		x = rand()%6;
		k->off[i] = x*8;
	}
	return k;
}

int mkpath(char *path) {
	struct stat st;

	if (stat(path, &st) == 0) return 0;
	if (mkdir(path, S_IRWXU) != 0) {
		perror("cannot create dir");
		return -1;
	}
	return 0;
}

int createIndex(char *path, uint16_t size,
                nowdb_index_desc_t  *desc) {
	nowdb_err_t    err;

	// if (mkpath(path) != 0) return -1;
	err = nowdb_index_create(path, size, desc);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int dropIndex(char *path) {
	nowdb_err_t    err;

	err = nowdb_index_drop(path);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int getConfig(char *path, beet_config_t *cfg) {
	beet_err_t          ber;
	char *p;

	p = nowdb_path_append(path, "host");
	if (p == NULL) return -1;

	sprintf(p, "%s/host", path);

	ber = beet_config_get(p, cfg);
	if (ber != BEET_OK) {
		fprintf(stderr, "beet error: %s (%d)\n",
		                 beet_errdesc(ber), ber);
		free(p); return -1;
	}
	free(p);
	return 0;
}

int testCreateSizes(char *path) {
	nowdb_index_desc_t desc;
	nowdb_context_t     ctx;
	beet_config_t       cfg;
	int x;
	uint32_t sz;

	ctx.name = CTXNAME;
	desc.name = IDXNAME;
	desc.ctx  = &ctx;

	for(int i=0; i<10; i++) {

		x = rand()%5;

		desc.keys = makekeys(x);
		desc.idx  = NULL;

		x = rand()%6;

		sz = 1; sz <<= x;

		if (createIndex(path, sz, &desc) != 0) {
			free(desc.keys->off); free(desc.keys);
			return -1;
		}
		free(desc.keys->off); free(desc.keys);

		if (getConfig(IDXPATH, &cfg) != 0) return -1;

		if (cfg.leafPageSize != (cfg.keySize + cfg.dataSize) * 
		                         cfg.leafNodeSize + 12) {
			fprintf(stderr,
			"config not correct: (%u + %u) * %u + 12 != %u\n",
			cfg.keySize, cfg.dataSize,
			cfg.leafNodeSize, cfg.leafPageSize);
			beet_config_destroy(&cfg);
			return -1;
		}
		if (cfg.intPageSize != (cfg.keySize + 4) * 
		                        cfg.intNodeSize + 8) {
			fprintf(stderr,
			"config not correct: (%u + 4) * %u + 12 != %u\n",
			cfg.keySize,
			cfg.intNodeSize, cfg.intPageSize);
			beet_config_destroy(&cfg);
			return -1;
		}
		beet_config_destroy(&cfg);
		if (dropIndex(IDXPATH) != 0) return -1;
	}
	return 0;
}

int openIndex(char *path, void *handle,
              nowdb_index_desc_t *desc) {
	nowdb_err_t    err;

	err = nowdb_index_open(path, handle, desc);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int closeIndex(nowdb_index_t *idx) {
	nowdb_err_t    err;

	err = nowdb_index_close(idx);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int main() {
	nowdb_index_desc_t desc;
	nowdb_context_t     ctx;
	int rc = EXIT_SUCCESS;
	int haveIdx = 0;
	void *handle;

	srand(time(NULL));

	handle = beet_lib_init(NULL);
	if (handle == NULL) {
		fprintf(stderr, "cannot init lib\n");
		return EXIT_FAILURE;
	}
	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init error\n");
		return EXIT_FAILURE;
	}

	ctx.name = CTXNAME;

	desc.name = IDXNAME;
	desc.ctx  = &ctx;
	desc.keys = makekeys(1);
	desc.idx  = NULL;

	if (createIndex(BASEPATH, NOWDB_CONFIG_SIZE_SMALL, &desc) != 0) {
		fprintf(stderr, "createIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (dropIndex(IDXPATH) != 0) {
		fprintf(stderr, "dropIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testCreateSizes(BASEPATH) != 0) {
		fprintf(stderr, "testCreateSizes failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (createIndex(BASEPATH, NOWDB_CONFIG_SIZE_SMALL, &desc) != 0) {
		fprintf(stderr, "createIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (openIndex(BASEPATH, handle, &desc) != 0) {
		fprintf(stderr, "openIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (closeIndex(desc.idx) != 0) {
		fprintf(stderr, "openIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	nowdb_index_destroy(desc.idx); free(desc.idx);


cleanup:
	if (haveIdx) {
		nowdb_index_destroy(desc.idx);
		free(desc.idx);
	}
	if (desc.keys != NULL) {
		free(desc.keys->off);
		free(desc.keys);
	}
	if (handle != NULL) beet_lib_close(handle);
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
