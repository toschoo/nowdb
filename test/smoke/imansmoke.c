/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for index manager
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/index/index.h>
#include <nowdb/index/man.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/stat.h>

#define IDXPATH "rsc/iman10"
#define CTXPATH "rsc/ctx10"
#define VXPATH "rsc/vertex10"

static ts_algo_cmp_t ctxcompare(void *ignore, void *left, void *right) {
	int cmp = strcmp(((nowdb_context_t*)left)->name,
	                 ((nowdb_context_t*)right)->name);
	if (cmp < 0) return ts_algo_cmp_less;
	if (cmp > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Tree callbacks for contexts: update (do nothing!)
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Tree callbacks for contexts: delete and destroy
 * ------------------------------------------------------------------------
 */
static void ctxdestroy(void *ignore, void **n) {
	if (*n != NULL) {
		nowdb_context_destroy((nowdb_context_t*)(*n));
		free(*n); *n = NULL;
	}
}

void removeICat(char *path) {
	struct stat st;
	if (stat(path, &st) == 0) remove(path);
}

int makectxdir(char *ctxname) {
	struct stat st;
	char *p, *i;

	if (stat(CTXPATH, &st) != 0) {
		if (mkdir(CTXPATH, S_IRWXU) != 0) {
			fprintf(stderr, "cannot create %s\n", CTXPATH);
			return -1;
		}
	}
	p = nowdb_path_append(CTXPATH, ctxname);
	if (p == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	if (stat(p, &st) != 0) {
		if (mkdir(p, S_IRWXU) != 0) {
			fprintf(stderr, "cannot create %s\n", p);
			free(p); return -1;
		}
	}
	i = nowdb_path_append(p, "index"); free(p);
	if (i == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	if (stat(i, &st) != 0) {
		if (mkdir(i, S_IRWXU) != 0) {
			fprintf(stderr, "cannot create %s\n", i);
			free(i); return -1;
		}
	}
	free(i);
	return 0;
}

int makevxdir() {
	struct stat st;
	char *i;

	if (stat(VXPATH, &st) != 0) {
		if (mkdir(VXPATH, S_IRWXU) != 0) {
			fprintf(stderr, "cannot create %s\n", VXPATH);
			return -1;
		}
	}
	i = nowdb_path_append(VXPATH, "index");
	if (i == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	if (stat(i, &st) != 0) {
		if (mkdir(i, S_IRWXU) != 0) {
			fprintf(stderr, "cannot create %s\n", i);
			free(i); return -1;
		}
	}
	free(i);
	return 0;
}

int createIndex(char *path, uint16_t size,
                nowdb_index_desc_t  *desc) {
	nowdb_err_t    err;
	char *p;

	p = nowdb_path_append(path, "index");
	if (p == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	err = nowdb_index_create(p, size, desc); free(p);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

ts_algo_tree_t *fakeCtx() {
	ts_algo_tree_t *ctx;
	ctx = ts_algo_tree_new(&ctxcompare, NULL, &noupdate,
	                       &ctxdestroy, &ctxdestroy);
	/* insert some contexts ... */

	if (makevxdir() != 0) {
		ts_algo_tree_destroy(ctx); free(ctx);
		return NULL;
	}
	if (makectxdir("CTX_TEST") != 0) {
		ts_algo_tree_destroy(ctx); free(ctx);
		return NULL;
	}
	return ctx;
}

int addCtx(ts_algo_tree_t *ctx, char *name) {
	nowdb_context_t *c;

	c = malloc(sizeof(nowdb_context_t));
	if (c == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	c->name = name;
	return 0;
}

int initMan(nowdb_index_man_t *iman,
            ts_algo_tree_t     *ctx,
            void            *handle) {
	nowdb_err_t err;

	err = nowdb_index_man_init(iman, ctx,
	                           IDXPATH,
	                           CTXPATH,
	                           VXPATH,
	                           handle);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init iman\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int testOpenClose(nowdb_index_man_t *iman,
                  ts_algo_tree_t     *ctx,
                  void            *handle) {
	if (initMan(iman, handle, ctx) != 0) return -1;
	nowdb_index_man_destroy(iman);
	return 0;
}

nowdb_index_keys_t *makekeys(uint32_t ksz) {
	nowdb_index_keys_t *k;
	int x;

	k = malloc(sizeof(nowdb_index_keys_t));
	if (k == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return NULL;
	}
	k->sz = (uint16_t)ksz;
	k->off = calloc(ksz,sizeof(uint16_t));
	if (k->off == NULL) {
		fprintf(stderr, "out-of-mem\n");
		free(k); return NULL;
	}
	for(uint16_t i=0;i<ksz;i++) {
		x = rand()%6;
		k->off[i] = (uint16_t)(x*8);
	}
	return k;
}

nowdb_index_keys_t *copykeys(nowdb_index_keys_t *k) {
	nowdb_index_keys_t *c;

	c = calloc(1,sizeof(nowdb_index_keys_t));
	if (c == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return NULL;
	}
	c->off = calloc(k->sz,sizeof(uint16_t));
	if (c->off == NULL) {
		fprintf(stderr, "out-of-mem\n");
		free(k); return NULL;
	}
	c->sz = k->sz;
	memcpy(c->off, k->off, k->sz*sizeof(uint16_t));
	return c;
}

int testRegister(nowdb_index_man_t *iman,
                 char             *iname,
                 nowdb_index_keys_t   *k) {
	nowdb_err_t err;

	err = nowdb_index_man_register(iman, iname, NULL, k, NULL);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int testUnregister(nowdb_index_man_t *iman,
                   char             *iname) {
	nowdb_err_t err;

	err = nowdb_index_man_unregister(iman, iname);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int testGetIdx(nowdb_index_man_t *iman,
               char             *iname,
               nowdb_index_keys_t   *k) {
	nowdb_err_t err;
	nowdb_index_t    *idx;

	err = nowdb_index_man_getByName(iman, iname, &idx);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	err = nowdb_index_man_getByKeys(iman, NULL, k, &idx);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

void destroyKeys(nowdb_index_keys_t **k) {
	if (k == NULL) return;
	if (*k == NULL) return;
	if ((*k)->off != NULL) free((*k)->off);
	free(*k); *k=NULL;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_index_man_t man;
	void *handle = NULL;
	int haveMan = 0;
	nowdb_index_keys_t *k1=NULL, *c1=NULL;
	nowdb_index_keys_t *k2=NULL, *c2=NULL;
	ts_algo_tree_t *ctx=NULL;
	nowdb_index_desc_t desc;

	handle = beet_lib_init(NULL);
	if (handle == NULL) {
		fprintf(stderr, "cannot init lib\n");
		return EXIT_FAILURE;
	}
	fprintf(stderr, "have handle: %p\n", handle);
	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init error\n");
		return EXIT_FAILURE;
	}

	// removeICat(IDXPATH);

	ctx = fakeCtx();
	if (ctx == NULL) {
		fprintf(stderr, "cannot fake context\n");
		return EXIT_FAILURE;
	}
	if (testOpenClose(&man, handle, ctx) != 0) {
		fprintf(stderr, "testOpenClose failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (initMan(&man, handle, ctx) != 0) {
		fprintf(stderr, "initMan failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	haveMan = 1;

	k1 = makekeys(1);
	if (k1 == NULL) return -1;

	c1 = copykeys(k1);
	if (c1 == NULL) {
		destroyKeys(&k1);
		rc = EXIT_FAILURE; goto cleanup;
	}

	desc.name = "idx0";
	desc.keys = k1;
	desc.ctx = NULL;
	desc.idx = NULL;

	if (createIndex(VXPATH, NOWDB_CONFIG_SIZE_TINY, &desc) != 0) {
		fprintf(stderr, "create Index failed for idx0\n");
		destroyKeys(&k1);
		rc = EXIT_FAILURE; goto cleanup;
	}

	if (testRegister(&man, "idx0", k1) != 0) {
		fprintf(stderr, "testRegister failed for k1\n");
		destroyKeys(&k1);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "idx0", c1) != 0) {
		fprintf(stderr, "testGetIdx failed for k1\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	k2 = makekeys(2);
	if (k2 == NULL) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	c2 = copykeys(k2);
	if (c2 == NULL) {
		destroyKeys(&k2); 
		rc = EXIT_FAILURE; goto cleanup;
	}

	desc.name = "idx1";
	desc.keys = k2;
	desc.ctx = NULL;
	desc.idx = NULL;

	if (createIndex(VXPATH, NOWDB_CONFIG_SIZE_TINY, &desc) != 0) {
		fprintf(stderr, "create Index failed for idx1\n");
		destroyKeys(&k2);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testRegister(&man, "idx1", k2) != 0) {
		fprintf(stderr, "testRegister failed for k2\n");
		destroyKeys(&k2);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "idx1", c2) != 0) {
		fprintf(stderr, "testGetIdx failed for k2\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	
	nowdb_index_man_destroy(&man); haveMan = 0;

	if (initMan(&man, handle, ctx) != 0) {
		fprintf(stderr, "initMan failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	haveMan = 1;
	
	if (testGetIdx(&man, "idx0", c1) != 0) {
		fprintf(stderr, "testGetIdx failed for k1 (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "idx1", c2) != 0) {
		fprintf(stderr, "testGetIdx failed for k2 (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testUnregister(&man, "idx0") != 0) {
		fprintf(stderr, "testUnregister failed for idx0\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "idx0", c1) == 0) {
		fprintf(stderr, "testGetIdx passed for unregistered idx0\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	if (haveMan) nowdb_index_man_destroy(&man);
	if (ctx != NULL) {
		ts_algo_tree_destroy(ctx); free(ctx);
	}
	if (handle != NULL) beet_lib_close(handle);
	if (c1 != NULL) destroyKeys(&c1);
	if (c2 != NULL) destroyKeys(&c2);
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
