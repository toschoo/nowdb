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

#define BASE "rsc/idxdb10"
#define IMNPATH "rsc/idxdb10/iman10"
#define CTXPATH "context"

/* ------------------------------------------------------------------------
 * Tree callbacks for contexts: compare name
 * ------------------------------------------------------------------------
 */
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
		free(*n); *n = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Start all over again
 * ------------------------------------------------------------------------
 */
void removeICat(char *path) {
	struct stat st;
	if (stat(path, &st) == 0) remove(path);
}

/* ------------------------------------------------------------------------
 * Make context directory (scope would do it for us!)
 * ------------------------------------------------------------------------
 */
int makectxdir(char *ctxname) {
	struct stat st;
	char *f, *p, *i;

	f = nowdb_path_append(BASE, CTXPATH);
	if (f == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	if (stat(f, &st) != 0) {
		if (mkdir(f, S_IRWXU) != 0) {
			fprintf(stderr, "cannot create %s\n", CTXPATH);
			free(f); return -1;
		}
	}
	p = nowdb_path_append(f, ctxname); free(f);
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

/* ------------------------------------------------------------------------
 * Make vertex directory (scope would do it for us!)
 * ------------------------------------------------------------------------
 */
/*
int makevxdir() {
	struct stat st;
	char *i;
	char *p;

	p = nowdb_path_append(BASE, VXPATH);
	if (p == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}

	if (stat(p, &st) != 0) {
		if (mkdir(p, S_IRWXU) != 0) {
			fprintf(stderr, "cannot create %s\n", VXPATH);
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
*/

/* ------------------------------------------------------------------------
 * Create an index descriptor
 * ------------------------------------------------------------------------
 */
nowdb_index_desc_t *createIndexDesc(char                *name,
                                    nowdb_context_t     *ctx,
                                    nowdb_index_keys_t  *keys,
                                    nowdb_index_t       *idx) {
	nowdb_err_t err;
	nowdb_index_desc_t *desc;

	err = nowdb_index_desc_create(name, ctx, keys, idx, &desc);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return desc;
}

/* ------------------------------------------------------------------------
 * Create an index (it must exist to be able to open the iman)
 * ------------------------------------------------------------------------
 */
int createIndex(char *base, char *path, uint16_t size,
                nowdb_index_desc_t  *desc) {
	nowdb_err_t    err;
	char *p;

	p = nowdb_path_append(path, "index");
	if (p == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	err = nowdb_index_create(base, p, size, desc); free(p);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * Add a context (scope would do this for us)
 * ------------------------------------------------------------------------
 */
int addCtx(ts_algo_tree_t *ctx, char *name) {
	nowdb_context_t *c;

	c = malloc(sizeof(nowdb_context_t));
	if (c == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	c->name = name;
	c->store.cont = NOWDB_CONT_EDGE;
	if (ts_algo_tree_insert(ctx, c) != TS_ALGO_OK) {
		fprintf(stderr, "cannot insert context\n");
		return -1;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * Helper to make the path to the context
 * ------------------------------------------------------------------------
 */
char *makeCtxPath(char *path) {
	char *p1;

	p1 = nowdb_path_append(CTXPATH, path);
	if (p1 == NULL) return NULL;

	return p1;
}

/* ------------------------------------------------------------------------
 * Fake contexts (and paths)
 * ------------------------------------------------------------------------
 */
ts_algo_tree_t *fakeCtx() {
	ts_algo_tree_t *ctx;
	ctx = ts_algo_tree_new(&ctxcompare, NULL, &noupdate,
	                       &ctxdestroy, &ctxdestroy);

	if (makectxdir("CTX_TEST") != 0) {
		ts_algo_tree_destroy(ctx); free(ctx);
		return NULL;
	}
	/* insert some contexts ... */
	if (addCtx(ctx, "CTX_TEST") != 0) {
		ts_algo_tree_destroy(ctx); free(ctx);
		return NULL;
	}
	return ctx;
}

/* ------------------------------------------------------------------------
 * Fake database
 * ------------------------------------------------------------------------
 */
int fakeDB() {
	struct stat st;
	
	if (stat(BASE, &st) != 0) {
		if (mkdir(BASE, S_IRWXU) != 0) {
			fprintf(stderr, "cannot create %s\n", BASE);
			return -1;
		}
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * init iman
 * ------------------------------------------------------------------------
 */
int initMan(nowdb_index_man_t *iman,
            void            *handle,
            ts_algo_tree_t     *ctx) {
	nowdb_err_t err;

	err = nowdb_index_man_init(iman,
	                           ctx,
	                           handle,
	                           BASE,
	                           IMNPATH);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init iman\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * test open/close
 * ------------------------------------------------------------------------
 */
int testOpenClose(nowdb_index_man_t *iman,
                  void            *handle,
                  ts_algo_tree_t     *ctx) {
	if (initMan(iman, handle, ctx) != 0) return -1;
	nowdb_index_man_destroy(iman);
	return 0;
}

/* ------------------------------------------------------------------------
 * make random keys
 * ------------------------------------------------------------------------
 */
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
		x = rand()%5;
		k->off[i] = (uint16_t)(x*8);
	}
	return k;
}

/* ------------------------------------------------------------------------
 * copy keys
 * ------------------------------------------------------------------------
 */
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

/* ------------------------------------------------------------------------
 * register an index
 * ------------------------------------------------------------------------
 */
int testRegister(nowdb_index_man_t  *iman,
                 nowdb_index_desc_t *desc) {
	nowdb_err_t err;

	err = nowdb_index_man_register(iman, desc);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * unregister an index
 * ------------------------------------------------------------------------
 */
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

/* ------------------------------------------------------------------------
 * test getting an index by name and by key
 * ------------------------------------------------------------------------
 */
int testGetIdx(nowdb_index_man_t *iman,
               char             *iname,
               nowdb_context_t    *ctx,
               nowdb_index_keys_t   *k) {
	nowdb_err_t err;
	nowdb_index_desc_t *desc;

	err = nowdb_index_man_getByName(iman, iname, &desc);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	if (desc->idx != NULL) err = nowdb_index_enduse(desc->idx);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	err = nowdb_index_man_getByKeys(iman, ctx, k, &desc);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	if (desc->idx != NULL) err = nowdb_index_enduse(desc->idx);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * destroy keys
 * ------------------------------------------------------------------------
 */
void destroyKeys(nowdb_index_keys_t **k) {
	if (k == NULL) return;
	nowdb_index_keys_destroy(*k);
	*k=NULL;
}

/* ------------------------------------------------------------------------
 * better elaborate some test cases instead of putting everything
 * together here...
 * ------------------------------------------------------------------------
 */
int main() {
	int rc = EXIT_SUCCESS;
	nowdb_index_man_t man;
	void *handle = NULL;
	int haveMan = 0;
	nowdb_index_keys_t *k1=NULL, *c1=NULL;
	nowdb_index_keys_t *k2=NULL, *c2=NULL;
	nowdb_index_keys_t *k3=NULL, *c3=NULL;
	nowdb_index_keys_t *k4=NULL, *c4=NULL;
	char *ctxpath = NULL;
	ts_algo_tree_t *ctx=NULL;
	nowdb_context_t *myctx,tmp;
	nowdb_index_desc_t *desc;

	/* ----------------------------------------------------------------
	 * prepare everything
	 * ----------------------------------------------------------------
	 */
	handle = beet_lib_init(NULL);
	if (handle == NULL) {
		fprintf(stderr, "cannot init lib\n");
		return EXIT_FAILURE;
	}
	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init error\n");
		return EXIT_FAILURE;
	}

	removeICat(IMNPATH);

	if (fakeDB() != 0) {
		fprintf(stderr, "cannot fake database\n");
		return EXIT_FAILURE;
	}

	ctx = fakeCtx();
	if (ctx == NULL) {
		fprintf(stderr, "cannot fake context\n");
		return EXIT_FAILURE;
	}

	/* ----------------------------------------------------------------
	 * Get a fake ctx
	 * ----------------------------------------------------------------
	 */
	tmp.name = "CTX_TEST";
	myctx = ts_algo_tree_find(ctx, &tmp);

	/* ----------------------------------------------------------------
	 * simple open/close test
	 * ----------------------------------------------------------------
	 */
	if (testOpenClose(&man, handle, ctx) != 0) {
		fprintf(stderr, "testOpenClose failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	ctxpath = makeCtxPath("CTX_TEST");
	if (ctxpath == NULL) {
		fprintf(stderr, "out-of-mem making ctxpath\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}

	/* ----------------------------------------------------------------
	 * initialise iman for next series of tests
	 * ----------------------------------------------------------------
	 */
	if (initMan(&man, handle, ctx) != 0) {
		fprintf(stderr, "initMan failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	haveMan = 1;

	/* ----------------------------------------------------------------
	 * create and register idx0
	 * ----------------------------------------------------------------
	 */
	k1 = makekeys(1);
	if (k1 == NULL) return -1;

	c1 = copykeys(k1);
	if (c1 == NULL) {
		destroyKeys(&k1);
		rc = EXIT_FAILURE; goto cleanup;
	}

	desc = createIndexDesc("idx0", myctx, k1, NULL);
	if (desc == NULL) {
		destroyKeys(&k1);
		fprintf(stderr, "cannot create desc for idx0\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (createIndex(BASE, ctxpath, NOWDB_CONFIG_SIZE_TINY, desc) != 0) {
		fprintf(stderr, "create Index failed for idx0\n");
		nowdb_index_desc_destroy(desc); free(desc);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testRegister(&man, desc) != 0) {
		fprintf(stderr, "testRegister failed for k1\n");
		nowdb_index_desc_destroy(desc); free(desc);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "idx0", myctx, c1) != 0) {
		fprintf(stderr, "testGetIdx failed for k1\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	/* ----------------------------------------------------------------
	 * create and register idx1
	 * ----------------------------------------------------------------
	 */
	k2 = makekeys(2);
	if (k2 == NULL) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	c2 = copykeys(k2);
	if (c2 == NULL) {
		destroyKeys(&k2); 
		rc = EXIT_FAILURE; goto cleanup;
	}
	for (int i=0; i<c2->sz; i++) {
		fprintf(stderr, "%hu|", c2->off[i]);
	}
	fprintf(stderr, "\n");

	desc = createIndexDesc("idx1", myctx, k2, NULL);
	if (desc == NULL) {
		destroyKeys(&k2);
		fprintf(stderr, "cannot create desc for idx1\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (createIndex(BASE, ctxpath, NOWDB_CONFIG_SIZE_TINY, desc) != 0) {
		fprintf(stderr, "create Index failed for idx1\n");
		nowdb_index_desc_destroy(desc); free(desc);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testRegister(&man, desc) != 0) {
		fprintf(stderr, "testRegister failed for k2\n");
		nowdb_index_desc_destroy(desc); free(desc);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "idx1", myctx, c2) != 0) {
		fprintf(stderr, "testGetIdx failed for k2\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	
	/* ----------------------------------------------------------------
	 * close and open iman
	 * ----------------------------------------------------------------
	 */
	nowdb_index_man_destroy(&man); haveMan = 0;

	if (initMan(&man, handle, ctx) != 0) {
		fprintf(stderr, "initMan failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	haveMan = 1;
	
	/* ----------------------------------------------------------------
	 * make sure it's still all there
	 * ----------------------------------------------------------------
	 */
	if (testGetIdx(&man, "idx0", myctx, c1) != 0) {
		fprintf(stderr, "testGetIdx failed for k1 (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "idx1", myctx, c2) != 0) {
		fprintf(stderr, "testGetIdx failed for k2 (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	/* ----------------------------------------------------------------
	 * unregister idx0
	 * ----------------------------------------------------------------
	 */
	if (testUnregister(&man, "idx0") != 0) {
		fprintf(stderr, "testUnregister failed for idx0\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "idx0", myctx, c1) == 0) {
		fprintf(stderr, "testGetIdx passed for unregistered idx0\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "idx1", myctx, c2) != 0) {
		fprintf(stderr, "testGetIdx failed for idx1\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	/* ----------------------------------------------------------------
	 * create and register cdx1 (with context)
	 * ----------------------------------------------------------------
	 */
	k3 = makekeys(2);
	if (k3 == NULL) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	c3 = copykeys(k3);
	if (c3 == NULL) {
		destroyKeys(&k3); 
		rc = EXIT_FAILURE; goto cleanup;
	}

	desc = createIndexDesc("cdx1", myctx, k3, NULL);
	if (desc == NULL) {
		fprintf(stderr, "cannot create desc for cdx1\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	if (createIndex(BASE, ctxpath, NOWDB_CONFIG_SIZE_TINY, desc) != 0) {
		fprintf(stderr, "create Index failed for ctx1\n");
		nowdb_index_desc_destroy(desc); free(desc);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testRegister(&man, desc) != 0) {
		fprintf(stderr, "testRegister failed for k3\n");
		nowdb_index_desc_destroy(desc); free(desc);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "cdx1", myctx, c3) != 0) {
		fprintf(stderr, "testGetIdx failed for k3\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	/* ----------------------------------------------------------------
	 * create and register cdx2 (with context)
	 * ----------------------------------------------------------------
	 */
	k4 = makekeys(3);
	if (k4 == NULL) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	c4 = copykeys(k4);
	if (c4 == NULL) {
		destroyKeys(&k4); 
		rc = EXIT_FAILURE; goto cleanup;
	}

	desc = createIndexDesc("cdx2", myctx, k4, NULL);
	if (desc == NULL) {
		fprintf(stderr, "cannot create desc for idx1\n");
		destroyKeys(&k4);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (createIndex(BASE, ctxpath, NOWDB_CONFIG_SIZE_TINY, desc) != 0) {
		fprintf(stderr, "create Index failed for cdx2\n");
		nowdb_index_desc_destroy(desc); free(desc);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testRegister(&man, desc) != 0) {
		fprintf(stderr, "testRegister failed for k4\n");
		nowdb_index_desc_destroy(desc); free(desc);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "cdx2", myctx, c4) != 0) {
		fprintf(stderr, "testGetIdx failed for k4\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	
	/* ----------------------------------------------------------------
	 * close and open iman
	 * ----------------------------------------------------------------
	 */
	nowdb_index_man_destroy(&man); haveMan = 0;

	if (initMan(&man, handle, ctx) != 0) {
		fprintf(stderr, "initMan failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	haveMan = 1;

	/* ----------------------------------------------------------------
	 * make sure it's still all there!
	 * ----------------------------------------------------------------
	 */
	if (testGetIdx(&man, "idx1", myctx, c2) != 0) {
		fprintf(stderr, "testGetIdx failed for k2 (3)\n");
		for (int i=0; i<c2->sz; i++) {
			fprintf(stderr, "%hu|", c2->off[i]);
		}
		fprintf(stderr, "\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "cdx1", myctx, c3) != 0) {
		fprintf(stderr, "testGetIdx failed for k3 (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "cdx2", myctx, c4) != 0) {
		fprintf(stderr, "testGetIdx failed for k4 (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testUnregister(&man, "idx0") == 0) {
		fprintf(stderr, "testUnregister succeeded for idx0\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	/* ----------------------------------------------------------------
	 * unregister cdx1
	 * ----------------------------------------------------------------
	 */
	if (testUnregister(&man, "cdx1") != 0) {
		fprintf(stderr, "testUnregister failed for cdx1\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "cdx1", myctx, c3) == 0) {
		fprintf(stderr, "testGetIdx passed for unregistered cdx1\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "cdx2", myctx, c4) != 0) {
		fprintf(stderr, "testGetIdx failed for k4 (3)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "idx1", myctx, c2) != 0) {
		fprintf(stderr, "testGetIdx failed for k2 (4)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	
	/* ----------------------------------------------------------------
	 * close and open iman
	 * ----------------------------------------------------------------
	 */
	nowdb_index_man_destroy(&man); haveMan = 0;

	if (initMan(&man, handle, ctx) != 0) {
		fprintf(stderr, "initMan failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	haveMan = 1;

	/* ----------------------------------------------------------------
	 * make sure it's still all there!
	 * ----------------------------------------------------------------
	 */
	if (testGetIdx(&man, "cdx2", myctx, c4) != 0) {
		fprintf(stderr, "testGetIdx failed for k4 (4)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetIdx(&man, "idx1", myctx, c2) != 0) {
		fprintf(stderr, "testGetIdx failed for k2 (5)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	if (haveMan) nowdb_index_man_destroy(&man);
	if (ctx != NULL) {
		ts_algo_tree_destroy(ctx); free(ctx);
	}
	if (ctxpath != NULL) free(ctxpath);
	if (handle != NULL) beet_lib_close(handle);
	if (c1 != NULL) destroyKeys(&c1);
	if (c2 != NULL) destroyKeys(&c2);
	if (c3 != NULL) destroyKeys(&c3);
	if (c4 != NULL) destroyKeys(&c4);
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
