/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for stored procedure manager 
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/scope/procman.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/stat.h>

#define PATH "rsc/pman10"
#define PMAN "rsc/pman10/pcat"

int preparepm(char *path) {
	struct stat st;

	if (stat(path, &st) == 0) {
		remove(PMAN);
		if (remove(path) != 0) {
			fprintf(stderr, "cannot remove path\n");
			return -1;
		}
	}
	if (mkdir(path, S_IRWXU) != 0) {
		fprintf(stderr, "cannot create path\n");
		return -1;
	}
	return 0;
}

nowdb_procman_t *makepm(char *path) {
	nowdb_err_t err;
	nowdb_procman_t *pm;

	err = nowdb_procman_new(&pm, path);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create procman:\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return pm;
}

int testLoad(nowdb_procman_t *pm) {
	nowdb_err_t err;

	err = nowdb_procman_load(pm);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot load pm\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

char *rstring(int sz) {
	int  n;
	char *str;

	do {n = rand()%sz;} while (n == 0);

	str = malloc(n+1);
	if (str == NULL) return NULL;

	for (int i=0; i<n; i++) {
		str[i] = rand()%25;
		str[i] += 65;
	}
	str[n] = 0;
	return str;
}

int makeArgs(nowdb_proc_desc_t *pd,
             uint16_t         argn) {

	pd->argn = argn;

	pd->args = calloc(argn, sizeof(nowdb_proc_arg_t));
	if (pd->args == NULL) {
		fprintf(stderr, "out-of-mem allocating args\n");
		return -1;
	}
	for(uint16_t i = 0; i<argn; i++) {
		pd->args[i].pos = i;
		pd->args[i].typ = (uint16_t)NOWDB_TYP_UINT;
		pd->args[i].name = rstring(10);
		if (pd->args[i].name == NULL) {
			fprintf(stderr, "out-of-mem allocating string\n");
			return -1;
		}
		fprintf(stderr, "arg %hu: %s\n", i, pd->args[i].name);
	}
	return 0;
}

int testAdd(nowdb_procman_t *pm,
            char          *name,
            char        *module,
            uint16_t       argn) {
	nowdb_proc_desc_t *pd;
	nowdb_err_t err;

	pd = calloc(1,sizeof(nowdb_proc_desc_t));
	if (pd == NULL) {
		fprintf(stderr, "out-of-mem allocating proc desc\n");
		return -1;
	}
	pd->name = strdup(name);
	if (pd->name == NULL) {
		fprintf(stderr, "out-of-mem allocating name\n");
		free(pd); return -1;
	}
	pd->module = strdup(module);
	if (pd->module == NULL) {
		fprintf(stderr, "out-of-mem allocating module\n");
		nowdb_proc_desc_destroy(pd);
		free(pd); return -1;
	}
	pd->type = NOWDB_STORED_PROC;
	pd->lang = NOWDB_STORED_PYTHON;
	pd->argn = 0;

	if (argn == 0) {
		pd->args = NULL;
	} else {
		if (makeArgs(pd, argn) != 0) {
			fprintf(stderr, "cannot make args\n");
			nowdb_proc_desc_destroy(pd);
			free(pd); return -1;
		}
	}

	err = nowdb_procman_add(pm, pd);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot add pm\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_proc_desc_destroy(pd); free(pd);
		return -1;
	}
	return 0;
}

int testAdd0(nowdb_procman_t *pm,
             char          *name,
             char        *module) {
	return testAdd(pm, name, module, 0);
}

int testGet(nowdb_procman_t *pm,
            char          *name) {
	nowdb_err_t err;
	nowdb_proc_desc_t *pd;

	fprintf(stderr, "getting %s\n", name);
	err = nowdb_procman_get(pm, name, &pd);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot get %s\n", name);
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	if (strcmp(pd->name, name) != 0) {
		fprintf(stderr, "got wrong one %s != %s\n", name, pd->name);
		nowdb_proc_desc_destroy(pd); free(pd);
		return -1;
	}
	nowdb_proc_desc_destroy(pd); free(pd);
	return 0;
}

int testRemove(nowdb_procman_t *pm,
               char          *name) {
	nowdb_err_t err;

	fprintf(stderr, "removing %s\n", name);
	err = nowdb_procman_remove(pm, name);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot remove %s\n", name);
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_procman_t *pm=NULL;

	srand(time(NULL) & (uint64_t)&printf);

	if (!nowdb_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}

	if (preparepm(PATH) != 0) {
		fprintf(stderr, "preparepm failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}

	pm = makepm(PATH);
	if (pm == NULL) {
		fprintf(stderr, "makepm failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testLoad(pm) != 0) {
		fprintf(stderr, "load failed (1)\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testAdd0(pm, "recommend", "sales") != 0) {
		fprintf(stderr, "testAdd0 failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	nowdb_procman_destroy(pm); free(pm);
	pm = makepm(PATH);
	if (pm == NULL) {
		fprintf(stderr, "makepm failed (2)\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testLoad(pm) != 0) {
		fprintf(stderr, "load failed (2)\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testGet(pm, "recommend") != 0) {
		fprintf(stderr, "testGet failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testGet(pm, "nosuchproc") == 0) {
		fprintf(stderr, "testGet passed with nosuchproc\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testAdd(pm, "promote", "sales", 3) != 0) {
		fprintf(stderr, "testAdd0 'promote' failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testAdd(pm, "sell", "sales", 2) != 0) {
		fprintf(stderr, "testAdd0 'sell' failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testAdd(pm, "post_sales", "sales",1) != 0) {
		fprintf(stderr, "testAdd0 'post_sales' failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testAdd0(pm, "bad_one", "sales") != 0) {
		fprintf(stderr, "testAdd0 'bad_one' failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	/*
	if (testAdd(pm, "big_one", "sales", 99) != 0) {
		fprintf(stderr, "testAdd0 'big_one' failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	*/
	if (testGet(pm, "promote") != 0) {
		fprintf(stderr, "testGet failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	nowdb_procman_destroy(pm); free(pm);
	pm = makepm(PATH);
	if (pm == NULL) {
		fprintf(stderr, "makepm failed (3)\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testLoad(pm) != 0) {
		fprintf(stderr, "load failed (3)\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	fprintf(stderr, "have: %d\n", pm->procs->count);
	if (testGet(pm, "promote") != 0) {
		fprintf(stderr, "testGet failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testGet(pm, "post_sales") != 0) {
		fprintf(stderr, "testGet failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testGet(pm, "sell") != 0) {
		fprintf(stderr, "testGet failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testGet(pm, "recommend") != 0) {
		fprintf(stderr, "testGet failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	/*
	if (testGet(pm, "big_one") != 0) {
		fprintf(stderr, "testGet failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	*/
	if (testGet(pm, "bad_one") != 0) {
		fprintf(stderr, "testGet failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testRemove(pm, "bad_one") != 0) {
		fprintf(stderr, "testRemove failed bad_one\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (testGet(pm, "bad_one") == 0) {
		fprintf(stderr, "testGet passed for bad_one\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}

cleanup:
	if (pm != NULL) {
		nowdb_procman_destroy(pm); free(pm);
	}
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	nowdb_close();
	return rc;
}

