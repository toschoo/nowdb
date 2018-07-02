/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for model
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/model/types.h>
#include <nowdb/model/model.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define MPATH "rsc/model10"

nowdb_model_t *mkModel(char *path) {
	nowdb_err_t err;
	nowdb_model_t *model;

	model = calloc(1, sizeof(nowdb_model_t));
	if (model == NULL) {
		perror("out-of-mem\n");
		return NULL;
	}

	err = nowdb_model_init(model, path);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);	
		return NULL;
	}
	return model;
}

int addVertex(nowdb_model_t *model,
              nowdb_roleid_t role,
              char          *name) {
	nowdb_err_t err;
	nowdb_model_vertex_t *v;

	fprintf(stderr, "adding vertex %u - %s\n", role, name);

	v = calloc(1,sizeof(nowdb_vertex_t));
	if (v == NULL) {
		perror("out-of-mem");
		return -1;
	}
	v->roleid = role;
	v->vid = NOWDB_MODEL_TEXT;
	v->name = strdup(name);
	if (v->name == NULL) {
		perror("out-of-mem");
		return -1;
	}
	err = nowdb_model_addVertex(model, v);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);	
		free(v); return -1;
	}
	return 0;
}

int removeVertex(nowdb_model_t *model,
                 nowdb_roleid_t  role) {
	nowdb_err_t err;

	fprintf(stderr, "removing vertex %u\n", role);

	err = nowdb_model_removeVertex(model, role);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int testGetVertex(nowdb_model_t *model,
                  nowdb_roleid_t role,
                  char          *name) {
	nowdb_err_t err;
	nowdb_model_vertex_t *v;

	err = nowdb_model_getVertexById(model, role, &v);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);	
		return -1;
	}
	if (v == NULL) {
		fprintf(stderr, "no vertex\n");
		return -1;
	}
	if (strcmp(v->name, name) != 0) {
		fprintf(stderr, "wrong vertex: %s\n", v->name);
		return -1;
	}
	v = NULL;
	err = nowdb_model_getVertexByName(model, name, &v);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);	
		return -1;
	}
	if (v == NULL) {
		fprintf(stderr, "no vertex\n");
		return -1;
	}
	if (v->roleid != role) {
		fprintf(stderr, "wrong vertex: %u\n", v->roleid);
		return -1;
	}
	return 0;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_model_t *model = NULL;

	if (!nowdb_init()) {
		fprintf(stderr, "FAILED: cannot init\n");
		return EXIT_FAILURE;
	}

	model = mkModel(MPATH);
	if (model == NULL) {
		fprintf(stderr, "cannot make model\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addVertex(model, 1, "V1") != 0) {
		fprintf(stderr, "add vertex 1, V1 failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 1, "V1") != 0) {
		fprintf(stderr, "get vertex 1, V1 failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (removeVertex(model, 1) != 0) {
		fprintf(stderr, "remove vertex 1, V1 failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 1, "V1") == 0) {
		fprintf(stderr, "get vertex 1, V1 passed after remove\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (removeVertex(model, 1) == 0) {
		fprintf(stderr, "remove vertex 1, V1 passed after remove\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addVertex(model, 1, "V1") != 0) {
		fprintf(stderr, "add vertex 1, V1 failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 1, "V1") != 0) {
		fprintf(stderr, "get vertex 1, V1 failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addVertex(model, 2, "V2") != 0) {
		fprintf(stderr, "add vertex 2, V2 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 2, "V2") != 0) {
		fprintf(stderr, "get vertex 2, V2 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addVertex(model, 3, "V3") != 0) {
		fprintf(stderr, "add vertex 3, V3 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 3, "V3") != 0) {
		fprintf(stderr, "get vertex 3, V3 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addVertex(model, 4, "V3") == 0) {
		fprintf(stderr, "add vertex 4, V3 passed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addVertex(model, 3, "V4") == 0) {
		fprintf(stderr, "add vertex 3, V4 passed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 4, "V3") == 0) {
		fprintf(stderr, "get vertex 4, V3 passed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 3, "V3") != 0) {
		fprintf(stderr, "get vertex 3, V3 failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	if (model != NULL) {
		nowdb_model_destroy(model); free(model);
	}
	nowdb_close();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}

