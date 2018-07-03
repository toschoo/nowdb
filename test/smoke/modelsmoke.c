/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for model
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/io/dir.h>
#include <nowdb/model/types.h>
#include <nowdb/model/model.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define MPATH "rsc/model10"

int preparePath() {
	nowdb_err_t err;
	struct stat  st;

	if (stat(MPATH, &st) == 0) {
		err = nowdb_path_rRemove(MPATH);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return -1;
		}
	}
	if (mkdir(MPATH, NOWDB_DIR_MODE) != 0) {
		perror("cannot create path");
		return -1;
	}
	return 0;
}

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

nowdb_model_t *openModel(char *path) {
	nowdb_err_t err;
	nowdb_model_t *model;

	fprintf(stderr, "opening model\n");

	model = mkModel(path);
	if (model == NULL) return NULL;

	err = nowdb_model_load(model);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_model_destroy(model);
		free(model);
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
		free(v->name); free(v);
		return -1;
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

int addEdge(nowdb_model_t *model,
            nowdb_key_t   edgeid,
            char          *name) {
	nowdb_err_t err;
	nowdb_model_edge_t *e;

	fprintf(stderr, "adding edge %lu - %s\n", edgeid, name);

	e = calloc(1,sizeof(nowdb_edge_t));
	if (e == NULL) {
		perror("out-of-mem");
		return -1;
	}
	e->edgeid = edgeid;
	e->origin = 1;
	e->destin = 2;
	e->edge = NOWDB_MODEL_TEXT;
	e->label= NOWDB_MODEL_NUM;
	e->weight = NOWDB_TYP_UINT;
	e->weight2 = NOWDB_TYP_NOTHING;
	e->name = strdup(name);
	if (e->name == NULL) {
		perror("out-of-mem");
		return -1;
	}
	err = nowdb_model_addEdge(model, e);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);	
		free(e->name); free(e);
		return -1;
	}
	return 0;
}

int removeEdge(nowdb_model_t *model,
               nowdb_key_t   edgeid) {
	nowdb_err_t err;

	fprintf(stderr, "removing edge %lu\n", edgeid);

	err = nowdb_model_removeEdge(model, edgeid);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int testGetEdge(nowdb_model_t *model,
                nowdb_key_t     edge,
                char           *name) {
	nowdb_err_t err;
	nowdb_model_edge_t *e;

	err = nowdb_model_getEdgeById(model, edge, &e);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);	
		return -1;
	}
	if (e == NULL) {
		fprintf(stderr, "no edge\n");
		return -1;
	}
	if (strcmp(e->name, name) != 0) {
		fprintf(stderr, "wrong edge: %s\n", e->name);
		return -1;
	}
	e = NULL;
	err = nowdb_model_getEdgeByName(model, name, &e);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);	
		return -1;
	}
	if (e == NULL) {
		fprintf(stderr, "no edge by this name\n");
		return -1;
	}
	if (e->edgeid != edge) {
		fprintf(stderr, "wrong edge: %lu\n", e->edgeid);
		return -1;
	}
	return 0;
}

int addProp(nowdb_model_t  *model,
            nowdb_key_t    propid,
            nowdb_roleid_t roleid,
            char           *name) {
	nowdb_err_t err;
	nowdb_model_prop_t *p;

	fprintf(stderr, "adding property %lu - %s\n", propid, name);

	p = calloc(1,sizeof(nowdb_model_prop_t));
	if (p == NULL) {
		perror("out-of-mem");
		return -1;
	}
	p->propid = propid;
	p->roleid = roleid;
	p->prop = NOWDB_MODEL_TEXT;
	p->value  = NOWDB_TYP_UINT;
	p->name = strdup(name);
	if (p->name == NULL) {
		perror("out-of-mem");
		return -1;
	}
	err = nowdb_model_addProperty(model, p);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);	
		free(p->name); free(p);
		return -1;
	}
	return 0;
}

int removeProp(nowdb_model_t  *model,
               nowdb_key_t    propid,
               nowdb_roleid_t roleid) {
	nowdb_err_t err;

	fprintf(stderr, "removing property %u.%lu\n", roleid, propid);

	err = nowdb_model_removeProperty(model, roleid, propid);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int testGetProp(nowdb_model_t  *model,
                nowdb_key_t    propid,
                nowdb_roleid_t roleid,
                char            *name) {
	nowdb_err_t err;
	nowdb_model_prop_t *p;

	err = nowdb_model_getPropById(model, roleid, propid, &p);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);	
		return -1;
	}
	if (p == NULL) {
		fprintf(stderr, "no prop\n");
		return -1;
	}
	if (strcmp(p->name, name) != 0) {
		fprintf(stderr, "wrong prop: %s\n", p->name);
		return -1;
	}
	p = NULL;
	err = nowdb_model_getPropByName(model, roleid, name, &p);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);	
		return -1;
	}
	if (p == NULL) {
		fprintf(stderr, "no prop by this name\n");
		return -1;
	}
	if (p->propid != propid) {
		fprintf(stderr, "wrong prop: %lu\n", p->propid);
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
	if (preparePath() != 0) {
		fprintf(stderr, "FAILED: prepare path\n");
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
	if (addVertex(model, 5, "V5") != 0) {
		fprintf(stderr, "add vertex 5, V5 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 5, "V5") != 0) {
		fprintf(stderr, "get vertex 5, V5 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (removeVertex(model, 5) != 0) {
		fprintf(stderr, "remove vertex 5 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 5, "V5") == 0) {
		fprintf(stderr, "get removed vertex 5, V5 passed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addEdge(model, 112, "one2two") != 0) {
		fprintf(stderr, "add edge 112 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetEdge(model, 112, "one2two") != 0) {
		fprintf(stderr, "get edge 112 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addEdge(model, 113, "one2three") != 0) {
		fprintf(stderr, "add edge 113 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetEdge(model, 113, "one2three") != 0) {
		fprintf(stderr, "get edge 113 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addEdge(model, 123, "two2three") != 0) {
		fprintf(stderr, "add edge 123 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetEdge(model, 123, "two2three") != 0) {
		fprintf(stderr, "get edge 123 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addEdge(model, 123, "two2five") == 0) {
		fprintf(stderr, "add edge 123 passed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addEdge(model, 125, "two2three") == 0) {
		fprintf(stderr, "add edge 125 passed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addEdge(model, 127, "two2seven") != 0) {
		fprintf(stderr, "add edge 127 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetEdge(model, 127, "two2seven") != 0) {
		fprintf(stderr, "get edge 127 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (removeEdge(model, 127) != 0) {
		fprintf(stderr, "remove edge 127 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetEdge(model, 127, "two2seven") == 0) {
		fprintf(stderr, "get removed edge 127 passed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addProp(model, 12, 2, "P12") != 0) {
		fprintf(stderr, "add prop 12 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetProp(model, 12, 2, "P12") != 0) {
		fprintf(stderr, "get prop P12 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addProp(model, 22, 2, "P22") != 0) {
		fprintf(stderr, "add prop 22 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetProp(model, 22, 2, "P22") != 0) {
		fprintf(stderr, "get prop P22 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addProp(model, 32, 2, "P32") != 0) {
		fprintf(stderr, "add prop 32 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetProp(model, 32, 2, "P32") != 0) {
		fprintf(stderr, "get prop P32 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addProp(model, 22, 1, "P22") != 0) {
		fprintf(stderr, "add prop 22 on 1 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetProp(model, 22, 1, "P22") != 0) {
		fprintf(stderr, "get prop P22 on 1 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addProp(model, 22, 1, "P22") == 0) {
		fprintf(stderr, "add prop 22 on 1 passed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetProp(model, 22, 1, "P22") != 0) {
		fprintf(stderr, "get prop P22 on 1 failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (addProp(model, 43, 3, "P43") != 0) {
		fprintf(stderr, "add prop 43 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetProp(model, 43, 3, "P43") != 0) {
		fprintf(stderr, "get prop P43 on 3 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (removeProp(model, 43, 3) != 0) {
		fprintf(stderr, "remove prop 43 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetProp(model, 43, 3, "P43") == 0) {
		fprintf(stderr, "get removed prop P43 passed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	/* destroy and open again */
	nowdb_model_destroy(model); free(model);
	model = openModel(MPATH);
	if (model == NULL) {
		fprintf(stderr, "cannot open model\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 1, "V1") != 0) {
		fprintf(stderr, "get vertex 1, V1 failed (3)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 2, "V2") != 0) {
		fprintf(stderr, "get vertex 2, V2 failed (3)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 3, "V3") != 0) {
		fprintf(stderr, "get vertex 3, V3 failed (3)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetEdge(model, 112, "one2two") != 0) {
		fprintf(stderr, "get edge 112 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetEdge(model, 113, "one2three") != 0) {
		fprintf(stderr, "get edge 113 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetEdge(model, 123, "two2three") != 0) {
		fprintf(stderr, "get edge 123 failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetProp(model, 12, 2, "P12") != 0) {
		fprintf(stderr, "get prop P12 failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetProp(model, 22, 2, "P22") != 0) {
		fprintf(stderr, "get prop P22 failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetProp(model, 32, 2, "P32") != 0) {
		fprintf(stderr, "get prop P32 failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetProp(model, 22, 1, "P22") != 0) {
		fprintf(stderr, "get prop P22 on 1 failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetProp(model, 43, 3, "P43") == 0) {
		fprintf(stderr, "get removed prop P43 passed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetEdge(model, 127, "two2seven") == 0) {
		fprintf(stderr, "get removed edge 127 passed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testGetVertex(model, 5, "V5") == 0) {
		fprintf(stderr, "get removed vertex 5, V5 passed (2)\n");
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

