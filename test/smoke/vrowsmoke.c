/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for vertex rows
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/time.h>
#include <nowdb/fun/expr.h>
#include <nowdb/reader/vrow.h>

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <math.h>
#include <limits.h>
#include <float.h>

#define IT 100
#define MAXSTR 20

#define VSIZE 999
#define PROPS 3
#define ROLE 9

nowdb_vertex_t vertex[VSIZE];

nowdb_model_vertex_t _v;
nowdb_model_prop_t   _p01;
nowdb_model_prop_t   _p02;
nowdb_model_prop_t   _p03;
nowdb_vertex_helper_t _veval01;
nowdb_vertex_helper_t _veval02;
nowdb_vertex_helper_t _veval03;
nowdb_eval_t _eval;

void initEval() {
	
	_p01.propid = 1001;
	_p01.name   = "PROP01";
	_p01.roleid = ROLE;
	_p01.pos = 0;
	_p01.value = NOWDB_TYP_UINT;
	_p01.pk = TRUE;

	_p02.propid = 1002;
	_p02.name   = "PROP02";
	_p02.roleid = ROLE;
	_p02.pos = 1;
	_p02.value = NOWDB_TYP_UINT;
	_p02.pk = FALSE;

	_p03.propid = 1003;
	_p03.name   = "PROP03";
	_p03.roleid = ROLE;
	_p03.pos = 2;
	_p03.value = NOWDB_TYP_UINT;
	_p03.pk = FALSE;

	_v.name = "VÖRTEX";
	_v.roleid = ROLE;
	_v.vid = NOWDB_TYP_UINT;

	_veval01.v = &_v;
	_veval01.p = &_p01;

	_veval02.v = &_v;
	_veval02.p = &_p02;

	_veval03.v = &_v;
	_veval03.p = &_p03;

	_eval.ce = NULL;
	_eval.cv = NULL;
	_eval.model = NULL;
	_eval.text = NULL;
	_eval.tlru = NULL;

	ts_algo_list_init(&_eval.em);
	ts_algo_list_init(&_eval.vm);
	ts_algo_list_append(&_eval.vm, &_veval01);
	ts_algo_list_append(&_eval.vm, &_veval02);
	ts_algo_list_append(&_eval.vm, &_veval03);
}

void destroyEval() {
	ts_algo_list_destroy(&_eval.vm);
	ts_algo_list_destroy(&_eval.em);
}

void initVertex() {
	memset(vertex,0,VSIZE);
	for(int i=0; i<VSIZE;i+=3) {

		vertex[i].vertex = 1000001 + i;
		vertex[i].property = 1001;
		vertex[i].value = rand()%1000000;
		vertex[i].role  = ROLE;
		vertex[i].vtype = NOWDB_TYP_UINT;

		vertex[i+1].vertex = 1000001 + i;
		vertex[i+1].property = 1002;
		vertex[i+1].value = rand()%1000000;
		vertex[i+1].role  = ROLE;
		vertex[i+1].vtype = NOWDB_TYP_UINT;

		vertex[i+2].vertex = 1000001 + i;
		vertex[i+2].property = 1003;
		vertex[i+2].value = rand()%1000000;
		vertex[i+2].role  = ROLE;
		vertex[i+2].vtype = NOWDB_TYP_UINT;
	}
}

nowdb_vrow_t *createVrow(nowdb_expr_t filter) {
	nowdb_vrow_t *vrow;
	nowdb_err_t err;

	err = nowdb_vrow_fromFilter(ROLE, &vrow, filter, &_eval);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create VRow: ");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return vrow;
}

nowdb_expr_t mkFilter(int idx) {
	nowdb_err_t err;
	nowdb_expr_t z=NULL, p=NULL;
	nowdb_expr_t r=NULL, c=NULL;
	nowdb_expr_t q1=NULL, q2=NULL, q3=NULL;
	nowdb_expr_t a1=NULL, a2=NULL, a3=NULL;
	nowdb_roleid_t role=ROLE;


	// and(=(0,ROLE), and(=(p1,val), and(=(p2,val), =(p3,val))))
	// 0 = ROLE
	err = nowdb_expr_newVertexOffField(&z, NOWDB_OFF_ROLE);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create role field\n");
		goto error;
	}

	err = nowdb_expr_newConstant(&r, &role, NOWDB_TYP_SHORT);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create role constant\n");
		goto error;
	}

	err = nowdb_expr_newOp(&q1, NOWDB_EXPR_OP_EQ, z, r);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create equal 1\n");
		goto error;
	}

	z=NULL; r=NULL;

	// p3 = val
	err = nowdb_expr_newVertexField(&p, _p03.name, ROLE,
	                                    _p03.propid);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create prop field\n");
		goto error;
	}
	NOWDB_EXPR_TOFIELD(p)->type = _p03.value;

	err = nowdb_expr_newConstant(&c, &vertex[idx+2].value,
	                                       NOWDB_TYP_UINT);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create prop constant\n");
		goto error;
	}

	err = nowdb_expr_newOp(&q2, NOWDB_EXPR_OP_EQ, p, c);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create equal 2\n");
		goto error;
	}

	p=NULL; c=NULL;

	// p2 = val
	err = nowdb_expr_newVertexField(&p, _p02.name, ROLE,
	                                    _p02.propid);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create prop field\n");
		goto error;
	}
	NOWDB_EXPR_TOFIELD(p)->type = _p02.value;

	err = nowdb_expr_newConstant(&c, &vertex[idx+1].value,
	                                      NOWDB_TYP_UINT);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create prop constant\n");
		goto error;
	}

	err = nowdb_expr_newOp(&q3, NOWDB_EXPR_OP_EQ, p, c);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create equal 3\n");
		goto error;
	}

	p=NULL; c=NULL;

	// and (q2, q3)
	err = nowdb_expr_newOp(&a1, NOWDB_EXPR_OP_AND, q2, q3);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create and 3\n");
		goto error;
	}

	q2=NULL; q3=NULL;

	// p1 = val
	err = nowdb_expr_newVertexField(&p, _p01.name, ROLE,
	                                    _p01.propid);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create prop field\n");
		goto error;
	}
	NOWDB_EXPR_TOFIELD(p)->type = _p01.value;

	err = nowdb_expr_newConstant(&c, &vertex[idx].value,
	                                     NOWDB_TYP_UINT);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create prop constant\n");
		goto error;
	}

	err = nowdb_expr_newOp(&q2, NOWDB_EXPR_OP_EQ, p, c);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create equal 1\n");
		goto error;
	}

	p=NULL; c=NULL;

	// and (q1, a1)
	err = nowdb_expr_newOp(&a2, NOWDB_EXPR_OP_AND, q2, a1);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create and 3\n");
		goto error;
	}

	q2=NULL; a1=NULL;

	// and (q1, a2)
	err = nowdb_expr_newOp(&a1, NOWDB_EXPR_OP_AND, q1, a2);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create and 3\n");
		goto error;
	}

	q1=NULL; a2=NULL;

	nowdb_expr_show(a1, stderr); fprintf(stderr, "\n");
	return a1;

error:
	nowdb_err_print(err); nowdb_err_release(err);
	if (z!=NULL) {
		nowdb_expr_destroy(z); free(z);
	}
	if (r!=NULL) {
		nowdb_expr_destroy(r); free(r);
	}
	if (p!=NULL) {
		nowdb_expr_destroy(p); free(p);
	}
	if (c!=NULL) {
		nowdb_expr_destroy(c); free(c);
	}
	if (a1!=NULL) {
		nowdb_expr_destroy(a1); free(a1);
	}
	if (a2!=NULL) {
		nowdb_expr_destroy(a2); free(a2);
	}
	if (a3!=NULL) {
		nowdb_expr_destroy(a3); free(a3);
	}
	if (q1!=NULL) {
		nowdb_expr_destroy(q1); free(q1);
	}
	if (q2!=NULL) {
		nowdb_expr_destroy(q2); free(q2);
	}
	if (q3!=NULL) {
		nowdb_expr_destroy(q3); free(q3);
	}
	return NULL;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_err_t err;
	nowdb_vrow_t *vrow=NULL;
	nowdb_expr_t  filter=NULL;
	nowdb_key_t vid;
	int idx;
	char ok, found, x;

	srand(time(NULL));

	if (!nowdb_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}

	initEval();
	initVertex();

	for(int i=0;i<IT;i++) {
		idx=rand()%VSIZE;
		idx /= 3; idx *= 3;

		filter = mkFilter(idx);
		if (filter == NULL) {
			fprintf(stderr, "cannot create filter\n");
			rc = EXIT_FAILURE;
			goto cleanup;
		}
	
		vrow = createVrow(filter);
		if (vrow == NULL) {
			fprintf(stderr, "cannot create VRow\n");
			rc = EXIT_FAILURE;
			goto cleanup;
		}

		vid = 0; found=0; x=0;
		for(int z=0;z<VSIZE;z++) {
			if (vertex[z].vertex == vertex[idx].vertex) {
				fprintf(stderr, "adding %lu %lu %lu\n",
				        vertex[z].vertex,
				        vertex[z].property,
				        vertex[z].value);
			}
			err = nowdb_vrow_add(vrow, vertex+z, &ok);
			if (err != NOWDB_OK) {
				fprintf(stderr, "cannot add: ");
				nowdb_err_print(err);
				nowdb_err_release(err);
				rc = EXIT_FAILURE;
				goto cleanup;
			}
			if (ok) {
				err = nowdb_vrow_eval(vrow, &vid, &x);
				if (err != NOWDB_OK) {
					fprintf(stderr, "cannot evaluate: ");
					nowdb_err_print(err);
					nowdb_err_release(err);
					rc = EXIT_FAILURE;
					goto cleanup;
				}
				if (vertex[z].vertex == vertex[idx].vertex) {
					fprintf(stderr, "evaluated: %lu (%d)\n", vid, x);
				}
				if (x) {
					fprintf(stderr, "success: %lu / %lu\n", vid, vertex[idx].vertex); 
					if (vid == vertex[idx].vertex) found=1;
					fprintf(stderr, "FOUND: %lu\n", vid);
					x=0;
				}
			}
		}
		if (!found) {
			fprintf(stderr, "did not find my vid\n");
			rc = EXIT_FAILURE;
			goto cleanup;
		}
		nowdb_vrow_destroy(vrow); free(vrow); vrow = NULL;
		nowdb_expr_destroy(filter); free(filter); filter= NULL;
	}

cleanup:
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	destroyEval();
	if (vrow != NULL) {
		nowdb_vrow_destroy(vrow); free(vrow);
	}
	if (filter != NULL) {
		nowdb_expr_destroy(filter); free(filter);
	}
	nowdb_close();
	return rc;
}

