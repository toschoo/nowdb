/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for vertex rows
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/time.h>
#include <nowdb/reader/filter.h>
#include <nowdb/reader/vrow.h>

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <math.h>
#include <limits.h>
#include <float.h>

#define IT 10
#define MAXSTR 20

#define VSIZE 999
#define PROPS 3
#define ROLE 9

nowdb_vertex_t vertex[VSIZE];

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

nowdb_vrow_t *createVrow(nowdb_filter_t *filter) {
	nowdb_vrow_t *vrow;
	nowdb_err_t err;

	err = nowdb_vrow_fromFilter(ROLE, &vrow, filter);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create VRow: ");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return vrow;
}

nowdb_filter_t *makeRole() {
	nowdb_err_t err;
	nowdb_filter_t *f;
	nowdb_roleid_t *r;

	r = malloc(4);
	if (r == NULL) {
		fprintf(stderr, "cannot allocate role\n");
		return NULL;
	}
	*r = ROLE;

	err = nowdb_filter_newCompare(&f, NOWDB_FILTER_EQ,
	                                  NOWDB_OFF_ROLE, 4,
	                                  NOWDB_TYP_UINT, 
	                                  r, NULL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot make role filter\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(r);
		return NULL;
	}
	nowdb_filter_own(f);
	return f;
}

nowdb_filter_t *makeProp(nowdb_key_t prop, nowdb_value_t val) {
	nowdb_err_t err;
	nowdb_filter_t *f, *p, *v;
	nowdb_key_t *myprop;
	void *myval;

	myprop = malloc(8);
	if (myprop == NULL) {
		fprintf(stderr, "cannot allocate prop\n");
		return NULL;
	}
	memcpy(myprop, &prop, 8);

	myval = malloc(8);
	if (myval == NULL) {
		fprintf(stderr, "cannot allocate val\n");
		free(myprop);
		return NULL;
	}
	memcpy(myval, &val, 8);

	err = nowdb_filter_newCompare(&p, NOWDB_FILTER_EQ,
	                                  NOWDB_OFF_PROP, 8,
	                                  NOWDB_TYP_UINT, 
	                                  myprop, NULL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot make prop filter\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(myprop); free(myval);
		return NULL;
	}
	nowdb_filter_own(p);

	err = nowdb_filter_newCompare(&v, NOWDB_FILTER_EQ,
	                                  NOWDB_OFF_VALUE, 8,
	                                  NOWDB_TYP_UINT, // vary
	                                  myval, NULL);
	if (err != NOWDB_OK) {
		nowdb_filter_destroy(p); free(p);
		fprintf(stderr, "cannot make val filter\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(myval);
		return NULL;
	}
	nowdb_filter_own(v);

	err = nowdb_filter_newBool(&f, NOWDB_FILTER_AND);
	if (err != NOWDB_OK) {
		nowdb_filter_destroy(p); free(p);
		nowdb_filter_destroy(v); free(v);
		fprintf(stderr, "cannot make bool filter\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	f->left = p; f->right = v;
	return f;
}

nowdb_filter_t *makeAnd(nowdb_filter_t *f1, nowdb_filter_t *f2) {
	nowdb_err_t err;
	nowdb_filter_t *and;

	err = nowdb_filter_newBool(&and, NOWDB_FILTER_AND);
	if (err != NOWDB_OK) return NULL;

	and->left = f1; and->right = f2;
	return and;
}

nowdb_filter_t *mkFilter(int idx) {
	nowdb_filter_t *r, *p1, *p2, *p3;
	nowdb_filter_t *a1, *a2, *a3;

	// make role
	r = makeRole();
	if (r == NULL) return NULL;
	
	// make prop/value 1
	p1 = makeProp(vertex[idx].property, vertex[idx].value);
	if (p1 == NULL) {
		nowdb_filter_destroy(r); free(r);
		return NULL;
	}

	// make prop/value 2
	p2 = makeProp(vertex[idx+1].property, vertex[idx+1].value);
	if (p2 == NULL) {
		nowdb_filter_destroy(r); free(r);
		nowdb_filter_destroy(p1); free(p1);
		return NULL;
	}

	// make prop/value 3
	p3 = makeProp(vertex[idx+2].property, vertex[idx+2].value);
	if (p3 == NULL) {
		nowdb_filter_destroy(r); free(r);
		nowdb_filter_destroy(p1); free(p1);
		nowdb_filter_destroy(p2); free(p2);
		return NULL;
	}

	a1 = makeAnd(p2, p3);
	if (a1 == NULL) {
		nowdb_filter_destroy(r); free(r);
		nowdb_filter_destroy(p1); free(p1);
		nowdb_filter_destroy(p2); free(p2);
		nowdb_filter_destroy(p3); free(p3);
		return NULL;
	}

	a2 = makeAnd(p1, a1);
	if (a2 == NULL) {
		nowdb_filter_destroy(r); free(r);
		nowdb_filter_destroy(p1); free(p1);
		nowdb_filter_destroy(p2); free(p2);
		nowdb_filter_destroy(p3); free(p3);
		nowdb_filter_destroy(a1); free(a1);
		return NULL;
	}

	a3 = makeAnd(r, a2);
	if (a3 == NULL) {
		nowdb_filter_destroy(r); free(r);
		nowdb_filter_destroy(p1); free(p1);
		nowdb_filter_destroy(p2); free(p2);
		nowdb_filter_destroy(p3); free(p3);
		nowdb_filter_destroy(a1); free(a1);
		nowdb_filter_destroy(a2); free(a2);
		return NULL;
	}

	nowdb_filter_show(a3, stderr); fprintf(stderr, "\n");
	return a3;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_err_t err;
	nowdb_vrow_t *vrow=NULL;
	nowdb_filter_t *filter=NULL;
	nowdb_key_t vid;
	int idx;
	char ok, found;

	srand(time(NULL));

	if (!nowdb_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}

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

		vid = 0; found=0;
		for(int z=0;z<VSIZE;z++) {
			// fprintf(stderr, "adding %lu\n", vertex[z].vertex);
			err = nowdb_vrow_add(vrow, vertex+z, &ok);
			if (err != NOWDB_OK) {
				fprintf(stderr, "cannot add: ");
				nowdb_err_print(err);
				nowdb_err_release(err);
				rc = EXIT_FAILURE;
				goto cleanup;
			}
			if (ok) {
				if (nowdb_vrow_eval(vrow, &vid)) {
					if (vid == vertex[idx].vertex) found=1;
					fprintf(stderr, "FOUND: %lu\n", vid);
				}
			}
		}
		if (!found) {
			fprintf(stderr, "did not find my vid\n");
			rc = EXIT_FAILURE;
			goto cleanup;
		}
		nowdb_vrow_destroy(vrow); free(vrow); vrow = NULL;
		nowdb_filter_destroy(filter); free(filter); filter= NULL;
	}

cleanup:
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	if (vrow != NULL) {
		nowdb_vrow_destroy(vrow); free(vrow);
	}
	if (filter != NULL) {
		nowdb_filter_destroy(filter); free(filter);
	}
	nowdb_close();
	return rc;
}

