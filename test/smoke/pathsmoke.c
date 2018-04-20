/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for paths
 * ========================================================================
 */
#include <nowdb/io/dir.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

void randomstring(char *str, int len) {
	for(int i=0; i<len; i++) {
		str[i] = rand()%26;
		str[i] += 65;
	}
	str[len] = 0;
}

nowdb_bool_t manualpath() {
	char p1[32];
	char p2[32];
	char *p3=NULL, *p4=NULL;

	strcpy(p1, "/path/to");
	strcpy(p2, "file");

	p3 = nowdb_path_append(p1, p2);
	if (p3 == NULL) {
		fprintf(stderr, "cannot append\n");
		return FALSE;
	}
	if (strcmp(p3, "/path/to/file") != 0) {
		fprintf(stderr, "not correct: %s\n", p3);
		free(p3); return FALSE;
	}
	p4 = nowdb_path_filename(p3);
	if (p4 == NULL) {
		fprintf(stderr, "cannot get filename\n");
		return FALSE;
	}
	if (strcmp(p4, "file") != 0) {
		fprintf(stderr, "not correct: %s\n", p3);
		free(p3); free(p4); return FALSE;
	}
	free(p3); free(p4);
	
	strcpy(p2, "another/path/with/file");
	p3 = nowdb_path_append(p1, p2);
	if (p3 == NULL) {
		fprintf(stderr, "cannot append\n");
		return FALSE;
	}
	if (strcmp(p3, "/path/to/another/path/with/file") != 0) {
		fprintf(stderr, "not correct: %s\n", p3);
		free(p3); return FALSE;
	}
	p4 = nowdb_path_filename(p3);
	if (p4 == NULL) {
		fprintf(stderr, "cannot get filename\n");
		free(p3); return FALSE;
	}
	if (strcmp(p4, "file") != 0) {
		fprintf(stderr, "not correct: %s\n", p3);
		free(p3); free(p4); return FALSE;
	}
	free(p3); free(p4);
	
	strcpy(p2, "another/path/");
	p3 = nowdb_path_append(p1, p2);
	if (p3 == NULL) {
		fprintf(stderr, "cannot append\n");
		return FALSE;
	}
	if (strcmp(p3, "/path/to/another/path/") != 0) {
		fprintf(stderr, "not correct: %s\n", p3);
		free(p3); return FALSE;
	}
	p4 = nowdb_path_filename(p3);
	if (p4 != NULL) {
		fprintf(stderr, "extracted filename from directory: %s\n", p4);
		free(p3); free(p4);
		return FALSE;
	}
	free(p3); 

	p4 = nowdb_path_filename("file");
	if (p4 == NULL) {
		fprintf(stderr, "cannot get filename\n");
		return FALSE;
	}
	if (strcmp(p4, "file") != 0) {
		fprintf(stderr, "not correct: %s\n", p3);
		free(p4); return FALSE;
	}
	free(p4);
	return TRUE;
}

nowdb_bool_t randompath() {
	char *p1=NULL, *p2=NULL, *p3=NULL;
	char *p4=NULL, *p5=NULL, *p6=NULL;
	uint32_t l;

	for(int i=0; i<10; i++) {
		do l = rand()%65; while(l==0);
		p1 = malloc(l+1);
		if (p1==NULL) {
			if (p2 != NULL) free(p2);
			if (p3 != NULL) free(p3);
			fprintf(stderr, "no mem\n");
			return FALSE;
		}
		randomstring(p1, l);

		do l = rand()%65; while(l==0);
		p2 = malloc(l+1);
		if (p2==NULL) {
			if (p1 != NULL) free(p1);
			if (p3 != NULL) free(p3);
			fprintf(stderr, "no mem\n");
			return FALSE;
		}
		randomstring(p2, l);

		do l = rand()%65; while(l==0);
		p3 = malloc(l+1);
		if (p3==NULL) {
			if (p1 != NULL) free(p1);
			if (p2 != NULL) free(p2);
			fprintf(stderr, "no mem\n");
			return FALSE;
		}
		randomstring(p3, l);

		p4 = nowdb_path_append(p1, p2);
		if (p4 == NULL) {
			if (p1 != NULL) free(p1);
			if (p2 != NULL) free(p2);
			if (p3 != NULL) free(p3);
			fprintf(stderr, "no mem\n");
			return FALSE;
		}

		p5 = nowdb_path_append(p4, p3);
		if (p5 == NULL) {
			if (p1 != NULL) free(p1);
			if (p2 != NULL) free(p2);
			if (p3 != NULL) free(p3);
			if (p4 != NULL) free(p4);
			fprintf(stderr, "no mem\n");
			return FALSE;
		}

		fprintf(stderr, "%s\n", p5);

		p6 = nowdb_path_filename(p5);
		if (p6 == NULL) {
			if (p1 != NULL) free(p1);
			if (p2 != NULL) free(p2);
			if (p3 != NULL) free(p3);
			if (p5 != NULL) free(p5);
			fprintf(stderr, "no mem\n");
			return FALSE;
		}
		if (strcmp(p6, p3) != 0) {
			fprintf(stderr,
			"paths do not match (1): '%s' '%s'\n", p6, p3);
			free(p6); free(p5); free(p4);
			free(p3); free(p2); free(p1);
			return FALSE;
		}
		free(p5); p5 = nowdb_path_filename(p4);
		if (p5 == NULL) {
			if (p1 != NULL) free(p1);
			if (p2 != NULL) free(p2);
			if (p3 != NULL) free(p3);
			if (p4 != NULL) free(p4);
			if (p6 != NULL) free(p6);
			fprintf(stderr, "no mem\n");
			return FALSE;
		}
		if (strcmp(p5, p2) != 0) {
			fprintf(stderr,
			"paths do not match (2): '%s' '%s'\n", p5, p2);
			free(p6); free(p5); free(p4);
			free(p3); free(p2); free(p1);
			return FALSE;
		}
		free(p1); free(p2); free(p3);
		free(p4); free(p5); free(p6);
	}
	return TRUE;
}

int main() {
	int rc = EXIT_SUCCESS;
	srand(time(NULL) ^ (uint64_t)&printf);

	if (!manualpath()) {
		fprintf(stderr, "manual paths failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}
	if (!randompath()) {
		fprintf(stderr, "random paths failed\n");
		rc = EXIT_FAILURE;
		goto cleanup;
	}

cleanup:
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}


