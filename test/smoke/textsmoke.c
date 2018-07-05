/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for text
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/io/dir.h>
#include <nowdb/text/text.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define PATH "rsc/text10"

int preparePath() {
	nowdb_err_t err;
	struct stat  st;

	if (stat(PATH, &st) == 0) {
		err = nowdb_path_rRemove(PATH);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return -1;
		}
	}
	if (mkdir(PATH, NOWDB_DIR_MODE) != 0) {
		perror("cannot create path");
		return -1;
	}
	return 0;
}

nowdb_text_t *mkText(char *path) {
	nowdb_err_t err;
	nowdb_text_t *txt;

	txt = calloc(1, sizeof(nowdb_text_t));
	if (txt == NULL) {
		perror("out-of-mem\n");
		return NULL;
	}
	err = nowdb_text_init(txt, path);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);	
		return NULL;
	}
	return txt;
}

int createText(nowdb_text_t *txt) {
	nowdb_err_t err;

	err = nowdb_text_create(txt);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int dropText(nowdb_text_t *txt) {
	nowdb_err_t err;

	err = nowdb_text_drop(txt);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int openText(nowdb_text_t *txt) {
	nowdb_err_t err;

	err = nowdb_text_open(txt);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int closeText(nowdb_text_t *txt) {
	nowdb_err_t err;

	err = nowdb_text_close(txt);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

typedef struct {
	uint64_t key;
	char    *str;
} string_t;

char *randomString(uint32_t mx) {
	uint32_t sz;
	char *str;

	do {sz = rand()%mx;} while(sz == 0);
	str = malloc(sz+1);
	if (str == NULL) return NULL;
	str[sz] = 0;
	for(int i=0; i<sz; i++) {
		str[i] = rand()%26;
		str[i] += 64;
	}
	return str;
}

int insertText(nowdb_text_t *txt, string_t *strs, int n) {
	nowdb_err_t err;
	char         x;

	for(int i=0;i<n;i++) {
		x = rand()%9;
		switch(x) {
		case 0: strs[i].str = randomString(8); break;
		case 1: strs[i].str = randomString(32); break;
		case 2: strs[i].str = randomString(128); break;
		case 3: strs[i].str = randomString(256); break;
		default: strs[i].str = randomString(32);
		}
		if (strs[i].str == NULL) {
			fprintf(stderr, "out-of-mem\n");
			return -1;
		}
		err = nowdb_text_insert(txt, strs[i].str, &strs[i].key);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			free(strs[i].str); strs[i].str = NULL;
			return -1;
		}
		/*
		fprintf(stderr, "inserting '%s': %lu\n",
		              strs[i].str, strs[i].key);
		*/
	}
	return 0;
}

int testGetKey(nowdb_text_t *txt, string_t *strs, int n) {
	nowdb_err_t err;
	nowdb_key_t key;

	for(int i=0;i<n;i++) {
		if (strs[i].str == NULL) continue;
		err = nowdb_text_getKey(txt, strs[i].str, &key);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot find key\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return -1;
		}
		if (key != strs[i].key) {
			fprintf(stderr, "wrong key: %lu != %lu\n",
			                key, strs[i].key);
			return -1;
		}
		/*
		fprintf(stderr, "found: %s -> %lu\n",
		           strs[i].str, strs[i].key);
		*/
	}
	return 0;
}

int testGetString(nowdb_text_t *txt, string_t *strs, int n) {
	nowdb_err_t err;
	char  *str=NULL;

	for(int i=0;i<n;i++) {
		if (strs[i].str == NULL) continue;
		err = nowdb_text_getText(txt, strs[i].key, &str);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot find string\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return -1;
		}
		if (str == NULL) {
			fprintf(stderr, "no error, no string :-(\n");
			return -1;
		}
		if (strcmp(str, strs[i].str) != 0) {
			fprintf(stderr, "wrong string: '%s' != '%s'\n",
			                str, strs[i].str);
			free(str);
			return -1;
		}
		/*
		fprintf(stderr, "found: %s -> %lu\n",
		           strs[i].str, strs[i].key);
		*/
		free(str);
	}
	return 0;
}

void freeStrings(string_t *strs, int n) {
	for(int i=0;i<n;i++) {
		if (strs[i].str != NULL) {
			free(strs[i].str); strs[i].str = NULL;
		}
	}
	free(strs);
}

int testInsertStrings(nowdb_text_t *txt, int n) {
	string_t *strs=NULL;
	strs = calloc(n, sizeof(string_t));
	if (strs == NULL) {
		fprintf(stderr, "cannot alloc strings\n");
		return -1;
	}
	if (insertText(txt, strs, n) != 0) {
		fprintf(stderr, "insertText 1 failed\n");
		freeStrings(strs,n);
		return -1;
	}
	if (n > 1000) {
		if (closeText(txt) != 0) {
			fprintf(stderr, "cannot close text\n");
			freeStrings(strs,n);
			return -1;
		}
		if (openText(txt) != 0) {
			fprintf(stderr, "cannot open text\n");
			freeStrings(strs,n);
			return -1;
		}
	}
	if (testGetKey(txt, strs, n) != 0) {
		fprintf(stderr, "testGetKey failed\n");
		freeStrings(strs,n);
		return -1;
	}
	if (testGetString(txt, strs, n) != 0) {
		fprintf(stderr, "testGetString failed\n");
		freeStrings(strs,n);
		return -1;
	}
	freeStrings(strs,n);
	return 0;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_text_t *txt=NULL;

	srand(time(NULL) ^ (uint64_t)&printf);

	if (!nowdb_init()) {
		fprintf(stderr, "FAILED: cannot init\n");
		return EXIT_FAILURE;
	}
	if (preparePath() != 0) {
		fprintf(stderr, "FAILED: prepare path\n");
		return EXIT_FAILURE;
	}
	txt = mkText(PATH);
	if (txt == NULL) {
		fprintf(stderr, "mkText failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (createText(txt) != 0) {
		fprintf(stderr, "createText failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (dropText(txt) != 0) {
		fprintf(stderr, "dropText failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (preparePath() != 0) {
		fprintf(stderr, "FAILED: prepare path\n");
		return EXIT_FAILURE;
	}
	if (createText(txt) != 0) {
		fprintf(stderr, "createText failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (openText(txt) != 0) {
		fprintf(stderr, "openText failed (1)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (closeText(txt) != 0) {
		fprintf(stderr, "closeText failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (openText(txt) != 0) {
		fprintf(stderr, "openText failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testInsertStrings(txt,1) != 0) {
		fprintf(stderr, "testInsertStrings(1) failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testInsertStrings(txt,10) != 0) {
		fprintf(stderr, "testInsertStrings(10) failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testInsertStrings(txt,100) != 0) {
		fprintf(stderr, "testInsertStrings(100) failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testInsertStrings(txt,1000) != 0) {
		fprintf(stderr, "testInsertStrings(1000) failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (closeText(txt) != 0) {
		fprintf(stderr, "closeText failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (openText(txt) != 0) {
		fprintf(stderr, "openText failed (2)\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testInsertStrings(txt,999) != 0) {
		fprintf(stderr, "testInsertStrings(999) failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testInsertStrings(txt,1500) != 0) {
		fprintf(stderr, "testInsertStrings(1500) failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testInsertStrings(txt,101) != 0) {
		fprintf(stderr, "testInsertStrings(101) failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	
cleanup:
	if (txt != NULL) {
		nowdb_text_destroy(txt); free(txt);
	}
	nowdb_close();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}

