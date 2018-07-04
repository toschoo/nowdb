/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Text
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/io/dir.h>
#include <nowdb/text/text.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define KEY32 1073741824
#define KEY64 4294967296

#define CACHEMAX 250000

#define SZIDX     0
#define TINYSTR   1
#define TINYNUM   2
#define SMALLSTR  3
#define SMALLNUM  4
#define MEDIUMSTR 5
#define MEDIUMNUM 6
#define BIGSTR    7
#define BIGNUM    8

static char *OBJECT = "TEXT";

#define TEXTNULL() \
	if (txt == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                        FALSE, OBJECT, "text is NULL");
#define NOMEM(x) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, x);

#define OPENIDX(x, p, h, s) \
	beet_open_config_ignore(&ocfg); \
	ber = beet_index_open(p, h, &ocfg, &x); \
	if (ber != BEET_OK) err = makeBeetErr(ber, s);

typedef struct {
	nowdb_key_t key;
	char       *str;
} lrunode_t;

#define NODE(x) \
	((lrunode_t*)x)

/* ------------------------------------------------------------------------
 * LRU Callbacks: compare
 * ------------------------------------------------------------------------
 */
ts_algo_cmp_t strcompare(void *ignore, void *one, void *two) {
	int x = strcmp(NODE(one)->str, NODE(two)->str);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

ts_algo_cmp_t numcompare(void *ignore, void *one, void *two) {
	if (NODE(one)->key < NODE(two)->key) return ts_algo_cmp_less;
	if (NODE(one)->key > NODE(two)->key) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * LRU Callbacks: delete
 * ------------------------------------------------------------------------
 */
void lrudelete(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (NODE(*n)->str != NULL) {
		free(NODE(*n)->str);
	}
	free(*n); *n=NULL;
}

/* ------------------------------------------------------------------------
 * LRU Callbacks: update
 * ------------------------------------------------------------------------
 */
ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	return TS_ALGO_OK;
}

#define KEY(x) \
	(*(nowdb_key_t*)x)

/* ------------------------------------------------------------------------
 * IDX Callbacks: strcompare
 * ------------------------------------------------------------------------
 */
char nowdb_text_strcompare(const void *left,
                           const void *right,
                           void *ignore) {
	int x = strcmp(left, right);
	if (x < 0) return BEET_CMP_LESS;
	if (x > 0) return BEET_CMP_GREATER;
	return BEET_CMP_EQUAL;
}

/* ------------------------------------------------------------------------
 * IDX Callbacks: keycompare
 * ------------------------------------------------------------------------
 */
char nowdb_text_keycompare(const void *left,
                           const void *right,
                           void *ignore) {
	if (KEY(left) < KEY(right)) return BEET_CMP_LESS;
	if (KEY(left) > KEY(right)) return BEET_CMP_GREATER;
	return BEET_CMP_EQUAL;
}

/* -----------------------------------------------------------------------
 * Helper: make beet errors
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t makeBeetError(beet_err_t ber) {
	nowdb_err_t err, err2=NOWDB_OK;

	if (ber == BEET_OK) return NOWDB_OK;
	if (ber < BEET_OSERR_ERRNO) {
		err2 = nowdb_err_get(nowdb_err_beet, FALSE, OBJECT,
		                          (char*)beet_oserrdesc());
	}
	err = nowdb_err_get(nowdb_err_beet, FALSE, OBJECT,
	                         (char*)beet_errdesc(ber));
	if (err == NOWDB_OK) return err2; else err->cause = err2;
	return err;
}

/* -----------------------------------------------------------------------
 * Helper: compute index size
 * -----------------------------------------------------------------------
 */
#define INTDSZ sizeof(beet_pageid_t)
static void computeSize(int which,
               beet_config_t *cfg) 
{
	uint32_t leafsz = 0;
	uint32_t intsz = 0;

	switch(which) {
	case SZIDX:

		leafsz = 4096;
		intsz = 4096;
		cfg->keySize = 8;
		cfg->dataSize = 4;
		break;

	case TINYSTR:

		leafsz = 4096;
		intsz = 4096;
		cfg->keySize = 8;
		cfg->dataSize = 8;
		break;

	case TINYNUM:

		leafsz = 4096;
		intsz = 4096;
		cfg->keySize = 8;
		cfg->dataSize = 8;
		break;

	case SMALLSTR:

		leafsz = 8192;
		intsz = 8192;
		cfg->keySize = 32;
		cfg->dataSize = 8;
		break;

	case SMALLNUM:

		leafsz = 8192;
		intsz = 4096;
		cfg->keySize = 8;
		cfg->dataSize = 32;
		break;

	case MEDIUMSTR:

		leafsz = 8192;
		intsz = 8192;
		cfg->keySize = 128;
		cfg->dataSize = 8;
		break;

	case MEDIUMNUM:

		leafsz = 8192;
		intsz = 4096;
		cfg->keySize = 8;
		cfg->dataSize = 128;
		break;

	case BIGSTR:

		leafsz = 8192;
		intsz = 8192;
		cfg->keySize = 256;
		cfg->dataSize = 8;
		break;

	case BIGNUM:

		leafsz = 8192;
		intsz = 4096;
		cfg->keySize = 8;
		cfg->dataSize = 256;
		break;

	default: return;
	}

	cfg->leafNodeSize = (leafsz-12)/(cfg->keySize+cfg->dataSize);
	cfg->intNodeSize = (intsz-8)/(cfg->keySize+INTDSZ);

	cfg->leafPageSize = cfg->leafNodeSize *
	                   (cfg->keySize+cfg->dataSize) + 12;
	cfg->intPageSize = cfg->intNodeSize *
	                   (cfg->keySize+INTDSZ) + 8;
}

/* -----------------------------------------------------------------------
 * Helper: configure index
 * -----------------------------------------------------------------------
 */
static void configure(int which,
             beet_config_t *cfg)  
{
	cfg->indexType = BEET_INDEX_PLAIN;
	cfg->subPath = NULL;
	
	cfg->leafCacheSize = 10000;
	cfg->intCacheSize = 10000;

	cfg->rscinit = NULL;
	cfg->rscdest = NULL;

	switch(which) {
	case TINYSTR:
	case SMALLSTR:
	case MEDIUMSTR:
	case BIGSTR:
		cfg->compare = "nowdb_text_strcompare"; break;

	case SZIDX:
	case TINYNUM:
	case SMALLNUM:
	case MEDIUMNUM:
	case BIGNUM:
		cfg->compare = "nowdb_text_keycompare"; break;

	default: return;
	}

	computeSize(which, cfg);
}

/* ------------------------------------------------------------------------
 * Init text manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_init(nowdb_text_t *txt, char *path) {
	nowdb_err_t err;

	TEXTNULL();

	txt->str2num = NULL;
	txt->num2str = NULL;

	txt->path = NULL;

	txt->szidx = NULL;
	txt->tinystr = NULL;
	txt->tinynum = NULL;
	txt->smallstr = NULL;
	txt->smallnum = NULL;
	txt->mediumstr = NULL;
	txt->mediumnum = NULL;
	txt->bigstr = NULL;
	txt->bignum = NULL;

	err = nowdb_rwlock_init(&txt->lock);
	if (err != NOWDB_OK) return err;

	txt->path = strdup(path);
	if (txt->path == NULL) {
		nowdb_text_destroy(txt);
		NOMEM("allocating path");
		return err;
	}

	txt->str2num = ts_algo_lru_new(CACHEMAX, &strcompare, &noupdate,
	                                         &lrudelete, &lrudelete);
	if (txt->str2num == NULL) {
		nowdb_text_destroy(txt);
		NOMEM("creating cache");
		return err;
	}

	txt->num2str = ts_algo_lru_new(CACHEMAX, &numcompare, &noupdate,
	                                         &lrudelete, &lrudelete);
	if (txt->num2str == NULL) {
		nowdb_text_destroy(txt);
		NOMEM("creating cache");
		return err;
	}

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: close all indices
 * ------------------------------------------------------------------------
 */
static void closeAll(nowdb_text_t *txt) {
	if (txt->szidx != NULL) {
		beet_index_close(txt->szidx);
		txt->szidx = NULL;
	}
	if (txt->tinystr != NULL) {
		beet_index_close(txt->tinystr);
		txt->tinystr = NULL;
	}
	if (txt->tinynum != NULL) {
		beet_index_close(txt->tinynum);
		txt->tinynum = NULL;
	}
	if (txt->smallstr != NULL) {
		beet_index_close(txt->smallstr);
		txt->smallstr = NULL;
	}
	if (txt->smallnum != NULL) {
		beet_index_close(txt->smallnum);
		txt->smallnum = NULL;
	}
	if (txt->mediumstr != NULL) {
		beet_index_close(txt->mediumstr);
		txt->mediumstr = NULL;
	}
	if (txt->mediumnum != NULL) {
		beet_index_close(txt->mediumnum);
		txt->mediumnum = NULL;
	}
	if (txt->bigstr != NULL) {
		beet_index_close(txt->bigstr);
		txt->bigstr = NULL;
	}
	if (txt->bignum != NULL) {
		beet_index_close(txt->bignum);
		txt->bignum = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Destroy text manager
 * ------------------------------------------------------------------------
 */
void nowdb_text_destroy(nowdb_text_t *txt) {
	if (txt == NULL) return;

	nowdb_rwlock_destroy(&txt->lock);
	if (txt->path != NULL) {
		free(txt->path); txt->path = NULL;
	}
	if (txt->num2str != NULL) {
		ts_algo_lru_destroy(txt->num2str);
		free(txt->num2str); txt->num2str = NULL;
	}
	if (txt->str2num != NULL) {
		ts_algo_lru_destroy(txt->str2num);
		free(txt->str2num); txt->str2num = NULL;
	}
	closeAll(txt);
}

/* ------------------------------------------------------------------------
 * Helper: get path for index
 * ------------------------------------------------------------------------
 */
static nowdb_path_t getpath(int which, nowdb_path_t base) {
	nowdb_path_t p;

	switch(which) {
	case SZIDX: p="size"; break;
	case TINYSTR: p="tinystr"; break;
	case SMALLSTR: p="smallstr"; break;
	case MEDIUMSTR: p="mediumstr"; break;
	case BIGSTR: p="bigstr"; break;
	case TINYNUM: p="tinykey"; break;
	case SMALLNUM: p="smallkey"; break;
	case MEDIUMNUM: p="mediumkey"; break;
	case BIGNUM: p="bigkey"; break;
	default: return NULL;
	}
	return nowdb_path_append(base, p);
}

/* ------------------------------------------------------------------------
 * Create text manager physically on disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_create(nowdb_text_t *txt) {
	nowdb_path_t p;
	beet_err_t ber;
	nowdb_err_t err;
	beet_config_t cfg;

	TEXTNULL();
	if (txt->path == NULL) return nowdb_err_get(nowdb_err_invalid,
		                  FALSE, OBJECT, "text path is NULL");
	beet_config_init(&cfg);
	for(int i=0;i<9; i++) {
		configure(i, &cfg);
		p = getpath(i, txt->path);
		if (p == NULL) {
			NOMEM("allocating path");
			return err;
		}
		ber = beet_index_create(p, 1, &cfg); free(p);
		if (ber != BEET_OK) return makeBeetError(ber);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Drop text manager physically from disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_drop(nowdb_text_t *txt) {
	beet_err_t ber;
	nowdb_err_t err;
	nowdb_path_t p;

	TEXTNULL();
	if (txt->path == NULL) return nowdb_err_get(nowdb_err_invalid,
		                  FALSE, OBJECT, "text path is NULL");
	for(int i=0; i<9; i++) {
		p = getpath(i, txt->path);
		if (p == NULL) {
			NOMEM("allocating path");
			return err;
		}
		ber = beet_index_drop(p);
		if (ber != BEET_OK) {
			free(p); return makeBeetError(ber);
		}
		err = nowdb_path_remove(p); free(p);
		if (err != NOWDB_OK) return err;
	}
	return nowdb_path_remove(txt->path);
}

/* ------------------------------------------------------------------------
 * Open existing text manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_open(nowdb_text_t *txt) {
	beet_index_t idx;
	beet_open_config_t cfg;
	beet_err_t ber;
	nowdb_err_t err;
	nowdb_path_t p;

	TEXTNULL();
	if (txt->path == NULL) return nowdb_err_get(nowdb_err_invalid,
		                  FALSE, OBJECT, "text path is NULL");

	beet_open_config_ignore(&cfg);
	for(int i=0; i<9; i++) {
		p = getpath(i, txt->path);
		if (p == NULL) {
			NOMEM("allocating path");
			return err;
		}
		ber = beet_index_open(p, nowdb_lib(), &cfg, &idx); free(p);
		if (ber != BEET_OK) {
			return makeBeetError(ber);
		}
		switch(i) {
		case SZIDX: txt->szidx = idx; break;
		case TINYSTR: txt->tinystr = idx; break;
		case SMALLSTR: txt->smallstr= idx; break;
		case MEDIUMSTR: txt->mediumstr= idx; break;
		case BIGSTR: txt->bigstr= idx; break;
		case TINYNUM: txt->tinynum= idx; break;
		case SMALLNUM: txt->smallnum= idx; break;
		case MEDIUMNUM: txt->mediumnum= idx; break;
		case BIGNUM: txt->bignum= idx; break;
		default: break;
		}
	}
	/* set next32 and next64 */
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Close text manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_close(nowdb_text_t *txt) {
	closeAll(txt);
	/* write next32 and next64 back */
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Insert text and get a unique identifier back
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_insert(nowdb_text_t *txt,
                              char         *str,
                              nowdb_key_t  *key);

/* ------------------------------------------------------------------------
 * Insert text and get a unique identifier back;
 * the identifier is guaranteed to fit into a 32bit integer.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_insertLow(nowdb_text_t *txt,
                                 char         *str,
                                 uint32_t     *key);

/* ------------------------------------------------------------------------
 * Get key for this text
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_getKey(nowdb_text_t *txt,
                              char         *str,
                              nowdb_key_t  *key);

/* ------------------------------------------------------------------------
 * Get text for this key
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_getText(nowdb_text_t *txt,
                               nowdb_key_t   key,
                               char        **str);

/* ------------------------------------------------------------------------
 * Reserve n key32s
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_reserve32(nowdb_text_t *txt,
                                 uint32_t      n,
                                 uint32_t     *lo,
                                 uint32_t     *hi);

/* ------------------------------------------------------------------------
 * Reserve n key64s
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_reserve64(nowdb_text_t *txt,
                                 uint32_t      n,
                                 nowdb_key_t  *lo,
                                 nowdb_key_t  *hi);
