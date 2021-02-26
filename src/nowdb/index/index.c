/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Index
 * ========================================================================
 */

#include <nowdb/index/index.h>
#include <nowdb/io/dir.h>

#include <beet/index.h>

#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>

static char *OBJECT = "idx";

#define HOSTPATH "host"
#define EMBPATH "emb"

/* ------------------------------------------------------------------------
 * host: data is the root of embbedded, i.e. a beet page
 * emb : key is a nowdb page
 *       data is a 128 bitmap
 * ------------------------------------------------------------------------
 */
#define HOSTDSZ sizeof(beet_pageid_t)
#define EMBKSZ sizeof(nowdb_pageid_t)
#define EMBDSZ 4
#define INTDSZ sizeof(beet_pageid_t)

/* -----------------------------------------------------------------------
 * Macro: check index NULL
 * -----------------------------------------------------------------------
 */
#define IDXNULL() \
	if (idx == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                            FALSE, OBJECT, "index NULL");

/* -----------------------------------------------------------------------
 * Macro: out of memory
 * -----------------------------------------------------------------------
 */
#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

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
 * Create index keys
 * -----------------
 * This is probably not very useful.
 * Usually, we will create keys in a loop,
 * not by passing all offsets at once.
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_keys_create(nowdb_index_keys_t **keys,
                                             uint16_t sz, ...) {
	va_list args;

	*keys = NULL;
	if (sz == 0) return NOWDB_OK;

	*keys = calloc(1,sizeof(nowdb_index_keys_t));
	if (*keys == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                       FALSE, OBJECT, "allocating keys");

	(*keys)->sz = sz;
	(*keys)->off = calloc(sz, sizeof(uint16_t));
	if ((*keys)->off == NULL) {
		free(*keys); *keys = NULL;
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                       "allocating offsets");
	}
	va_start(args, sz);
	for(int i=0;i<sz;i++) {
		(*keys)->off[i] = (uint16_t)va_arg(args, int);
	}
	va_end(args);
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Copy index keys
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_keys_copy(nowdb_index_keys_t *from,
                                  nowdb_index_keys_t **to) {

	*to = calloc(1,sizeof(nowdb_index_keys_t));
	if (*to == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                     FALSE, OBJECT, "allocating keys");
	(*to)->sz = from->sz;
	(*to)->off = calloc(from->sz, sizeof(uint16_t));
	if ((*to)->off == NULL) {
		free(*to); *to = NULL;
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                        "allocating offsets");
	}
	memcpy((*to)->off, from->off, from->sz*sizeof(uint16_t));
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Keysize vertex
 * ------------------------------------------------------------------------
 */
uint32_t nowdb_index_keySizeVertex(nowdb_index_keys_t *k) {
	uint32_t sz = 0;
	for(int i=0; i<k->sz; i++) {
		if (k->off[i] < NOWDB_OFF_VTYPE) sz += 8; else sz +=4;
	}
	return sz;
}

/* ------------------------------------------------------------------------
 * Keysize edge
 * ------------------------------------------------------------------------
 */
uint32_t nowdb_index_keySizeEdge(nowdb_index_keys_t *k) {
	return (k->sz * sizeof(nowdb_key_t));
}

/* ------------------------------------------------------------------------
 * Keysize (generic)
 * ------------------------------------------------------------------------
 */
uint32_t nowdb_index_keySize(nowdb_index_keys_t *k) {
	return (k->sz * sizeof(nowdb_key_t));
}

/* -----------------------------------------------------------------------
 * Destroy index keys
 * -----------------------------------------------------------------------
 */
void nowdb_index_keys_destroy(nowdb_index_keys_t *keys) {
	if (keys == NULL) return;
	if (keys->off != NULL) free(keys->off);
	free(keys);
}

/* -----------------------------------------------------------------------
 * Make safe index descriptor
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_desc_create(char                *name,
                                    nowdb_context_t     *ctx,
                                    nowdb_index_keys_t  *keys,
                                    nowdb_index_t       *idx,
                                    nowdb_index_desc_t **desc) {

	if (ctx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                            FALSE, OBJECT, "no context");

	*desc = calloc(1, sizeof(nowdb_index_desc_t));
	if (*desc == NULL) return nowdb_err_get(nowdb_err_no_mem,
	           FALSE, OBJECT, "allocating index descriptor");

	(*desc)->name = strdup(name);
	if ((*desc)->name == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                         FALSE, OBJECT, "allocating index name");
	(*desc)->cont = ctx->store.cont;
	(*desc)->ctx = ctx;
	(*desc)->keys = keys;
	(*desc)->idx = idx;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Destroy index descriptor
 * -----------------------------------------------------------------------
 */
void nowdb_index_desc_destroy(nowdb_index_desc_t *desc) {
	if (desc == NULL) return;
	if (desc->name != NULL) {
		free(desc->name); desc->name = NULL;
	}
	if (desc->keys != NULL) {
		free(desc->keys->off);
		free(desc->keys);
		desc->keys = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Helper: create dir if it does not exist
 * ------------------------------------------------------------------------
 */
static nowdb_err_t mkIdxDir(char *path) {
	struct stat st;
	if (stat(path, &st) == 0) return NOWDB_OK;
	return nowdb_dir_create(path);
}

/* ------------------------------------------------------------------------
 * Helper: remove path
 * ------------------------------------------------------------------------
 */
static nowdb_err_t removePath(char *path) {
	struct stat st;
	if (stat(path, &st) != 0) return NOWDB_OK;
	return nowdb_path_remove(path);
}

/* ------------------------------------------------------------------------
 * Helper: compute key size from desc
 * ------------------------------------------------------------------------
 */
static uint32_t computeKeySize(nowdb_index_desc_t *desc) {
	uint32_t s = 0;

	// better to use sizeByOff!
	for(int i=0;i<desc->keys->sz;i++) {
		if (desc->cont == NOWDB_CONT_VERTEX &&
		    desc->keys->off[i] >= NOWDB_OFF_VTYPE)
			s+=4;
		else s+=8;
	}
	return s;
}

/* ------------------------------------------------------------------------
 * Helper: set compare for embedded
 * ------------------------------------------------------------------------
 */
static void setEmbCompare(beet_config_t *cfg) {
	cfg->compare = "nowdb_index_pageid_compare";
	cfg->rscinit = NULL;
	cfg->rscdest = NULL;
}

/* ------------------------------------------------------------------------
 * Helper: set compare for host
 * ------------------------------------------------------------------------
 */
static void setHostCompare(nowdb_index_desc_t *desc, beet_config_t *cfg) {
	if (desc->cont == NOWDB_CONT_VERTEX) {
		cfg->compare = "nowdb_index_vertex_compare";
	} else {
		cfg->compare = "nowdb_index_edge_compare";
	}
	cfg->rscinit = "nowdb_index_keysinit";
	cfg->rscdest = "nowdb_index_keysdestroy";
}

/* ------------------------------------------------------------------------
 * Helper: compute index sizing for embedded
 * ------------------------------------------------------------------------
 */
static void computeEmbSize(nowdb_index_desc_t *desc,
                           uint16_t            size,
                           beet_config_t      *cfg) {
	uint32_t targetsz = 0;

	cfg->indexType = BEET_INDEX_PLAIN;
	cfg->subPath = NULL;

	cfg->keySize = EMBKSZ;
	cfg->dataSize = desc->ctx != NULL &&
	                desc->ctx->store.setsize > 0 ?
	                desc->ctx->store.setsize     : EMBDSZ;

	switch(size) {
		case NOWDB_CONFIG_SIZE_TINY:

			targetsz = 128;

			cfg->leafCacheSize = 1000;
			cfg->intCacheSize = 1000;

			break;
			
		case NOWDB_CONFIG_SIZE_SMALL: 

			targetsz = 512;

			cfg->leafCacheSize = 10000;
			cfg->intCacheSize = 10000;

			break;

		case NOWDB_CONFIG_SIZE_MEDIUM:

			targetsz = 512;

			cfg->leafCacheSize = 10000;
			cfg->intCacheSize = 10000;

			break;

		case NOWDB_CONFIG_SIZE_BIG: 

			targetsz = 4096;

			cfg->leafCacheSize = 10000;
			cfg->intCacheSize = 10000;

			break;

		case NOWDB_CONFIG_SIZE_LARGE: 

			targetsz = 8192;

			cfg->leafCacheSize = 10000;
			cfg->intCacheSize = 10000;

			break;

		case NOWDB_CONFIG_SIZE_HUGE: 

			targetsz = 16384;

			cfg->leafCacheSize = 10000;
			cfg->intCacheSize = 10000;

			break;
	}

	cfg->leafNodeSize = (targetsz-12)/(EMBKSZ+cfg->dataSize);
	cfg->intNodeSize = (targetsz-8)/(EMBKSZ+INTDSZ);

	cfg->leafPageSize = cfg->leafNodeSize * (EMBKSZ + cfg->dataSize) + 12;
	cfg->intPageSize = cfg->intNodeSize * (EMBKSZ + INTDSZ) + 8;

	fprintf(stderr, "CREATING EMB  IDX with size: %d/%u/%u/%u/%u/%u\n",
	                cfg->keySize, cfg->dataSize, cfg->leafNodeSize, cfg->intNodeSize,
	                                             cfg->leafPageSize, cfg->intPageSize);
}

/* ------------------------------------------------------------------------
 * Helper: compute index sizing for host
 * ------------------------------------------------------------------------
 */
static void computeHostSize(nowdb_index_desc_t *desc,
                            uint16_t            size,
                            beet_config_t      *cfg) {

	cfg->indexType = BEET_INDEX_HOST;
	uint32_t targetsz=0;

	cfg->keySize = computeKeySize(desc);
	cfg->dataSize = HOSTDSZ;
	
	switch(size) {
		case NOWDB_CONFIG_SIZE_TINY:

			targetsz = 1024;

			cfg->leafCacheSize = 1000;
			cfg->intCacheSize = 1000;

			break;
			
		case NOWDB_CONFIG_SIZE_SMALL: 
		case NOWDB_CONFIG_SIZE_MEDIUM:
		
			targetsz = 4096;

			cfg->leafCacheSize = 10000;
			cfg->intCacheSize = 10000;

			break;

		case NOWDB_CONFIG_SIZE_BIG: 
		case NOWDB_CONFIG_SIZE_LARGE: 

			targetsz = 8192;

			cfg->leafCacheSize = 10000;
			cfg->intCacheSize = 10000;

			break;

		case NOWDB_CONFIG_SIZE_HUGE: 

			targetsz = 16384;

			cfg->leafCacheSize = 10000;
			cfg->intCacheSize = 10000;

			break;
	}

	cfg->leafNodeSize = (targetsz-12)/(cfg->keySize+HOSTDSZ);
	cfg->intNodeSize = (targetsz-8)/(cfg->keySize+INTDSZ);

	cfg->leafPageSize = (cfg->keySize+HOSTDSZ)*cfg->leafNodeSize+12;
	cfg->intPageSize = (cfg->keySize+INTDSZ)*cfg->intNodeSize+8;

	fprintf(stderr, "CREATING HOST IDX with size: %d/%lu/%u/%u/%u/%u\n",
	                cfg->keySize, HOSTDSZ, cfg->leafNodeSize, cfg->intNodeSize,
	                                       cfg->leafPageSize, cfg->intPageSize);
}

/* ------------------------------------------------------------------------
 * Create index
 * "index" is a host beetree
 * there are other usages for beetree (strings),
 * but they are not covered here!
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_create(char *base,
                               char *path,
                               uint16_t size,
                               nowdb_index_desc_t  *desc) {
	nowdb_err_t   err;
	beet_err_t    ber;
	beet_config_t cfg;
	char *ep, *hp, *ip, *fp;

	if (base == NULL) return nowdb_err_get(nowdb_err_invalid,
	                     FALSE, OBJECT, "base path is NULL");
	if (path == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "path is NULL");
	if (desc == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "desc is NULL");
	if (desc->name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "desc name is NULL");
	if (desc->keys == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "desc keys is NULL");

	/* paths */
	ip = nowdb_path_append(path, desc->name);
	if (ip == NULL) return nowdb_err_get(nowdb_err_no_mem,
	               FALSE, OBJECT, "allocating index path");

	fp = nowdb_path_append(base, ip);
	if (fp == NULL) {
		free(ip);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                     "allocating index path");
	}

	err = mkIdxDir(fp); free(fp);
	if (err != NOWDB_OK) {
		free(ip); return err;
	}

	hp = nowdb_path_append(ip, HOSTPATH);
	if (hp == NULL) {
		free(ip);
		return nowdb_err_get(nowdb_err_no_mem,
	        FALSE, OBJECT, "allocating host path");
	}

	ep = nowdb_path_append(ip, EMBPATH); free(ip);
	if (ep == NULL) {
		free(hp);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                     "allocating emb. path");
	}

	/* create host */
	beet_config_init(&cfg);
	computeHostSize(desc, size, &cfg);
	setHostCompare(desc, &cfg);
	cfg.subPath = ep;

	ber = beet_index_create(base, hp, 1, &cfg);
	if (ber != BEET_OK) {
		free(ep); free(hp);
		return makeBeetError(ber);
	}

	/* create embedded */
	beet_config_init(&cfg);
	computeEmbSize(desc, size, &cfg);
	setEmbCompare(&cfg);
	cfg.subPath = NULL;

	ber = beet_index_create(base, ep, 0, &cfg);
	if (ber != BEET_OK) {
		free(ep); free(hp);
		return makeBeetError(ber);
	}
	free(ep); free(hp);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Drop index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_drop(char *base, char *path) {
	nowdb_err_t err;
	beet_err_t  ber;
	char *ep, *hp, *tmp;

	if (base == NULL) return nowdb_err_get(nowdb_err_invalid,
	                     FALSE, OBJECT, "base path is NULL");
	if (path == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "path is NULL");

	hp = nowdb_path_append(path, HOSTPATH);
	if (hp == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "allocating host path");

	ep = nowdb_path_append(path, EMBPATH);
	if (ep == NULL) {
		free(hp);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                     "allocating emb. path");
	}
	ber = beet_index_drop(base,ep);
	if (ber != BEET_OK) {
		free(ep); free(hp);
		return makeBeetError(ber);
	}
	ber = beet_index_drop(base,hp);
	if (ber != BEET_OK) {
		free(ep); free(hp);
		return makeBeetError(ber);
	}
	tmp = nowdb_path_append(base, ep);
	if (tmp == NULL) {
		free(ep); free(hp);
		NOMEM("allocating index path");
		return err;
	}
	err = removePath(tmp);
	free(tmp); free(ep);
	if (err != NOWDB_OK) {
		free(hp);
		return err;
	}
	tmp = nowdb_path_append(base, hp);
	if (tmp == NULL) {
		free(hp);
		NOMEM("allocating index path");
		return err;
	}
	err = removePath(tmp);
	free(tmp); free(hp);
	if (err != NOWDB_OK) return err;

	tmp = nowdb_path_append(base, path);
	if (tmp == NULL) {
		NOMEM("allocating index path");
		return err;
	}
	err = removePath(tmp); free(tmp);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Open index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_open(char *base,
                             char *path,
                             void *handle,
                             nowdb_index_desc_t *desc) {
	nowdb_err_t        err;
	beet_open_config_t cfg;
	beet_err_t         ber;
	char *hp, *ip;

	if (base == NULL) return nowdb_err_get(nowdb_err_invalid,
	                     FALSE, OBJECT, "base path is NULL");
	if (path == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "path is NULL");
	if (handle == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "handle is NULL");
	if (desc == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "desc is NULL");
	if (desc->name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "desc name is NULL");
	if (desc->keys == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "desc keys is NULL");

	beet_open_config_ignore(&cfg);
	cfg.rsc = desc->keys;

	ip = nowdb_path_append(path, desc->name);
	if (ip == NULL) return nowdb_err_get(nowdb_err_no_mem,
	               FALSE, OBJECT, "allocating index path");

	hp = nowdb_path_append(ip, HOSTPATH); free(ip);
	if (hp == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "allocating host path");

	desc->idx = calloc(1, sizeof(nowdb_index_t));
	if (desc->idx == NULL) {
		free(hp);
		return nowdb_err_get(nowdb_err_no_mem,
		      FALSE, OBJECT, "allocating idx");
	}

	err = nowdb_rwlock_init(&desc->idx->lock);
	if (err != NOWDB_OK) {
		free(desc->idx); desc->idx = NULL; free(hp);
		return err;
	}

	ber = beet_index_open(base, hp, handle, &cfg, &desc->idx->idx);
	if (ber != BEET_OK) {
		free(desc->idx); desc->idx = NULL; free(hp);
		return makeBeetError(ber);
	}
	free(hp);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Close index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_close(nowdb_index_t *idx) {
	IDXNULL();
	beet_index_close(idx->idx);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * destroy index
 * ------------------------------------------------------------------------
 */
void nowdb_index_destroy(nowdb_index_t *idx) {
	if (idx == NULL) return;
	nowdb_rwlock_destroy(&idx->lock);
}

/* ------------------------------------------------------------------------
 * Announce usage of this index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_use(nowdb_index_t *idx) {
	IDXNULL();
	return nowdb_lock_read(&idx->lock);
}

/* ------------------------------------------------------------------------
 * Announce end of usage of this index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_enduse(nowdb_index_t *idx) {
	IDXNULL();
	return nowdb_unlock_read(&idx->lock);
}

/* ------------------------------------------------------------------------
 * Insert into index 
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_insert(nowdb_index_t    *idx,
                               char            *keys,
                               nowdb_pageid_t    pge,
                               nowdb_bitmap8_t  *map) {
	beet_err_t  ber;
	beet_pair_t pair;

	IDXNULL();

	pair.key = &pge;
	pair.data = map;

	ber = beet_index_insert(idx->idx, keys, &pair);
	if (ber != BEET_OK) return makeBeetError(ber);

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get index 'compare' method
 * ------------------------------------------------------------------------
 */
beet_compare_t nowdb_index_getCompare(nowdb_index_t *idx) {
	if (idx == NULL) return NULL;
	return beet_index_getCompare(idx->idx);
}

/* ------------------------------------------------------------------------
 * Get index resource
 * ------------------------------------------------------------------------
 */
void *nowdb_index_getResource(nowdb_index_t *idx) {
	if (idx == NULL) return NULL;
	return beet_index_getResource(idx->idx);
}

