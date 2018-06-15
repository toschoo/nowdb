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

static char *OBJECT = "idx";

#define HOSTPATH "host"
#define EMBPATH "emb"

#define IDXNULL() \
	if (idx == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                            FALSE, OBJECT, "index NULL");

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
 * Helper: compute key size from desc
 * ------------------------------------------------------------------------
 */
static uint32_t computeKeySize(nowdb_index_desc_t *desc) {
	uint32_t s = 0;

	for(int i=0;i<desc->keys->sz;i++) {
		if ((desc->ctx == NULL &&
		     desc->keys->off[i] == NOWDB_OFF_VTYPE) ||
		    (desc->ctx != NULL &&
		     desc->keys->off[i] >= NOWDB_OFF_WTYPE))
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
	if (desc->ctx == NULL) {
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

#define HOSTDSZ sizeof(nowdb_pageid_t)
#define EMBKSZ sizeof(nowdb_pageid_t)
#define EMBDSZ 16
#define INTDSZ sizeof(beet_pageid_t)

static void computeEmbSize(nowdb_index_desc_t *desc,
                           uint16_t            size,
                           beet_config_t      *cfg) {
	uint32_t targetsz = 0;

	cfg->indexType = BEET_INDEX_PLAIN;
	cfg->subPath = NULL;

	cfg->keySize = EMBKSZ;
	cfg->dataSize = EMBDSZ;
	
	switch(size) {
		case NOWDB_CONFIG_SIZE_TINY:

			targetsz = 512;

			cfg->leafCacheSize = 1000;
			cfg->intCacheSize = 1000;

			break;
			
		case NOWDB_CONFIG_SIZE_SMALL: 
		case NOWDB_CONFIG_SIZE_MEDIUM:

			targetsz = 1024;

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

	cfg->leafNodeSize = (targetsz-12)/(EMBKSZ+EMBDSZ);
	cfg->intNodeSize = (targetsz-8)/(EMBKSZ+INTDSZ);

	cfg->leafPageSize = cfg->leafNodeSize * (EMBKSZ + EMBDSZ) + 12;
	cfg->intPageSize = cfg->intNodeSize * (EMBKSZ + INTDSZ) + 8;
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
}

/* ------------------------------------------------------------------------
 * Create index
 * "index" is a host beetree
 * there are other usages for beetree (vertex and strings),
 * but they are not covered here!
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_create(char *path, uint16_t size,
                               nowdb_index_desc_t  *desc) {
	beet_err_t    ber;
	beet_config_t cfg;
	char *ep, *hp;

	/* paths */
	hp = nowdb_path_append(path, HOSTPATH);
	if (hp == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "allocating host path");

	ep = nowdb_path_append(hp, EMBPATH);
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

	ber = beet_index_create(hp, 1, &cfg);
	if (ber != BEET_OK) {
		free(ep); free(hp);
		return makeBeetError(ber);
	}

	/* create embedded */
	beet_config_init(&cfg);
	computeEmbSize(desc, size, &cfg);
	setEmbCompare(&cfg);
	cfg.subPath = NULL;

	ber = beet_index_create(ep, 0, &cfg);
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
nowdb_err_t nowdb_index_drop(char *path) {
	beet_err_t  ber;
	char *ep, *hp;

	/* paths */
	hp = nowdb_path_append(path, HOSTPATH);
	if (hp == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "allocating host path");

	ep = nowdb_path_append(hp, EMBPATH);
	if (ep == NULL) {
		free(hp);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                     "allocating emb. path");
	}
	ber = beet_index_drop(ep);
	if (ber != BEET_OK) {
		fprintf(stderr, "dropping %s\n", ep);
		free(ep); free(hp);
		return makeBeetError(ber);
	}
	ber = beet_index_drop(hp);
	if (ber != BEET_OK) {
		free(ep); free(hp);
		return makeBeetError(ber);
	}
	free(ep); free(hp);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Open index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_open(char *path, void *handle,
                             nowdb_index_desc_t *desc) {
	nowdb_err_t        err;
	beet_open_config_t cfg;
	beet_err_t         ber;
	char *hp;

	beet_open_config_ignore(&cfg);
	cfg.rsc = desc->keys;

	hp = nowdb_path_append(path, HOSTPATH);
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

	ber = beet_index_open(hp, handle, &cfg, &desc->idx->idx);
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
	nowdb_err_t err;

	IDXNULL();

	err = nowdb_lock_write(&idx->lock);
	if (err != NOWDB_OK) return err;

	beet_index_close(idx->idx);

	return nowdb_unlock_write(&idx->lock);
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

