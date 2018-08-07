/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Text
 * ========================================================================
 * ========================================================================
 */
#ifndef nowdb_text_decl
#define nowdb_text_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/mem/lru.h>

#include <beet/index.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------------
 * text manager
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_rwlock_t    lock; /* read/write lock         */
	char             *path; /* path to text manager    */
	nowdb_lru_t   *str2num; /* lru cache               */
	nowdb_lru_t   *num2str; /* lru cache               */
	beet_index_t     szidx; /* maps id to str size     */
	beet_index_t   tinystr; /* maps string < 8 to id   */
	beet_index_t   tinynum; /* maps id to string < 8   */
	beet_index_t  smallstr; /* maps string < 32 to id  */
	beet_index_t  smallnum; /* maps id to string < 32  */
	beet_index_t mediumstr; /* maps string < 128 to id */
	beet_index_t mediumnum; /* maps id to string < 128 */
	beet_index_t    bigstr; /* maps string < 256 to id */
	beet_index_t    bignum; /* maps id to string < 256 */
	uint32_t        next32; /* next key32 to be used   */
	uint64_t        next64; /* next key64 to be used   */
} nowdb_text_t;

/* ------------------------------------------------------------------------
 * Init text manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_init(nowdb_text_t *txt, char *path);

/* ------------------------------------------------------------------------
 * Destroy text manager
 * ------------------------------------------------------------------------
 */
void nowdb_text_destroy(nowdb_text_t *txt);

/* ------------------------------------------------------------------------
 * Create text manager physically on disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_create(nowdb_text_t *txt);

/* ------------------------------------------------------------------------
 * Drop text manager physically from disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_drop(nowdb_text_t *txt);

/* ------------------------------------------------------------------------
 * Open existing text manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_open(nowdb_text_t *txt);

/* ------------------------------------------------------------------------
 * Close text manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_close(nowdb_text_t *txt);

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
#endif
