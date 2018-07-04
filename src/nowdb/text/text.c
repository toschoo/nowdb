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

/* ------------------------------------------------------------------------
 * Init text manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_text_init(nowdb_text_t *txt);

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

