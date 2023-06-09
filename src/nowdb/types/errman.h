/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Error Memory Manager
 * ========================================================================
 */
#ifndef nowdb_errman_decl
#define nowdb_errman_decl

#include <nowdb/types/error.h>

/* ------------------------------------------------------------------------
 * initialise error manager
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_errman_init();

/* ------------------------------------------------------------------------
 * destroy error manager
 * ------------------------------------------------------------------------
 */
void nowdb_errman_destroy(); 

/* ------------------------------------------------------------------------
 * get an error descriptor; fails only, if no memory is available
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_errman_get();

/* ------------------------------------------------------------------------
 * release error descriptor
 * ------------------------------------------------------------------------
 */
void nowdb_errman_release(nowdb_err_t err);

#endif

