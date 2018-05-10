/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Error Handling
 * ========================================================================
 * Services and data types for error handling
 * ========================================================================
 */
#ifndef nowdb_error_decl
#define nowdb_error_decl

#include <stdlib.h>
#include <string.h>

#include <nowdb/types/types.h>

/* ------------------------------------------------------------------------
 * NoWDB Error Code
 * ------------------------------------------------------------------------
 */
typedef uint32_t nowdb_errcode_t;

/* ------------------------------------------------------------------------
 * NoWDB Error Descriptor
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_errdesc_st {
	nowdb_errcode_t        errcode; /* nowdb error code */
	int32_t                  oserr; /* OS error code    */
	char                object[32]; /* nowdb object     */
	char                     *info; /* additional info  */
	struct nowdb_errdesc_st *cause; /* previous error   */
} nowdb_errdesc_t;

/* ------------------------------------------------------------------------
 * NoWDB Error
 * ------------------------------------------------------------------------
 */
typedef nowdb_errdesc_t* nowdb_err_t;

/* ------------------------------------------------------------------------
 * Special cases:
 * - OK (no error)
 * - out-of-mem
 * ------------------------------------------------------------------------
 */
#define NOWDB_OK NULL

extern nowdb_err_t NOWDB_NO_MEM;

/* ------------------------------------------------------------------------
 * Macro: true if no error occured
 * ------------------------------------------------------------------------
 */
inline nowdb_bool_t NOWDB_SUCCESS(nowdb_err_t rc) {
	if (rc == NULL) return TRUE;
	return FALSE;
}

/* ------------------------------------------------------------------------
 * Macro: true if error occurred and error is out-of-memory
 * ------------------------------------------------------------------------
 */
inline nowdb_bool_t NOWDB_HAS_NO_MEM(nowdb_err_t rc) {
	if (rc == NULL) return FALSE;
	if (rc == NOWDB_NO_MEM) return TRUE;
	return FALSE;
}

/* ------------------------------------------------------------------------
 * Initialise error memory manager
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_err_init();

/* ------------------------------------------------------------------------
 * Destroy error memory manager
 * ------------------------------------------------------------------------
 */
void nowdb_err_destroy();

/* ------------------------------------------------------------------------
 * Get and fill error descriptor
 * -----------------------------
 *
 * Parameters:
 * - error code
 * - indicate whether we should read errno
 * - the object reporting the error
 * - additional info
 *
 * Return:
 * the error descriptor or NULL in case we ran out of memory
 * 
 * NOTE: 'cause' is set to NULL and should be set
 *       explicitly  by the application.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_err_get(nowdb_errcode_t errcode,
                          nowdb_bool_t   getOsErr,
                          char            *object,
                          char             *info);


/* ------------------------------------------------------------------------
 * Get and fill error descriptor with explicit OS error code
 * -----------------------------
 *
 * Parameters:
 * - error code
 * - OS error code
 * - the object reporting the error
 * - additional info
 *
 * Return:
 * the error descriptor or NULL in case we ran out of memory
 * 
 * NOTE: 'cause' is set to NULL and should be set
 *       explicitly  by the application.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_err_getRC(nowdb_errcode_t errcode,
                            int               osErr,
                            char            *object,
                            char             *info);

/* ------------------------------------------------------------------------
 * Cascase error
 * -------------
 * corresponds to err.cause = cause.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_err_cascade(nowdb_err_t error,
                              nowdb_err_t cause);

/* ------------------------------------------------------------------------
 * Release error descriptor
 * ------------------------
 * NOTE: the error descriptor shall not be freed by the application!
 *       In situations where you want to ignore the error,
 *       call: nowdb_err_release(yourfunction);
 *         or: NOWDB_IGNORE(yourfunction);
 * ------------------------------------------------------------------------
 */
void nowdb_err_release(nowdb_err_t err);

/* ------------------------------------------------------------------------
 * Release the error descriptor
 * ------------------------------------------------------------------------
 */
inline void NOWDB_IGNORE(nowdb_err_t err) {
	nowdb_err_release(err);
}

/* ------------------------------------------------------------------------
 * Produces a human readable description of the error code
 * ------------------------------------------------------------------------
 */
const char *nowdb_err_desc(nowdb_errcode_t rc);

/* ------------------------------------------------------------------------
 * Transform error descriptor into human readable string
 * -----------------------------------------------------
 * 
 * Parameters:
 * - the error descriptor
 * - a terminal character (like ';' or '\n') 
 *
 * Return:
 * - a string describing the error;
 *   the application is responsible for freeing this string.
 * ------------------------------------------------------------------------
 */
char *nowdb_err_describe(nowdb_err_t err, char term);

/* ------------------------------------------------------------------------
 * Print human readable string to stderr
 * -------------------------------------
 * The function creates the descriptive string of 'err' (using describe)
 * and prints it on one line of of stderr;
 * if cause->cause is not NULL,
 * the function calls itself on err->cause
 * ------------------------------------------------------------------------
 */
void nowdb_err_print(nowdb_err_t err);

/* ------------------------------------------------------------------------
 * Write human readable string to filedescriptor
 * -------------------------------------
 * The function creates the descriptive string of 'err'
 * (using describe with term='\n')
 * and writes it to 'fd';
 * if cause->cause is not NULL,
 * the function calls itself on err->cause.
 * ------------------------------------------------------------------------
 */
void nowdb_err_send(nowdb_err_t err, int fd);

/* ------------------------------------------------------------------------
 * Error Codes
 * ------------------------------------------------------------------------
 */
#define nowdb_err_no_mem           1
#define nowdb_err_invalid          2
#define nowdb_err_no_rsc           3
#define nowdb_err_busy             4
#define nowdb_err_too_big          5
#define nowdb_err_lock             6
#define nowdb_err_ulock            7
#define nowdb_err_eof              8
#define nowdb_err_not_supp         9
#define nowdb_err_bad_path        10
#define nowdb_err_bad_name        11
#define nowdb_err_map             12
#define nowdb_err_umap            13
#define nowdb_err_read            14
#define nowdb_err_write           15
#define nowdb_err_open            16
#define nowdb_err_close           17
#define nowdb_err_remove          18
#define nowdb_err_seek            19
#define nowdb_err_panic           20
#define nowdb_err_catalog         21
#define nowdb_err_time            22
#define nowdb_err_nosuch_scope    23
#define nowdb_err_nosuch_context  24
#define nowdb_err_nosuch_index    25
#define nowdb_err_key_not_found   26
#define nowdb_err_dup_key         27
#define nowdb_err_dup_name        28
#define nowdb_err_collision       29 
#define nowdb_err_sync            30
#define nowdb_err_thread          31
#define nowdb_err_sleep           32
#define nowdb_err_queue           33
#define nowdb_err_enqueue         34
#define nowdb_err_worker          35
#define nowdb_err_timeout         36
#define nowdb_err_reserve         37
#define nowdb_err_bad_block       38
#define nowdb_err_bad_filesize    39
#define nowdb_err_maxfiles        40
#define nowdb_err_move            41
#define nowdb_err_index           42
#define nowdb_err_version         43
#define nowdb_err_comp            44
#define nowdb_err_decomp          45
#define nowdb_err_compdict        46
#define nowdb_err_store           47
#define nowdb_err_context         48
#define nowdb_err_scope           49
#define nowdb_err_stat            50
#define nowdb_err_create          51
#define nowdb_err_drop            52
#define nowdb_err_magic           53
#define nowdb_err_unknown       9999

#endif

