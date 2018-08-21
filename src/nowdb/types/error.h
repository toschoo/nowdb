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
#include <nowdb/errcode.h>

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
	nowdb_errcode_t        errcode; /* nowdb error code  */
	int32_t                  oserr; /* OS error code     */
	char                object[32]; /* nowdb object      */
	char                     *info; /* additional info   */
	struct nowdb_errdesc_st *cause; /* previous error    */
	uint64_t              reserved; /* now it's 64 bytes */
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
 * Error does contain a certain error code 
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_err_contains(nowdb_err_t err, nowdb_errcode_t rc);

/* ------------------------------------------------------------------------
 * Retrieves the errorcode from the error
 * ------------------------------------------------------------------------
 */
nowdb_errcode_t nowdb_err_code(nowdb_err_t err);

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

#endif

