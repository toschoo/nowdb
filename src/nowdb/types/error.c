/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Error Handling
 * ========================================================================
 *
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/types/errman.h>

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdint.h>

nowdb_errdesc_t nowdb_no_mem = {1, 0, "unknown", NULL, NULL};
nowdb_err_t NOWDB_NO_MEM = &nowdb_no_mem;

/* ------------------------------------------------------------------------
 * Initialise error memory manager
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_err_init() {
	return nowdb_errman_init();
}

/* ------------------------------------------------------------------------
 * Destroy error memory manager
 * ------------------------------------------------------------------------
 */
void nowdb_err_destroy() {
	return nowdb_errman_destroy();
}

/* ------------------------------------------------------------------------
 * Helper: get and fill error descriptor
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t errget(nowdb_errcode_t errcode,
                                 int               osErr,
                                 char            *object,
                                 char              *info)
{
	nowdb_err_t err = nowdb_errman_get();
	if (err == NULL) return NULL;

	err->info = NULL;
	err->cause = NULL;
	err->oserr = osErr;
	strcpy(err->object, "unknown");

	err->errcode = errcode;
	if (object != NULL) strncpy(err->object, object, 7);
	if (info != NULL) {
		int l = strnlen(info, NOWDB_MAX_NAME+1);
		if (l >= NOWDB_MAX_NAME) {
			nowdb_errman_release(err);
			return NULL;
		}
		err->info = malloc(l+1);
		if (err->info == NULL) {
			nowdb_errman_release(err);
			return NULL;
		}
		strncpy(err->info, info, NOWDB_MAX_NAME-1);
		err->info[l] = 0;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Get and fill error descriptor
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_err_get(nowdb_errcode_t errcode,
                          nowdb_bool_t   getOsErr,
                          char            *object,
                          char             *info) {
	return errget(errcode, getOsErr?errno:0, object, info);
}

/* ------------------------------------------------------------------------
 * Get and fill error descriptor with explicit OS error code
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_err_getRC(nowdb_errcode_t errcode,
                            int               osErr,
                            char            *object,
                            char             *info) {
	return errget(errcode, osErr, object, info);
}

/* ------------------------------------------------------------------------
 * Cascade error
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_err_cascade(nowdb_err_t error,
                              nowdb_err_t cause) {
	if (error == NOWDB_OK) {
		nowdb_err_release(cause);
		return NOWDB_OK;
	}
	if (error == cause) return error;
	if (error->cause == cause) return error;
	if (error->cause != NOWDB_OK) {
		return nowdb_err_cascade(error->cause, cause);
	} else {
		error->cause = cause;
	}
	return error;
}

/* ------------------------------------------------------------------------
 * Release error descriptor
 * ------------------------------------------------------------------------
 */
void nowdb_err_release(nowdb_err_t err) {
	if (err == NULL) return;
	if (err == NOWDB_NO_MEM) return;
	if (err->info != NULL) {
		free(err->info); err->info = NULL;
	}
	if (err->cause != NULL) nowdb_err_release(err->cause);
	nowdb_errman_release(err);
}

/* ------------------------------------------------------------------------
 * Error does contain a certain error code 
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_err_contains(nowdb_err_t err, nowdb_errcode_t rc) {
	if (err == NULL) return FALSE;
	if (err->errcode == rc) return TRUE;
	return nowdb_err_contains(err->cause, rc);
}

/* ------------------------------------------------------------------------
 * Transform error descriptor into human readable string (helper)
 * ------------------------------------------------------------------------
 */
static char *describe(nowdb_err_t err, char *indent, char term) {
	char *str;

	if (err == NULL) {
		str = malloc(4);
		if (str == NULL) return NULL;
		sprintf(str, "OK%c", term);
		return str;
	}

	const char *d = nowdb_err_desc(err->errcode);
	const char *o = "nowdb error";
	size_t len = 8; /* <indent><d> (o) in object: <info><term>" */
	                /*  4 spaces,
	                    2 ()
	                    1 :
	                    1 <term>
	                   ------------
	                    8 */

	len += strlen(indent);
	len += strlen(d) + 2;
	len += strlen(err->object);

	if (err->oserr != 0) o = strerror(err->oserr);
	len += strlen(o);

	if (err->info != NULL) {
		len += strnlen(err->info, NOWDB_MAX_NAME);
	}
	len++; /* '\0' */
	
	str = malloc(len);
	if (str == NULL) return NULL;
	
	if (err->info != NULL) {
		sprintf(str, "%s%s (%s) in %s: %s%c",
		   indent, d, o, err->object, err->info, term);
	} else {
		sprintf(str, "%s%s (%s) in %s%c",
		   indent, d, o, err->object, term);
	}
	return str;
}

/* ------------------------------------------------------------------------
 * Transform error descriptor into human readable strings
 * ------------------------------------------------------------------------
 */
char *nowdb_err_describe(nowdb_err_t err, char term) {
	return describe(err, "", term);
}

/* ------------------------------------------------------------------------
 * Print human readable string to stderr
 * ------------------------------------------------------------------------
 */
void nowdb_err_print(nowdb_err_t err) {
	char *str = describe(err, "", '\n');
	if (str == NULL) {
		fprintf(stderr, "out of memory\n");
		return;
	}
	fprintf(stderr, "%s", str);
	free(str);
	if (err == NOWDB_OK || err == NOWDB_NO_MEM) return;
	if (err->cause != NULL) nowdb_err_print(err->cause);
}

/* ------------------------------------------------------------------------
 * Write human readable string to filedescriptor
 * ------------------------------------------------------------------------
 */
void nowdb_err_send(nowdb_err_t err, int fd) {
	char *str = describe(err, "", '\n');
	if (str == NULL) {
		if (write(fd, "out of memory\n", 14) != 14) {
			fprintf(stderr, "PANIC: cannot write!\n");
		}
		return;
	}
	int x = strlen(str);
	if (write(fd, str, x) != x) {
		fprintf(stderr, "PANIC: cannot write!\n");
	}
	free(str);
	if (err == NOWDB_OK || err == NOWDB_NO_MEM) return;
	if (err->cause != NULL) nowdb_err_send(err->cause, fd);
}

/* ------------------------------------------------------------------------
 * Retrieves the errorcode from the error
 * ------------------------------------------------------------------------
 */
nowdb_errcode_t nowdb_err_code(nowdb_err_t err) {
	if (err == NOWDB_OK) return 0;
	return err->errcode;
}

/* ------------------------------------------------------------------------
 * Human readable error descriptions
 * ------------------------------------------------------------------------
 */
const char* nowdb_err_desc(nowdb_errcode_t rc) {
	switch(rc) {
	case nowdb_err_no_mem: return "out of memory";
	case nowdb_err_invalid: return "invalid parameter";
	case nowdb_err_no_rsc: return "resource not available";
	case nowdb_err_busy: return "resource busy";          
	case nowdb_err_too_big: return "you request too much";
	case nowdb_err_lock: return "lock operation failed";
	case nowdb_err_ulock: return "unlock operation failed";
	case nowdb_err_eof: return "end of file";
	case nowdb_err_not_supp: return "operation not supported";
	case nowdb_err_bad_path: return "bad path";
	case nowdb_err_bad_name: return "bad name";
	case nowdb_err_map: return "map operation failed";
	case nowdb_err_umap: return "unmap operation failed";
	case nowdb_err_read: return "read operation failed";
	case nowdb_err_write: return "write operation failed";
	case nowdb_err_open: return "open operation failed";
	case nowdb_err_close: return "close operation failed";
	case nowdb_err_remove: return "remove operation failed";
	case nowdb_err_seek: return "seek operation failed";
	case nowdb_err_panic: return "internal error: this is a bug!";
	case nowdb_err_catalog: return "operation on catalog failed";
	case nowdb_err_time: return "time operation failed";
	case nowdb_err_nosuch_scope: return "no such scope";
	case nowdb_err_nosuch_context: return "no such context";
	case nowdb_err_nosuch_index: return "no such index";
	case nowdb_err_key_not_found: return "key not found";
	case nowdb_err_dup_key: return "duplicated key";
	case nowdb_err_dup_name: return "duplicated name";
	case nowdb_err_collision: return "hash collision!";
	case nowdb_err_sync: return "sync operation failed";
	case nowdb_err_thread: return "thread operation failed";
	case nowdb_err_sleep: return "cannot sleep"; 
	case nowdb_err_queue: return "queue operation failed";
	case nowdb_err_enqueue: return "enqueue operation failed";
	case nowdb_err_worker: return "worker thread lost";
	case nowdb_err_timeout: return "timeout";
	case nowdb_err_reserve: return "no reserve available";
	case nowdb_err_bad_block: return "bad block";
	case nowdb_err_bad_filesize: return "bad filesize";
	case nowdb_err_maxfiles: return "cannot set max number of open files";
	case nowdb_err_move: return "move operation failed";
	case nowdb_err_index: return "index operation failed";
	case nowdb_err_version: return "cannot read version";
	case nowdb_err_comp: return "compression failed";
	case nowdb_err_decomp: return "decompression failed";
	case nowdb_err_compdict: return "error creating compression dictionary";
	case nowdb_err_store: return "operation on store failed";
	case nowdb_err_context: return "operation on context failed";
	case nowdb_err_scope: return "operation on scope failed";
	case nowdb_err_stat: return "stat operation failed";
	case nowdb_err_create: return "create operation failed";
	case nowdb_err_drop: return "drop operation failed";
	case nowdb_err_magic: return "wrong magic number in catalog";
	case nowdb_err_loader: return "loader failed";
	case nowdb_err_trunc: return "truncate operation failed";
	case nowdb_err_flush: return "flush operation failed";
	case nowdb_err_beet: return "beet library error";
	case nowdb_err_fun: return "unknown function";
	case nowdb_err_not_found: return "resource not found";
	case nowdb_err_parser: return "parser error";
	case nowdb_err_sigwait: return "operation sigwait failed";
	case nowdb_err_signal: return "operation pthread_kill failed";
	case nowdb_err_sigset: return "operation sigmask failed";
	case nowdb_err_socket: return "operation sigmask failed";
	case nowdb_err_bind: return "operation bind failed";
	case nowdb_err_listen: return "operation listen failed";
	case nowdb_err_accept: return "operation accept failed";
	case nowdb_err_protocol: return "protocol error";
	case nowdb_err_server: return "internal server error";
	case nowdb_err_addr: return "cannot find address information";
	case nowdb_err_python: return "python api error";
	case nowdb_err_lua: return "lua api error";
	case nowdb_err_unk_symbol: return "unknown symbol";
	case nowdb_err_usrerr: return "error in stored procedure";
	default: return "unknown";
	}
}
