/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Stored Procedure Interface (internal)
 * ========================================================================
 */
#include <nowdb/ifc/proc.h>
#include <nowdb/ifc/nowdb.h>

static char *OBJECT = "proc";

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

nowdb_err_t nowdb_proc_create(nowdb_proc_t **proc,
                              void          *lib,
                              nowdb_scope_t *scope) {
	nowdb_err_t err;

	*proc = calloc(1, sizeof(nowdb_proc_t));
	if (*proc == NULL) {
		NOMEM("allocating proc interface");
		return err;
	}

	(*proc)->lib = lib;
	(*proc)->scope = scope;

	(*proc)->parser = calloc(1,sizeof(nowdbsql_parser_t));
	if ((*proc)->parser == NULL) {
		NOMEM("allocating parser");
		return err;
	}
	if (nowdbsql_parser_init((*proc)->parser, stdin) != 0) {
		err = nowdb_err_get(nowdb_err_parser, FALSE, OBJECT,
		                              "cannot init parser");
		free((*proc)->parser); (*proc)->parser = NULL;
		return err;
	}
	return NOWDB_OK;
}

void nowdb_proc_destroy(nowdb_proc_t *proc) {
	if (proc == NULL) return;
	if (proc->parser != NULL) {
		nowdbsql_parser_destroy(proc->parser);
		free(proc->parser); proc->parser = NULL;
	}
	if (proc->scope != NULL) {
		proc->scope = NULL;
	}
}

void nowdb_proc_setScope(nowdb_proc_t  *proc, 
                         nowdb_scope_t *scope) {
	proc->scope = scope;
}

