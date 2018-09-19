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

#define LIB(x) \
	((nowdb_t*)x)

/* ------------------------------------------------------------------------
 * Create proc interface
 * ------------------------------------------------------------------------
 */
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
		free(*proc); *proc = NULL;
		return err;
	}
	if (nowdbsql_parser_init((*proc)->parser, stdin) != 0) {
		err = nowdb_err_get(nowdb_err_parser, FALSE, OBJECT,
		                              "cannot init parser");
		free((*proc)->parser); (*proc)->parser = NULL;
		free(*proc); *proc = NULL;
		return err;
	}

	/* create new sub-interpreter */
#ifdef _NOWDB_WITH_PYTHON
	if (LIB(lib)->mst != NULL) {
		PyEval_RestoreThread(LIB(lib)->mst); // acquire lock
		(*proc)->pyIntp = Py_NewInterpreter();
		if ((*proc)->pyIntp == NULL) {
			nowdb_proc_destroy(*proc);
			free(*proc); *proc = NULL;
			return nowdb_err_get(nowdb_err_python, FALSE, OBJECT,
			                     "cannot create new interpreter");
		}
		PyThreadState_Swap(LIB(lib)->mst);   // swap back to main
		LIB(lib)->mst = PyEval_SaveThread(); // release lock
	}
#endif
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy proc interface
 * ------------------------------------------------------------------------
 */
void nowdb_proc_destroy(nowdb_proc_t *proc) {
	if (proc == NULL) return;
	if (proc->parser != NULL) {
		nowdbsql_parser_destroy(proc->parser);
		free(proc->parser); proc->parser = NULL;
	}
	if (proc->scope != NULL) {
		proc->scope = NULL;
	}
#ifdef _NOWDB_WITH_PYTHON
	if (proc->pyIntp != NULL) {
		PyThreadState_Clear(proc->pyIntp);
		PyThreadState_Delete(proc->pyIntp);
		proc->pyIntp = NULL;
	}
#endif
}

/* ------------------------------------------------------------------------
 * Set current scope
 * ------------------------------------------------------------------------
 */
void nowdb_proc_setScope(nowdb_proc_t  *proc, 
                         nowdb_scope_t *scope) {
	proc->scope = scope;
}

/* ------------------------------------------------------------------------
 * Get scope from proc interface
 * ------------------------------------------------------------------------
 */
nowdb_scope_t *nowdb_proc_getScope(nowdb_proc_t *proc) {
	return proc->scope;
}

/* ------------------------------------------------------------------------
 * Get lib from proc interface
 * ------------------------------------------------------------------------
 */
void *nowdb_proc_getLib(nowdb_proc_t *proc) {
	if (proc == NULL) return NULL;
	return proc->lib;
}

/* ------------------------------------------------------------------------
 * Get PyThread from proc interface
 * ------------------------------------------------------------------------
 */
void *nowdb_proc_getInterpreter(nowdb_proc_t *proc) {
#ifdef _NOWDB_WITH_PYTHON
	return proc->pyIntp;
#else
	return NULL;
#endif
}

/* ------------------------------------------------------------------------
 * Get PyThread from proc interface
 * ------------------------------------------------------------------------
 */
void nowdb_proc_updateInterpreter(nowdb_proc_t *proc) {
#ifdef _NOWDB_WITH_PYTHON
	proc->pyIntp = PyEval_SaveThread();
#endif
}


