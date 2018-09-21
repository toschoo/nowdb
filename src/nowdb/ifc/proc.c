/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Stored Procedure Interface
 * ========================================================================
 */
#include <nowdb/scope/scope.h>
#include <nowdb/ifc/proc.h>
#include <nowdb/ifc/nowdb.h>

static char *OBJECT = "proc";

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

#define INVALID(s) \
	err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define LIB(x) \
	((nowdb_t*)x)

#define ENABLED(p) \
	if ((*p)->lang == NOWDB_STORED_PYTHON && \
	    !LIB(proc->lib)->pyEnabled) { \
		nowdb_proc_desc_destroy(*p); \
		free(*p); *p = NULL; \
		INVALID("feature not enabled: Python"); \
		return err; \
	}

#ifdef _NOWDB_WITH_PYTHON
#define LOCK() \
	PyEval_RestoreThread(proc->pyIntp);
#define UNLOCK() \
	nowdb_proc_updateInterpreter(proc);
#else
#define LOCK()
#define UNLOCK()
#endif

/* -------------------------------------------------------------------------
 * Macro for the very common error "python error"
 * -------------------------------------------------------------------------
 */
#define PYTHONERR(s) \
	err = nowdb_err_get(nowdb_err_python, FALSE, OBJECT, s); \
	PyErr_Print()

/* ------------------------------------------------------------------------
 * Imported modules
 * ------------------------------------------------------------------------
 */
typedef struct {
	char *modname;    /* module name */
#ifdef _NOWDB_WITH_PYTHON
	PyObject   *m;    /* module      */
	PyObject   *d;    /* dictionary  */
#endif
} module_t;

#define MODULE(x) \
	((module_t*)x)

/* ------------------------------------------------------------------------
 * Module callbacks
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t modulecompare(void *rsc, void *one, void *two) {
	char x;
	x = strcmp(MODULE(one)->modname, MODULE(two)->modname);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static void moduledestroy(void *rsc, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (MODULE(*n)->modname != NULL) {
		free(MODULE(*n)->modname);
	}
#ifdef _NOWDB_WITH_PYTHON
	Py_XDECREF(MODULE(*n)->m);
	MODULE(*n)->m = NULL;
	MODULE(*n)->d = NULL;
#endif
	free(*n); *n=NULL;
}

/* -----------------------------------------------------------------------
 * generic no update
 * -----------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *rsc, void *o, void *n) {
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Loaded functions
 * ------------------------------------------------------------------------
 */
typedef struct {
	char         *funname;
	nowdb_proc_desc_t *pd;
#ifdef _NOWDB_WITH_PYTHON
	PyObject           *f;
#else
	void               *f;
#endif
} fun_t;

#define FUN(x) \
	((fun_t*)x)

/* ------------------------------------------------------------------------
 * Function callbacks
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t funcompare(void *rsc, void *one, void *two) {
	char x;
	x = strcmp(FUN(one)->funname, FUN(two)->funname);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static void fundestroy(void *rsc, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (FUN(*n)->funname != NULL) {
		free(FUN(*n)->funname);
		FUN(*n)->funname=NULL;
	}
	if (FUN(*n)->pd != NULL) {
		nowdb_proc_desc_destroy(FUN(*n)->pd);
		free(FUN(*n)->pd);
		FUN(*n)->pd =NULL;
	}
#ifdef _NOWDB_WITH_PYTHON
	FUN(*n)->f = NULL;
#endif
	free(*n); *n=NULL;
}

/* ------------------------------------------------------------------------
 * Helper: destroy interpreter
 * ------------------------------------------------------------------------
 */
static void destroyInterpreter(nowdb_proc_t *proc) {
	if (proc == NULL) return;

#ifdef _NOWDB_WITH_PYTHON
	if (proc->pyIntp == NULL) return;
	PyThreadState_Clear(proc->pyIntp);
	PyThreadState_Delete(proc->pyIntp);
	proc->pyIntp = NULL;
#endif
}

/* ------------------------------------------------------------------------
 * Helper: create interpreter
 * ------------------------------------------------------------------------
 */
static nowdb_err_t createInterpreter(nowdb_proc_t *proc) {
	nowdb_err_t err;

	if (proc == NULL) return NOWDB_OK;
	if (proc->lib == NULL) return NOWDB_OK;

#ifdef _NOWDB_WITH_PYTHON
	if (proc->pyIntp != NULL) {
		PyEval_RestoreThread(proc->pyIntp); // acquire lock
	}
#endif
	if (proc->mods != NULL) {
		ts_algo_tree_destroy(proc->mods);
		if (ts_algo_tree_init(proc->mods,
		                      &modulecompare, NULL,
		                      &noupdate,
		                      &moduledestroy,
		                      &moduledestroy) != 0) {
			NOMEM("tree.init");
			proc->pyIntp = PyEval_SaveThread(); // release lock
			return err;
		}
	}
	if (proc->funs != NULL) {
		ts_algo_tree_destroy(proc->funs);
		if (ts_algo_tree_init(proc->funs,
		                      &funcompare, NULL,
		                      &noupdate,
		                      &fundestroy,
		                      &fundestroy) != 0) {
			NOMEM("tree.init");
			proc->pyIntp = PyEval_SaveThread(); // release lock
			return err;
		}
	}

#ifdef _NOWDB_WITH_PYTHON
	/*
	PyThreadState *ts = PyThreadState_Get();
	Py_EndInterpreter(ts); // proc->pyIntp);
	*/

	if (proc->pyIntp != NULL) {
		Py_EndInterpreter(proc->pyIntp);
		proc->pyIntp = NULL;
		PyEval_ReleaseLock();
		// proc->pyIntp = PyEval_SaveThread(); // release lock
	}
	if (LIB(proc->lib)->mst != NULL) {
		PyEval_RestoreThread(LIB(proc->lib)->mst); // acquire lock

		destroyInterpreter(proc); // destroy thread state

		proc->pyIntp = Py_NewInterpreter(); // new thread state
		if (proc->pyIntp == NULL) {
			return nowdb_err_get(nowdb_err_python, FALSE, OBJECT,
			                     "cannot create new interpreter");
		}
		PyThreadState_Swap(LIB(proc->lib)->mst);   // swap back to main
		LIB(proc->lib)->mst = PyEval_SaveThread(); // release lock
	}
#endif
	return NOWDB_OK;
} 

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

	(*proc)->mods = ts_algo_tree_new(
		             &modulecompare, NULL,
		             &noupdate,
		             &moduledestroy,
		             &moduledestroy);
	if ((*proc)->mods == NULL) {
		nowdb_proc_destroy(*proc);
		free(*proc); *proc = NULL;
		NOMEM("tree.create");
		return err;
	}

	(*proc)->funs = ts_algo_tree_new(
		             &funcompare, NULL,
		             &noupdate,
		             &fundestroy,
		             &fundestroy);
	if ((*proc)->funs == NULL) {
		nowdb_proc_destroy(*proc);
		free(*proc); *proc = NULL;
		NOMEM("tree.create");
		return err;
	}

	/* create new sub-interpreter */
	err = createInterpreter(*proc);
	if (err != NOWDB_OK) {
		nowdb_proc_destroy(*proc);
		free(*proc); *proc = NULL;
		return err;
	}
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
	if (proc->mods != NULL) {
		ts_algo_tree_destroy(proc->mods);
		free(proc->mods); proc->mods = NULL;
	}
	if (proc->funs != NULL) {
		ts_algo_tree_destroy(proc->funs);
		free(proc->funs); proc->funs = NULL;
	}
	destroyInterpreter(proc);
}

/* ------------------------------------------------------------------------
 * Reinit proc interface
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_proc_reinit(nowdb_proc_t *proc) {
	return createInterpreter(proc);
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
 * update PyThread
 * ------------------------------------------------------------------------
 */
void nowdb_proc_updateInterpreter(nowdb_proc_t *proc) {
#ifdef _NOWDB_WITH_PYTHON
	proc->pyIntp = PyEval_SaveThread();
#endif
}

/* ------------------------------------------------------------------------
 * Helper: load module
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadModule(nowdb_proc_t *proc,
                              char        *mname,
                              module_t   **module) 
{
	nowdb_err_t err = NOWDB_OK;

#ifdef _NOWDB_WITH_PYTHON
	PyObject *mn=NULL;
	PyObject *m=NULL;
	PyObject *d=NULL;
#endif

#ifdef _NOWDB_WITH_PYTHON
	mn = PyString_FromString(mname);
	if (mn == NULL) {
		PYTHONERR("cannot convert module name to PyString");
    		return err;
	}

  	m = PyImport_Import(mn); Py_XDECREF(mn);
	if (m == NULL) {
		PYTHONERR("cannot import module");
    		return err;
	}

	d = PyModule_GetDict(m);
	if (d == NULL) {
		PYTHONERR("cannot get dictionary");
		Py_XDECREF(m);
    		return NULL;
	}

	*module = calloc(1, sizeof(module_t));
	if (*module == NULL) {
		NOMEM("allocating module");
		Py_XDECREF(m);
		return err;
	}

	(*module)->modname = strdup(mname);
	if ((*module)->modname == NULL) {
		NOMEM("allocating module name");
		Py_XDECREF(m); free(*module);
		return err;
	}

	(*module)->m = m;
	(*module)->d = d;

	if (ts_algo_tree_insert(proc->mods, *module) != TS_ALGO_OK) {
		NOMEM("tree.insert");
		Py_XDECREF(m); free((*module)->modname); free(*module);
		return err;
	}
#endif
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: load fun from module
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadFun(nowdb_proc_t    *proc,
                           nowdb_proc_desc_t *pd,
                           module_t      *module,
                           fun_t           **fun) 
{
	nowdb_err_t err = NOWDB_OK;

#ifdef _NOWDB_WITH_PYTHON
	PyObject *f=NULL;
#endif

#ifdef _NOWDB_WITH_PYTHON
	f = PyDict_GetItemString(module->d, pd->name);
	if (f == NULL) {
		PYTHONERR("cannot find function in module");
		return err;
	}
	/* if f == NULL) reload module and try again */
	if (!PyCallable_Check(f)) {
		PYTHONERR("function not callable");
		return err;
	}
	*fun = calloc(1, sizeof(fun_t));
	if (*fun == NULL) {
		NOMEM("allocating function");
		return err;
	}
	(*fun)->funname = strdup(pd->name);
	if ((*fun)->funname == NULL) {
		free(*fun); *fun = NULL;
		NOMEM("allocating function name");
		return err;
	}

	(*fun)->f = f;
	(*fun)->pd = pd;

	if (ts_algo_tree_insert(proc->funs, *fun) != TS_ALGO_OK) {
		NOMEM("tree.insert");
		free((*fun)->funname); free(*fun);
		return err;
	}
#endif
	return err;
}

/* ------------------------------------------------------------------------
 * Load function
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_proc_loadFun(nowdb_proc_t     *proc,
                               char            *fname,
                               nowdb_proc_desc_t **pd,
                               void             **fun) {
	nowdb_err_t       err;
	module_t *m, mpattern;
	fun_t    *f, fpattern;

	if (proc->scope == NULL) {
		INVALID("no scope set");
		return err;
	}

	fpattern.funname = fname;
	f = ts_algo_tree_find(proc->funs, &fpattern);
	if (f != NULL) {
		ENABLED(&f->pd);
		*pd  = f->pd;
		*fun = f->f;
		return NOWDB_OK;
	}

	err = nowdb_scope_getProcedure(proc->scope, fname, pd);
	if (err != NOWDB_OK) return err;

	ENABLED(pd);

	LOCK();

	mpattern.modname = (*pd)->module;
	m = ts_algo_tree_find(proc->mods, &mpattern);
	if (m == NULL) {
		err = loadModule(proc, (*pd)->module, &m);
		if (err != NOWDB_OK) {
			nowdb_proc_desc_destroy(*pd);
			free(*pd); *pd = NULL;
			UNLOCK();
			return err;
		}
	}

	err = loadFun(proc, *pd, m, &f);
	if (err != NOWDB_OK) {
		nowdb_proc_desc_destroy(*pd);
		free(*pd); *pd = NULL;
		UNLOCK();
		return err;
	}

	*fun = f->f;

	UNLOCK();
	return NOWDB_OK;
}
