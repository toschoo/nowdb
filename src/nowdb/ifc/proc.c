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
	PyObject   *c;    /* caller      */
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
	PyObject           *c;
#else
	void               *f;
	void               *c;
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
	
	Py_EndInterpreter(proc->pyIntp);
	proc->pyIntp = NULL;
	PyEval_ReleaseLock(); // deprecated
	
	/*
	PyThreadState_Clear(proc->pyIntp);
	PyThreadState_Delete(proc->pyIntp);
	*/
	proc->pyIntp = NULL;
#endif
}

#ifdef _NOWDB_WITH_PYTHON
/* ------------------------------------------------------------------------
 * Helper: set DB
 * ------------------------------------------------------------------------
 */
static nowdb_err_t setDB(nowdb_proc_t *proc,
                         PyObject     *dict) 
{
	nowdb_err_t err;
	PyObject  *args;
	PyObject  *r;
	PyObject  *f;
	PyObject  *m, *d;

	/*
	PyObject  *k=NULL, *v=NULL;
	Py_ssize_t pos = 0;

	while(PyDict_Next(dict, &pos, &k, &v)) {
		PyObject *str = PyObject_Repr(k);
		if (str == NULL) {
			fprintf(stderr, "no string\n"); continue;
		}
		const char* s = PyString_AsString(str);
		Py_DECREF(str);
		fprintf(stderr, "KEY %d: %s\n", (int)pos, s);
	}
	*/

	m = PyDict_GetItemString(dict, "nowdb");
	if (m == NULL) {
		PYTHONERR("nowdb not imported");
		return err;
	}

	d = PyModule_GetDict(m);
	if (d == NULL) {
		PYTHONERR("cannot get dictionary");
    		return err;
	}
	
	f = PyDict_GetItemString(d, "_setDB");
	if (f == NULL) {
		PYTHONERR("no _setDB in module");
		return err;
	}
	if (!PyCallable_Check(f)) {
		PYTHONERR("_setDB not callable");
		return err;
	}

	args = PyTuple_New((Py_ssize_t)1);
	if (args == NULL) {
		NOMEM("allocating Python tuple");
		return err;
	}

	if (PyTuple_SetItem(args,
	   (Py_ssize_t)0,
	    Py_BuildValue("K", (uint64_t)proc)) != 0) 
	{
		Py_DECREF(args);
		PYTHONERR("cannot set item to tuple");
		return err;
	}

	r = PyObject_CallObject(f, args);
	if (args != NULL) Py_DECREF(args);
	if (r != NULL) Py_DECREF(r);

	return NOWDB_OK;
}
static PyObject *getCaller(PyObject *dict)
{
	PyObject  *f;
	PyObject  *m, *d;

	m = PyDict_GetItemString(dict, "nowdb");
	if (m == NULL) return NULL;

	d = PyModule_GetDict(m);
	if (d == NULL) return NULL;
	
	f = PyDict_GetItemString(d, "_caller");
	if (f == NULL) return NULL;

	if (!PyCallable_Check(f)) return NULL;

	return f;
}
#endif

#define TREE(x) \
	((ts_algo_tree_t*)x)

#ifdef _NOWDB_WITH_PYTHON
/* ------------------------------------------------------------------------
 * Map: reload module
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t reloadModule(void *tree, void *module) {
	nowdb_err_t err;
	PyObject *m;

	if (MODULE(module)->m != NULL) {
		m = PyImport_ReloadModule(MODULE(module)->m);
		if (m == NULL) {
			fprintf(stderr, "cannot reload\n");
			PyErr_Print();
			return TS_ALGO_NO_MEM;
		}
		Py_DECREF(MODULE(module)->m);
		MODULE(module)->m = m;

		MODULE(module)->d = PyModule_GetDict(m);
		if (MODULE(module)->d == NULL) {
			fprintf(stderr, "cannot get dictionary\n");
			PyErr_Print();
    			return TS_ALGO_NO_MEM;
		}

		err = setDB(TREE(tree)->rsc,MODULE(module)->d);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return TS_ALGO_ERR;
		}

		MODULE(module)->c = getCaller(MODULE(module)->d);
		if (MODULE(module)->c == NULL) {
			fprintf(stderr, "cannot get caller\n");
			PyErr_Print();
    			return TS_ALGO_NO_MEM;
		}
	}
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Helper: reload modules
 * ------------------------------------------------------------------------
 */
static nowdb_err_t reloadModules(nowdb_proc_t *proc) {
	nowdb_err_t err;

	proc->mods->rsc = proc;
	if (ts_algo_tree_map(proc->mods, &reloadModule) != TS_ALGO_OK) {
		PYTHONERR("cannot reload modules");
		return err;
	}
	if (proc->funs != NULL) {
		ts_algo_tree_destroy(proc->funs);
		if (ts_algo_tree_init(proc->funs,
		                      &funcompare, NULL,
		                      &noupdate,
		                      &fundestroy,
		                      &fundestroy) != 0) {
			NOMEM("tree.init");
			free(proc->funs); proc->funs = NULL;
			return err;
		}
	}
	return NOWDB_OK;
}
#endif

/* ------------------------------------------------------------------------
 * Helper: reinit interpreter
 * ------------------------------------------------------------------------
 */
static nowdb_err_t reinitInterpreter(nowdb_proc_t *proc) {
	nowdb_err_t err = NOWDB_OK;

	if (proc == NULL) return NOWDB_OK;
	if (proc->lib == NULL) return NOWDB_OK;

#ifdef _NOWDB_WITH_PYTHON
	
	/* do we need this???
	if (LIB(proc->lib)->mst != NULL) {
		if (rand()%100 == 0) {
			PyEval_RestoreThread(LIB(proc->lib)->mst);
			PyGC_Collect();
			LIB(proc->lib)->mst = PyEval_SaveThread();
		}
	}
	*/

	if (proc->pyIntp != NULL) {
		PyEval_RestoreThread(proc->pyIntp); // acquire lock
	}

	err = reloadModules(proc);

	if (proc->pyIntp != NULL) {
		proc->pyIntp = PyEval_SaveThread(); // release lock
	}
#endif
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: create interpreter
 * ------------------------------------------------------------------------
 */
static nowdb_err_t createInterpreter(nowdb_proc_t *proc) {

	if (proc == NULL) return NOWDB_OK;
	if (proc->lib == NULL) return NOWDB_OK;

#ifdef _NOWDB_WITH_PYTHON
	fprintf(stderr, "CREATING INTERPRETER\n");
	if (LIB(proc->lib)->mst != NULL) {
		PyEval_RestoreThread(LIB(proc->lib)->mst); // acquire lock

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

	(*proc)->dml = calloc(1, sizeof(nowdb_dml_t));
	if ((*proc)->dml == NULL) {
		NOMEM("allocating parser");
		free(*proc); *proc = NULL;
	}

	err = nowdb_dml_init((*proc)->dml, (*proc)->scope, 1);
	if (err != NOWDB_OK) {
		free((*proc)->dml); (*proc)->dml = NULL;
		free(*proc); *proc = NULL;
		return err;
	}

	(*proc)->parser = calloc(1,sizeof(nowdbsql_parser_t));
	if ((*proc)->parser == NULL) {
		NOMEM("allocating parser");
		free((*proc)->dml); (*proc)->dml = NULL;
		free(*proc); *proc = NULL;
		return err;
	}
	if (nowdbsql_parser_init((*proc)->parser, stdin) != 0) {
		err = nowdb_err_get(nowdb_err_parser, FALSE, OBJECT,
		                              "cannot init parser");
		free((*proc)->dml); (*proc)->dml = NULL;
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
	if (proc->dml != NULL) {
		nowdb_dml_destroy(proc->dml);
		free(proc->dml); proc->dml = NULL;
	}
#ifdef _NOWDB_WITH_PYTHON
	if (proc->pyIntp != NULL) {
		PyEval_RestoreThread(proc->pyIntp);
	}
#endif
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
	return reinitInterpreter(proc);
}

/* ------------------------------------------------------------------------
 * Set current scope
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_proc_setScope(nowdb_proc_t  *proc, 
                                nowdb_scope_t *scope) {
	proc->scope = scope;
	nowdb_dml_destroy(proc->dml);
	return nowdb_dml_init(proc->dml, proc->scope, 1);
}

/* ------------------------------------------------------------------------
 * Get scope from proc interface
 * ------------------------------------------------------------------------
 */
nowdb_scope_t *nowdb_proc_getScope(nowdb_proc_t *proc) {
	return proc->scope;
}

/* ------------------------------------------------------------------------
 * Get DML helper from proc interface
 * ------------------------------------------------------------------------
 */
nowdb_dml_t *nowdb_proc_getDML(nowdb_proc_t *proc) {
	return proc->dml;
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
	PyObject *c=NULL;
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
    		return err;
	}

	err = setDB(proc, d);
	if (err != NOWDB_OK) {
		Py_XDECREF(m);
		return err;
	}

	c = getCaller(d);
	if (c == NULL) {
		Py_XDECREF(m);
		return err;
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
	(*module)->c = c;

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
	(*fun)->c = module->c;
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
                               void             **fun,
                               void            **call) {
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
		*call = f->c;
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
        *call = m->c;

	UNLOCK();
	return NOWDB_OK;
}

PyObject *nowdb_proc_call(void *call, void *fun, void *args) {
	if (call == NULL) {
		fprintf(stderr, "NO CALLER\n");
		return NULL;
	}
	if (args == NULL) {
		return PyObject_CallFunctionObjArgs(call, fun, Py_None, NULL);
	} else {
		return PyObject_CallFunctionObjArgs(call, fun, args, NULL);
	}
}

