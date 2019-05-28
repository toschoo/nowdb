/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Stored Procedure Interface
 * ========================================================================
 */
#include <nowdb/scope/scope.h>
#include <nowdb/ifc/proc.h>
#include <nowdb/ifc/luaproc.h>
#include <nowdb/ifc/nowdb.h>

static char *OBJECT = "proc";

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

#define INVALID(s) \
	err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define LIB(x) \
	((nowdb_t*)x)

#define LUA proc->luaIntp

/* -------------------------------------------------------------------------
 * Check whether Language is enabled
 * -------------------------------------------------------------------------
 */
#define ENABLED(p) \
	if ((*p)->lang == NOWDB_STORED_PYTHON && \
	    !LIB(proc->lib)->pyEnabled) { \
		nowdb_proc_desc_destroy(*p); \
		free(*p); *p = NULL; \
		INVALID("feature not enabled: Python"); \
		return err; \
	} else if ((*p)->lang == NOWDB_STORED_LUA && \
	    !LIB(proc->lib)->luaEnabled) { \
		nowdb_proc_desc_destroy(*p); \
		free(*p); *p = NULL; \
		INVALID("feature not enabled: Lua"); \
		return err; \
	}

/* -------------------------------------------------------------------------
 * Lock Python Sub-Interpreter
 * -------------------------------------------------------------------------
 */
#ifdef _NOWDB_WITH_PYTHON
#define LOCK(p) \
	if ((*p)->lang == NOWDB_STORED_PYTHON) { \
		PyEval_RestoreThread(proc->pyIntp); \
	}
#define UNLOCK(p) \
	if ((*p)->lang == NOWDB_STORED_PYTHON) { \
		nowdb_proc_updateInterpreter(proc); \
	}
#else
#define LOCK(p)
#define UNLOCK(p)
#endif

/* -------------------------------------------------------------------------
 * Macro for the very common error "python error"
 * -------------------------------------------------------------------------
 */
#define PYTHONERR(s) \
	err = nowdb_err_get(nowdb_err_python, FALSE, OBJECT, s); \
	PyErr_Print()

/* -------------------------------------------------------------------------
 * Macro for the very common error "lua error"
 * -------------------------------------------------------------------------
 */
#define LUAERR(s) \
	err = nowdb_err_get(nowdb_err_lua, FALSE, OBJECT, s);

/* -------------------------------------------------------------------------
 * Get error message from lua
 * -------------------------------------------------------------------------
 */
static inline nowdb_err_t mkLuaErr(nowdb_proc_t *proc, char *m) {
	nowdb_err_t err;
	const char *msg = NULL; 

	if (lua_isstring(LUA, -1)) {
		msg = lua_tostring(LUA,-1);
	}
	if (msg == NULL) { 
		LUAERR(m);
	} else {
		fprintf(stderr, "ERROR: %s\n", msg);
		int s = strlen(msg) + strlen(m) + 3;
		char *desc = malloc(s);
		if (desc == NULL) {
			NOMEM("allocating error descriptor");
			return err;
		}
		sprintf(desc, "%s: %s", m, msg);
		LUAERR(desc); free(desc);
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Imported modules
 * ------------------------------------------------------------------------
 */
typedef struct {
	char *modname;    /* module name */
	int  lang;        // language
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
	if (MODULE(*n)->lang == NOWDB_STORED_PYTHON) {
		if (MODULE(*n)->m != NULL) {
			Py_XDECREF(MODULE(*n)->m);
		}
		MODULE(*n)->m = NULL;
		MODULE(*n)->d = NULL;
	}
#endif
	free(*n); *n=NULL;
}

/* -----------------------------------------------------------------------
 * compare string (locks!)
 * -----------------------------------------------------------------------
 */
static ts_algo_cmp_t stringcompare(void *rsc, void *one, void *two) {
	int x = strcasecmp(one, two);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* -----------------------------------------------------------------------
 * destroy string
 * -----------------------------------------------------------------------
 */
static void stringdestroy(void *rsc, void **n) {
	free(*n); *n = NULL;
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
	PyObject           *f; // the function
	PyObject           *c; // the caller
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
	FUN(*n)->c = NULL;
#endif
	free(*n); *n=NULL;
}

/* ------------------------------------------------------------------------
 * Helper: destroy interpreter
 * ------------------------------------------------------------------------
 */
static void destroyInterpreter(nowdb_proc_t *proc) {
	if (proc == NULL) return;

	if (LUA != NULL) {
		lua_close(LUA);
		LUA = NULL;
	}

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

/* ------------------------------------------------------------------------
 * Helper: load nowdb module (nowdb.py)
 * ------------------------------------------------------------------------
 */
static PyObject *loadNoWDB(nowdb_proc_t *proc) {
	PyObject *mn, *m;

	// if already loaded, return reference
	if (proc->nowmod != NULL) return proc->nowmod;

#ifdef _NOWDB_WITH_PYTHON
	// load
	mn = PyString_FromString("nowdb");
	if (mn == NULL) return NULL;

  	m = PyImport_Import(mn); Py_XDECREF(mn);
	if (m == NULL) return NULL;

	// and remember
	proc->nowmod = m;
#endif

	return m;
}

/* ------------------------------------------------------------------------
 * Helper: push simple value to lua stack
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t pushvalue(nowdb_proc_t *proc,
                             nowdb_simple_value_t *arg) {
	nowdb_err_t err;

	switch(arg->type) {
	case NOWDB_TYP_NOTHING:
		lua_pushnil(LUA); break;

	case NOWDB_TYP_TEXT:
		lua_pushstring(LUA, arg->value); break;

	case NOWDB_TYP_TIME:
	case NOWDB_TYP_DATE:
	case NOWDB_TYP_INT:
	case NOWDB_TYP_UINT:
		lua_pushinteger(LUA, *(uint64_t*)arg->value); break;

	case NOWDB_TYP_FLOAT:
		lua_pushnumber(LUA, *(double*)arg->value); break;

	case NOWDB_TYP_BOOL:
		lua_pushboolean(LUA, (int64_t)*(int64_t*)arg->value); break;

	default:
		LUAERR("unknown value type");
		return err;
	}
	return NOWDB_OK;
}

extern nowdb_dbresult_t nowdb_dbresult_fromError(nowdb_err_t err);

static inline nowdb_dbresult_t dberr(nowdb_err_t err) {
	return nowdb_dbresult_fromError(err);
}

/* ------------------------------------------------------------------------
 * Helper: call lua
 * ------------------------------------------------------------------------
 */
static void *luacall(nowdb_proc_t           *proc,
                     char *funname, uint16_t argn,
                     nowdb_simple_value_t  **args) {
	nowdb_err_t err;

	if (lua_getglobal(LUA, "_nowdb_caller") == 0) {
		LUAERR("not a global function: nowdb_caller");
		return dberr(err);
	}
	if (lua_getglobal(LUA, funname) == 0) {
		LUAERR("not a global function");
		lua_settop(LUA, 0);
		return dberr(err);
	}
	for(int i=0; i<argn; i++) {
		err = pushvalue(proc, args[i]);
		if (err != NOWDB_OK) {
			lua_settop(LUA, 0);
			return dberr(err);
		}
	}
	if (lua_pcall(LUA, (int)(argn+1), 1, 0) != 0) {
		nowdb_dbresult_t r = dberr(mkLuaErr(proc,
		            "Error in stored procedure"));
		lua_pop(LUA, 1);
		return r;
	}
	if (!lua_isinteger(LUA, -1)) {
		LUAERR("Invalid result");
		lua_settop(LUA, 0);
		return dberr(err);
	}
	nowdb_dbresult_t r = (nowdb_dbresult_t)lua_tointeger(LUA,-1);
	lua_pop(LUA, 1);
	return r;
}

/* ------------------------------------------------------------------------
 * Helper: set DB (Lua)
 * ------------------------------------------------------------------------
 */
static nowdb_err_t setLuaDB(nowdb_proc_t *proc) {
	lua_getglobal(LUA, "nowdb");
	lua_pushinteger(LUA, (uint64_t)proc);
	lua_setfield(LUA, 1, "_db");
	lua_settop(LUA, 0);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: set DB (Python)
 * ------------------------------------------------------------------------
 */
static nowdb_err_t setPyDB(nowdb_proc_t *proc) {
	nowdb_err_t err;
#ifdef _NOWDB_WITH_PYTHON
	PyObject  *args;
	PyObject  *r;
	PyObject  *f;
	PyObject  *m, *d;

	m = loadNoWDB(proc);
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
#endif
	return NOWDB_OK;
}

#ifdef _NOWDB_WITH_PYTHON
/* ------------------------------------------------------------------------
 * Helper: Get caller
 * ------------------------------------------------------------------------
 */
static PyObject *getCaller(nowdb_proc_t *proc)
{
	PyObject  *f;
	PyObject  *m, *d;

	m = loadNoWDB(proc);
	if (m == NULL) return NULL;

	d = PyModule_GetDict(m);
	if (d == NULL) {
		return NULL;
	}
	
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
			Py_DECREF(MODULE(module)->m);
			MODULE(module)->m=NULL;
			MODULE(module)->d=NULL;
			MODULE(module)->c=NULL;
			PyErr_Print();
			return TS_ALGO_NO_MEM;
		}
		Py_DECREF(MODULE(module)->m);
		MODULE(module)->m = m;

		MODULE(module)->d = PyModule_GetDict(m);
		if (MODULE(module)->d == NULL) {
			fprintf(stderr, "cannot get dictionary\n");
			Py_DECREF(MODULE(module)->m);
			MODULE(module)->m=NULL;
			MODULE(module)->d=NULL;
			MODULE(module)->c=NULL;
			PyErr_Print();
    			return TS_ALGO_NO_MEM;
		}

		err = setPyDB(TREE(tree)->rsc);
		if (err != NOWDB_OK) {
			Py_DECREF(MODULE(module)->m);
			MODULE(module)->m=NULL;
			MODULE(module)->d=NULL;
			MODULE(module)->c=NULL;
			nowdb_err_print(err);
			nowdb_err_release(err);
			return TS_ALGO_ERR;
		}

		MODULE(module)->c = getCaller(TREE(tree)->rsc);
		if (MODULE(module)->c == NULL) {
			fprintf(stderr, "cannot get caller\n");
			Py_DECREF(MODULE(module)->m);
			MODULE(module)->m=NULL;
			MODULE(module)->d=NULL;
			MODULE(module)->c=NULL;
			PyErr_Print();
    			return TS_ALGO_NO_MEM;
		}
	}
	return TS_ALGO_OK;
}
#endif

static ts_algo_bool_t luamodule(void *ignore, const void *pattern, const void *node) {
	return (MODULE(node)->lang == NOWDB_STORED_LUA);
}

static nowdb_err_t removeLuaModules(ts_algo_tree_t *tree) {
	nowdb_err_t err;
	ts_algo_list_t result;
	ts_algo_list_node_t *run;

	ts_algo_list_init(&result);
	if (ts_algo_tree_filter(tree, &result,
	      NULL, &luamodule) != TS_ALGO_OK) 
	{
		NOMEM("filtering tree");
		ts_algo_list_destroy(&result);
		return err;
	}
	for(run=result.head; run!=NULL; run=run->nxt) {
		ts_algo_tree_delete(tree, run->cont);
	}
	ts_algo_list_destroy(&result);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: reload modules
 * ------------------------------------------------------------------------
 */
static nowdb_err_t reloadModules(nowdb_proc_t *proc) {
	nowdb_err_t err;

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

	err = removeLuaModules(proc->mods);
	if (err != NOWDB_OK) return err;

#ifdef _NOWDB_WITH_PYTHON
	proc->mods->rsc = proc;
	if (ts_algo_tree_map(proc->mods, &reloadModule) != TS_ALGO_OK) {
		PYTHONERR("cannot reload modules");
		return err;
	}
#endif
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: set lua path
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t setLuaPath(nowdb_proc_t *proc) {
	nowdb_err_t err;
	char *frm = "package.path = package.path .. ';%s'";
	char *stmt;

	char *p = getenv("LUA_PATH");
	if (p == NULL) {
		fprintf(stderr, "path is NULL\n");
		return NOWDB_OK;
	} else {
		fprintf(stderr, "LUA_PATH: %s\n", p);
	}

	size_t s = strnlen(p, 1025);
	if (s >= 1024) {
		INVALID("LUA_PATH too long (max: 1024)");
	}

	stmt = malloc(strlen(frm) + s + 1);
	if (stmt == NULL) {
		NOMEM("allocating lua path");
		return err;
	}
	sprintf(stmt, frm, p);
	if (luaL_dostring(LUA, stmt) != 0) {
		LUAERR("cannot set LUA_PATH");
		free(stmt); return err;
	}
	free(stmt);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: create lua interpreter
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t createLua(nowdb_proc_t *proc) {
	nowdb_err_t err;

	LUA = luaL_newstate();
	if (LUA == NULL) {
		LUAERR("cannot create interpreter");
		return err;
	}

	luaL_openlibs(LUA);

	err = nowdb_registerNow2Lua(LUA);
	if (err != NOWDB_OK) {
		lua_close(LUA); LUA = NULL;
		return err;
	}
	if (proc->nowpath == NULL) {
		proc->nowpath = malloc(strlen(proc->luapath) + 12);
		if (proc->nowpath == NULL) {
			NOMEM("allocating nowdb path");
			lua_close(LUA); LUA = NULL;
			return err;
		}
		sprintf(proc->nowpath, "%s/nowdb.lua", proc->luapath);
	}
	int x = luaL_dofile(LUA, proc->nowpath);
	if (x != 0) {
		err = mkLuaErr(proc, "cannot load 'nowdb' module");
		lua_close(LUA); LUA = NULL;
		return err;
	}
	err = setLuaDB(proc);
	if (err != NOWDB_OK) {
		err = mkLuaErr(proc, "cannot set database in 'nowdb'");
		lua_close(LUA); LUA = NULL;
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: create interpreter
 * ------------------------------------------------------------------------
 */
static nowdb_err_t createInterpreter(nowdb_proc_t *proc) {
	nowdb_err_t err;

	if (proc == NULL) return NOWDB_OK;
	if (proc->lib == NULL) return NOWDB_OK;

	err = createLua(proc);
	if (err != NOWDB_OK) return err;

#ifdef _NOWDB_WITH_PYTHON
	if (LIB(proc->lib)->mst != NULL) {
		PyEval_RestoreThread(LIB(proc->lib)->mst); // acquire lock

		proc->pyIntp = Py_NewInterpreter(); // new thread state
		if (proc->pyIntp == NULL) {
			return nowdb_err_get(nowdb_err_python, FALSE, OBJECT,
			                     "cannot create new interpreter");
		}
		PyThreadState_Swap(LIB(proc->lib)->mst);   // switch back to main
		LIB(proc->lib)->mst = PyEval_SaveThread(); // release lock
	}
#endif
	return NOWDB_OK;
} 

/* ------------------------------------------------------------------------
 * Helper: reinit interpreter
 * ------------------------------------------------------------------------
 */
static nowdb_err_t reinitInterpreter(nowdb_proc_t *proc) {
	nowdb_err_t err = NOWDB_OK;

	if (proc == NULL) return NOWDB_OK;
	if (proc->lib == NULL) return NOWDB_OK;

	if (LUA != NULL) {
		lua_close(LUA); LUA = NULL;
	}
	if (LUA == NULL) {
		err = createLua(proc);
		if (err != NOWDB_OK) return err;
	}

#ifdef _NOWDB_WITH_PYTHON
	// acquire lock
	if (LIB(proc->lib)->pyEnabled) {
		if (proc->pyIntp != NULL) {
			PyEval_RestoreThread(proc->pyIntp);
		}

		if (proc->nowmod != NULL) {
			Py_XDECREF(proc->nowmod); proc->nowmod = NULL;
		}
	}
#endif

	err = reloadModules(proc);

#ifdef _NOWDB_WITH_PYTHON
	// release lock
	if (LIB(proc->lib)->pyEnabled) {
		if (proc->pyIntp != NULL) {
			proc->pyIntp = PyEval_SaveThread();
		}
	}
#endif
	return err;
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
	(*proc)->nowmod = NULL;
	(*proc)->luaIntp = NULL;

	(*proc)->luapath = LIB(lib)->luapath;

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

	(*proc)->locks = ts_algo_tree_new(
		             &stringcompare, NULL,
		             &noupdate,
		             &stringdestroy,
		             &stringdestroy);
	if ((*proc)->locks == NULL) {
		nowdb_proc_destroy(*proc);
		free(*proc); *proc = NULL;
		NOMEM("tree.create");
		return err;
	}

	/* create new (sub-)interpreter */
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
	if (proc->nowpath != NULL) {
		free(proc->nowpath); proc->nowpath = NULL;
	}
#ifdef _NOWDB_WITH_PYTHON
	if (proc->pyIntp != NULL) {
		PyEval_RestoreThread(proc->pyIntp);
	}
	if (proc->nowmod != NULL) {
		Py_XDECREF(proc->nowmod);
		proc->nowmod = NULL;
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
	if (proc->locks != NULL) {
		ts_algo_tree_destroy(proc->locks);
		free(proc->locks); proc->locks = NULL;
	}
	destroyInterpreter(proc);
}

/* ------------------------------------------------------------------------
 * Free locks still hold
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t freelocks(nowdb_proc_t *proc) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_t *list;
	ts_algo_list_node_t *run;
	nowdb_scope_t *scope = proc->scope;

	if (scope == NULL) return NOWDB_OK;
	if (proc->locks->count <= 0) return NOWDB_OK;

	list = ts_algo_tree_toList(proc->locks);
	if (list == NULL) {
		NOMEM("tree.toList");
		return err;
	}
	for(run=list->head; run!=NULL; run=run->nxt) {
		err = nowdb_ipc_unlock(scope->ipc, run->cont);
		if (err != NOWDB_OK) break;
	}
	ts_algo_list_destroy(list); free(list);
	return err;
}

/* ------------------------------------------------------------------------
 * Reinit proc interface
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_proc_reinit(nowdb_proc_t *proc) {
	nowdb_err_t err;
	if (proc->locks != NULL) {
		err = freelocks(proc);
		if (err != NOWDB_OK) return err;
		ts_algo_tree_destroy(proc->locks);
		free(proc->locks); proc->locks = NULL;
	}
	proc->locks = ts_algo_tree_new(
		             &stringcompare, NULL,
		             &noupdate,
		             &stringdestroy,
		             &stringdestroy);
	if (proc->locks == NULL) {
		nowdb_proc_destroy(proc);
		free(proc); proc = NULL;
		NOMEM("tree.create");
		return err;
	}
	return reinitInterpreter(proc);
}

/* ------------------------------------------------------------------------
 * Set current scope
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_proc_setScope(nowdb_proc_t  *proc, 
                                nowdb_scope_t *scope) {

	if (proc->scope != scope) {
		nowdb_err_t err = nowdb_proc_reinit(proc);
		if (err != NOWDB_OK) return err;
		proc->scope = scope;
		nowdb_dml_destroy(proc->dml);
		return nowdb_dml_init(proc->dml, proc->scope, 1);
	}
	return NOWDB_OK;
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
 * Helper: get lock
 * ------------------------------------------------------------------------
 */
static inline char *getLock(nowdb_proc_t *proc, char *name) {
	char *lock;
	lock = ts_algo_tree_find(proc->locks, name);
	return lock;
}

/* ------------------------------------------------------------------------
 * Check if session holds lock
 * ------------------------------------------------------------------------
 */
char nowdb_proc_holdsLock(nowdb_proc_t *proc, char *name) {
	return (getLock(proc, name) != NULL);
}

/* ------------------------------------------------------------------------
 * Add Lock to session
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_proc_addLock(nowdb_proc_t *proc, char *name) {
	nowdb_err_t err;
	if (getLock(proc, name) != NULL) {
		return nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT, name);
	}
	char *lock = strdup(name);
	if (lock == NULL) {
		NOMEM("duplicating lock name"); return err;
	}
	if (ts_algo_tree_insert(proc->locks, lock) != TS_ALGO_OK) {
		NOMEM("tree.insert");
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Remove Lock from session
 * ------------------------------------------------------------------------
 */
void nowdb_proc_removeLock(nowdb_proc_t *proc, char *name) {
	if (getLock(proc, name) == NULL) return;
	ts_algo_tree_delete(proc->locks, name);
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
 * Helper: load Lua module
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadLuaModule(nowdb_proc_t *proc,
                                char         *mname,
                                module_t   **module) {
	nowdb_err_t err;

	if (proc->luapath == NULL) {
		LUAERR("no luapath");
	}
	char *p = malloc(strlen(proc->luapath) + strlen(mname) + 6);
	if (p == NULL) {
		NOMEM("allocating module path");
		return err;
	}
	sprintf(p, "%s/%s.lua", proc->luapath, mname);
	int x = luaL_dofile(LUA, p); free(p);
	if (x != 0) {
		return mkLuaErr(proc, "cannot load module");
	}

	*module = calloc(1, sizeof(module_t));
	if (*module == NULL) {
		NOMEM("allocating module");
		return err;
	}

	(*module)->modname = strdup(mname);
	if ((*module)->modname == NULL) {
		NOMEM("allocating module name");
		free(*module);
		return err;
	}
	(*module)->lang = NOWDB_STORED_LUA;

	if (ts_algo_tree_insert(proc->mods, *module) != TS_ALGO_OK) {
		NOMEM("tree.insert");
		free((*module)->modname); free(*module);
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load Py module
 * ------------------------------------------------------------------------
 */
#ifdef _NOWDB_WITH_PYTHON
static nowdb_err_t loadPyModule(nowdb_proc_t *proc,
                                char        *mname,
                                module_t   **module) 
{
	nowdb_err_t err = NOWDB_OK;

	// check module language
	// load python in one case and lua in the other

	PyObject *mn=NULL;
	PyObject *m=NULL;
	PyObject *d=NULL;
	PyObject *c=NULL;

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

	err = setPyDB(proc);
	if (err != NOWDB_OK) {
		Py_XDECREF(m);
		return err;
	}

	c = getCaller(proc);
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
	(*module)->lang = NOWDB_STORED_PYTHON;

	(*module)->m = m;
	(*module)->d = d;
	(*module)->c = c;

	if (ts_algo_tree_insert(proc->mods, *module) != TS_ALGO_OK) {
		NOMEM("tree.insert");
		Py_XDECREF(m); free((*module)->modname); free(*module);
		return err;
	}
	return err;
}
#endif

/* ------------------------------------------------------------------------
 * Helper: load module
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadModule(nowdb_proc_t    *proc,
                              nowdb_proc_desc_t *pd,
                              module_t     **module) 
{
#ifdef _NOWDB_WITH_PYTHON
	if (pd->lang == NOWDB_STORED_PYTHON) {
		return loadPyModule(proc, pd->module, module);
	} else {
		return loadLuaModule(proc, pd->module, module);
	}
#else
	return loadLuaModule(proc, pd->module, module);
#endif
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
	void *f = NULL;

#ifdef _NOWDB_WITH_PYTHON
	if (pd->lang == NOWDB_STORED_PYTHON) {
		// PyObject *f=NULL;

		f = PyDict_GetItemString(module->d, pd->name);
		if (f == NULL) {
			PYTHONERR("cannot find function in module");
			return err;
		}
		if (!PyCallable_Check(f)) {
			PYTHONERR("function not callable");
			return err;
		}
	}
#endif

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
		*fun = f;
		*call = f->c;
		return NOWDB_OK;
	}

	err = nowdb_scope_getProcedure(proc->scope, fname, pd);
	if (err != NOWDB_OK) return err;

	/*
	fprintf(stderr, "LOADING %d FUN %s (%d/%d)\n",
	                     (*pd)->lang, (*pd)->name,
	                            proc->funs->count,
                                    proc->mods->count);
	*/

	ENABLED(pd);

	LOCK(pd);

	mpattern.modname = (*pd)->module;
	m = ts_algo_tree_find(proc->mods, &mpattern);
	if (m == NULL) {
		err = loadModule(proc, *pd, &m);
		if (err != NOWDB_OK) {
			UNLOCK(pd);
			nowdb_proc_desc_destroy(*pd);
			free(*pd); *pd = NULL;
			return err;
		}
	}

	err = loadFun(proc, *pd, m, &f);
	if (err != NOWDB_OK) {
		UNLOCK(pd);
		nowdb_proc_desc_destroy(*pd);
		free(*pd); *pd = NULL;
		return err;
	}

	*fun = f;
        *call = m->c;

	// fprintf(stderr, "LOADED: %p | %p | %p\n", f, f->pd, *pd);

	UNLOCK(pd);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Call function
 * ------------------------------------------------------------------------
 */
void *nowdb_proc_call(nowdb_proc_t *proc, void *call,
                               void *fun, void *args) {
#ifdef _NOWDB_WITH_PYTHON
	if (FUN(fun)->pd->lang == NOWDB_STORED_PYTHON) {
		if (call == NULL) {
			fprintf(stderr, "NO CALLER\n");
			return NULL;
		}
		if (args == NULL) {
			return PyObject_CallFunctionObjArgs(call,
			                             FUN(fun)->f,
			                           Py_None, NULL);
		} else {
			return PyObject_CallFunctionObjArgs(call,
			                             FUN(fun)->f,
			                             args, NULL);
		}
	} else {
		return luacall(proc, FUN(fun)->funname,
		                     FUN(fun)->pd->argn, args);
	}
#else
	return luacall(proc, FUN(fun)->funname,
	                     FUN(fun)->pd->argn, args);
#endif
}

