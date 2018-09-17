/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Stored Procedure/Function Manager
 * ========================================================================
 */

#include <nowdb/scope/procman.h>

static char *OBJECT = "procman";

static char *MYPATH = "procman";

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

#define INVALID(s) \
	err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define ARGNULL(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define DESC(x) \
	((nowdb_proc_desc_t*)x)

/* ------------------------------------------------------------------------
 * Helper: destroy arguments
 * ------------------------------------------------------------------------
 */
static void destroyArgs(uint16_t argn, nowdb_proc_arg_t *args) {
	for(int i=0; i<argn; i++) {
		if (args[i].name != NULL) {
			free(args[i].name);
			args[i].name = NULL;
		}
		if (args[i].def != NULL) {
			free(args[i].def);
			args[i].def= NULL;
		}
	}
}

/* ------------------------------------------------------------------------
 * Helper: destroy proc descriptor
 * ------------------------------------------------------------------------
 */
static void destroyDesc(nowdb_proc_desc_t *desc) {
	if (desc->name != NULL) {
		free(desc->name); desc->name = NULL;
	}
	if (desc->module != NULL) {
		free(desc->module); desc->module = NULL;
	}
	if (desc->args != NULL) {
		destroyArgs(desc->argn, desc->args);
		free(desc->args); desc->args = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Tree callbacks
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t namecompare(void *ignore, void *one, void *two) {
	int x = strcasecmp(DESC(one)->name, DESC(two)->name);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	return TS_ALGO_OK;
}

static void descdestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	destroyDesc(*n);
	free(*n); *n=NULL;
}

/* ------------------------------------------------------------------------
 * Allocate new proc manager and initialise it
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_procman_new(nowdb_procman_t **pm,
                              nowdb_path_t    base) 
{
	nowdb_err_t err;

	if (pm == NULL) ARGNULL("pointer to proc manager is NULL");

	*pm = calloc(1, sizeof(nowdb_procman_t));
	if (*pm == NULL) {
		NOMEM("allocating proc manager");
		return err;
	}

	err = nowdb_procman_init(*pm, base);
	if (err != NOWDB_OK) {
		free(*pm); *pm = NULL;
		return err;
	}

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Initialise already allocated proc manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_procman_init(nowdb_procman_t *pm,
                               nowdb_path_t   base)
{
	size_t s;
	nowdb_err_t err;

	if (pm == NULL) ARGNULL("proc manager is NULL");
	if (base == NULL) ARGNULL("base path is NULL");

	pm->lock = calloc(1, sizeof(nowdb_rwlock_t));
	if (pm->lock == NULL) {
		NOMEM("allocating lock");
		return err;
	}

	err = nowdb_rwlock_init(pm->lock);
	if (err != NOWDB_OK) {
		nowdb_procman_destroy(pm);
		return err;
	}

	s = strnlen(pm->base, 4097);
	if (s > 4096) {
		INVALID("base path too long (max: 4096)");
		nowdb_procman_destroy(pm);
		return err;
	}

	pm->base = strdup(base);
	if (pm->base == NULL) {
		NOMEM("allocating base path");
		nowdb_procman_destroy(pm);
		return err;
	}

	s += strlen(MYPATH) + 2;
	
	pm->cfg = malloc(s);
	if (pm->cfg == NULL) {
		NOMEM("allocating config path");
		nowdb_procman_destroy(pm);
		return err;
	}

	sprintf(pm->cfg, "%s/%s", pm->base, MYPATH);

	pm->procs = ts_algo_tree_new(&namecompare, NULL,
	                             &noupdate,
	                             &descdestroy,
	                             &descdestroy);

	if (pm->procs == NULL) {
		NOMEM("allocating proc tree");
		nowdb_procman_destroy(pm);
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy proc manager
 * ------------------------------------------------------------------------
 */
void nowdb_procman_destroy(nowdb_procman_t *pm) {
	if (pm == NULL) return;
	if (pm->procs != NULL) {
		ts_algo_tree_destroy(pm->procs);
		free(pm->procs); pm->procs = NULL;
	}
	if (pm->cfg != NULL) {
		free(pm->cfg); pm->cfg = NULL;
	}
	if (pm->base != NULL) {
		free(pm->base); pm->base = NULL;
	}
	if (pm->lock != NULL) {
		nowdb_rwlock_destroy(pm->lock);
		free(pm->lock); pm->lock = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Helper: read one argument from buffer
 * ------------------------------------------------------------------------
 */
static nowdb_err_t readArg(nowdb_proc_arg_t *arg,
                           char              *buf,
                           uint32_t          *idx,
                           uint32_t           sz)
{
	nowdb_err_t err;
	size_t s;

	// name
	s = strnlen(buf+*idx, sz-*idx-1);
	if (s+1 >= sz - *idx) {
		INVALID("procedure name in config too long");
		return err;
	}
	if (s == 0) {
		INVALID("no procedure name in config");
		return err;
	}
	arg->name = malloc(s+1);
	if (arg->name == NULL) {
		NOMEM("allocating proc name");
		return err;
	}
	strcpy(arg->name, buf+*idx);
	(*idx) += s+1;

	if (*idx+4 >= s) {
		INVALID("proc config incomplete");
	}

	memcpy(&arg->typ, buf+*idx, 2);
	memcpy(&arg->pos, buf+*idx, 2);

	arg->def = NULL;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: read all arguments from buffer
 * ------------------------------------------------------------------------
 */
static nowdb_err_t readArgs(nowdb_proc_desc_t *proc,
                            char              *buf,
                            uint32_t          *idx,
                            uint32_t           sz) {
	nowdb_err_t err;

	proc->args = calloc(proc->argn, sizeof(nowdb_proc_arg_t));
	if (proc->args == NULL) {
		NOMEM("allocating proc parameters");
		return err;
	}

	for(uint16_t i=0; i<proc->argn; i++) {
		err = readArg(proc->args+i, buf, idx, sz);
		if (err != NOWDB_OK) {
			destroyArgs(i, proc->args);
			free(proc->args); proc->args = NULL;
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: read one proc descriptor from buffer
 * ------------------------------------------------------------------------
 */
static nowdb_err_t readProc(nowdb_procman_t *pm, char *buf,
                            uint32_t *idx,     uint32_t sz) {
	nowdb_proc_desc_t *desc;
	nowdb_err_t err;
	size_t s;

	desc = calloc(1,sizeof(nowdb_proc_desc_t));
	if (desc == NULL) {
		NOMEM("allocating proc desc");
		return err;
	}

	// name
	s = strnlen(buf+*idx, sz-*idx-1);
	if (s+1 >= sz - *idx) {
		INVALID("procedure name in config too long");
		free(desc);
		return err;
	}
	if (s == 0) {
		INVALID("no procedure name in config");
		free(desc);
		return err;
	}
	desc->name = malloc(s+1);
	if (desc->name == NULL) {
		NOMEM("allocating proc name");
		free(desc);
		return err;
	}
	strcpy(desc->name, buf+*idx);
	(*idx) += s+1;

	// type and language
	if (*idx+2 >= sz) {
		INVALID("proc config incomplete");
		return err;
	}
	memcpy(&desc->type, buf+*idx, 1); (*idx)++;
	memcpy(&desc->lang, buf+*idx, 1); (*idx)++;

	// module
	s = strnlen(buf+*idx, sz-*idx-1);
	if (s+1 >= sz - *idx) {
		INVALID("procedure name in config too long");
		free(desc->name); free(desc);
		return err;
	}
	if (s == 0) {
		INVALID("no procedure name in config");
		free(desc->name); free(desc);
		return err;
	}

	desc->module = malloc(s+1);
	if (desc->module == NULL) {
		NOMEM("allocating proc module");
		free(desc->name); free(desc);
		return err;
	}
	strcpy(desc->module, buf+*idx);
	(*idx) += s+1;

	if (*idx+2 >= sz) {
		INVALID("proc config incomplete");
		return err;
	}
	memcpy(&desc->argn, buf+*idx, 2); (*idx)+=2;

	err = readArgs(desc, buf, idx, s);
	if (err != NOWDB_OK) {
		destroyDesc(desc); free(desc);
		return err;
	}

	if (ts_algo_tree_insert(pm->procs, desc) != TS_ALGO_OK) {
		NOMEM("tree.insert");
		destroyDesc(desc); free(desc);
		return err;
	}

	(*idx)++;
	
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: read all proc descriptors from buffer
 * ------------------------------------------------------------------------
 */
static nowdb_err_t readProcs(nowdb_procman_t    *pm,
                             char *buf, uint32_t sz) {
	uint32_t idx = 0;

	for(idx=0; idx < sz;) {
		readProc(pm, buf, &idx, sz);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: write one arg to buffer
 * ------------------------------------------------------------------------
 */
static nowdb_err_t writeArg(nowdb_proc_arg_t *arg,
                             char            *buf,
                             uint32_t          sz,
                             uint32_t        *osz) {
	nowdb_err_t err;
	size_t s;

	s = strlen(arg->name);
	if (s+1 + *osz >= sz) {
		INVALID("buffer too small");
		return err;
	}
	strcpy(buf+*osz, arg->name);
	*osz += s+1;

	if (*osz + 4 >= sz) {
		INVALID("buffer too small");
		return err;
	}

	memcpy(buf+*osz, &arg->typ, 2); *osz+=2;
	memcpy(buf+*osz, &arg->pos, 2); *osz+=2;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: write all args to buffer
 * ------------------------------------------------------------------------
 */
static nowdb_err_t writeArgs(nowdb_proc_desc_t *desc,
                             char              *buf,
                             uint32_t           sz,
                             uint32_t          *osz) {
	nowdb_err_t err;

	for(uint16_t i=0; i<desc->argn; i++) {
		err = writeArg(desc->args+i, buf, sz, osz);
		if (err != NOWDB_OK) return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: write one proc descriptor to buffer
 * ------------------------------------------------------------------------
 */
static nowdb_err_t writeProc(nowdb_proc_desc_t *desc,
                             char              *buf,
                             uint32_t           sz,
                             uint32_t          *osz) 
{
	nowdb_err_t err;
	size_t s;

	s = strlen(desc->name);
	if (s+1+*osz >= sz) {
		INVALID("buffer too small");
		return err;
	}
	strcpy(buf+*osz, desc->name);
	*osz += s+1;

	if (*osz + 2 >= sz) {
		INVALID("buffer too small");
		return err;
	}

	memcpy(buf+*osz, &desc->type, 1); (*osz)++;
	memcpy(buf+*osz, &desc->lang, 1); (*osz)++;

	s = strlen(desc->module);
	if (s+1+*osz >= sz) {
		INVALID("buffer too small");
		return err;
	}
	strcpy(buf+*osz, desc->module);
	*osz += s+1;

	if (*osz + 2 >= sz) {
		INVALID("buffer too small");
		return err;
	}

	memcpy(buf+*osz, &desc->argn, 2); (*osz)+=2;

	return writeArgs(desc, buf, sz, osz);
}

/* ------------------------------------------------------------------------
 * Helper: write all proc descriptors to buffer
 * ------------------------------------------------------------------------
 */
static nowdb_err_t writeProcs(nowdb_procman_t *pm,
                              char           *buf,
                              uint32_t         sz,
                              uint32_t       *osz) 
{
	nowdb_err_t err=NOWDB_OK;
	ts_algo_list_t *list;
	ts_algo_list_node_t *runner;

	list = ts_algo_tree_toList(pm->procs);
	if (list == NULL) {
		NOMEM("tree.toList");
		return err;
	}
	for(runner=list->head; runner!=NULL; runner=runner->nxt) {
		err = writeProc(runner->cont, buf, sz, osz);
		if (err != NOWDB_OK) break;
	}
	ts_algo_list_destroy(list); free(list);
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: compute size of one argument
 * ------------------------------------------------------------------------
 */
static inline void argSize(nowdb_proc_arg_t *arg, 
                           uint32_t *agg) {
	(*agg)+=strlen(arg->name)+1;
	(*agg)+=2*sizeof(uint16_t);
}

/* ------------------------------------------------------------------------
 * Helper: compute size of all arguments
 * ------------------------------------------------------------------------
 */
static inline void argsSize(uint16_t argn,
                            nowdb_proc_arg_t *args, 
                            uint32_t *agg) {
	for(int i=0; i<argn; i++) {
		argSize(args+i, agg);
		(*agg)++;
	}
}

/* ------------------------------------------------------------------------
 * Helper: compute size of one proc descriptor
 * ------------------------------------------------------------------------
 */
static inline void descSize(const nowdb_proc_desc_t *desc, uint32_t *agg) {
	(*agg)+=strlen(desc->name) + 1;
	(*agg)+=strlen(desc->module) + 1;
	(*agg)+=sizeof(uint16_t);
	(*agg)+=3;
	argsSize(desc->argn, desc->args, agg);
}

/* ------------------------------------------------------------------------
 * Wrapper to make descSize a reducer
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t cfgSize(void *ignore, void *agg, const void *node) {
	descSize(node, agg);
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Helper: compute size of all proc descriptors
 * ------------------------------------------------------------------------
 */
static inline uint32_t fold2size(nowdb_procman_t *pm) {
	uint32_t sz;
	ts_algo_tree_reduce(pm->procs, &sz, &cfgSize);
	return sz;
}

/* ------------------------------------------------------------------------
 * Helper: write config
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t writeCfg(nowdb_procman_t *pm) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2;
	char *buf=NULL;
	uint32_t sz,sz2;

	err = nowdb_lock_write(pm->lock);
	if (err != NOWDB_OK) return err;

	sz = fold2size(pm);
	if (sz == 0) goto unlock;

	buf = malloc(sz);
	if (buf == NULL) {
		NOMEM("allocating buffer");
		goto unlock;
	}

	err = writeProcs(pm, buf, sz, &sz2);
	if (err != NOWDB_OK) goto unlock;

	err = nowdb_writeFileWithBkp(pm->base, MYPATH, buf, sz2);

unlock:
	if (buf != NULL) free(buf);
	err2 = nowdb_unlock_write(pm->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: load config
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_procman_load(nowdb_procman_t *pm) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2;
	char *buf=NULL;
	uint32_t sz;

	err = nowdb_lock_write(pm->lock);
	if (err != NOWDB_OK) return err;

	if (!nowdb_path_exists(pm->cfg, NOWDB_DIR_TYPE_DIR)) goto unlock;
	
	sz = nowdb_path_filesize(pm->cfg);
	if (sz == 0) goto unlock;

	buf = malloc(sz);
	if (buf == NULL) {
		NOMEM("allocating buffer");
		goto unlock;
	}

	err = nowdb_readFile(pm->cfg, buf, sz);
	if (err != NOWDB_OK) goto unlock;

	err = readProcs(pm, buf, sz);
	if (err != NOWDB_OK) goto unlock;

unlock:
	if (buf != NULL) free(buf);
	err2 = nowdb_unlock_write(pm->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Add a proc
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_procman_add(nowdb_procman_t   *pm,
                              nowdb_proc_desc_t *pd);

/* ------------------------------------------------------------------------
 * Remove a proc 
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_procman_remove(nowdb_procman_t *pm, char *name);

/* ------------------------------------------------------------------------
 * Get proc 
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_procman_get(nowdb_procman_t   *pm,
                              char              *name,
                              nowdb_proc_desc_t **pd);

/* ------------------------------------------------------------------------
 * Get all procs
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_procman_all(nowdb_procman_t *pm,
                              ts_algo_list_t  *plist);
