/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Stored Procedure/Function Manager
 * ========================================================================
 */

#include <nowdb/scope/procman.h>

static char *OBJECT = "procman";

static char *MYPATH = "pcat";

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
 * Helper: deep copy descriptor
 * ------------------------------------------------------------------------
 */
static nowdb_err_t copyDesc(nowdb_proc_desc_t *src,
                            nowdb_proc_desc_t *trg) {
	nowdb_err_t err;

	trg->name = strdup(src->name);
	if (trg->name == NULL) {
		NOMEM("allocating proc name");
		return err;
	}
	trg->module = strdup(src->module);
	if (trg->module == NULL) {
		NOMEM("allocating proc module");
		free(trg->name); trg->name = NULL;
		return err;
	}
	trg->argn = src->argn;
	trg->type = src->type;
	trg->lang = src->lang;

	trg->args = calloc(src->argn, sizeof(nowdb_proc_arg_t));
	if (trg->args == NULL) {
		NOMEM("allocating proc args");
		destroyDesc(trg);
		return err;
	}
	for(uint16_t i=0;i<src->argn;i++) {
		trg->args[i].name = strdup(src->args[i].name);
		if (trg->args[i].name == NULL) {
			NOMEM("allocating proc arg name");
			destroyDesc(trg);
			return err;
		}
		trg->args[i].typ = src->args[i].typ;
		trg->args[i].pos = src->args[i].pos;
		trg->args[i].def = NULL;
	}
	return NOWDB_OK;
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

static ts_algo_rc_t updateDesc(void *ignore, void *o, void *n) {
	destroyDesc(o);
	DESC(o)->name = DESC(n)->name;
	DESC(o)->module = DESC(n)->module;
	DESC(o)->args = DESC(n)->args;
	DESC(o)->argn = DESC(n)->argn;
	DESC(o)->type = DESC(n)->type;
	DESC(o)->lang = DESC(n)->lang;
	free(n);
	return TS_ALGO_OK;
}

static void descdestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	destroyDesc(*n);
	free(*n); *n=NULL;
}

void nowdb_proc_desc_destroy(nowdb_proc_desc_t *pd) {
	destroyDesc(pd);
}

void nowdb_proc_args_destroy(uint16_t argn, nowdb_proc_arg_t *args) {
	destroyArgs(argn, args);
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

	s = strnlen(base, 4097);
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
	                             &updateDesc,
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
	memcpy(arg->name, buf+*idx, s+1);
	arg->name[s] = 0;

	(*idx) += s+1;

	if (*idx+4 >= sz) {
		INVALID("proc config incomplete");
		return err;
	}

	memcpy(&arg->typ, buf+*idx, 2); (*idx)+=2;
	memcpy(&arg->pos, buf+*idx, 2); (*idx)+=2;

	// fprintf(stderr, "ARG %hu: %s\n", arg->pos, arg->name);

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

	if (proc->argn == 0) return NOWDB_OK;

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

	// type, language and return type
	if (*idx+2 >= sz) {
		free(desc->name); free(desc);
		INVALID("proc config incomplete");
		return err;
	}
	memcpy(&desc->type, buf+*idx, 1); (*idx)++;
	memcpy(&desc->lang, buf+*idx, 1); (*idx)++;
	memcpy(&desc->rtype, buf+*idx, 2); (*idx)+=2;

	/*
	fprintf(stderr, "loading %s, language: %d\n",
	                      desc->name, desc->lang);
	*/

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
		free(desc->module); free(desc->name); free(desc);
		return err;
	}
	memcpy(&desc->argn, buf+*idx, 2); (*idx)+=2;

	err = readArgs(desc, buf, idx, sz);
	if (err != NOWDB_OK) {
		destroyDesc(desc); free(desc);
		return err;
	}

	if (ts_algo_tree_insert(pm->procs, desc) != TS_ALGO_OK) {
		NOMEM("tree.insert");
		destroyDesc(desc); free(desc);
		return err;
	}

	if (buf[*idx] != '\n') {
		fprintf(stderr, "NOT EOL: %d\n", (int)buf[*idx]);
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
	nowdb_err_t err=NOWDB_OK;
	uint32_t idx = 0;

	for(idx=0; idx < sz;) {
		err = readProc(pm, buf, &idx, sz);
		if (err != NOWDB_OK) break;
	}
	return err;
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

	s = strnlen(arg->name,255);
	if (s+1 + *osz >= sz) {
		INVALID("buffer too small");
		return err;
	}

	memcpy(buf+*osz, arg->name, s);

	*osz += s;
	buf[*osz] = 0;
	(*osz)++;

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

	if (*osz + 6 >= sz) {
		INVALID("buffer too small");
		return err;
	}

	memcpy(buf+*osz, &desc->type, 1); (*osz)++;
	memcpy(buf+*osz, &desc->lang, 1); (*osz)++;
	memcpy(buf+*osz, &desc->rtype, 2); (*osz)+=2;

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
		buf[*osz] = '\n'; (*osz)++;
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
	size_t s;

	s = arg->name==NULL?0:strnlen(arg->name,256);
	(*agg)+=s+1;
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
	uint32_t sz=0;
	ts_algo_tree_reduce(pm->procs, &sz, &cfgSize);
	return sz;
}

/* ------------------------------------------------------------------------
 * Helper: write config no-lock
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t writeCfgNL(nowdb_procman_t *pm) {
	nowdb_err_t err=NOWDB_OK;
	char *buf=NULL;
	uint32_t sz,sz2=0;

	sz = fold2size(pm);
	if (sz == 0) return NOWDB_OK;

	buf = malloc(sz);
	if (buf == NULL) {
		NOMEM("allocating buffer");
		return err;
	}

	err = writeProcs(pm, buf, sz, &sz2);
	if (err != NOWDB_OK) {
		free(buf); return err;
	}

	err = nowdb_writeFileWithBkp(pm->base, pm->cfg, buf, sz2);
	free(buf);

	return err;
}

/* ------------------------------------------------------------------------
 * Helper: write config
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t writeCfg(nowdb_procman_t *pm) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2;

	err = nowdb_lock_write(pm->lock);
	if (err != NOWDB_OK) return err;

	err = writeCfgNL(pm);

	err2 = nowdb_unlock_write(pm->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Load config
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_procman_load(nowdb_procman_t *pm) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2;
	char *buf=NULL;
	uint32_t sz;

	err = nowdb_lock_write(pm->lock);
	if (err != NOWDB_OK) return err;

	ts_algo_tree_destroy(pm->procs);

	if (ts_algo_tree_init(pm->procs,
	             &namecompare, NULL,
	                    &updateDesc,
	                   &descdestroy,
	                   &descdestroy) != TS_ALGO_OK)
	{
		free(pm->procs); pm->procs = NULL;
		NOMEM("tree.init");
		goto unlock;
	}

	if (!nowdb_path_exists(pm->cfg, NOWDB_DIR_TYPE_FILE)) goto unlock;
	
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
                              nowdb_proc_desc_t *pd) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_proc_desc_t *tmp;

	if (pm == NULL) ARGNULL("proc manager is NULL");
	if (pd == NULL) ARGNULL("descriptor is NULL");

	err = nowdb_lock_write(pm->lock);
	if (err != NOWDB_OK) return err;

	tmp = ts_algo_tree_find(pm->procs, pd);
	if (tmp != NULL) {
		err = nowdb_err_get(nowdb_err_dup_key,
		              FALSE, OBJECT, pd->name);
		goto unlock;
	}
	if (ts_algo_tree_insert(pm->procs, pd) != 0) {
		NOMEM("tree.insert");
		goto unlock;
	}

	err = writeCfgNL(pm);

unlock:
	err2 = nowdb_unlock_write(pm->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Remove a proc 
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_procman_remove(nowdb_procman_t *pm, char *name) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_proc_desc_t pattern;

	if (pm == NULL) ARGNULL("proc manager is NULL");
	if (name == NULL) ARGNULL("name is NULL");

	err = nowdb_lock_write(pm->lock);
	if (err != NOWDB_OK) return err;

	pattern.name = name;
	ts_algo_tree_delete(pm->procs, &pattern);

	err = writeCfgNL(pm);

	err2 = nowdb_unlock_write(pm->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Get proc 
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_procman_get(nowdb_procman_t   *pm,
                              char              *name,
                              nowdb_proc_desc_t **pd) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_proc_desc_t pattern;
	nowdb_proc_desc_t *tmp;

	if (pm == NULL) ARGNULL("proc manager is NULL");
	if (name == NULL) ARGNULL("name is NULL");
	if (pd == NULL) ARGNULL("pointer to descriptor is NULL");

	err = nowdb_lock_read(pm->lock);
	if (err != NOWDB_OK) return err;

	pattern.name = name;
	tmp = ts_algo_tree_find(pm->procs, &pattern);

	if (tmp == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                        FALSE, OBJECT, name);
		goto unlock;
	}

	*pd = calloc(1, sizeof(nowdb_proc_desc_t));
	if (*pd == NULL) {
		NOMEM("allocating descriptor");
		goto unlock;
	}

	err = copyDesc(tmp, *pd);
	if (err != NOWDB_OK) {
		free(*pd); *pd = NULL;
		goto unlock;
	}

unlock:
	err2 = nowdb_unlock_read(pm->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Get all procs
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_procman_all(nowdb_procman_t *pm,
                              ts_algo_list_t  *plist);
