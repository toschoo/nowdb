/* ========================================================================
 * (c) Tobias Schoofs, 2018 -- 2019
 * ========================================================================
 */
#include <nowdb/scope/ipc.h>
#include <nowdb/io/dir.h>
#include <nowdb/task/task.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

static char *OBJECT = "ipc";

static char *catalogue = "ipc";

#define INVALID(m) \
	{return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, m);}

#define NOMEM(m) \
	{err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, m);}

#define DUPKEY(m) \
	{err = nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT, m);}

#define KEYNOF(m) \
	{err = nowdb_err_get(nowdb_err_key_not_found, FALSE, OBJECT, m);}

#define L 1
#define V 2
#define Q 3

#define FREE 0

#define TINYDELAY 10000000
#define DELAYUNIT 1000000

/* ------------------------------------------------------------------------
 * Lock
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_lock_t lock; // protect this structure
 	pthread_t   owner; // session id
	int       readers; // number of readers holding the lock
	char        *name; // name of this lock
	char        state; // state free -> rlock|wlock -> free
        ts_algo_list_t  q; // do we need to guarantee order?
} lock_t;

/* ------------------------------------------------------------------------
 * Make lock
 * ------------------------------------------------------------------------
 */
static lock_t *mkLock(char *name, nowdb_err_t *err) {
	lock_t *l;

	l = calloc(1,sizeof(lock_t));
	if (l == NULL) return NULL;

	l->name = name;

	*err = nowdb_lock_init(&l->lock);
	if (*err != NOWDB_OK) return NULL;

	l->state = FREE;
	l->readers = 0;

	ts_algo_list_init(&l->q);

	return l;
}

/* ------------------------------------------------------------------------
 * destroy lock 
 * ------------------------------------------------------------------------
 */
void destroyLock(lock_t *lock) {
	if (lock == NULL) return;
	if (lock->name != NULL) {
		free(lock->name); lock->name = NULL;
	}
	ts_algo_list_destroy(&lock->q);
	nowdb_lock_destroy(&lock->lock);
}

/* ------------------------------------------------------------------------
 * type or edge descriptor
 * ------------------------------------------------------------------------
 */
typedef struct {
	char *name; /* name of the thing    */
	void *ptr;  /* pointer to the thing */
	char  t;    /* type of the thing    */
} thing_t;

#define THING(x) \
	((thing_t*)x)

/* ------------------------------------------------------------------------
 * thing by name
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t thingnamecompare(void *ignore, void *one, void *two) {
	int x = strcasecmp(THING(one)->name, THING(two)->name);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * currently no update
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * destroy thing
 * ------------------------------------------------------------------------
 */
static void thingdestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (THING(*n)->ptr != NULL) {
		switch(THING(*n)->t) {
		case L: destroyLock(THING(*n)->ptr); break;
		case V: break; // nowdb_ipc_event_destroy(THING(*n)->ptr); break;
		case Q: break; // nowdb_ipc_queue_destroy(THING(*n)->ptr); break;
		}
		free(THING(*n)->ptr); THING(*n)->ptr = NULL;
	}
	free(*n); *n = NULL;
}

/* ------------------------------------------------------------------------
 * Allocate and initialise IPC Manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_new(nowdb_ipc_t **ipc, char *path) {
	nowdb_err_t err;

	if (ipc == NULL) INVALID("ipc pointer is NULL");

	*ipc = calloc(1,sizeof(nowdb_ipc_t));
	if (*ipc == NULL) {
		NOMEM("allocating ipc struct");
		return err;
	}
	err = nowdb_ipc_init(*ipc, path);
	if (err != NOWDB_OK) {
		free(*ipc); *ipc = NULL;
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Initialise IPC Manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_init(nowdb_ipc_t *ipc, char *path) {
	nowdb_err_t err;

	if (ipc == NULL) INVALID("ipc is NULL");
	if (path == NULL) INVALID("path is NULL");

	err = nowdb_rwlock_init(&ipc->lock);
	if (err != NOWDB_OK) return err;

	ipc->base = NULL;
	ipc->path = NULL;
	ipc->things = NULL;
	ipc->free = NULL;

	uint32_t s = strnlen(path, NOWDB_MAX_PATH+1);
	if (s >= NOWDB_MAX_PATH) {
		nowdb_ipc_destroy(ipc);
		INVALID("path too long (max 4096)");
	}
	s += strlen(catalogue);
	s += 2;
	ipc->path = malloc(s);
	if (ipc->path == NULL) {
		nowdb_ipc_destroy(ipc);
		NOMEM("allocating path");
		return err;
	}
	sprintf(ipc->path, "%s/%s", path, catalogue);

	ipc->base = strdup(path);
	if (ipc->base == NULL) {
		nowdb_ipc_destroy(ipc);
		INVALID("copying base path");
	}

	ipc->things = ts_algo_tree_new(thingnamecompare, NULL,
	                               noupdate,
	                               thingdestroy,
	                               thingdestroy);
	if (ipc->things == NULL) {
		nowdb_ipc_destroy(ipc);
		NOMEM("allocating tree of ipc operators");
		return err;
	}

	ipc->free = calloc(1, sizeof(ts_algo_list_t));
	if (ipc->free == NULL) {
		nowdb_ipc_destroy(ipc);
		NOMEM("allocating free list");
		return err;
	}
	ts_algo_list_init(ipc->free);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: mkFile if not exists
 * ------------------------------------------------------------------------
 */
static nowdb_err_t mkFile(char *path) {
	FILE *f;

	f = fopen(path, "wb");
	if (f == NULL) return nowdb_err_get(nowdb_err_open,
	                                TRUE, OBJECT, path);
	fclose(f);

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load thing
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadThing(nowdb_ipc_t *ipc, uint32_t sz,
                                 uint32_t *off, char *buf) {
	ts_algo_tree_t *t;
	nowdb_err_t err=NOWDB_OK;
	thing_t *thing;
	uint32_t s = 0;
	
	thing = calloc(1,sizeof(thing_t));
	thing->t = buf[*off]; (*off)++;

	while(s < sz) {
		if (buf[*off+s] == 0) break;
		s++;
		
	}
	if (s == 0) {
		free(thing->ptr); free(thing);
		INVALID("No name in IPC operation");
	}
	if (s >= sz) {
		free(thing->ptr); free(thing);
		INVALID("Unterminated string in IPC catalogue");
	}
	thing->name = malloc(s+1);
	if (thing->name == NULL) {
		free(thing);
		NOMEM("allocating ipc operation");
		return err;
	}
	memcpy(thing->name, buf+(*off), s);
	thing->name[s] = 0; (*off)+=s; (*off)++;
	switch(thing->t) {
	case L: thing->ptr = mkLock(thing->name, &err);
	        t = ipc->things; break;
	default: t = NULL;
	}
	if (thing->ptr == NULL) {
		free(thing->name); free(thing);
		if (err != NOWDB_OK) return err;
		NOMEM("allocating ipc operation");
		return err;
	}
	if (ts_algo_tree_insert(t, thing) != TS_ALGO_OK) {
		thingdestroy(NULL,(void**)&thing);
		NOMEM("tree.insert");
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load things
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadThings(nowdb_ipc_t *ipc, uint32_t s, char *buf) {
	nowdb_err_t err=NOWDB_OK;
	uint32_t off=0;

	while(off < s) {
		err = loadThing(ipc, s, &off, buf);
		if (err != NOWDB_OK) break;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: load file
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadFile(nowdb_ipc_t *ipc) {
	nowdb_err_t err=NOWDB_OK;
	char *buf=NULL;
	struct stat st;

	if (stat(ipc->path, &st) != 0) {
		return mkFile(ipc->path);
	}
	
	buf = malloc(st.st_size);
	if (buf == NULL) {
		NOMEM("allocating read buffer");
		return err;
	}
	err = nowdb_readFile(ipc->path, buf, (uint32_t)st.st_size);
	if (err != NOWDB_OK) {
		free(buf); return err;
	}
	err = loadThings(ipc, (uint32_t)st.st_size, buf);
	free(buf); 
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: write Buffer to file
 * ------------------------------------------------------------------------
 */
static nowdb_err_t writeBuf(nowdb_ipc_t      *ipc,
                            ts_algo_list_t   *list,
                            uint32_t sz, char *buf) {
	ts_algo_list_node_t *run;
	uint32_t off=0;

	for(run=list->head; run!=NULL; run=run->nxt) {
		buf[off] = THING(run->cont)->t; off++;
		if (THING(run->cont)->name != NULL) { // should not be null!
			size_t s = strlen(THING(run->cont)->name);
			memcpy(buf+off, THING(run->cont)->name, s);
			off += s; buf[off] = 0; off++;
		}
	}
	return nowdb_writeFileWithBkp(ipc->base, ipc->path, buf, sz);
}

/* ------------------------------------------------------------------------
 * Helper: compute size
 * ------------------------------------------------------------------------
 */
static uint32_t computeSize(ts_algo_list_t *list) {
	uint32_t s=0;
	ts_algo_list_node_t *run;
	for(run=list->head; run!=NULL; run=run->nxt) { // should not be null!
		if (THING(run->cont)->name != NULL) {
			s += strlen(THING(run->cont)->name); s++;
		}
		s++;
	}
	return s;
}

/* ------------------------------------------------------------------------
 * Helper: write file
 * ------------------------------------------------------------------------
 */
static nowdb_err_t writeFile(nowdb_ipc_t *ipc) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_t *list;
	char *buf;
	uint32_t s;

	if (ipc->things == NULL) INVALID("not initiated");
	if (ipc->things->count == 0) return mkFile(ipc->path);
	list = ts_algo_tree_toList(ipc->things);
	if (list == NULL) {
		NOMEM("tree.toList");
		return err;
	}
	s = computeSize(list);
	if (s == 0) {
		ts_algo_list_destroy(list); free(list);
		return nowdb_err_get(nowdb_err_panic,
		       FALSE, OBJECT, "size != count");
	}
	buf = calloc(1,s);
	if (buf == NULL) {
		NOMEM("allocating ipc buffer");
		free(list); return err;
	}
	err = writeBuf(ipc, list, s, buf);
	ts_algo_list_destroy(list); free(list); free(buf);
	return err;
}

/* ------------------------------------------------------------------------
 * Load IPC Catalogue
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_load(nowdb_ipc_t *ipc) {
	nowdb_err_t err;

	if (ipc->things == NULL) {
		ipc->things = ts_algo_tree_new(thingnamecompare, NULL,
		                               noupdate,
		                               thingdestroy,
		                               thingdestroy);
	}
	if (ipc->things == NULL) {
		NOMEM("allocating tree of ipc operators");
		return err;
	}
	return loadFile(ipc);
}

/* ------------------------------------------------------------------------
 * Close IPC Manager
 * ------------------------------------------------------------------------
 */
void nowdb_ipc_close(nowdb_ipc_t *ipc) {
	if (ipc->things != NULL) {
		ts_algo_tree_destroy(ipc->things);
		free(ipc->things); ipc->things = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Destroy IPC Manager
 * ------------------------------------------------------------------------
 */
void nowdb_ipc_destroy(nowdb_ipc_t *ipc) {
	if (ipc == NULL) return;
	if (ipc->free != NULL) {
		ts_algo_list_destroy(ipc->free);
		free(ipc->free); ipc->free = NULL;
	}
	if (ipc->things != NULL) {
		ts_algo_tree_destroy(ipc->things);
		free(ipc->things); ipc->things = NULL;
	}
	if (ipc->path != NULL) {
		free(ipc->path); ipc->path = NULL;
	}
	if (ipc->base != NULL) {
		free(ipc->base); ipc->base = NULL;
	}
	nowdb_rwlock_destroy(&ipc->lock);
}

/* ------------------------------------------------------------------------
 * Create lock
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_createLock(nowdb_ipc_t *ipc, char *name) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2;
	thing_t pattern;
	thing_t  *thing;

	err = nowdb_lock_write(&ipc->lock);
	if (err != NOWDB_OK) return err;

	pattern.name = name;
	thing = ts_algo_tree_find(ipc->things, &pattern);
	if (thing != NULL) {
		DUPKEY(name);
		goto unlock;
	}
	thing = calloc(1,sizeof(thing_t));
	if (thing == NULL) {
		NOMEM("allocating thing");
		goto unlock;
	}
	thing->t = L;
	thing->name = strdup(name);
	if (thing->name == NULL) {
		free(thing);
		NOMEM("allocating lock name");
		goto unlock;
	}
	thing->ptr = mkLock(thing->name, &err);
	if (thing->ptr == NULL) {
		free(thing->name); free(thing);
		if (err != NOWDB_OK) return err;
		NOMEM("allocating lock");
		goto unlock;
	}
	if (ts_algo_tree_insert(ipc->things, thing) != TS_ALGO_OK) {
		thingdestroy(NULL, (void**)&thing);
		NOMEM("tree.insert");
		goto unlock;
	}
	err = writeFile(ipc);
	if (err != NOWDB_OK) {
		ts_algo_tree_delete(ipc->things, thing);
		goto unlock;
	}
unlock:
	err2 = nowdb_unlock_write(&ipc->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Drop Lock
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_dropLock(nowdb_ipc_t *ipc, char *name) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2;
	thing_t pattern;
	thing_t  *thing;

	err = nowdb_lock_write(&ipc->lock);
	if (err != NOWDB_OK) return err;

	pattern.name = name;
	thing = ts_algo_tree_find(ipc->things, &pattern);
	if (thing == NULL) {
		KEYNOF(name);
		goto unlock;
	}
	ts_algo_tree_delete(ipc->things, thing);
	err = writeFile(ipc);
unlock:
	err2 = nowdb_unlock_write(&ipc->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Lock
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_lock(nowdb_ipc_t *ipc, char *name,
                           char mode, int tmo) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2=NOWDB_OK;
	thing_t pattern, *t;
	lock_t *l;
	nowdb_time_t delay = TINYDELAY;
	nowdb_time_t wmo = tmo*DELAYUNIT;
	char locked = 0;
	pthread_t myself = pthread_self();

	/* we should set an "in-use" marker here!
	*/

	pattern.name = name;
	t = ts_algo_tree_find(ipc->things, &pattern);
	if (t == NULL) {
		KEYNOF(name); return err;
	}
	l = t->ptr;
	for(;;) {

		err = nowdb_lock(&l->lock);
		if (err != NOWDB_OK) return err;

		locked = 1;

		if (l->state == FREE) {
			l->state = mode;
			l->owner = myself;
			if (mode == NOWDB_IPC_RLOCK) l->readers++;
			break;
		}
		if (pthread_equal(l->owner, myself)) {
			err = nowdb_err_get(nowdb_err_selflock,
			                   FALSE, OBJECT, name);
			break;
		}
		if (l->state == NOWDB_IPC_RLOCK &&
		    mode     == NOWDB_IPC_RLOCK) {
			l->owner = myself;
			l->readers++;
			break;
		}

		if (wmo == 0) {
			err = nowdb_err_get(nowdb_err_timeout,
			                 FALSE, OBJECT, name);
			break;
		}

		err = nowdb_unlock(&l->lock);
		if (err != NOWDB_OK) return err;

		locked = 0;

		err = nowdb_task_sleep(delay);
		if (err != NOWDB_OK) break;

		if (wmo < 0) continue;
		if (delay > wmo) wmo = 0; else wmo -= delay;
	}
	if (locked) {
		err2 = nowdb_unlock(&l->lock);
		if (err2 != NOWDB_OK) {
			err2->cause = err;
			return err2;
		}
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Unlock
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_unlock(nowdb_ipc_t *ipc, char *name) {
	nowdb_err_t err=NOWDB_OK;
	thing_t pattern, *t;
	lock_t *l;

	pattern.name = name;
	t = ts_algo_tree_find(ipc->things, &pattern);
	if (t == NULL) {
		KEYNOF(name); return err;
	}
	l = t->ptr;

	err = nowdb_lock(&l->lock);
	if (err != NOWDB_OK) return err;

	if (l->state == NOWDB_IPC_RLOCK) l->readers--;

	if (l->readers == 0) {
		l->state = FREE;
		l->owner = (pthread_t)0;
	}
	return nowdb_unlock(&l->lock);
}

