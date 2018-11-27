/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Private 12 Byte LRU Cache (i.e. roleid/key)
 * ========================================================================
 */
#include <nowdb/mem/plru12.h>

static char *OBJECT = "LRU12";

/* ------------------------------------------------------------------------
 * Private 12Byte LRU
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_roleid_t role;
	nowdb_key_t     key;
} key12_t;

#define KEY(x) \
	((key12_t*)x)

#define LRUNULL() \
	if (lru == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                           FALSE, OBJECT, "LRU is NULL");

#define NOMEM(x) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, x);

/* ------------------------------------------------------------------------
 * LRU Callbacks: compare
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t compare(void *ignore, void *one, void *two) {
	if (KEY(one)->role < KEY(two)->role) return ts_algo_cmp_less;
	if (KEY(one)->role > KEY(two)->role) return ts_algo_cmp_greater;
	if (KEY(one)->key  < KEY(two)->key) return ts_algo_cmp_less;
	if (KEY(one)->key  > KEY(two)->key) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * LRU Callbacks: delete
 * ------------------------------------------------------------------------
 */
static void destroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	free(*n); *n=NULL;
}

/* ------------------------------------------------------------------------
 * LRU Callbacks: update
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	if (n != NULL) free(n);
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Init LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plru12_init(nowdb_plru12_t *lru,
                              uint32_t       max) {
	nowdb_err_t err;

	LRUNULL();

	if (ts_algo_lru_init(lru, max, &compare,
	                               &noupdate, 
	                               &destroy, 
	                               &destroy) != TS_ALGO_OK) {
		NOMEM("allocating lru");
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy LRU
 * ------------------------------------------------------------------------
 */
void nowdb_plru12_destroy(nowdb_plru12_t *lru) {
 	if (lru == NULL) return;
	ts_algo_lru_destroy(lru);
}


/* ------------------------------------------------------------------------
 * Get data from LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plru12_get(nowdb_plru12_t  *lru,
                             nowdb_roleid_t  role,
                             nowdb_key_t      key,
                             char          *found) {
	key12_t k, *x;

	LRUNULL();

	k.role = role;
	k.key = key;

	x = ts_algo_lru_get(lru, &k);

	*found = x!=NULL;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Add data to LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plru12_add(nowdb_plru12_t *lru,
                            nowdb_roleid_t  role,
                            nowdb_key_t      key) {
	nowdb_err_t err=NOWDB_OK;
	key12_t *k;
	
	LRUNULL();

	k = malloc(sizeof(key12_t));
	if (k == NULL) {
		NOMEM("allocating lru node");
		return err;
	}

	k->role = role;
	k->key = key;

	if (ts_algo_lru_addResident(lru, k) != TS_ALGO_OK) {
		NOMEM("adding content to LRU");
		free(k);
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Revoke residence
 * ------------------------------------------------------------------------
 */
void nowdb_plru12_revoke(nowdb_plru12_t *lru,
                         nowdb_roleid_t  role,
                         nowdb_key_t      key) {
	key12_t k;
	k.key = key; k.role = role;
	ts_algo_lru_revokeResidence(lru, &k);
}
