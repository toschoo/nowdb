/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Generic SQL functions
 * ========================================================================
 */
#ifndef nowdb_fun_decl
#define nowdb_fun_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/index/index.h>
#include <nowdb/mem/blist.h>

#include <stdint.h>

#include <tsalgo/list.h>

#define NOWDB_FUN_ZERO  1
#define NOWDB_FUN_ONE   2
#define NOWDB_FUN_MANY  3
#define NOWDB_FUN_TREE  4

#define NOWDB_FUN_COUNT     10
#define NOWDB_FUN_SUM       20
#define NOWDB_FUN_PROD      30
#define NOWDB_FUN_MAX       40
#define NOWDB_FUN_MIN       50
#define NOWDB_FUN_SPREAD    60
#define NOWDB_FUN_AVG       70
#define NOWDB_FUN_MEDIAN    80
#define NOWDB_FUN_MODE      90
#define NOWDB_FUN_STDDEV   100
#define NOWDB_FUN_INTEGRAL 110

typedef struct {
	uint32_t              fun; /* the function code             */
	int                 ftype; /* function type                 */
	nowdb_content_t     ctype; /* content type                  */
	uint32_t            recsz; /* record size                   */
	uint16_t            field; /* identifies the field          */
	uint16_t            fsize; /* field size                    */
	nowdb_type_t        dtype; /* field type                    */
	ts_algo_list_t       many; /* list of blocks                */
	nowdb_value_t        init; /* initial value                 */
	nowdb_value_t       value; /* the running value and result  */
	nowdb_value_t         hlp; /* helper field                  */
	nowdb_blist_t      *flist; /* free block list               */
	uint32_t              off; /* current offset into block     */
	char                first; /* first round                   */
	/* key tree    */          /* tree to find values quickly   */
} nowdb_fun_t;

/* -----------------------------------------------------------------------
 * Init already allocated function
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_fun_init(nowdb_fun_t          *fun,
                           uint32_t             type,
                           nowdb_content_t     ctype,
                           uint16_t            field,
                           uint16_t            fsize,
                           nowdb_type_t        dtype,
                           nowdb_value_t       *init);

/* -----------------------------------------------------------------------
 * Destroy
 * -----------------------------------------------------------------------
 */
void nowdb_fun_destroy(nowdb_fun_t *fun);

/* -----------------------------------------------------------------------
 * Reset
 * -----------------------------------------------------------------------
 */
void nowdb_fun_reset(nowdb_fun_t *fun);

/* -----------------------------------------------------------------------
 * Collect
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_fun_map(nowdb_fun_t *fun, void *record);

/* -----------------------------------------------------------------------
 * Reduce
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_fun_reduce(nowdb_fun_t *fun);

#endif
