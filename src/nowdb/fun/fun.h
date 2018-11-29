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
#include <nowdb/fun/expr.h>

#include <stdint.h>

#include <tsalgo/list.h>

#define NOWDB_FUN_ZERO  1
#define NOWDB_FUN_ONE   2
#define NOWDB_FUN_MANY  3
#define NOWDB_FUN_TREE  4

#define NOWDB_FUN_COUNT     10000000
#define NOWDB_FUN_SUM       20000000
#define NOWDB_FUN_PROD      30000000
#define NOWDB_FUN_MAX       40000000
#define NOWDB_FUN_MIN       50000000
#define NOWDB_FUN_SPREAD    60000000
#define NOWDB_FUN_AVG       70000000
#define NOWDB_FUN_MEDIAN    80000000
#define NOWDB_FUN_MODE      90000000
#define NOWDB_FUN_STDDEV   100000000
#define NOWDB_FUN_INTEGRAL 110000000

typedef struct {
	uint32_t              fun; /* the function code             */
	int                 ftype; /* function type                 */
	nowdb_content_t     ctype; /* content type                  */
	uint16_t            field; /* to be removed                 */
	uint16_t            fsize; /* size of the field             */
	nowdb_type_t        dtype; /* type of the field             */
	nowdb_expr_t         expr; /* expression to evaluate        */
	nowdb_type_t        otype; /* output type of that function  */
	ts_algo_list_t       many; /* list of blocks                */
	nowdb_value_t        init; /* initial value                 */
	nowdb_value_t          r1; /* first register and result     */
	nowdb_value_t          r2; /* second register               */
	nowdb_value_t          r3; /* third register                */
	nowdb_value_t          r4; /* fourth register               */
	nowdb_blist_t      *flist; /* free block list               */
	uint32_t              off; /* current offset into block     */
	char                first; /* first round                   */
	/* key tree    */          /* tree to find values quickly   */
} nowdb_fun_t;

/* -----------------------------------------------------------------------
 * Allocate and init function
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_fun_new(nowdb_fun_t    **fun,
                          uint32_t        type,
                          nowdb_content_t ctype,
                          nowdb_expr_t    expr,
                          nowdb_value_t  *init);

/* -----------------------------------------------------------------------
 * Init already allocated function
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_fun_init(nowdb_fun_t      *fun,
                           uint32_t         type,
                           nowdb_content_t ctype,
                           nowdb_expr_t     expr,
                           nowdb_value_t   *init);

/* -----------------------------------------------------------------------
 * Convert fun to aggfun (for compatibility with expr)
 * -----------------------------------------------------------------------
 */
#define NOWDB_FUN_AS_AGG(f) \
	((nowdb_aggfun_t)f)

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
nowdb_err_t nowdb_fun_map(nowdb_fun_t  *fun,
                          nowdb_eval_t *hlp,
                          uint64_t     rmap,
                          void      *record);

/* -----------------------------------------------------------------------
 * Reduce
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_fun_reduce(nowdb_fun_t *fun);

/* -----------------------------------------------------------------------
 * Function type from name
 * -----------------------------------------------------------------------
 */
int nowdb_fun_fromName(const char *name);
#endif
