/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Execution Plan
 * --------------
 * quite simple now, but will be the most complex thing
 * ========================================================================
 */
#include <nowdb/query/plan.h>

static char *OBJECT = "plan";

#define INVALIDAST(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s)

/* -----------------------------------------------------------------------
 * Over-simplistic to get it going:
 * - we assume an ast with a simple target object
 * - we create 1 reader fullscan+ on either vertex or context
 * - that's it
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_plan_fromAst(nowdb_ast_t *ast, ts_algo_list_t *plan) {
	nowdb_ast_t *trg;
	nowdb_plan_t *stp;

	trg = nowdb_ast_target(ast);
	if (trg == NULL) INVALIDAST("no target in AST");

	stp = malloc(sizeof(nowdb_plan_t));
	if (stp == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                      FALSE, OBJECT, "allocating plan");
	stp->ntype = NOWDB_PLAN_SUMMARY;
	stp->stype = 0;
	stp->target = 1;
	stp->name = NULL;

	if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "list append");
	}
	stp = malloc(sizeof(nowdb_plan_t));
	if (stp == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                      FALSE, OBJECT, "allocating plan");
	stp->ntype = NOWDB_PLAN_READER;
	stp->stype = NOWDB_READER_FS_;
	stp->target = trg->stype;
	stp->name = trg->value;

	if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "list append");
	}
	return NOWDB_OK;
}

