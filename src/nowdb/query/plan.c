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
	nowdb_err_t   err;
	nowdb_ast_t  *trg, *from;
	nowdb_plan_t *stp;

	from = nowdb_ast_from(ast);
	if (from == NULL) INVALIDAST("no 'from' in DQL");

	trg = nowdb_ast_target(from);
	if (trg == NULL) INVALIDAST("no target in from");

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
	if (stp == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                         "allocating plan");
		nowdb_plan_destroy(plan); return err;
	}
	stp->ntype = NOWDB_PLAN_READER;
	stp->stype = NOWDB_READER_FS_;
	stp->target = trg->stype;
	stp->name = trg->value;

	if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem,
	               FALSE, OBJECT, "list append");
		nowdb_plan_destroy(plan); return err;
	}
	return NOWDB_OK;
}

void nowdb_plan_destroy(ts_algo_list_t *plan) {
	ts_algo_list_node_t *runner, *tmp;

	if (plan == NULL) return;
	runner = plan->head;
	while(runner!=NULL) {
		free(runner->cont); tmp = runner->nxt;
		ts_algo_list_remove(plan, runner);
		free(runner); runner = tmp;
	}
}

