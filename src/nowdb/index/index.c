/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Index
 * ========================================================================
 */

#include <nowdb/index/index.h>

#include <beet/index.h>

#include <stdlib.h>
#include <stdint.h>

/* -----------------------------------------------------------------------
 * Destroy index descriptor
 * -----------------------------------------------------------------------
 */
void nowdb_index_desc_destroy(nowdb_index_desc_t *desc) {
	if (desc == NULL) return;
	if (desc->name != NULL) {
		free(desc->name); desc->name = NULL;
	}
	if (desc->keys != NULL) {
		free(desc->keys->off);
		free(desc->keys);
		desc->keys = NULL;
	}
}

