/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Uitilities to ease working with result rows
 * ========================================================================
 */

#ifndef nowdb_rowutl_decl
#define nowdb_rowutl_decl

#include <nowdb/types/types.h>

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

/* ------------------------------------------------------------------------
 * write (e.g. print) buffer
 * ------------------------------------------------------------------------
 */
int nowdb_row_print(char *buf, uint32_t sz, FILE *stream);

/* ------------------------------------------------------------------------
 * Manually create a row buffer with containing one value
 * ------------------------------------------------------------------------
 */
char *nowdb_row_fromValue(nowdb_type_t t, void *value, uint32_t *sz); 

/* ------------------------------------------------------------------------
 * Add a value to a row buffer
 * ------------------------------------------------------------------------
 */
char *nowdb_row_addValue(char *row, nowdb_type_t t,
                         void *value, uint32_t *sz);

/* ------------------------------------------------------------------------
 * Add EOR
 * ------------------------------------------------------------------------
 */
void nowdb_row_addEOR(char *row, uint32_t *sz);

/* ------------------------------------------------------------------------
 * Compute the length of a row (assuming the row is correct!)
 * ------------------------------------------------------------------------
 */
int nowdb_row_len(char *row);

/* ------------------------------------------------------------------------
 * Find last complete row in a bunch of rows
 * ------------------------------------------------------------------------
 */
int nowdb_row_findLastRow(char *buf, int sz);

/* ------------------------------------------------------------------------
 * Find end of row (row starting at idx)
 * ------------------------------------------------------------------------
 */
int nowdb_row_findEOR(char *buf, uint32_t sz, int idx);

/* ------------------------------------------------------------------------
 * End of string (string starting at idx)
 * ------------------------------------------------------------------------
 */
int nowdb_row_findEndOfStr(char *buf, int sz, int idx);

#endif
