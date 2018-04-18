/* ------------------------------------------------------------------------------
 * A simple progressbar
 * (c) Tobias Schoofs, 2016
 * ------------------------------------------------------------------------------
 */
#include <stdio.h>

/* -------------------------------------------------------------------------------
 * The progress bar
 * -------------------------------------------------------------------------------
 */
typedef struct {
	FILE *stream; /* where to write the output to                    */
	int  size;    /* size of the bar in terms of progress indicators */
	int  step;    /* one indicator represents n bits of work         */
	char st;      /* internal state for xes doing exercises          */
} progress_t;

/* -------------------------------------------------------------------------------
 * init progress bar
 * -------------------------------------------------------------------------------
 */
void init_progress(progress_t *p, FILE *stream, int all); 

/* -------------------------------------------------------------------------------
 * update progress bar
 * -------------------------------------------------------------------------------
 */
void update_progress(progress_t *p, int i); 

/* -------------------------------------------------------------------------------
 * close progress bar
 * -------------------------------------------------------------------------------
 */
void close_progress(progress_t *p); 
