/* ------------------------------------------------------------------------
 * Implements the most beautiful progress bar on the planet
 * --------------------------------------------------------
 * (c) Tobias Schoofs, 2016
 * ------------------------------------------------------------------------
 */
#include "progress.h"

/* ------------------------------------------------------------------------
 * initialise the progress bar
 * ------------------------------------------------------------------------
 */
void init_progress(progress_t *p, FILE *stream, int all) {
	int i;

	if (stream == NULL) {
		p->step = 0; return;
	}
	p->stream = stream;

	if (all <= 0) {
		p->size = 1;
		p->step = 1;
		return;
	}

	p->size = all<20?all:20;
	p->step = all/p->size; 

	p->size += (all-p->size*p->step)/p->step;
	p->size += all>p->size*p->step?1:0;

	p->st   = 0;

	fprintf(p->stream, "[");
	for(i=0;i<p->size;i++) fprintf(p->stream, " ");
	fprintf(p->stream, "]");
	for(i=0;i<p->size;i++) fprintf(p->stream, "\b");
	fflush(p->stream);
}

/* ------------------------------------------------------------------------
 * update the progress bar
 * ------------------------------------------------------------------------
 */
void update_progress(progress_t *p, int i) {
	if (p->step == 0) return;
	if (i % p->step == 0) {
		fprintf(p->stream,"\bX"); 
		if (i > 0) fprintf(p->stream,"X");
	} else if (p->st == 0) {
		fprintf(p->stream,"\bx"); p->st = 1;
	} else {
		fprintf(p->stream,"\bX"); p->st = 0;
	}
	fflush(p->stream);
}

/* ------------------------------------------------------------------------
 * close the progress bar
 * ------------------------------------------------------------------------
 */
void close_progress(progress_t *p) {
	if (p->step == 0) return;
	fprintf(p->stream,"\bX"); fflush(p->stream);
}
