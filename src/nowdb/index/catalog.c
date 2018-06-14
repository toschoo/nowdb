/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Index catalogue
 * ========================================================================
 */
#include <nowdb/index/catalog.h>

#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <stdint.h>

static char *OBJECT = "icat";

#define BUF(x) \
	((nowdb_index_buf_t*)x)

/* ------------------------------------------------------------------------
 * Tree callbacks: compare buf
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t bufcompare(void *ignore, void *left, void *right) {
	int cmp = strcmp(BUF(left)->buf,
	                 BUF(right)->buf);
	if (cmp < 0) return ts_algo_cmp_less;
	if (cmp > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Tree callbacks: update (do nothing!)
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Tree callbacks: delete and destroy
 * ------------------------------------------------------------------------
 */
static void bufdestroy(void *ignore, void **n) {
	if (*n != NULL) {
		free(BUF(*n)->buf);
		free(*n); *n = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Helper: create file if not exists
 * ------------------------------------------------------------------------
 */
static nowdb_err_t createFile(nowdb_index_cat_t *cat) {
	struct stat  st;
	FILE *f;

	if (stat(cat->path, &st) == 0) return NOWDB_OK;

	f = fopen(cat->path, "wb");
	if (f == NULL) return nowdb_err_get(nowdb_err_open,
	                          TRUE, OBJECT, cat->path);
	if (fclose(f) != 0) return nowdb_err_get(nowdb_err_close,
                                        TRUE, OBJECT, cat->path);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: open file
 * ------------------------------------------------------------------------
 */
static nowdb_err_t openFile(nowdb_index_cat_t *cat) {
	nowdb_err_t err;

	err = createFile(cat);
	if (err != NOWDB_OK) return err;

	cat->file = fopen(cat->path, "rb+");
	if (cat->file == NULL) return nowdb_err_get(nowdb_err_open,
	                                  TRUE, OBJECT, cat->path);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load line
 * ------------------------------------------------------------------------
 */
static nowdb_err_t getIBuf(char *buf, uint32_t sz, uint32_t *off,
                           nowdb_index_buf_t **ibuf) {
	uint32_t i = *off;
	size_t   s;

	/* get line */
	while(i < sz && buf[i] != '\n') i++;
	s = i-(*off);

	/* check that the first string is terminated */
	for(i=*off;i<s;i++) {}
	if (i==s) return nowdb_err_get(nowdb_err_catalog,
	          FALSE, OBJECT, "name in icat too long");

	*ibuf = calloc(1,sizeof(nowdb_index_buf_t));
	if (ibuf == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                  FALSE, OBJECT, "allocating index buf");
	
	(*ibuf)->buf = malloc(s);
	if ((*ibuf)->buf == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                             FALSE, OBJECT, "allocating buffer");
	memcpy((*ibuf)->buf, buf+(*off), s);
	(*ibuf)->sz = s;

	(*off) += s;
	
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load file
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadfile(FILE *file, char *path,
                            char **buf, uint32_t *sz) {
	struct stat st;

	rewind(file);

	if (stat(path, &st) != 0) return nowdb_err_get(nowdb_err_stat,
	                                          TRUE, OBJECT, path);
	*sz = (uint32_t)st.st_size;
	*buf = malloc(*sz);
	if (*buf == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                    FALSE, OBJECT, "allocating buffer");
	if (fread(*buf, 1, *sz, file) != *sz) {
		free(*buf); return nowdb_err_get(nowdb_err_read,
		                            TRUE, OBJECT, path);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load buf from file
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadICat(nowdb_index_cat_t *cat) {
	nowdb_index_buf_t *ibuf=NULL;
	nowdb_err_t err;
	char *tmp=NULL;
	uint32_t off=0;
	uint32_t  sz=0;

	err = loadfile(cat->file, cat->path, &tmp, &sz);
	if (err != NOWDB_OK) return err;
	
	while(off < sz) {
		err = getIBuf(tmp, sz, &off, &ibuf);
		if (err != NOWDB_OK) break;

		if (ts_algo_tree_insert(cat->buf, ibuf) != TS_ALGO_OK) {
			err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
			                                      "buf.insert");
			bufdestroy(NULL, (void**)&ibuf); break;
		}
		
	}
	free(tmp);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: write catalog to file
 * ------------------------------------------------------------------------
 */
static nowdb_err_t writeICat(nowdb_index_cat_t *icat) {
	ts_algo_list_t *buf;
	ts_algo_list_node_t *runner;
	nowdb_index_buf_t *ibuf;

	if (icat->ctx->count == 0) {
		if (ftruncate(fileno(icat->file), 0) != 0)
			return nowdb_err_get(nowdb_err_trunc,
			           TRUE, OBJECT, icat->path);
	}
	buf = ts_algo_tree_toList(icat->buf);
	if (buf == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                           TRUE, OBJECT, "tree.toList");

	rewind(icat->file);
	for(runner=buf->head;runner!=NULL;runner=runner->nxt) {
		ibuf = runner->cont;
		if (fwrite(ibuf->buf, 1, ibuf->sz, icat->file) != ibuf->sz) {
			ts_algo_list_destroy(buf); free(buf);
			return nowdb_err_get(nowdb_err_write,
			           TRUE, OBJECT, icat->path);
		}
	}
	ts_algo_list_destroy(buf); free(buf);
	if (fflush(icat->file) != 0) return nowdb_err_get(nowdb_err_flush,
			                         TRUE, OBJECT, icat->path);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Open catalog
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_cat_open(char              *path,
                                 ts_algo_tree_t     *ctx,
                                 nowdb_index_cat_t **cat) {
	nowdb_err_t err;

	if (path == NULL) return nowdb_err_get(nowdb_err_invalid,
                                  FALSE, OBJECT, "path is NULL");
	if (ctx  == NULL) return nowdb_err_get(nowdb_err_invalid,
                              FALSE, OBJECT, "contexts is NULL");
	if (cat  == NULL) return nowdb_err_get(nowdb_err_invalid,
                       FALSE, OBJECT, "catalog pointer is NULL");

	*cat = calloc(1,sizeof(nowdb_index_cat_t));
	if (*cat == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                    FALSE, OBJECT, "allocating catalog");

	(*cat)->file = NULL;
	(*cat)->buf  = NULL;
	(*cat)->ctx  = ctx;

	(*cat)->path = strdup(path); 
	if ((*cat)->path == NULL) {
		nowdb_index_cat_close(*cat); free(*cat);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                          "allocating path");
	}
	err = openFile(*cat);
	if (err != NOWDB_OK) {
		nowdb_index_cat_close(*cat); free(*cat);
		return err;
	}
	(*cat)->buf = ts_algo_tree_new(bufcompare, NULL,
	                               noupdate,
	                               bufdestroy,
	                               bufdestroy);
	if ((*cat)->buf == NULL) {
		nowdb_index_cat_close(*cat); free(*cat);
		return nowdb_err_get(nowdb_err_no_mem,
		             FALSE, OBJECT, "buf.new");
	}
	err = loadICat(*cat);
	if (err != NOWDB_OK) {
		nowdb_index_cat_close(*cat); free(*cat);
		return err;
	}
	return NOWDB_OK;
}

void nowdb_index_cat_close(nowdb_index_cat_t *cat) {
	if (cat == NULL) return;
	if (cat->buf != NULL) {
		ts_algo_tree_destroy(cat->buf);
		free(cat->buf); cat->buf = NULL;
	}
	if (cat->file != NULL) {
		fclose(cat->file); cat->file = NULL;
	}
	if (cat->path != NULL) {
		free(cat->path); cat->path = NULL;
	}
}

nowdb_err_t nowdb_index_cat_add(nowdb_index_cat_t  *cat,
                                nowdb_index_desc_t *desc);

nowdb_err_t nowdb_index_cat_remove(nowdb_index_cat_t *cat,
                                   char            *name);

