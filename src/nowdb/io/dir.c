/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Directories
 * ========================================================================
 *
 * ========================================================================
 */
#include <nowdb/io/dir.h>

#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

static char *OBJECT = "dir";

/* ------------------------------------------------------------------------
 * append path to path
 * ------------------------------------------------------------------------
 */
nowdb_path_t nowdb_path_append(nowdb_path_t base, nowdb_path_t toadd) {
	size_t s1, s2;
	nowdb_path_t path;

	if (base == NULL) return NULL;
 	s1 = strnlen(base, NOWDB_MAX_PATH+1);
	if (s1 >= NOWDB_MAX_PATH) return NULL;

	if (toadd == NULL) return NULL;
 	s2 = strnlen(toadd, NOWDB_MAX_PATH+1);
	if (s2 >= NOWDB_MAX_PATH) return NULL;

	if (s1 + s2 + 1 > NOWDB_MAX_PATH) return NULL;

	path = malloc(s1+1+s2+1);
	if (path == NULL) return NULL;

	sprintf(path, "%s/%s", base, toadd);
	return path;
}

/* ------------------------------------------------------------------------
 * extract filename from a path
 * ------------------------------------------------------------------------
 */
nowdb_path_t nowdb_path_filename(nowdb_path_t path) {
	nowdb_path_t nm;
	size_t s = strlen(path);
	size_t l;
	int    i;
	for(i=s-1;i>=0;i--) {
		if (path[i] == '/') break;
	}
	if (i == s-1) return NULL;
	if (i == 0) {
		nm = malloc(s+1);
		if (nm == NULL) return NULL;
		strcpy(nm, path);
		return nm;
	}
	l = s-i;
	nm = malloc(l);
	if (nm == NULL) return NULL;
	for(int z=0; z<l; z++) {
		nm[z] = path[i+z+1];
	}
	nm[l-1] = 0;
	return nm;
}

/* ------------------------------------------------------------------------
 * filesize
 * ------------------------------------------------------------------------
 */
uint32_t nowdb_path_filesize(nowdb_path_t path) {
	struct stat st;
	if (stat(path, &st) != 0) return 0;
	return (uint32_t)st.st_size;
}

/* ------------------------------------------------------------------------
 * object represented by path exists
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_path_exists(nowdb_path_t  path,
                              nowdb_dir_type_t t) {
	struct stat st;
	if (stat(path, &st) != 0) return FALSE;
	if ((t & NOWDB_DIR_TYPE_DIR) && S_ISDIR(st.st_mode)) return TRUE;
	if ((t & NOWDB_DIR_TYPE_FILE) && S_ISREG(st.st_mode)) return TRUE;
	if (t == NOWDB_DIR_TYPE_ANY) {
		if (S_ISLNK(st.st_mode)) return TRUE;
		if (S_ISSOCK(st.st_mode)) return TRUE;
		if (S_ISBLK(st.st_mode)) return TRUE;
		if (S_ISCHR(st.st_mode)) return TRUE;
		if (S_ISFIFO(st.st_mode)) return TRUE;
	}
	return FALSE;
}

/* ------------------------------------------------------------------------
 * Create directory
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dir_create(nowdb_path_t path) {
	if (mkdir(path,NOWDB_DIR_MODE) != 0) {
		return nowdb_err_get(nowdb_err_create, TRUE, OBJECT, path);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Remove file
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_path_remove(nowdb_path_t path) {
	if (remove(path) != 0) {
		return nowdb_err_get(nowdb_err_remove, TRUE, OBJECT, path);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Move src to trg
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_path_move(nowdb_path_t src,
                            nowdb_path_t trg) {
	if (rename(src,trg) != 0) return nowdb_err_get(nowdb_err_move,
	                                           TRUE, OBJECT, trg);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy content list
 * ------------------------------------------------------------------------
 */
void nowdb_dir_content_destroy(ts_algo_list_t *list) {
	ts_algo_list_node_t *runner;
	ts_algo_list_node_t *tmp;

	runner = list->head;
	while(runner!=NULL) {
		nowdb_dir_ent_t *ent = runner->cont;
		tmp = runner->nxt;
		if (ent != NULL) {
			if (ent->path != NULL) free(ent->path);
			free(ent);
		}
		free(runner); runner = tmp;
	}
}

/* ------------------------------------------------------------------------
 * Get content from a directory
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dir_content(nowdb_path_t path,
                           nowdb_dir_type_t   t,
                           ts_algo_list_t *list) {
	nowdb_err_t err;
	DIR *d;
	struct dirent *e;
	nowdb_dir_ent_t *ent;
	struct stat st;
	nowdb_path_t p;
	nowdb_bool_t x,y;
	

	d = opendir(path);
	if (d == NULL) return nowdb_err_get(nowdb_err_open,
	                               TRUE, OBJECT, path);

	for(e=readdir(d); e!=NULL; e=readdir(d)) {
		if (strcmp(e->d_name, ".") == 0) continue;
		if (strcmp(e->d_name, "..") == 0) continue;

		p = nowdb_path_append(path, e->d_name);
		if (p == NULL) {
			closedir(d);
			return nowdb_err_get(nowdb_err_no_mem,
			                 FALSE, OBJECT, NULL);
		}
		if (stat(p, &st) != 0) { 
			err = nowdb_err_get(nowdb_err_stat,
			                   TRUE, OBJECT, p);
			free(p); return err;
		}
		x = S_ISREG(st.st_mode);
		if (x) y = FALSE; else y = S_ISDIR(st.st_mode);
		if (((t & NOWDB_DIR_TYPE_FILE) && x) ||
		    ((t & NOWDB_DIR_TYPE_DIR)  && y)) {
			ent = malloc(sizeof(nowdb_dir_ent_t));
			if (ent == NULL) {
				free(p); closedir(d);
				nowdb_dir_content_destroy(list);
				return nowdb_err_get(nowdb_err_no_mem,
				                 FALSE, OBJECT, NULL);
			}
			ent->path = p;
			ent->t    = x?NOWDB_DIR_TYPE_FILE:NOWDB_DIR_TYPE_DIR;
			if (ts_algo_list_append(list, ent) != TS_ALGO_OK) {
				closedir(d); nowdb_dir_content_destroy(list);
				return nowdb_err_get(nowdb_err_no_mem,
				                 FALSE, OBJECT, NULL);
			}
		} else free(p);
	}
	closedir(d);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Write file with backup
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_writeFileWithBkp(nowdb_path_t base,
                                   nowdb_path_t file,
                            char *buf, uint32_t size) {
	nowdb_err_t  err;
	nowdb_path_t p=NULL;
	int bkp;
	ssize_t x;
	FILE *f;

	bkp = nowdb_path_exists(file, NOWDB_DIR_TYPE_ANY);
	if (bkp) {
		p = nowdb_path_append(base, ".bkp");
		if (p == NULL) {
			err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                  "allocating backup path");
			return err;
		}
		err = nowdb_path_move(file, p);
		if (err != NOWDB_OK) {
			free(p); return err;
		}
	}
	f = fopen(file, "w");
	if (f == NULL) {
		if (bkp) {
			NOWDB_IGNORE(nowdb_path_move(p, file));
			free(p);
		}
		return nowdb_err_get(nowdb_err_open, TRUE, OBJECT, file);
	}
	x = fwrite(buf, 1, size, f);
	if (x != size) {
		fprintf(stderr, "%d - %lu\n", size, x);
		fclose(f);
		if (bkp) {
			NOWDB_IGNORE(nowdb_path_move(p, file));
			free(p);
		}
		return nowdb_err_get(nowdb_err_write, TRUE, OBJECT, file);
	}
	if (fclose(f) != 0) {
		if (bkp) {
			NOWDB_IGNORE(nowdb_path_move(p, file));
			free(p);
		}
		return nowdb_err_get(nowdb_err_close, TRUE, OBJECT, file);
	}
	if (bkp) {
		nowdb_path_remove(p); free(p);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Read file 
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_readFile(nowdb_path_t file,
                   char *buf, uint32_t size)
{
	ssize_t x;
	FILE   *f;

	f = fopen(file, "rb");
	if (f == NULL) return nowdb_err_get(nowdb_err_open,
	                               TRUE, OBJECT, file);
	x = fread(buf, 1, size, f);
	if (x != size) {
		fclose(f);
		return nowdb_err_get(nowdb_err_read,
		                TRUE, OBJECT, file);
	}
	if (fclose(f) != 0) return nowdb_err_get(nowdb_err_close,
	                                     TRUE, OBJECT, file);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Remove dir and all its content
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_path_rRemove(nowdb_path_t path) {
	ts_algo_list_t dir;
	ts_algo_list_node_t *runner;
	nowdb_dir_ent_t *e;
	nowdb_err_t err =NOWDB_OK;

	ts_algo_list_init(&dir);
	err = nowdb_dir_content(path, NOWDB_DIR_TYPE_ANY, &dir);
	if (err != NOWDB_OK) {
		nowdb_dir_content_destroy(&dir);
		return err;
	}
	for (runner=dir.head; runner!=NULL; runner=runner->nxt) {
		e = runner->cont;
		if (e->t == NOWDB_DIR_TYPE_DIR) {
			err = nowdb_path_rRemove(e->path);
		} else {
			err = nowdb_path_remove(e->path);
		}
		if (err != NOWDB_OK) {
			nowdb_dir_content_destroy(&dir);
			return err;
		}
	}
	nowdb_dir_content_destroy(&dir);
	return nowdb_path_remove(path);
}


