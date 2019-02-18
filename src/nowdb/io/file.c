/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * File Abstraction
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

#include <pthread.h> // debug!!!

static char *OBJECT = "file";

#define MAX32 2147483647

#define NOMEM(x) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, x);

/* ------------------------------------------------------------------------
 * Allocate and initialise a new file descriptor
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_new(nowdb_file_t  **file,
                           uint32_t          id,
                           nowdb_path_t    path,
                           uint32_t         cap,
                           uint32_t        size,
                           uint32_t   blocksize,
                           uint32_t  recordsize,
                           nowdb_content_t cont,
                           nowdb_bitmap8_t ctrl,
                           nowdb_comp_t    comp,
                           nowdb_encp_t    encp,
                           nowdb_time_t   grain,
                           nowdb_time_t  oldest,
                           nowdb_time_t  newest) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "pointer to file descriptor is NULL");
	}
	*file = malloc(sizeof(nowdb_file_t));
	if (*file == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, NULL);
	}
	nowdb_err_t r = nowdb_file_init(*file,id,path,
	                                cap,size,
	                                blocksize,recordsize,
	                                cont,ctrl,comp,encp,
	                                grain,oldest,newest);
	if (r != NOWDB_OK) {
		free(*file); return r;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Initialise an already allocated file descriptor
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_init(nowdb_file_t   *file,
                            uint32_t          id,
                            nowdb_path_t    path,
                            uint32_t         cap,
                            uint32_t        size,
                            uint32_t   blocksize,
                            uint32_t  recordsize,
                            nowdb_content_t cont,
                            nowdb_bitmap8_t ctrl,
                            nowdb_comp_t    comp,
                            nowdb_encp_t    encp,
                            nowdb_time_t   grain,
                            nowdb_time_t  oldest,
                            nowdb_time_t  newest) {
	nowdb_err_t err;

	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}

	/* defaults */
	file->path     = NULL;
	file->mptr     = NULL;
	file->bptr     = NULL;
	file->tmp      = NULL;
	file->cdict    = NULL;
	file->ddict    = NULL;
	file->cctx     = NULL;
	file->dctx     = NULL;
	file->hdr      = NULL;
	file->off      = 0;
	file->size     = size;
	file->capacity = cap;
	file->cont     = cont;
	file->tmpsize  = 0;
	file->pos      = 0;
	file->dirty    = FALSE;
	file->used     = FALSE;
	file->fd       = -1;
	file->state    = nowdb_file_state_closed;
	file->order    = 0;

	file->setsize = nowdb_edge_pagectrlSize(recordsize);
	file->hdrsize = NOWDB_HDR_BASE_SIZE + file->setsize;

	/*
	fprintf(stderr, "CONTENT: %u\n", file->cont);
	fprintf(stderr, "HEADERSIZE: %u\n", file->hdrsize);
	fprintf(stderr, "   SETSIZE: %u\n", file->setsize);
	*/

	if ((ctrl & NOWDB_FILE_WRITER) && cap == 0) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                     "capacity is 0");
	}
	if (blocksize == 0) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                    "blocksize is 0");
	}
	if (recordsize == 0) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                   "recordsize is 0");
	}
	if (ctrl & NOWDB_FILE_READER) {
		file->bufsize  = blocksize;
	} else if (ctrl & NOWDB_FILE_WRITER) {
		file->bufsize  = NOWDB_FILE_MAPSIZE > cap ? cap :
		                              NOWDB_FILE_MAPSIZE;
	} else {
		file->bufsize = NOWDB_CMP_PAGE;
	}

	/* path */
	if (path == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                      "path is NULL");
	}
	size_t s = strnlen(path, NOWDB_MAX_PATH+1);
	if (s == 0 || s > NOWDB_MAX_PATH) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		             "path too small or too big (max: 4096)");
	}
	file->path = malloc(s+1);
	if (file->path == NULL) {
		NOMEM("allocating path");
		return err;
	}
	strcpy(file->path, path);

	file->hdr = calloc(1, file->hdrsize);
	if (file->hdr == NULL) {
		nowdb_file_destroy(file);
		NOMEM("allocating header");
		return err;
	}

	/* the other stuff */
	file->id         = id;
	file->capacity   = cap;
	file->blocksize  = blocksize;
	file->recordsize = recordsize;
	file->ctrl       = ctrl;
	file->comp       = comp;
	file->encp       = encp;
	file->grain      = grain;
	file->oldest     = oldest;
	file->newest     = newest;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Change file from writer to reader
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_makeReader(nowdb_file_t *file) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                           "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_closed) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "file descriptor is in wrong state");
	}
	file->bufsize = file->blocksize;
	if (file->ctrl & NOWDB_FILE_WRITER) file->ctrl ^= NOWDB_FILE_WRITER;
	if (file->ctrl & NOWDB_FILE_SPARE) file->ctrl ^= NOWDB_FILE_SPARE;
	file->ctrl |= NOWDB_FILE_READER;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Change file from reader to writer
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_makeWriter(nowdb_file_t *file) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                           "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_closed) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "file descriptor is in wrong state");
	}
	if (file->capacity == 0) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                     "capacity is 0");
	}
	file->bufsize  = NOWDB_FILE_MAPSIZE > file->capacity ?
	                                      file->capacity :
		                           NOWDB_FILE_MAPSIZE;
	if (file->ctrl & NOWDB_FILE_READER) file->ctrl ^= NOWDB_FILE_READER;
	if (file->ctrl & NOWDB_FILE_SPARE ) file->ctrl ^= NOWDB_FILE_SPARE;
	file->ctrl |= NOWDB_FILE_WRITER;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Change file to spare
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_makeSpare(nowdb_file_t *file) {
	nowdb_err_t err = nowdb_file_makeWriter(file);
	if (err != NOWDB_OK) return err;
	file->ctrl |= NOWDB_FILE_SPARE;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy file descriptor
 * ------------------------------------------------------------------------
 */
void nowdb_file_destroy(nowdb_file_t *file) {
	if (file == NULL) return;
	if (file->state == nowdb_file_state_mapped) {
		NOWDB_IGNORE(nowdb_file_umap(file));
	}
	if (file->state != nowdb_file_state_closed) {
		NOWDB_IGNORE(nowdb_file_close(file));
	}
	if (file->bptr != NULL) {
		free(file->bptr); file->bptr = NULL;
	}
	if (file->tmp != NULL) {
		free(file->tmp); file->tmp = NULL;
	}
	if (file->path != NULL) {
		free(file->path); file->path = NULL;
	}
	if (file->hdr != NULL) {
		free(file->hdr); file->hdr = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Copy file descriptor
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_copy(nowdb_file_t *source, nowdb_file_t *target) {
	nowdb_err_t err;

	if (source == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                         "source descriptor is NULL");
	}
	if (target == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                         "target descriptor is NULL");
	}
	err = nowdb_file_init(target,
	                      source->id,
	                      source->path,
	                      source->capacity,
	                      source->size,
	                      source->blocksize,
	                      source->recordsize,
	                      source->cont,
	                      source->ctrl,
	                      source->comp,
	                      source->encp,
	                      source->grain,
	                      source->oldest,
	                      source->newest);
	if (err != NOWDB_OK) return err;
	if (target->comp == NOWDB_COMP_ZSTD) {
		target->cdict = source->cdict;
		target->ddict = source->ddict;
		target->cctx  = source->cctx;
		target->dctx  = source->dctx;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Update file descriptor
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_update(nowdb_file_t *source, nowdb_file_t *target) {
	if (source == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                         "source descriptor is NULL");
	}
	if (target == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                         "target descriptor is NULL");
	}
	target->order = source->order;
	target->size = source->size;
	target->capacity = source->capacity;
	target->blocksize = source->blocksize;
	target->recordsize = source->recordsize;
	target->ctrl = source->ctrl;
	target->used = source->used;
	target->comp = source->comp;
	target->encp = source->encp;
	target->grain = source->grain;
	target->oldest = source->oldest;
	target->newest = source->newest;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create file physically on disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_create(nowdb_file_t *file) {
	nowdb_err_t err = NOWDB_OK;
	uint32_t max = file->capacity / file->bufsize;

	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                           "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_closed) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "file descriptor is in wrong state");
	}
	if (file->tmp == NULL) {
		file->tmp = malloc(file->bufsize);
		if (file->tmp == NULL) {
			return nowdb_err_get(nowdb_err_no_mem,
			                 FALSE, OBJECT, NULL);
		}
	}
	memset(file->tmp, 0, file->bufsize);
	file->fd = open(file->path, O_WRONLY | O_CREAT, NOWDB_FILE_MODE);
	if (file->fd < 0) {
		err = nowdb_err_get(nowdb_err_open, TRUE, OBJECT,
		                                     file->path);
		goto cleanup;
	}
	for(int i=0;i<max;i++) {
		if (write(file->fd, file->tmp, file->bufsize) !=
		                               file->bufsize) {
			err = nowdb_err_get(nowdb_err_write, TRUE, OBJECT,
			                                      file->path);
		}
	}
cleanup:
	free(file->tmp); file->tmp = NULL;
	if (file->fd > 0) {
		close(file->fd); file->fd = -1;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Remove file physically from disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_remove(nowdb_file_t *file) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, NULL);
	}
	if (file->path == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                      "path is NULL");
	}
	if (file->state == nowdb_file_state_mapped) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                    "file descriptor in wrong state");
	}
	if (remove(file->path) != 0) {
		return nowdb_err_get(nowdb_err_remove, TRUE, OBJECT,
		                                        file->path);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Erase file content
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_erase(nowdb_file_t *file) {
	nowdb_err_t err;
	ssize_t x;

	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, NULL);
	}
	if (file->path == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                      "path is NULL");
	}
	if (file->state != nowdb_file_state_closed) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                    "file descriptor in wrong state");
	}
	err = nowdb_file_open(file);
	if (err != NOWDB_OK) return err;
	memset(file->bptr, 0, file->bufsize);
	for(int i=0; i<file->size; i+=file->bufsize) {
		x = write(file->fd, file->bptr, file->bufsize);
		if (x != file->bufsize) return nowdb_err_get(nowdb_err_write,
		                                   TRUE, OBJECT, file->path);
		
	}
	file->size = 0;
	return nowdb_file_close(file);
}

/* ------------------------------------------------------------------------
 * Helper function to compute time deltas
 * ------------------------------------------------------------------------
 */
static inline void deltas(char *buf,
                          uint32_t size,
                          uint32_t recsize,
                          nowdb_time_t *f,
                          nowdb_time_t *t) {
	nowdb_time_t to   = NOWDB_TIME_DAWN;
	nowdb_time_t from = NOWDB_TIME_DUSK;
	nowdb_time_t *tmp;
	uint32_t mx = (size/recsize)*recsize;

	for(int i=0; i<mx; i+=recsize) {
		tmp = (nowdb_time_t*)(buf+i+NOWDB_OFF_TMSTMP);
		if (*tmp > to) to = *tmp;
		if (*tmp < from) from = *tmp;
	}
	*f = from;
	*t = to;
}

/* ------------------------------------------------------------------------
 * Helper function to compress a block using ZSTD and write to the file
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t zstdcomp(nowdb_file_t *file,
                             char *buf, uint32_t size) {
	ssize_t x;
	size_t sz=0;
	nowdb_time_t from=0;
	nowdb_time_t to=0;

	if (file->cdict != NULL) {
		/*
		fprintf(stderr, "[%lu] pointers: %p, %p, %p, %p\n", pthread_self(),
				file->cctx, file->tmp, buf, file->cdict);
		*/
		sz = ZSTD_compress_usingCDict(file->cctx,
		                              file->tmp, file->bufsize,
		                              buf, size,
		                              file->cdict);
	} else {
		sz = ZSTD_compress(file->tmp, file->bufsize,
		               buf, size, NOWDB_ZSTD_LEVEL);
	}
	if (ZSTD_isError(sz)) {
		return nowdb_err_get(nowdb_err_comp, FALSE, OBJECT,
			              (char*)ZSTD_getErrorName(sz));
	}

	if (file->ctrl & NOWDB_FILE_TS) deltas(buf, size,
	                                       file->recordsize,
	                                       &from, &to);

	file->hdr->reserved = 0;
	file->hdr->size = (uint32_t)sz;
	file->hdr->from = from;
	file->hdr->to = to;
	memset((char*)(file->hdr)+NOWDB_HDR_BASE_SIZE, 0xff, file->setsize);

	x = write(file->fd, file->hdr, file->hdrsize);
	if (x != file->hdrsize) {
		return nowdb_err_get(nowdb_err_write, TRUE, OBJECT,
			                               file->path);
	}
	x = write(file->fd, file->tmp, sz);
	if (x != sz) {
		return nowdb_err_get(nowdb_err_write, TRUE, OBJECT,
			                               file->path);
	}
	file->size += sz + file->hdrsize;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper function to write a block to the file
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t flatwrite(nowdb_file_t *file,
                              char *buf, uint32_t size) {
	ssize_t x = write(file->fd, buf, size);
	if (x != size) {
		return nowdb_err_get(nowdb_err_write, TRUE, OBJECT,
			                               file->path);
	}
	file->size += size;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Write a block to a ("reader") file
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_writeBuf(nowdb_file_t *file,
                          char *buf, uint32_t size) {
	nowdb_err_t err = NOWDB_OK;
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_open) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "file descriptor is in wrong state");
	}
	if (file->comp == NOWDB_COMP_FLAT) {
		err = flatwrite(file, buf, size);
	} else if (file->comp == NOWDB_COMP_ZSTD) {
		err = zstdcomp(file, buf, size);
	} else return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
	                                       "unknown compression");
	if (file->capacity < file->size) file->capacity = file->size;
	return err;
}

/* ------------------------------------------------------------------------
 * Open file
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_open(nowdb_file_t *file) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_closed) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                  "file descriptor is in wrong state");
	}
	file->fd = open(file->path, O_RDWR);
	if (file->fd < 0) {
		return nowdb_err_get(nowdb_err_open, TRUE, OBJECT,
		                                       file->path);
	}
	if (file->bptr == NULL) {
		file->bptr = malloc(file->bufsize);
		if (file->bptr == NULL) {
			return nowdb_err_get(nowdb_err_no_mem,
			                  FALSE, OBJECT, NULL);
		}
	}
	if (file->comp != NOWDB_COMP_FLAT && file->tmp == NULL) {
		file->tmp = malloc(file->bufsize);
		if (file->tmp == NULL) {
			return nowdb_err_get(nowdb_err_no_mem,
			                  FALSE, OBJECT, NULL);
		}
	}
	file->state = nowdb_file_state_open;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Close file
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_close(nowdb_file_t *file) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state == nowdb_file_state_closed) return NOWDB_OK;
	if (file->state != nowdb_file_state_open) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                  "file descriptor is in wrong state");
	}
	if (file->bptr != NULL) {
		free(file->bptr); file->bptr = NULL;
	}
	if (file->tmp != NULL) {
		free(file->tmp); file->tmp = NULL;
	}
	if (close(file->fd) != 0) {
		return nowdb_err_get(nowdb_err_close, TRUE, OBJECT,
		                                        file->path);
	}
	file->fd = -1;
	file->state = nowdb_file_state_closed;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Map file to memory
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_map(nowdb_file_t *file) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state == nowdb_file_state_mapped) return NOWDB_OK;
	if (file->state == nowdb_file_state_closed) {
		file->fd = open(file->path, O_RDWR);
		if (file->fd < 0) {
			return nowdb_err_get(nowdb_err_open, TRUE, OBJECT,
		                                               file->path);
		}
		file->state = nowdb_file_state_open;
	}
	file->mptr = mmap(
		NULL,          /* any address is ok          */
		file->bufsize, /* we want to see 1 window    */
		PROT_WRITE,    /* file is writeable          */
		MAP_SHARED,    /* we map a file and share it */
		file->fd,      /* the file descriptor        */
		file->pos);    /* we start at the beginning  */
	if (file->mptr == MAP_FAILED) {
		return nowdb_err_get(nowdb_err_map, TRUE, OBJECT,
		                                      file->path);
	}
	file->off = 0;
	file->state = nowdb_file_state_mapped;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Map file at a given position
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_mapAt(nowdb_file_t *file, uint32_t pos) {
	nowdb_err_t err;
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state == nowdb_file_state_mapped) {
		err = nowdb_file_umap(file);
		if (err != NOWDB_OK) return err;
	}
	file->pos = pos/file->bufsize;
	file->pos *= file->bufsize;
	return nowdb_file_map(file);
}

/* ------------------------------------------------------------------------
 * Unmap file from memory
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_umap(nowdb_file_t *file) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_mapped) return NOWDB_OK;
	if (file->mptr == NULL) {
		return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT,
		                     "inconsistent file descriptor");
	}
	if (munmap(file->mptr, file->bufsize) != 0) {
		return nowdb_err_get(nowdb_err_umap, TRUE, OBJECT,
		                                       file->path);
	}
	file->mptr = NULL;
	file->state = nowdb_file_state_open;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Unmap current position and map next block
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t remap(nowdb_file_t *file) {
	nowdb_err_t err;

	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_mapped) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "file descriptor is in wrong state");
	}
	err = nowdb_file_umap(file);
	if (err != NOWDB_OK) return err;
	file->pos += file->bufsize;
	if (file->pos >= file->capacity) return nowdb_err_get(nowdb_err_eof,
	                                         FALSE, OBJECT, file->path);
	return nowdb_file_map(file);
}

/* ------------------------------------------------------------------------
 * Sync map to disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_sync(nowdb_file_t *file) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_mapped) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "file descriptor is in wrong state");
	}
	if (!file->dirty) return NOWDB_OK;
	if (msync(file->mptr, file->bufsize, MS_ASYNC) != 0) {
		return nowdb_err_get(nowdb_err_sync, TRUE, OBJECT,
		                                      file->path);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Rewind file (set back to initial position)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_rewind(nowdb_file_t *file) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_open) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "file descriptor is in wrong state");
	}
	file->pos = 0;

	if (file->ctrl & NOWDB_FILE_WRITER) return NOWDB_OK;

	if (lseek(file->fd, file->pos, SEEK_SET) < 0) {
		return nowdb_err_get(nowdb_err_seek, TRUE, OBJECT,
		                                      file->path);
	}
	file->off = 0;
	file->tmpsize = 0;
	memset(file->hdr, 0, file->hdrsize);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper function to load a block from a flat file
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t plainload(nowdb_file_t *file) {
	ssize_t x = read(file->fd, file->bptr, file->bufsize);
	if (x != file->bufsize) {
		if (x == 0) return nowdb_err_get(nowdb_err_eof,
		                    FALSE, OBJECT, file->path);
		return nowdb_err_get(nowdb_err_read,
		          TRUE, OBJECT, file->path);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Debug function to show contents of the tmp buffer
 * ------------------------------------------------------------------------
 */
static inline void showtmp(nowdb_file_t *file) {
	for(int i=0;i<file->hdr->size;i++) {
		fprintf(stderr, "%x ",
		       (unsigned char)(file->tmp+file->off)[i]);
	}
	fprintf(stderr, "\n");
}

/* ------------------------------------------------------------------------
 * Helper function to decompress a block using ZSTD
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t zstddecomp(nowdb_file_t *file) {
	size_t sz;
	if (file->ddict != NULL) {
		sz = ZSTD_decompress_usingDDict(file->dctx,
		                                file->bptr, file->bufsize,
		                                file->tmp+file->off,
		                                file->hdr->size,
		                                file->ddict);
	} else {
		sz = ZSTD_decompress(file->bptr, file->bufsize,
		                     file->tmp+file->off,
		                     file->hdr->size);
	}
	/*
	showtmp(file);
	*/
	if (ZSTD_isError(sz)) {
		return nowdb_err_get(nowdb_err_decomp, FALSE, OBJECT,
			               (char*)ZSTD_getErrorName(sz));
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: check header - worth decompressing the block?
 * -----------------------------------------------------------------------
 */
static inline char worthBlock(nowdb_file_t  *file,
                              nowdb_time_t  start,
                              nowdb_time_t    end) {

	if (start == NOWDB_TIME_DAWN &&
	    end   == NOWDB_TIME_DUSK) return 1;

	if (file->hdr->to == 0 && file->hdr->from == 0) return 1;

	/* 
	fprintf(stderr, "comparing %ld -- %ld and %ld -- %ld\n",
	                         start, end, file->hdr->from, to);
	*/
	if (end < file->hdr->from || start > file->hdr->to) return 0;

	return 1;
}

/* ------------------------------------------------------------------------
 * Helper function to load a block from a compressed file
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t compload(nowdb_file_t *file,
                                   nowdb_bool_t  hdr) {
	ssize_t x;

	/* if we are behind file end: set header.size 0 */
	if (file->pos >= file->size) {
		memset(file->hdr, 0, file->hdrsize);
		return NOWDB_OK;
	}

	/* if we have already something loaded
	 * preserve non-processed data */
	if (file->hdr->size != 0) {
		uint32_t sz = file->tmpsize - file->off;
		memcpy(file->tmp, file->tmp+file->off, sz);
		file->tmpsize = sz;
		x = read(file->fd, file->tmp+sz, file->bufsize-sz);
		if (x == 0) return nowdb_err_get(
		                   nowdb_err_bad_block,
		            FALSE, OBJECT, file->path);

	/* nothing loaded yet */
	} else {
		file->tmpsize = 0;
		x = read(file->fd, file->tmp, file->bufsize);
		if (x == 0) return nowdb_err_get(nowdb_err_eof,
		                    FALSE, OBJECT, file->path);
	}

	/* check for error */
	if (x < 0) return nowdb_err_get(nowdb_err_read,
	                     TRUE, OBJECT, file->path);

	/* remember file position and tmp size */
	file->pos += (uint32_t)x;
	file->tmpsize += (uint32_t)x;

	/* load header */
	if (hdr) {
		file->off = file->hdrsize;
		memcpy(file->hdr, file->tmp, file->hdrsize);
	} else {
		file->off = 0;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper function to move through a compressed file
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t compmove(nowdb_file_t *file,
                                   nowdb_time_t start,
                                   nowdb_time_t   end) {
	nowdb_err_t err = NOWDB_OK;

	/* no header loaded */
	if (file->hdr->size == 0) err = compload(file, TRUE);
	/* data missing from block for this header */
	if (file->tmpsize <
	    file->hdr->size + file->off) err = compload(file, FALSE);

	/* check for error */
	if (err != NOWDB_OK) return err;

	/* no header found: EOF */
	if (file->hdr->size == 0) return nowdb_err_get(nowdb_err_eof,
		                          FALSE, OBJECT, file->path);
	/* decompress using ZSTD */
	if (file->comp == NOWDB_COMP_ZSTD) {

		/* decompress */
		err = zstddecomp(file);
		if (err != NOWDB_OK) return err;

		/* move on to next header */
		file->off += file->hdr->size;

		/* next header incomplete */
		if (file->off + file->hdrsize >= file->tmpsize) {
			err = compload(file, FALSE);
			if (err != NOWDB_OK) return err;
			file->off = 0;
		}
		/* that was the last block */
		if (file->hdr->size == 0) return NOWDB_OK;

		/* load header */
		memcpy(file->hdr, file->tmp+file->off, file->hdrsize);
		file->off += file->hdrsize;
		return err;
	}
	/* unknown compression algorithm */
	return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
	                      "unknown compression algorithm");
}

/* ------------------------------------------------------------------------
 * Helper function to move through a flat file
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t plainmove(nowdb_file_t *file) {
	nowdb_err_t err = plainload(file);
	if (err != NOWDB_OK) return err;
	file->pos += file->bufsize;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Move file one block forward
 * ------------------------------------------------------------------------
 */
nowdb_err_t filemove(nowdb_file_t *file,
                     nowdb_time_t start,
                     nowdb_time_t   end) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_open &&
	    file->state != nowdb_file_state_mapped) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "file descriptor is in wrong state");
	}
	if (file->state == nowdb_file_state_mapped) return remap(file);
	if (file->comp == NOWDB_COMP_FLAT) return plainmove(file);
	return compmove(file, start, end);
}

/* ------------------------------------------------------------------------
 * Move file one block forward
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_move(nowdb_file_t *file) {
	return filemove(file, NOWDB_TIME_DAWN, NOWDB_TIME_DUSK);
}

/* ------------------------------------------------------------------------
 * Move file to next relevant block according to period
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_movePeriod(nowdb_file_t *file,
                                  nowdb_time_t start,
                                  nowdb_time_t   end) {
	return filemove(file, start, end);
}

/* ------------------------------------------------------------------------
 * Position file freely
 * TODO:
 * if the file is compressed, we should check whether we have
 * the compressed block already in the temporary buffer
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_position(nowdb_file_t *file, uint32_t pos) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_open) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "file descriptor is in wrong state");
	}
	if (lseek(file->fd, pos, SEEK_SET) < 0) {
		return nowdb_err_get(nowdb_err_seek, TRUE, OBJECT,
		                                      file->path);
	}
	file->pos = pos;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Load the current block into memory
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_loadBlock(nowdb_file_t *file) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_open) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "file descriptor is in wrong state");
	}
	if (file->comp == NOWDB_COMP_FLAT) return plainload(file);
	memset(file->hdr, 0, file->hdrsize);
	return compmove(file, NOWDB_TIME_DAWN, NOWDB_TIME_DUSK);
}

/* ------------------------------------------------------------------------
 * Load only the current header into memory
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_loadHeader(nowdb_file_t *file) {
	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_open) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "file descriptor is in wrong state");
	}
	if (file->comp == NOWDB_COMP_FLAT) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                "file has no headers");
	}
	memset(file->hdr, 0, file->hdrsize);
	if (read(file->fd, file->hdr, file->hdrsize) != file->hdrsize) {
		return nowdb_err_get(nowdb_err_read,
		           TRUE, OBJECT, file->path);
	}
	if (lseek(file->fd, -file->hdrsize, SEEK_CUR) < 0) {
		return nowdb_err_get(nowdb_err_seek,
		           TRUE, OBJECT, file->path);
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Is it worth to load this block?
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_worth(nowdb_file_t *file,
                             nowdb_time_t  start,
                             nowdb_time_t    end,
                             char         *worth) {
	nowdb_err_t err;

	if (file == NULL) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                            "file descriptor is NULL");
	}
	if (file->state != nowdb_file_state_open) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                 "file descriptor is in wrong state");
	}

	*worth = 1;

	if (file->comp == NOWDB_COMP_FLAT) return NOWDB_OK;

	err = nowdb_file_loadHeader(file);
	if (err != NOWDB_OK) return err;

	*worth = worthBlock(file, start, end);
	return NOWDB_OK;
}
