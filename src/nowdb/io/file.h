/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * File Abstraction
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_file_decl
#define nowdb_file_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/types/time.h>
#include <nowdb/io/dir.h>

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>

#include <zstd.h>

typedef uint32_t nowdb_comp_t;
typedef uint32_t nowdb_encp_t;

#define NOWDB_HDR_SIZE 32

#define NOWDB_COMP_FLAT 0
#define NOWDB_COMP_ZSTD 1
#define NOWDB_COMP_LZ4  2

#define NOWDB_ZSTD_LEVEL 3

#define NOWDB_ENCP_NONE 0

#define NOWDB_FILE_WRITER 1
#define NOWDB_FILE_SPARE  2 
#define NOWDB_FILE_READER 4
#define NOWDB_FILE_SORT   8

#define NOWDB_FILE_MAPSIZE 8388608
#define NOWDB_FILE_MAXSIZE 1073741824

/* ------------------------------------------------------------------------
 * File State
 * ------------------------------------------------------------------------
 */
typedef enum {
	nowdb_file_state_closed = 0, /* file not yet opened         */
	nowdb_file_state_open   = 1, /* file opened                 */
	nowdb_file_state_mapped = 2  /* file mapped to memory       */
} nowdb_file_state_t;

/* ------------------------------------------------------------------------
 * Block Header for encrypted files
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_bitmap64_t set[2]; /* set if not deleted              */
	uint32_t           size; /* compressed size                 */
	uint32_t       reserve4;
	uint64_t       reserve8;
} nowdb_block_hdr_t;

/* ------------------------------------------------------------------------
 * NoWDB File Descriptor
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_fileid_t     id; /* unique identifier                  */
	uint32_t        order; /* order in ordered stores            */
	nowdb_path_t     path; /* full path (relative to db path)    */
	uint32_t         size; /* used size of the file              */
	uint32_t     capacity; /* overall capcacity                  */
	uint32_t    blocksize; /* size of stored blocks              */
	uint32_t   recordsize; /* size of one record                 */
	uint32_t        state; /* current state of the descriptor    */
	uint32_t          pos; /* current position                   */
	nowdb_bool_t    dirty; /* map was written                    */
	int                fd; /* os file descriptor                 */
	char            *mptr; /* pointer for mapping                */
	char            *bptr; /* pointer for buffered reading       */
	char             *tmp; /* temporary buffer for decompression */
	int               off; /* current position within tmp/map    */
	void           *cdict; /* compression dictionary             */
	void           *ddict; /* decompression dictionary           */
	ZSTD_CCtx       *cctx; /* ZSTD compression context           */
	ZSTD_DCtx       *dctx; /* ZSTD decompression context         */
	uint32_t      bufsize; /* map or buf size                    */
	uint32_t      tmpsize; /* actual buf size                    */
	nowdb_block_hdr_t hdr; /* current header                     */
	nowdb_bitmap8_t  ctrl; /* writer, reader, spare, sorted      */
	nowdb_comp_t     comp; /* compression algorithm              */
	nowdb_encp_t     encp; /* encryption algorithm               */
	nowdb_time_t    grain; /* time granularity used for sorting  */
	nowdb_time_t   oldest; /* oldest timestamp                   */
	nowdb_time_t   newest; /* newest timestamp                   */
} nowdb_file_t;

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
                           nowdb_bitmap8_t ctrl,
                           nowdb_comp_t    comp,
                           nowdb_encp_t    encp,
                           nowdb_time_t   grain,
                           nowdb_time_t  oldest,
                           nowdb_time_t  newest);

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
                            nowdb_bitmap8_t ctrl,
                            nowdb_comp_t    comp,
                            nowdb_encp_t    encp,
                            nowdb_time_t   grain,
                            nowdb_time_t  oldest,
                            nowdb_time_t  newest);

/* ------------------------------------------------------------------------
 * Change file from writer (mapped) to reader (buffered)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_makeReader(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Change file from reader (buffered) to writer (mapped)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_makeWriter(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Change file from anything to spare (mapped)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_makeSpare(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Destroy file descriptor
 * ------------------------------------------------------------------------
 */
void nowdb_file_destroy(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Copy file descriptor
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_copy(nowdb_file_t *source, nowdb_file_t *target);

/* ------------------------------------------------------------------------
 * Update file descriptor
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_update(nowdb_file_t *source, nowdb_file_t *target);

/* ------------------------------------------------------------------------
 * Create file physically on disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_create(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Remove file physically from disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_remove(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Erase file content 
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_erase(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Write a block to a buffered file ("reader").
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_writeBuf(nowdb_file_t *file,
                         char *buf, uint32_t size);

/* ------------------------------------------------------------------------
 * Open file
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_open(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Close file
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_close(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Map file to memory
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_map(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Map file at given position
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_mapAt(nowdb_file_t *file, uint32_t pos);

/* ------------------------------------------------------------------------
 * Unmap file from memory
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_umap(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Sync map to disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_sync(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Rewind file (set back to initial position)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_rewind(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Move file one block forward
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_move(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Position file freely
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_position(nowdb_file_t *file, uint32_t pos);

/* ------------------------------------------------------------------------
 * Load the current block into memory
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_loadBlock(nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Load only the current header into memory
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_file_loadHeader(nowdb_file_t *file);

#endif
