/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Server-side error codes
 * ========================================================================
 */
#ifndef NOWDB_ERROCODE_DECL
#define NOWDB_ERROCODE_DECL

#define nowdb_err_no_mem           1
#define nowdb_err_invalid          2
#define nowdb_err_no_rsc           3
#define nowdb_err_busy             4
#define nowdb_err_too_big          5
#define nowdb_err_lock             6
#define nowdb_err_ulock            7
#define nowdb_err_eof              8
#define nowdb_err_not_supp         9
#define nowdb_err_bad_path        10
#define nowdb_err_bad_name        11
#define nowdb_err_map             12
#define nowdb_err_umap            13
#define nowdb_err_read            14
#define nowdb_err_write           15
#define nowdb_err_open            16
#define nowdb_err_close           17
#define nowdb_err_remove          18
#define nowdb_err_seek            19
#define nowdb_err_panic           20
#define nowdb_err_catalog         21
#define nowdb_err_time            22
#define nowdb_err_nosuch_scope    23
#define nowdb_err_nosuch_context  24
#define nowdb_err_nosuch_index    25
#define nowdb_err_key_not_found   26
#define nowdb_err_dup_key         27
#define nowdb_err_dup_name        28
#define nowdb_err_collision       29 
#define nowdb_err_sync            30
#define nowdb_err_thread          31
#define nowdb_err_sleep           32
#define nowdb_err_queue           33
#define nowdb_err_enqueue         34
#define nowdb_err_worker          35
#define nowdb_err_timeout         36
#define nowdb_err_reserve         37
#define nowdb_err_bad_block       38
#define nowdb_err_bad_filesize    39
#define nowdb_err_maxfiles        40
#define nowdb_err_move            41
#define nowdb_err_index           42
#define nowdb_err_version         43
#define nowdb_err_comp            44
#define nowdb_err_decomp          45
#define nowdb_err_compdict        46
#define nowdb_err_store           47
#define nowdb_err_context         48
#define nowdb_err_scope           49
#define nowdb_err_stat            50
#define nowdb_err_create          51
#define nowdb_err_drop            52
#define nowdb_err_magic           53
#define nowdb_err_loader          54
#define nowdb_err_trunc           55
#define nowdb_err_flush           56
#define nowdb_err_beet            57
#define nowdb_err_fun             58
#define nowdb_err_not_found       59
#define nowdb_err_parser          60
#define nowdb_err_sigwait         61
#define nowdb_err_signal          62
#define nowdb_err_sigset          63
#define nowdb_err_protocol        64
#define nowdb_err_socket          65
#define nowdb_err_bind            66
#define nowdb_err_listen          67
#define nowdb_err_accept          68
#define nowdb_err_server          69
#define nowdb_err_addr            70
#define nowdb_err_python          71
#define nowdb_err_lua             72
#define nowdb_err_unk_symbol      73
#define nowdb_err_usrerr          74
#define nowdb_err_unknown       9999
#endif

