#include <nowdb/store/store.h>

nowdb_store_t *mkStore(nowdb_path_t path);
nowdb_store_t *mkStoreBlock(nowdb_path_t path,
                            uint32_t recsz,
                            uint32_t block,
                            uint32_t large);
nowdb_bool_t createStore(nowdb_store_t *store);
nowdb_bool_t dropStore(nowdb_store_t *store);
nowdb_bool_t openStore(nowdb_store_t *store);
nowdb_bool_t closeStore(nowdb_store_t *store);
nowdb_store_t *bootstrap(nowdb_path_t path, uint32_t recsz);
nowdb_store_t *xBootstrap(nowdb_path_t path,
                          nowdb_comprsc_t compare,
                          nowdb_comp_t    compress,
                          uint32_t        tasknum,
                          uint32_t        recsz,
                          uint32_t        block,
                          uint32_t        large);

