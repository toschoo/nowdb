#include <nowdb/store/store.h>

nowdb_store_t *mkStore(nowdb_path_t path);
nowdb_bool_t createStore(nowdb_store_t *store);
nowdb_bool_t dropStore(nowdb_store_t *store);
nowdb_bool_t openStore(nowdb_store_t *store);
nowdb_bool_t closeStore(nowdb_store_t *store);
nowdb_store_t *bootstrap(nowdb_path_t path);

