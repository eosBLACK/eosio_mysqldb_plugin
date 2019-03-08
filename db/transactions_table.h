#ifndef TRANSACTIONS_TABLE_H
#define TRANSACTIONS_TABLE_H

#include <memory>
#include <chrono>

#include <fc/io/json.hpp>
#include <fc/variant.hpp>

#include <eosio/chain/transaction_metadata.hpp>
#include "connection_pool.h"

namespace eosio {

class transactions_table
{
public:
    transactions_table(std::shared_ptr<connection_pool> pool, uint32_t trace_bulk_max_count, uint32_t trx_bulk_max_count);

    void drop();
    void create(const string engine);

    void add_trx(uint32_t block_number, const chain::transaction& transaction);
    void add_trace(chain::transaction_id_type id, uint32_t block_number, int32_t cpu_usage_us, int32_t net_usage_words, int elapsed, int trx_status, bool scheduled);

    void finalize(); 
    void tick(const int64_t tick);
private:
    void post_trx_query();
    void post_trace_query();

    void post_query(bool force=false);

    std::shared_ptr<connection_pool> m_pool;

    uint32_t _trace_bulk_max_count;
    uint32_t _trx_bulk_max_count;

    uint32_t trace_bulk_count = 0;
    int64_t trace_bulk_insert_tick = 0;
    std::ostringstream trace_bulk_sql;

    uint32_t trx_bulk_count = 0;
    int64_t trx_bulk_insert_tick = 0;
    std::ostringstream trx_bulk_sql;
};

} // namespace

#endif // TRANSACTIONS_TABLE_H
