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
    transactions_table(std::shared_ptr<connection_pool> pool);

    void drop();
    void create();
    void add(uint32_t block_id, chain::transaction_receipt receipt, chain::transaction transaction);
    void add_trace(chain::transaction_id_type id, int32_t cpu_usage_us, int32_t net_usage_words, int elapsed, bool scheduled);
    void createTransactionStatement(uint32_t block_id, chain::transaction transaction, std::string* stmt_transaction);
private:
    std::shared_ptr<connection_pool> m_pool;
};

} // namespace

#endif // TRANSACTIONS_TABLE_H
