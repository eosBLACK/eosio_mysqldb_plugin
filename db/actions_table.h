#ifndef ACTIONS_TABLE_H
#define ACTIONS_TABLE_H

#include <memory>

#include <fc/io/json.hpp>
#include <fc/variant.hpp>

#include <boost/chrono.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include <eosio/chain/block_state.hpp>
#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/abi_def.hpp>
#include <eosio/chain/asset.hpp>
#include <eosio/chain/abi_serializer.hpp>

#include "connection_pool.h"

namespace eosio {

using std::string;

class actions_table
{
public:
    actions_table(std::shared_ptr<connection_pool> pool, uint32_t raw_bulk_max_count, uint32_t account_bulk_max_count);

    void drop();
    void create(const string engine);

    //uint64_t get_max_id();
    //uint64_t get_max_id_raw();

    void add_action(uint64_t action_id, uint64_t parent_action_id, const std::string receiver, 
        chain::action action, chain::transaction_id_type transaction_id, 
        const std::string block_id,  uint32_t block_num, uint32_t seq
    );

    void add_act_acc_only(uint64_t action_id, chain::action action, chain::transaction_id_type transaction_id, uint32_t block_num);

    void finalize(); 
    void tick(const int64_t tick);
private:
    void post_raw_query();
    void post_acc_query();
    void post_acc_r_query();

    //void post_query(bool force=false);

    std::shared_ptr<connection_pool> m_pool;

    uint32_t _raw_bulk_max_count;
    uint32_t _account_bulk_max_count;

    uint32_t raw_bulk_count = 0;
    int64_t raw_bulk_insert_tick = 0;
    std::ostringstream raw_bulk_sql;

    uint32_t account_bulk_count = 0;
    int64_t account_bulk_insert_tick = 0;
    std::ostringstream account_bulk_sql;

    uint32_t account_r_bulk_count = 0;
    int64_t account_r_bulk_insert_tick = 0;
    std::ostringstream account_r_bulk_sql;

};

} // namespace

#endif // ACTIONS_TABLE_H
