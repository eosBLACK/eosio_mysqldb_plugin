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
    actions_table(std::shared_ptr<connection_pool> pool);

    void drop();
    void create();
    int add(int parent_action_id, std::string receiver, chain::action action, chain::transaction_id_type transaction_id, uint32_t seq);
    uint32_t get_max_id();
    uint32_t get_max_id_raw();

    std::string get_abi_from_account(std::string account_name);
    void createInsertStatement_actions(uint32_t action_id, uint32_t parent_action_id, std::string receiver, chain::action action, chain::transaction_id_type transaction_id, uint32_t seq, std::string* stmt_actions, std::string* stmt_actions_account);
    void createInsertStatement_actions_raw(uint32_t action_id, uint32_t parent_action_id, std::string receiver, chain::action action, chain::transaction_id_type transaction_id, uint32_t seq, std::string* stmt_actions, std::string* stmt_actions_account);
    void createInsertStatement_actions_accounts();
    void executeActions(std::string sql_actions, std::string sql_actions_account);
    bool generate_actions_table(const uint32_t from_id);
private:
    std::shared_ptr<connection_pool> m_pool;

    void
    parse_actions(chain::action action, fc::variant variant);
};

} // namespace

#endif // ACTIONS_TABLE_H
