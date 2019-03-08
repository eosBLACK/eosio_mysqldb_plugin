#ifndef ACCOUNTS_TABLE_H
#define ACCOUNTS_TABLE_H

#include <eosio/chain_plugin/chain_plugin.hpp>
#include <appbase/application.hpp>
#include <memory>

#include "connection_pool.h"

namespace eosio {

using std::string;
using chain::account_name;
using chain::action_name;
using chain::block_id_type;
using chain::permission_name;
using chain::transaction;
using chain::signed_transaction;
using chain::signed_block;
using chain::transaction_id_type;
using chain::packed_transaction;

class accounts_table
{
public:
    accounts_table(std::shared_ptr<connection_pool> pool, uint32_t bulk_max_count);

    void drop();
    void create(const string engine);
    void update_account(chain::action action,string trx_id);

    void add(string name);


    void finalize(); 
    void tick(const int64_t tick);
private:
    void add(string name,string creator,string trx_id);

    void post_query();

    /*
    void add_account_control(const chain::vector<chain::permission_level_weight>& controlling_accounts,
                            const std::string& name, const permission_name& permission,
                            const std::chrono::milliseconds& now);
    void remove_account_control( const std::string& name, const permission_name& permission );
    void add_pub_keys(const vector<chain::key_weight>& keys, const std::string& name, const permission_name& permission);
    void remove_pub_keys(const std::string& name, const permission_name& permission);
    */
private:
    std::shared_ptr<connection_pool> m_pool;
    uint32_t _bulk_max_count;

    uint32_t bulk_count = 0;
    int64_t bulk_insert_tick = 0;
    std::ostringstream bulk_sql;

};

} // namespace

#endif // ACCOUNTS_TABLE_H

