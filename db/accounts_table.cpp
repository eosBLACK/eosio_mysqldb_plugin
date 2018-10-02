#include "accounts_table.h"
#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/chrono.hpp>
#include <boost/format.hpp>

#include <fc/log/logger.hpp>

#include <future>

#include "mysqlconn.h"

namespace eosio {

using chain::account_name;
using chain::action_name;
using chain::block_id_type;
using chain::permission_name;
using chain::transaction;
using chain::signed_transaction;
using chain::signed_block;
using chain::transaction_id_type;
using chain::packed_transaction;

accounts_table::accounts_table(std::shared_ptr<connection_pool> pool):
    m_pool(pool)
{

}

void accounts_table::drop()
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    ilog("accounts_table : m_pool->get_connection succeeded");

    try {
        con->execute("DROP TABLE IF EXISTS accounts_control;");
        con->execute("DROP TABLE IF EXISTS accounts_keys;");
        con->execute("DROP TABLE IF EXISTS accounts;");
    }
    catch(std::exception& e){
        wlog(e.what());
    } 

    m_pool->release_connection(*con);
}

void accounts_table::create()
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    try {
        con->execute("CREATE TABLE accounts("
                    "name VARCHAR(12) PRIMARY KEY,"
                    "creator VARCHAR(12) DEFAULT NULL,"
                    "abi JSON DEFAULT NULL,"
                    "trx_id VARCHAR(64),"
                    "created_at DATETIME DEFAULT NOW(),"
                    "updated_at DATETIME DEFAULT NOW()) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");

        con->execute("CREATE TABLE accounts_keys("
                "account VARCHAR(12),"
                "permission VARCHAR(12),"
                "public_key VARCHAR(53)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");
        
        con->execute("CREATE INDEX idx_accounts_keys ON accounts_keys (account,permission);");
        con->execute("CREATE INDEX idx_accounts_pubkey ON accounts_keys (public_key);");

        con->execute("CREATE TABLE accounts_control("
                "controlled_account VARCHAR(12), "
                "controlled_permission VARCHAR(10), "
                "controlling_account VARCHAR(12),"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                "PRIMARY KEY (controlled_account,controlled_permission)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");

        con->execute("CREATE INDEX idx_controlling_account ON accounts_control (controlling_account);");
    }
    catch(std::exception& e){
        wlog(e.what());
    }

    m_pool->release_connection(*con);
    
}

void accounts_table::add(string account_name)
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);

    std::ostringstream sql;
    try {
        sql << boost::format("INSERT INTO accounts (name) VALUES ('%1%');")
        % account_name;

        con->execute(sql.str());
    }
    catch(std::exception& e) {
        wlog(e.what());
    }    
    m_pool->release_connection(*con);
}

void accounts_table::add(string account_name,string creator,string trx_id)
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    std::ostringstream sql;
    try {
        sql << boost::format("INSERT INTO accounts (name,creator,trx_id) VALUES ('%1%','%2%','%3%');")
        % account_name
        % creator
        % trx_id;

        con->execute(sql.str());
    }
    catch(std::exception& e) {
        wlog(e.what());
    }    
    m_pool->release_connection(*con);
}

void accounts_table::add_account_control( const vector<chain::permission_level_weight>& controlling_accounts,
                                        const std::string& name, const permission_name& permission,
                                        const std::chrono::milliseconds& now)
{
    if(controlling_accounts.empty()) {
        return;
    }

    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    std::ostringstream sql;
    for( const auto& controlling_account : controlling_accounts ) {
        try {
            std::string conacc_name = controlling_account.permission.actor.to_string();
            sql << boost::format("INSERT INTO accounts_control (controlled_account,controlled_permission,controlling_account) VALUES ('%1%','%2%','%3%');")
            % name 
            % permission.to_string()
            % conacc_name;
            
            con->execute(sql.str());
        }
        catch(std::exception& e) {
            wlog(e.what());
        }
    }
    m_pool->release_connection(*con);
}

void accounts_table::remove_account_control( const std::string& name, const permission_name& permission )
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    std::ostringstream sql;
    try {
        sql << boost::format("DELETE FROM accounts_control WHERE controlled_account = '%1%' AND controlled_permission = '%2%' ")
        % name
        % permission.to_string();
                
        con->execute(sql.str());
    }
    catch(std::exception& e) {
        wlog(e.what());
    }
    m_pool->release_connection(*con);
}

void accounts_table::add_pub_keys(const vector<chain::key_weight>& keys, const std::string& name, const permission_name& permission)
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    std::ostringstream sql;

    for (const auto& key_weight : keys) {
        std::string key = static_cast<std::string>(key_weight.key);
        sql << boost::format("INSERT INTO accounts_keys(account, public_key, permission) VALUES ('%1%','%2%','%3%') ")
        % name
        % key 
        % permission.to_string();
        
        con->execute(sql.str());
    }
    m_pool->release_connection(*con);
}

void accounts_table::remove_pub_keys(const std::string& name, const permission_name& permission)
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    std::ostringstream sql;

    sql << boost::format("DELETE FROM accounts_keys WHERE account = '%1%' AND permission = '%2%';  ")
    % name
    % permission.to_string();

    con->execute(sql.str());

    m_pool->release_connection(*con);
}

void accounts_table::update_account(chain::action action, std::string trx_id)
{
    try {
        if (action.name == chain::setabi::get_name()) {
            shared_ptr<MysqlConnection> con = m_pool->get_connection();
            assert(con);
            std::ostringstream sql;

            chain::abi_def abi_setabi;
            chain::setabi action_data = action.data_as<chain::setabi>();
            chain::abi_serializer::to_abi(action_data.abi, abi_setabi);
            string abi_string = fc::json::to_string(abi_setabi);

            string escaped_abi_str = con->escapeString(abi_string);
            sql << boost::format("UPDATE accounts SET abi = '%1%', updated_at = NOW() WHERE name = '%2%';")
            % escaped_abi_str
            % action_data.account.to_string();
             
            con->execute(sql.str());
            
            m_pool->release_connection(*con);

        } else if (action.name == chain::newaccount::get_name()) {
            std::chrono::milliseconds now = std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()} );
            auto newacc = action.data_as<chain::newaccount>();
            
            add(newacc.name.to_string(), newacc.creator.to_string(), trx_id);
                        
            add_pub_keys(newacc.owner.keys, newacc.name.to_string(), chain::config::owner_name);            
            add_account_control(newacc.owner.accounts, newacc.name.to_string(), chain::config::owner_name, now);
            add_pub_keys(newacc.active.keys, newacc.name.to_string(), chain::config::active_name);            
            add_account_control(newacc.active.accounts, newacc.name.to_string(), chain::config::active_name, now);
        } else if( action.name == chain::updateauth::get_name() ) {
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()} );
            const auto update = action.data_as<chain::updateauth>();
            remove_pub_keys(update.account.to_string(), update.permission);
            remove_account_control(update.account.to_string(), update.permission);
            add_pub_keys(update.auth.keys, update.account.to_string(), update.permission);
            add_account_control(update.auth.accounts, update.account.to_string(), update.permission, now);
        } else if( action.name == chain::deleteauth::get_name() ) {
            const auto del = action.data_as<chain::deleteauth>();
            remove_pub_keys( del.account.to_string(), del.permission );
            remove_account_control(del.account.to_string(), del.permission);
        }
        
    }
    catch(std::exception& e) {
        wlog(e.what());
    }    
}

} // namespace
