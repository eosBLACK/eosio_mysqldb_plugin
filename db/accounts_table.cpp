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
//#include "eosio/mysql_db_plugin/mysql_db_plugin.hpp"

namespace eosio {

extern void post_query_str_to_queue(const std::string query_str);
extern const int64_t get_now_tick();


static const std::string ACCOUNT_INSERT_HEAD_STR =
    "INSERT INTO accounts (`name`,`creator`,`trx_id`) VALUES ";

static const std::string ACCOUNT_INSERT_TAIL_STR =
    " ON DUPLICATE KEY UPDATE"
        "`creator` = VALUES(`creator`), "
        "`trx_id`  = VALUES(`trx_id`);";



using chain::account_name;
using chain::action_name;
using chain::block_id_type;
using chain::permission_name;
using chain::transaction;
using chain::signed_transaction;
using chain::signed_block;
using chain::transaction_id_type;
using chain::packed_transaction;

accounts_table::accounts_table(std::shared_ptr<connection_pool> pool, uint32_t bulk_max_count):
    m_pool(pool), _bulk_max_count(bulk_max_count)
{

}

void accounts_table::drop()
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    //ilog("accounts_table : m_pool->get_connection succeeded");
    ilog(" wipe account tables");

    try {
        //con->execute("DROP TABLE IF EXISTS accounts_control;");
        //con->execute("DROP TABLE IF EXISTS accounts_keys;");
        con->execute("DROP TABLE IF EXISTS `accounts`;");
        con->execute("DROP TABLE IF EXISTS `accounts_seq`;");
        
    }
    catch(std::exception& e){
        wlog(e.what());
    } 

    m_pool->release_connection(*con);
}

void accounts_table::create(const string engine)
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    try {
        ilog(" create account tables");

        std::ostringstream query; 

        query << boost::format(
            "CREATE TABLE IF NOT EXISTS `accounts` ("
                "`name` VARCHAR(12) NOT NULL,"
                "`creator` VARCHAR(12) NULL DEFAULT NULL,"
                //"`abi` JSON NULL DEFAULT NULL,"
                "`trx_id` VARCHAR(64) NULL DEFAULT NULL,"
                "`action_seq` BIGINT(64) NOT NULL DEFAULT '0',"
                "`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "PRIMARY KEY (`name`),"
                "INDEX `idx_actions_created` (`created_at`)"
            ")"
            "COLLATE='utf8mb4_general_ci'"
            "ENGINE=%1%;\n"
        ) % engine;

        query << boost::format(
            "CREATE TABLE IF NOT EXISTS `accounts_seq` ("
                "`name` VARCHAR(12) NOT NULL,"
                "`action_seq` BIGINT(64) NOT NULL DEFAULT '0',"
                "PRIMARY KEY (`name`)"
            ")"
            "COLLATE='utf8mb4_general_ci'"
            "ENGINE=%1%;\n"
        ) % engine;


        con->execute( query.str() ); 


    }
    catch(std::exception& e){
        wlog(e.what());
    }

    m_pool->release_connection(*con);
    
}


void accounts_table::add(string account_name)
{

    if (bulk_count > 0) {
        bulk_sql << ", ";
    }

    bulk_sql << boost::format("('%1%',NULL,NULL)")
        % account_name;

    /*
    bulk_sql << boost::format("INSERT IGNORE INTO `accounts` (`name`) VALUES ('%1%');")
        % account_name;
    */
    bulk_count++; 
    if (!bulk_insert_tick)
        bulk_insert_tick = get_now_tick();
    //std::cout << "Account inc bulkcount " << bulk_insert_tick << std::endl; 

    if (bulk_count >= _bulk_max_count)
        post_query(); 

    /*
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
    */
}

void accounts_table::add(string account_name,string creator,string trx_id)
{
    if (bulk_count > 0) {
        bulk_sql << ", ";
    }

    bulk_sql << boost::format("('%1%','%2%','%3%')")
        % account_name
        % creator
        % trx_id;

    /*
    bulk_sql << boost::format("REPLACE INTO accounts (name,creator,trx_id) VALUES ('%1%','%2%','%3%');")
        % account_name
        % creator
        % trx_id;
    */
    bulk_count++; 
    if (!bulk_insert_tick)
        bulk_insert_tick = get_now_tick();

    //std::cout << "Account inc bulkcount " << bulk_insert_tick << std::endl; 
    if (bulk_count >= _bulk_max_count)
        post_query(); 

    /*
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
    */
}

void accounts_table::update_account(chain::action action, std::string trx_id)
{
    //return; 
    
    try {
        /*
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
        } else */

        if (action.name == chain::newaccount::get_name()) {
            std::chrono::milliseconds now = std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()} );
            auto newacc = action.data_as<chain::newaccount>();
            
            add(newacc.name.to_string(), newacc.creator.to_string(), trx_id);
            /*                        
            add_pub_keys(newacc.owner.keys, newacc.name.to_string(), chain::config::owner_name);            
            add_account_control(newacc.owner.accounts, newacc.name.to_string(), chain::config::owner_name, now);
            add_pub_keys(newacc.active.keys, newacc.name.to_string(), chain::config::active_name);            
            add_account_control(newacc.active.accounts, newacc.name.to_string(), chain::config::active_name, now);
            */
        } /*else if( action.name == chain::updateauth::get_name() ) {
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
        */
        
    }
    catch(std::exception& e) {
        wlog(e.what());
    }    
}

void accounts_table::finalize() {
    post_query(); 
}

void accounts_table::tick(const int64_t tick) {
    //std::cout << "Accounts table tick " << bulk_insert_tick << std::endl; 
    if (bulk_insert_tick && ((tick - bulk_insert_tick) > 5000 )) {
        /*
        std::cout << "Account table tick ans save " 
            << tick << ", " 
            << tick - bulk_insert_tick 
            << std::endl; 
        */
        post_query(); 
    }

}

void accounts_table::post_query() {
    if (bulk_count) {
        post_query_str_to_queue(
            ACCOUNT_INSERT_HEAD_STR +
            bulk_sql.str() +
            ACCOUNT_INSERT_TAIL_STR
        ); 

        bulk_sql.str(""); bulk_sql.clear(); 
        bulk_count = 0; 
        bulk_insert_tick = 0; 
    }

}

} // namespace
