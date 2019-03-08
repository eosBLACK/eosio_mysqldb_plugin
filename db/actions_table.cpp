#include "actions_table.h"
#include "mysqlconn.h"

#include <boost/chrono.hpp>
#include <boost/format.hpp>

#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <eosio/chain_plugin/chain_plugin.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <iostream>
#include <future>

namespace eosio {

#define MAX_JSON_LENGTH 250000
#define MAX_ROW_SIZE    100

extern void post_query_str_to_queue(const std::string query_str);
extern const int64_t get_now_tick();

extern bool call_sp_proc_action_accs; 
extern bool use_action_reversible; 


//static const uint32_t ACTIONS_RAW_INSERT_COUNT      = 100 * 3; 
//static const uint32_t ACTIONS_ACCOUNT_INSERT_COUNT  = 120 * 3;

static const std::string ACTIONS_RAW_INSERT_STR =
    // NOTICE!! INSERT is test. Use REPLACE when commit or release
    //"INSERT IGNORE INTO actions(`id`,`parent`,`receiver`,`account`,`seq`,`created_at`,`action_name`,`hex_data`,`data`,`transaction_id`,`block_id`,`block_number`) VALUES ";
    "REPLACE INTO actions(`id`,`parent`,`receiver`,`account`,`seq`,`created_at`,`action_name`,`hex_data`,`data`,`transaction_id`,`block_id`,`block_number`,`auth`) VALUES ";

static const std::string ACTIONS_ACCOUNT_INSERT_STR = 
    "INSERT IGNORE INTO actions_accounts(`action_id`,`actor`,`permission`) VALUES ";
    //"REPLACE INTO actions_accounts_r(`action_id`,`transaction_id`,`block_number`,`actor`,`permission`) VALUES ";

static const std::string ACTIONS_ACCOUNT_R_INSERT_STR = 
    "REPLACE INTO actions_accounts_r(`action_id`,`transaction_id`,`block_number`,`actor`,`permission`) VALUES ";

actions_table::actions_table(std::shared_ptr<connection_pool> pool, uint32_t raw_bulk_max_count, uint32_t account_bulk_max_count):
    m_pool(pool), _raw_bulk_max_count(raw_bulk_max_count), _account_bulk_max_count(account_bulk_max_count)
{
    //std::cout << "call_sp_proc_action_accs: " << call_sp_proc_action_accs << std::endl; 

}

void actions_table::drop()
{
    
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    //con->print();
    assert(con);
    //ilog("actions_table : m_pool->get_connection succeeded");
    ilog(" wipe action tables");
    
    try {
        con->execute("drop table IF EXISTS actions_accounts_r;");
        con->execute("drop table IF EXISTS actions_accounts;");
        //ilog("drop table IF EXISTS actions_accounts;");
        // con->execute("drop table IF EXISTS stakes;");
        // ilog("drop table IF EXISTS stakes;");
        // con->execute("drop table IF EXISTS votes;");
        // ilog("drop table IF EXISTS votes;");
        // con->execute("drop table IF EXISTS tokens;");
        // ilog("drop table IF EXISTS tokens;");
        con->execute("drop table IF EXISTS actions;");
        //ilog("drop table IF EXISTS actions;");
        //con->execute("drop table IF EXISTS actions_raw;");
        //ilog("drop table IF EXISTS actions_raw;");
    }
    catch(std::exception& e){
        wlog(e.what());
    }

    m_pool->release_connection(*con);
    //ilog("actions_table : m_pool->release_connection succeeded");
}

void actions_table::create(const string engine)
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    
    try {
        ilog(" create action tables");

        std::ostringstream query; 

        query << boost::format(
            "CREATE TABLE IF NOT EXISTS `actions` ("
                "`id` BIGINT(64) UNSIGNED NOT NULL,"
                "`receiver` VARCHAR(12) NOT NULL,"
                "`account` VARCHAR(12) NOT NULL,"
                "`transaction_id` VARCHAR(64) NULL DEFAULT NULL,"
                "`seq` SMALLINT(6) NOT NULL,"
                "`parent` BIGINT(64) UNSIGNED NOT NULL,"
                "`action_name` VARCHAR(13) NOT NULL,"
                "`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "`block_id` VARCHAR(64) NULL DEFAULT NULL,"
                "`block_number` INT(10) UNSIGNED NOT NULL DEFAULT '0',"
                "`hex_data` LONGTEXT NULL,"
                "`data` JSON NULL DEFAULT NULL,"
                "`auth` JSON NULL DEFAULT NULL,"
                "PRIMARY KEY (`id`),"
                "INDEX `idx_actions_receiver` (`receiver`),"
                "INDEX `idx_actions_account` (`account`),"
                "INDEX `idx_actions_name` (`action_name`),"
                "INDEX `idx_actions_tx_id` (`transaction_id`),"
                "INDEX `idx_actions_created` (`created_at`),"
                "INDEX `idx_action_block_number` (`block_number`)"
            ")"
            "COLLATE='utf8mb4_general_ci'"
            "ENGINE=%1%;\n"
        ) % engine;

        if (use_action_reversible) {
            query << boost::format(
                "CREATE TABLE IF NOT EXISTS `actions_accounts_r` ("
                    "`action_id` BIGINT(64) NOT NULL,"
                    "`transaction_id` VARCHAR(64) NOT NULL DEFAULT '',"
                    "`block_number` INT(10) UNSIGNED NOT NULL,"
                    "`actor` VARCHAR(12) NOT NULL,"
                    "`permission` VARCHAR(13) NOT NULL,"
                    "PRIMARY KEY (`action_id`, `transaction_id`, `actor`),"
                    //"INDEX `idx_action_id` (`action_id`),"
                    //"INDEX `idx_transaction_id` (`transaction_id`),"
                    //"INDEX `idx_block_number` (`block_number`)"
                    "INDEX `idx_actor` (`actor`)"
                ")"
                "COMMENT='reversible correct'"
                "COLLATE='utf8mb4_general_ci'"
                "ENGINE=%1%;\n"
            ) % engine; 
        }


        query << boost::format(
            "CREATE TABLE IF NOT EXISTS `actions_accounts` ("
                //"`primary_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
                "`primary_id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,"
                "`action_id` BIGINT(64) NOT NULL,"
                "`actor` VARCHAR(12) NOT NULL,"
                "`permission` VARCHAR(13) NOT NULL,"
                "PRIMARY KEY (`primary_id`),"
                "INDEX `idx_action_id` (`action_id`),"
                "INDEX `idx_actor` (`actor`)"
            ")"
            "COMMENT='irreversible arrange'"
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


/*
uint64_t actions_table::get_max_id()
{
    uint64_t last_action_id = 0;

    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    
    shared_ptr<MysqlData> rset = con->open("SELECT IFNULL(MAX(id),0) + 1 as action_id FROM actions ");
    assert(rset);

    if(rset->is_valid()) {
        auto row = rset->next();
        if(row)
            last_action_id = (uint64_t)std::stoull(row->get_value(0));
    }
    
    m_pool->release_connection(*con);

    return last_action_id;
}
*/
/*
uint64_t actions_table::get_max_id_raw()
{
    uint64_t last_action_id = 0;

    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    
    shared_ptr<MysqlData> rset = con->open("SELECT IFNULL(MAX(id),0) as action_id FROM actions_raw ");
    assert(rset);

    if(rset->is_valid()) {
        auto row = rset->next();
        if(row)
            last_action_id = (uint64_t)std::stoull(row->get_value(0));
    }
    
    m_pool->release_connection(*con);

    return last_action_id;
}
*/

// temporary function. For action_account table only. 
void actions_table::add_act_acc_only(uint64_t action_id, chain::action action, chain::transaction_id_type transaction_id, uint32_t block_num) {
    // action_account 테이블 인서트
    const auto transaction_id_str = transaction_id.str();
    for (const auto& auth : action.authorization) {
        if (account_bulk_count > 0) {
            account_bulk_sql << ", ";
        }

        //account_bulk_sql << boost::format(" ('%1%','%2%','%3%','%4%','%5%')") 
        account_bulk_sql << boost::format(" ('%1%','%2%','%3%')") 
            % action_id 
            //% transaction_id_str
            //% block_num
            % auth.actor.to_string()
            % auth.permission.to_string();

        account_bulk_count++;
        if (!account_bulk_insert_tick)
            account_bulk_insert_tick = get_now_tick(); 
        if (account_bulk_count >= _account_bulk_max_count) 
            post_acc_query();

    }

}


void actions_table::add_action(uint64_t action_id, uint64_t parent_action_id, const std::string receiver, 
    chain::action action, chain::transaction_id_type transaction_id, 
    const std::string block_id, uint32_t block_num, uint32_t seq
)
 
{
    chain::abi_def abi;
    std::string abi_def_account;
    chain::abi_serializer abis;
    
    const auto transaction_id_str = transaction_id.str();
    const auto action_account_name = action.account.to_string();
    int max_field_size = 6500000;
    string escaped_json_str;
    string hex_str;
    string auth_json_str;

    try {
        try {   

            // if (!(action.name == chain::setabi::get_name())) return; 
             /*
            if (//action.account == chain::config::system_account_name &&
                action.name == chain::setabi::get_name()) 
            {
                auto setabi = action.data_as<chain::setabi>();
                try {
                    const chain::abi_def &abi_def = fc::raw::unpack<chain::abi_def>(setabi.abi);
                    const string json_str = fc::json::to_string(abi_def);
                    
                    shared_ptr <MysqlConnection> con = m_pool->get_connection();
                    hex_str = "";
                    escaped_json_str = con->escapeString(json_str);
                    m_pool->release_connection(*con);
                    
                } catch (fc::exception &e) {
                    ilog("Unable to convert action abi_def to json for ${n}", ("n", setabi.account.to_string()));
                }
            } else 
            // */
            {
                /*
                if ( action.name == N(transfer) )
                //if ( action.name != N(onblock) )
                {
                    std::cout 
                        << action.account.to_string() << ", "
                        << action.name.to_string()
                        << std::endl; 

                    std::cout 
                        << "data:\n" 
                        << fc::variant( action.data ).as_string()
                        << std::endl; 

                    fc::blob b_data;
                    b_data.data = action.data; 
                    
                    std::cout 
                        << "blob:\n" 
                        << fc::variant( b_data ).as_string()
                        << std::endl; 
                    
                }
                // */

                // get abi definition from chain

                ///* // Do not return empty value... 
                chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
                EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, ""  );
                auto& db = chain_plug->chain();
                abi = db.db().find<chain::account_object, chain::by_name>(action.account)->get_abi();    

                if (!abi.version.empty()) {
                    static const fc::microseconds abi_serializer_max_time(1000000); // 1 second
                    abis.set_abi(abi, abi_serializer_max_time);

                    auto abi_data = abis.binary_to_variant(abis.get_action_type(action.name), action.data, abi_serializer_max_time);
                    string json = fc::json::to_string(abi_data);
                    
                    shared_ptr<MysqlConnection> con = m_pool->get_connection();
                    hex_str = "";
                    escaped_json_str = con->escapeString(json);
                    m_pool->release_connection(*con);
                } else if (action.account == chain::config::system_account_name) {
                    abi = chain::eosio_contract_abi(abi); 
                } else {
                    hex_str = fc::variant( action.data ).as_string();
                    escaped_json_str = "";
                }

                // */

                /*
                chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
                EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, ""  );
                auto& db = chain_plug->chain();
                chain::abi_def abi_chain = db.db().find<chain::account_object, chain::by_name>(action.account)->get_abi();    

                if (!abi_chain.version.empty()) {
                    abi = abi_chain;
                } else if (action.account == chain::config::system_account_name) {
                    abi = chain::eosio_contract_abi(abi); 
                } else {
                    return;         // no ABI no party. Should we still store it?
                }

                static const fc::microseconds abi_serializer_max_time(1000000); // 1 second
                abis.set_abi(abi, abi_serializer_max_time);

                auto abi_data = abis.binary_to_variant(abis.get_action_type(action.name), action.data, abi_serializer_max_time);
                string json = fc::json::to_string(abi_data);
                
                shared_ptr<MysqlConnection> con = m_pool->get_connection();
                hex_str = "";
                escaped_json_str = con->escapeString(json);
                m_pool->release_connection(*con);
                // */
                

            }   

        } catch( std::exception& e ) {
            // ilog( "Unable to convert action.data to ABI: ${s}::${n}, std what: ${e}",
            //       ("s", action.account)( "n", action.name )( "e", e.what()));
            hex_str = fc::variant( action.data ).as_string();
            escaped_json_str = "";
        } catch (fc::exception& e) {
            // if (action.name != "onblock") { // eosio::onblock not in original eosio.system abi
            //     ilog( "Unable to convert action.data to ABI: ${s}::${n}, fc exception: ${e}",
            //         ("s", action.account)( "n", action.name )( "e", e.to_detail_string()));
            // }
            hex_str = fc::variant( action.data ).as_string();
            escaped_json_str = "";
        } catch( ... ) {
            // ilog( "Unable to convert action.data to ABI: ${s}::${n}, unknown exception",
            //       ("s", action.account)( "n", action.name ));
            hex_str = fc::variant( action.data ).as_string();
            escaped_json_str = "";
        }


        // auth info
        {
            fc::variants auth_ar; 
            for (const auto& auth : action.authorization) {
                // Use for replay chain. Insert to actions_account table.
                if (!call_sp_proc_action_accs) {
                    if (account_bulk_count > 0) {
                        account_bulk_sql << ", ";
                    }

                    //account_bulk_sql << boost::format(" ('%1%','%2%','%3%','%4%','%5%')") 
                    account_bulk_sql << boost::format(" ('%1%','%2%','%3%')") 
                        % action_id 
                        //% transaction_id_str
                        //% block_num
                        % auth.actor.to_string()
                        % auth.permission.to_string();

                    account_bulk_count++;
                    if (!account_bulk_insert_tick)
                        account_bulk_insert_tick = get_now_tick(); 
                    if (account_bulk_count >= _account_bulk_max_count) 
                        post_acc_query();

                } else 
                // Use for real working. Insert to actions_accounts_r table for history_api. 
                if (use_action_reversible) {
                    if (account_r_bulk_count > 0) {
                        account_r_bulk_sql << ", ";
                    }

                    account_r_bulk_sql << boost::format(" ('%1%','%2%','%3%','%4%','%5%')") 
                        % action_id 
                        % transaction_id_str
                        % block_num
                        % auth.actor.to_string()
                        % auth.permission.to_string();

                    account_r_bulk_count++;
                    if (!account_r_bulk_insert_tick)
                        account_r_bulk_insert_tick = get_now_tick(); 
                    if (account_r_bulk_count >= _account_bulk_max_count) 
                        post_acc_r_query();
                }


                // Generate JSON Information.
                fc::mutable_variant_object auth_info =
                    fc::mutable_variant_object()
                    ("actor", auth.actor.to_string())
                    ("permission", auth.permission.to_string())
                    ;

                auth_ar.push_back(auth_info); 
                // std::cout 
                //     << fc::json::to_string( auth_info ) 
                //     << std::endl; 

            }
            // std::cout 
            //     << fc::json::to_string( auth_ar ) 
            //     << std::endl; 
            shared_ptr<MysqlConnection> con = m_pool->get_connection();
            auth_json_str = con->escapeString( fc::json::to_string( auth_ar ) );
            m_pool->release_connection(*con);
        }

        //auth_json_str = ""; 
        // Insert to actions table
        {
            if (raw_bulk_count > 0) {
                raw_bulk_sql << ", ";
            }

            raw_bulk_sql << boost::format("('%1%','%2%','%3%','%4%','%5%',CURRENT_TIMESTAMP,'%6%',%7%,%8%,'%9%','%10%','%11%',%12%)")
                % action_id
                % parent_action_id
                % receiver
                % action_account_name
                % seq
                % action.name.to_string()
                % (escaped_json_str.length()? "NULL": "'" + hex_str + "'")
                % (escaped_json_str.length()? "'" + escaped_json_str + "'":"NULL")
                % transaction_id_str
                % block_id
                % block_num
                % (auth_json_str.length()? "'" + auth_json_str + "'":"NULL");



            raw_bulk_count++;
            if (!raw_bulk_insert_tick)
                raw_bulk_insert_tick = get_now_tick();
            if (raw_bulk_count >= _raw_bulk_max_count)
                post_raw_query();
           
        }



    } catch( fc::exception& e ) {
        wlog(e.what());
    }    

}
void actions_table::finalize() {
    post_raw_query();
    post_acc_query();
    post_acc_r_query();
}

void actions_table::tick(const int64_t tick) {
    if (raw_bulk_insert_tick && ((tick - raw_bulk_insert_tick) > 5000 )) {
        /*
        std::cout << "action table tick ans save " 
            << tick << ", " 
            << tick - raw_bulk_insert_tick 
            << std::endl; 
        //*/
        post_raw_query(); 
    }

    if (account_bulk_insert_tick && ((tick - account_bulk_insert_tick) > 5000 )) {
        /*
        std::cout << "action acc table tick ans save " 
            << tick << ", " 
            << tick - account_bulk_insert_tick 
            << std::endl; 
        //*/
        post_acc_query(); 
    }

    if (account_r_bulk_insert_tick && ((tick - account_r_bulk_insert_tick) > 5000 )) {
        post_acc_r_query(); 
    }

}

void actions_table::post_raw_query() {
    if (raw_bulk_count) {
        post_query_str_to_queue(
            ACTIONS_RAW_INSERT_STR +
            raw_bulk_sql.str()
        ); 

        raw_bulk_sql.str(""); raw_bulk_sql.clear(); 
        raw_bulk_count = 0; 
        raw_bulk_insert_tick = 0; 
    }

}

void actions_table::post_acc_query() {
    if (account_bulk_count) {
        // std::cout 
        //     << ACTIONS_ACCOUNT_INSERT_STR + account_bulk_sql.str()
        //     << std::endl; 

        post_query_str_to_queue(
            ACTIONS_ACCOUNT_INSERT_STR +
            account_bulk_sql.str()
        ); 

        account_bulk_sql.str(""); account_bulk_sql.clear(); 
        account_bulk_count = 0; 
        account_bulk_insert_tick = 0;
    }
}

void actions_table::post_acc_r_query() {
    if (account_r_bulk_count) {
        // std::cout 
        //     << ACTIONS_ACCOUNT_INSERT_STR + account_bulk_sql.str()
        //     << std::endl; 

        post_query_str_to_queue(
            ACTIONS_ACCOUNT_R_INSERT_STR +
            account_r_bulk_sql.str()
        ); 

        account_r_bulk_sql.str(""); account_r_bulk_sql.clear(); 
        account_r_bulk_count = 0; 
        account_r_bulk_insert_tick = 0;
    }
}



} // namespace
