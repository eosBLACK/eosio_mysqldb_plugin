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

actions_table::actions_table(std::shared_ptr<connection_pool> pool):
    m_pool(pool)
{

}

void actions_table::drop()
{
    
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    con->print();
    assert(con);
    ilog("actions_table : m_pool->get_connection succeeded");
    
    try {
        con->execute("drop table IF EXISTS actions_accounts;");
        ilog("drop table IF EXISTS actions_accounts;");
        con->execute("drop table IF EXISTS stakes;");
        ilog("drop table IF EXISTS stakes;");
        con->execute("drop table IF EXISTS votes;");
        ilog("drop table IF EXISTS votes;");
        con->execute("drop table IF EXISTS tokens;");
        ilog("drop table IF EXISTS tokens;");
        con->execute("drop table IF EXISTS actions;");
        ilog("drop table IF EXISTS actions;");
        con->execute("drop table IF EXISTS actions_raw;");
        ilog("drop table IF EXISTS actions_raw;");
    }
    catch(std::exception& e){
        wlog(e.what());
    }

    m_pool->release_connection(*con);
    ilog("actions_table : m_pool->release_connection succeeded");
}

void actions_table::create()
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    
    try {
        con->execute("CREATE TABLE IF NOT EXISTS actions_raw("
                "id BIGINT(64) NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                "receiver VARCHAR(12),"
                "account VARCHAR(12),"
                "transaction_id VARCHAR(64),"
                "seq SMALLINT,"
                "parent BIGINT(64) DEFAULT NULL,"
                "action_name VARCHAR(12),"
                "created_at DATETIME DEFAULT NOW(),"
                "data LONGTEXT "
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");

        con->execute("CREATE INDEX idx_actions_raw_account ON actions_raw (account);");
        con->execute("CREATE INDEX idx_actions_raw_tx_id ON actions_raw (transaction_id);");
        con->execute("CREATE INDEX idx_actions_raw_created ON actions_raw (created_at);");

        con->execute("CREATE TABLE IF NOT EXISTS actions("
                "id BIGINT(64) NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                "receiver VARCHAR(12),"
                "account VARCHAR(12),"
                "transaction_id VARCHAR(64),"
                "seq SMALLINT,"
                "parent BIGINT(64) DEFAULT NULL,"
                "action_name VARCHAR(12),"
                "created_at DATETIME DEFAULT NOW(),"
                "hex_data LONGTEXT, "
                "data JSON "
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");

        con->execute("CREATE INDEX idx_actions_account ON actions (account,receiver,action_name);");
        con->execute("CREATE INDEX idx_actions_tx_id ON actions (transaction_id);");
        con->execute("CREATE INDEX idx_actions_created ON actions (created_at);");

        con->execute("CREATE TABLE IF NOT EXISTS actions_accounts("
                "actor VARCHAR(12),"
                "permission VARCHAR(12),"
                "action_id BIGINT(64) NOT NULL "
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");

        con->execute("CREATE INDEX idx_actions_actor ON actions_accounts (actor);");
        con->execute("CREATE INDEX idx_actions_action_id ON actions_accounts (action_id);");

        con->execute("CREATE TABLE IF NOT EXISTS tokens("
                "account VARCHAR(13),"
                "symbol VARCHAR(10),"
                "contract_owner VARCHAR(13),"
                "PRIMARY KEY (account,symbol,contract_owner) "
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");
        con->execute("CREATE INDEX idx_tokens_account ON tokens (account);");

        // con->execute("CREATE TABLE votes("
        //         "account VARCHAR(13) PRIMARY KEY,"
        //         "votes JSON"
        //         ", UNIQUE KEY account (account)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");

        // con->execute("CREATE TABLE stakes("
        //         "account VARCHAR(13) PRIMARY KEY,"
        //         "cpu REAL(14,4),"
        //         "net REAL(14,4) "
        //         ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");
    }
    catch(std::exception& e){
        wlog(e.what());
    }

    m_pool->release_connection(*con);
}

uint32_t actions_table::get_max_id()
{
    uint32_t last_action_id = 0;

    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    
    shared_ptr<MysqlData> rset = con->open("SELECT IFNULL(MAX(id),0) + 1 as action_id FROM actions ");
    assert(rset);

    if(rset->is_valid()) {
        auto row = rset->next();
        if(row)
            last_action_id = (uint32_t)std::stoull(row->get_value(0));
    }
    
    m_pool->release_connection(*con);

    return last_action_id;
}

uint32_t actions_table::get_max_id_raw()
{
    uint32_t last_action_id = 0;

    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    
    shared_ptr<MysqlData> rset = con->open("SELECT IFNULL(MAX(id),0) as action_id FROM actions_raw ");
    assert(rset);

    if(rset->is_valid()) {
        auto row = rset->next();
        if(row)
            last_action_id = (uint32_t)std::stoull(row->get_value(0));
    }
    
    m_pool->release_connection(*con);

    return last_action_id;
}

std::string actions_table::get_abi_from_account(std::string account_name)
{
    std::string abi_def_account;

    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    
    std::ostringstream sql;
    sql << boost::format("SELECT abi as `abi` FROM accounts WHERE `name` = '%1%'; ")
    % account_name;
    
    shared_ptr<MysqlData> rset = con->open(sql.str());

    if(rset->is_valid()) {
        auto row = rset->next();
        if(row)
            abi_def_account = row->get_value(0);
    }
    // abi_def_account.append("\0");
        
    m_pool->release_connection(*con);

    return abi_def_account;
}

bool actions_table::generate_actions_table(const uint32_t from_id)
{
    chain::abi_def abi;
    std::string abi_def_account;
    chain::abi_serializer abis;
    string escaped_json_str;
    
    int row_count = from_id;
    string r_hex_data;

    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);

    try {
        std::ostringstream s_values;
        std::string sql = "SELECT id, parent, receiver, account, seq, DATE_FORMAT(created_at,'%Y-%m-%d %H:%i:%s') as created_at, action_name, data, transaction_id "
                        "FROM actions_raw ";
        s_values << boost::format(" WHERE id > %1% LIMIT %2%; ")
        % from_id
        % MAX_ROW_SIZE;
        sql.append(s_values.str());

        shared_ptr<MysqlData> rset = con->open(sql);
        
        if(rset){
            std::string insert_sql = "INSERT INTO actions (id,parent,receiver,account,seq,created_at,action_name,hex_data,data,transaction_id) VALUES ";
            string escaped_json_str;
            int cnt = 0;

            auto row = rset->next();
            while(row)
            {
                auto r_id = row->get_value(0);
                auto r_parent = row->get_value(1);
                auto r_receiver = row->get_value(2);
                auto r_account = row->get_value(3);
                auto r_seq = row->get_value(4);
                auto r_created_at = row->get_value(5);
                auto r_action_name = row->get_value(6);
                auto r_data = row->get_value(7);
                auto r_transaction_id = row->get_value(8);
                
                chain::action_name i_action_name = chain::string_to_name(r_action_name.c_str());

                try {
                    if(i_action_name != chain::setabi::get_name()) {                       
                        // get abi definition from chain
                        chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
                        EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, ""  );
                        auto& db = chain_plug->chain();
                        abi = db.db().find<chain::account_object, chain::by_name>(chain::string_to_name(r_account.c_str()))->get_abi();

                        if(!abi.version.empty()) {

                        } else if (r_account == chain::name( chain::config::system_account_name ).to_string()) {
                            abi = chain::eosio_contract_abi(abi);
                        } else {
                            return true;
                        }
                        static const fc::microseconds abi_serializer_max_time(1000000); // 1 second
                        abis.set_abi(abi, abi_serializer_max_time);      

                        const std::vector<char> v_data = fc::variant(r_data).as_blob().data;
                        auto abi_data = abis.binary_to_variant(abis.get_action_type(i_action_name), v_data, abi_serializer_max_time);
                        string json = fc::json::to_string(abi_data);

                        shared_ptr<MysqlConnection> con = m_pool->get_connection();
                        escaped_json_str = con->escapeString(json);
                        m_pool->release_connection(*con);                    
                        r_hex_data = "";
                    } else if(i_action_name == chain::setabi::get_name()) {
                        shared_ptr<MysqlConnection> con = m_pool->get_connection();
                        escaped_json_str = con->escapeString(r_data);
                        m_pool->release_connection(*con);
                        r_hex_data = "";
                    } else {
                        r_hex_data = r_data;
                        escaped_json_str = "";
                    }
                } catch( std::exception& e ) {
                    ilog( "Unable to convert action.data to ABI: ${s}::${n}, std what: ${e}",
                          ("s", r_account)( "n", r_action_name )( "e", e.what()));
                    r_hex_data = r_data;
                    escaped_json_str = "";
                } catch (fc::exception& e) {
                    if (i_action_name != N(onblock)) { // eosio::onblock not in original eosio.system abi
                        ilog( "Unable to convert action.data to ABI: ${s}::${n}, fc exception: ${e}",
                              ("s", r_account)( "n", r_action_name )( "e", e.to_detail_string()));
                    }
                    r_hex_data = r_data;
                    escaped_json_str = "";
                } catch( ... ) {
                    ilog( "Unable to convert action.data to ABI: ${s}::${n}, unknown exception",
                          ("s", r_account)( "n", r_action_name ));

                    r_hex_data = r_data;
                    escaped_json_str = "";
                }
                
                s_values.str("");s_values.clear();
                if(cnt > 0)
                    insert_sql.append(",");

                if(escaped_json_str == "") {
                    s_values << boost::format("('%1%', '%2%', '%3%', '%4%', '%5%', '%6%', '%7%', '%8%', NULL, '%9%')")
                        % r_id
                        % r_parent
                        % r_receiver
                        % r_account
                        % r_seq
                        % r_created_at
                        % chain::name(i_action_name).to_string()
                        % r_hex_data
                        % r_transaction_id;
                } else {
                    s_values << boost::format("('%1%', '%2%', '%3%', '%4%', '%5%', '%6%', '%7%', '%8%', '%9%', '%10%')")
                        % r_id
                        % r_parent
                        % r_receiver
                        % r_account
                        % r_seq
                        % r_created_at
                        % chain::name(i_action_name).to_string()
                        % r_hex_data
                        % escaped_json_str
                        % r_transaction_id;
                }
                insert_sql.append(s_values.str());
                cnt++;

                row = rset->next();
            }
            if(cnt > 0) {

                con->execute(insert_sql);
            }
        }
    } catch (fc::exception& e) {
        ilog("generate_actions_table failed fc exception: ${e}",( "e", e.to_detail_string()));
    } catch (...) {
        ilog(con->lastError());
    }

    m_pool->release_connection(*con);
    return true;
}

void actions_table::createInsertStatement_actions_raw(uint32_t action_id, uint32_t parent_action_id, std::string receiver, chain::action action, chain::transaction_id_type transaction_id, uint32_t seq, 
                                                         std::string* stmt_actions, std::string* stmt_actions_account)
{
    chain::abi_def abi;
    std::string abi_def_account;
    chain::abi_serializer abis;
    
    const auto transaction_id_str = transaction_id.str();
    string m_account_name = action.account.to_string();
    int max_field_size = 6500000;
    string escaped_json_str;

    try {
        try {
            if (action.account == chain::config::system_account_name) {
                if (action.name == chain::setabi::get_name()) {
                    auto setabi = action.data_as<chain::setabi>();
                    try {
                        const chain::abi_def &abi_def = fc::raw::unpack<chain::abi_def>(setabi.abi);
                        const string json_str = fc::json::to_string(abi_def);
                        
                        shared_ptr <MysqlConnection> con = m_pool->get_connection();
                        escaped_json_str = con->escapeString(json_str);
                        m_pool->release_connection(*con);
                     
                    } catch (fc::exception &e) {
                        ilog("Unable to convert action abi_def to json for ${n}", ("n", setabi.account.to_string()));
                    }   
                }else{
                    fc::blob b_data;
                    b_data.data = action.data;
                    escaped_json_str = fc::variant( b_data ).as_string();                   
                }
            } else {
                fc::blob b_data;
                b_data.data = action.data;
                escaped_json_str = fc::variant( b_data ).as_string();
            }
        } catch( std::exception& e ) {
            ilog( "Unable to convert action.data to ABI: ${s}::${n}, std what: ${e}",
                  ("s", action.account)( "n", action.name )( "e", e.what()));
            escaped_json_str = fc::variant( action.data ).as_string();
        } catch (fc::exception& e) {
            if (action.name != "onblock") { // eosio::onblock not in original eosio.system abi
                ilog( "Unable to convert action.data to ABI: ${s}::${n}, fc exception: ${e}",
                      ("s", action.account)( "n", action.name )( "e", e.to_detail_string()));
            }

            escaped_json_str = fc::variant( action.data ).as_string();
        } catch( ... ) {
            ilog( "Unable to convert action.data to ABI: ${s}::${n}, unknown exception",
                  ("s", action.account)( "n", action.name ));

            escaped_json_str = fc::variant( action.data ).as_string();
        }


        std::ostringstream s_values;

        if(parent_action_id == 0 && seq == 0) {
            stmt_actions->append("INSERT INTO actions_raw(id, parent, receiver, account, seq, created_at, action_name, data, transaction_id) VALUES");
            s_values << boost::format("('%1%', '%2%', '%3%', '%4%', '%5%', CURRENT_TIMESTAMP, '%6%', '%7%', '%8%')")
                % action_id
                % parent_action_id
                % receiver
                % m_account_name
                % seq
                % action.name.to_string()
                % escaped_json_str
                % transaction_id_str;
            stmt_actions->append(s_values.str());
        } else {
            s_values << boost::format(",('%1%', '%2%', '%3%', '%4%', '%5%', CURRENT_TIMESTAMP, '%6%', '%7%', '%8%')")
                % action_id
                % parent_action_id
                % receiver
                % m_account_name
                % seq
                % action.name.to_string()
                % escaped_json_str
                % transaction_id_str;
            stmt_actions->append(s_values.str());
        }

        s_values.str("");s_values.clear();
        stmt_actions_account->append("INSERT INTO actions_accounts(action_id, actor, permission) VALUES ");
        int cnt=0;
        for (const auto& auth : action.authorization) {
            string m_actor_name = auth.actor.to_string();
            if(cnt>0)
                s_values << boost::format(",('%1%','%2%','%3%')") % action_id % m_actor_name % auth.permission.to_string();
            else
                s_values << boost::format("('%1%','%2%','%3%')") % action_id % m_actor_name % auth.permission.to_string();
            stmt_actions_account->append(s_values.str());
            cnt++;
        }
        return;
    } catch( fc::exception& e ) {
        wlog(e.what());
    }

}

void actions_table::executeActions(std::string sql_actions, std::string sql_actions_account)
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    
    try {
        if(sql_actions.length() > 0)
            con->execute(sql_actions);

        if(sql_actions_account.length() > 0)
            con->execute(sql_actions_account);

    }
    catch ( ... ) {
        ilog("sql_actions = ${s}",("s",sql_actions));
    }
    
    m_pool->release_connection(*con);
}

void actions_table::parse_actions(chain::action action, fc::variant abi_data)
{
    try {
        if (action.name == N(issue) || action.name == N(transfer)) {
            auto to_name = abi_data["to"].as<chain::name>().to_string();
            auto asset_quantity = abi_data["quantity"].as<chain::asset>();
            auto contract_owner = action.account.to_string();
            int exist = 0;

            shared_ptr<MysqlConnection> con = m_pool->get_connection();
            assert(con);
            std::ostringstream sql;

            // ignore if exists
            sql << boost::format("INSERT IGNORE INTO tokens(account, symbol, contract_owner) VALUES ('%1%', '%2%', '%3%')")
            % to_name.c_str()
            % asset_quantity.get_symbol().name()
            % contract_owner.c_str();
            
            con->execute(sql.str());

            m_pool->release_connection(*con);
        }

        if (action.account != chain::name(chain::config::system_account_name)) {
            return;
        }

        // if (action.name == N(voteproducer)) {
        //     auto voter = abi_data["voter"].as<chain::name>().to_string();
        //     string votes = fc::json::to_string(abi_data["producers"]);

        //     zdbcpp::Connection con = m_pool->get_connection();
        //     assert(con);

        //     zdbcpp::PreparedStatement pre = con.prepareStatement("INSERT INTO votes(account, votes) VALUES (?, ?) ON DUPLICATE KEY UPDATE votes = ?; ");
        //     pre.setString(1,voter.c_str());
        //     pre.setString(2,votes.c_str());
        //     pre.setString(3,votes.c_str());

        //     pre.execute();
        // }


        // if (action.name == N(delegatebw)) {
        //     auto account = abi_data["receiver"].as<chain::name>().to_string();
        //     auto cpu = abi_data["stake_cpu_quantity"].as<chain::asset>();
        //     auto net = abi_data["stake_net_quantity"].as<chain::asset>();

        //     zdbcpp::Connection con = m_pool->get_connection();
        //     assert(con);

        //     zdbcpp::PreparedStatement pre = con.prepareStatement("INSERT INTO stakes(account, cpu, net) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE cpu = ?, net = ? ;");
        //     pre.setString(1,account.c_str());
        //     pre.setDouble(2,cpu.to_real());
        //     pre.setDouble(3,net.to_real());
        //     pre.setDouble(4,cpu.to_real());
        //     pre.setDouble(5,net.to_real());
            
        //     pre.execute();
        // }
        
    } catch(std::exception& e){
        wlog(e.what());
    }

    // m_pool->release_connection(con);
}

} // namespace
