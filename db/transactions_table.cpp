#include "transactions_table.h"


#include <iostream>
#include <boost/format.hpp>

#include "mysqlconn.h"

namespace eosio {

transactions_table::transactions_table(std::shared_ptr<eosio::connection_pool> pool):
    m_pool(pool)
{

}

void transactions_table::drop()
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    ilog("transactions_table : m_pool->get_connection succeeded");

    try {
        con->execute("DROP TABLE IF EXISTS transactions;");
        con->execute("DROP TABLE IF EXISTS transaction_trace;");
    }
    catch(std::exception& e){
        wlog(e.what());
    }
    m_pool->release_connection(*con);
}

void transactions_table::create()
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    try {
        con->execute("CREATE TABLE IF NOT EXISTS transactions("
                "id VARCHAR(64) PRIMARY KEY,"
                "block_id BIGINT NOT NULL,"
                "ref_block_num BIGINT NOT NULL,"
                "ref_block_prefix BIGINT,"
                "expiration DATETIME DEFAULT NOW(),"
                "pending TINYINT(1),"
                "created_at DATETIME DEFAULT NOW(),"
                "num_actions INT DEFAULT 0,"
                "updated_at DATETIME DEFAULT NOW()) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");

        con->execute("CREATE INDEX transactions_block_id ON transactions (block_id);");

        con->execute("CREATE TABLE IF NOT EXISTS transaction_trace("
                "id VARCHAR(64) PRIMARY KEY,"
                "cpu_usage_us BIGINT,"
                "net_usage_words BIGINT,"
                "scheduled CHAR(1),"
                "elapsed INT,"
                "created_at DATETIME DEFAULT NOW(),"
                "updated_at DATETIME DEFAULT NOW()) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");

    }
    catch(std::exception& e){
        wlog(e.what());
    }
    m_pool->release_connection(*con);

}

void transactions_table::createTransactionStatement(uint32_t block_id, chain::transaction transaction, std::string* stmt_transaction)
{
    const auto transaction_id_str = transaction.id().str();
    const auto expiration = std::chrono::seconds{transaction.expiration.sec_since_epoch()}.count();
    
    std::ostringstream s_values;

    try {
        if(stmt_transaction->length()<1) {
            stmt_transaction->append("INSERT INTO transactions(id, block_id, ref_block_num, ref_block_prefix, expiration, pending, created_at, updated_at, num_actions) VALUES ");
            s_values << boost::format("('%1%', '%2%', '%3%', '%4%', FROM_UNIXTIME('%5%'), '0', FROM_UNIXTIME('%6%'), FROM_UNIXTIME('%7%'), '%8%')")
                        % transaction_id_str
                        % block_id
                        % transaction.ref_block_num
                        % transaction.ref_block_prefix
                        % expiration
                        % expiration
                        % expiration
                        % transaction.total_actions();

            
        } else {
            s_values << boost::format(",('%1%', '%2%', '%3%', '%4%', FROM_UNIXTIME('%5%'), '0', FROM_UNIXTIME('%6%'), FROM_UNIXTIME('%7%'), '%8%')")
                        % transaction_id_str
                        % block_id
                        % transaction.ref_block_num
                        % transaction.ref_block_prefix
                        % expiration
                        % expiration
                        % expiration
                        % transaction.total_actions();
        }
        stmt_transaction->append(s_values.str());
    }
    catch(std::exception& e){
        wlog(e.what());
    }
}

void transactions_table::add_trace(chain::transaction_id_type transaction_id, int32_t cpu_usage_us, int32_t net_usage_words, int elapsed, bool scheduled)
{
    const auto transaction_id_str = transaction_id.str();
    const auto created_at = std::chrono::seconds(std::time(NULL));

    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    std::ostringstream sql;

    try {
        sql << boost::format("INSERT INTO transaction_trace (id,cpu_usage_us,net_usage_words,elapsed,scheduled,created_at,updated_at) VALUES "
                                                            "('%1%', '%2%', '%3%', '%4%', '%5%', FROM_UNIXTIME('%6%'), FROM_UNIXTIME('%7%')) ")
        % transaction_id_str
        % cpu_usage_us
        % net_usage_words
        % elapsed
        % ((scheduled)?"1":"0")
        % created_at.count()
        % created_at.count();
        
        con->execute(sql.str());
    }
    catch(boost::exception& e){
        wlog(boost::diagnostic_information(e));
    }
    m_pool->release_connection(*con);
}


void transactions_table::add(uint32_t block_id, chain::transaction_receipt receipt, chain::transaction transaction)
{

}

} // namespace
