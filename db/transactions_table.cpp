#include "transactions_table.h"

#include <chrono>
#include <fc/log/logger.hpp>
#include "zdbcpp.h"

using namespace zdbcpp;
namespace eosio {

transactions_table::transactions_table(std::shared_ptr<eosio::connection_pool> pool):
    m_pool(pool)
{

}

void transactions_table::drop()
{
    zdbcpp::Connection con = m_pool->get_connection();
    assert(con);
    ilog("transactions_table : m_pool->get_connection succeeded");

    try {
        con.execute("DROP TABLE IF EXISTS transactions;");
    }
    catch(std::exception& e){
        wlog(e.what());
    }
    m_pool->release_connection(con);
}

void transactions_table::create()
{
    zdbcpp::Connection con = m_pool->get_connection();
    assert(con);
    try {
        con.execute("CREATE TABLE transactions("
                "id VARCHAR(64) PRIMARY KEY,"
                "block_id BIGINT NOT NULL,"
                "cpu_usage_us BIGINT,"
                "net_usage_words BIGINT,"
                "ref_block_num BIGINT NOT NULL,"
                "ref_block_prefix BIGINT,"
                "expiration DATETIME DEFAULT NOW(),"
                "pending TINYINT(1),"
                "created_at DATETIME DEFAULT NOW(),"
                "num_actions INT DEFAULT 0,"
                "updated_at DATETIME DEFAULT NOW()) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");

        con.execute("CREATE INDEX transactions_block_id ON transactions (block_id);");
    }
    catch(std::exception& e){
        wlog(e.what());
    }
    m_pool->release_connection(con);

}

void transactions_table::add(uint32_t block_id, chain::transaction_receipt receipt, chain::transaction transaction)
{
    const auto transaction_id_str = transaction.id().str();
    const auto expiration = std::chrono::seconds{transaction.expiration.sec_since_epoch()}.count();
    zdbcpp::Connection con = m_pool->get_connection();
    assert(con);
    try {
        zdbcpp::PreparedStatement p1 = con.prepareStatement("INSERT INTO transactions(id, block_id, cpu_usage_us, net_usage_words, ref_block_num, ref_block_prefix,"
            "expiration, pending, created_at, updated_at, num_actions) VALUES (?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?), ?, FROM_UNIXTIME(?), FROM_UNIXTIME(?), ?)");
        p1.setString(1,transaction_id_str.c_str()),
        p1.setDouble(2,block_id),
        p1.setDouble(3,receipt.cpu_usage_us),
        p1.setDouble(4,receipt.net_usage_words),
        p1.setDouble(5,transaction.ref_block_num),
        p1.setDouble(6,transaction.ref_block_prefix),
        p1.setDouble(7,expiration),
        p1.setInt(8,0),
        p1.setDouble(9,expiration),
        p1.setDouble(10,expiration),
        p1.setInt(11,transaction.total_actions());

        p1.execute();
    }
    catch(std::exception& e){
        wlog(e.what());
    }
    m_pool->release_connection(con);
}

} // namespace
