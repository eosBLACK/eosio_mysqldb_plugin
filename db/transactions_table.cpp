#include "transactions_table.h"


#include <iostream>
#include <boost/format.hpp>

#include "mysqlconn.h"


namespace eosio {

extern void post_query_str_to_queue(const std::string query_str);
extern const int64_t get_now_tick();

//static const uint32_t TRX_INSERT_COUNT            = 100 * 3;
//static const uint32_t TRX_TRACE_INSERT_COUNT      = 100 * 3; 

static const std::string TRX_INSERT_STR = 
    "REPLACE INTO transactions(`id`,`block_number`,`ref_block_num`,`ref_block_prefix`,`expiration`,`num_actions`) VALUES ";


static const std::string TRX_TRACE_INSERT_STR =
    "REPLACE INTO transaction_trace (`id`,`block_number`,`cpu_usage_us`,`net_usage_words`,`elapsed`,`scheduled`,`trx_status`) VALUES ";

transactions_table::transactions_table(std::shared_ptr<eosio::connection_pool> pool, uint32_t trace_bulk_max_count, uint32_t trx_bulk_max_count):
    m_pool(pool), _trace_bulk_max_count(trace_bulk_max_count), _trx_bulk_max_count(trx_bulk_max_count)
{

}

void transactions_table::drop()
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    //ilog("transactions_table : m_pool->get_connection succeeded");
    ilog(" wipe transaction tables");

    try {
        con->execute("DROP TABLE IF EXISTS `transactions`;");
        con->execute("DROP TABLE IF EXISTS `transaction_trace`;");
    }
    catch(std::exception& e){
        wlog(e.what());
    }
    m_pool->release_connection(*con);
}

void transactions_table::create(const string engine)
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    try {
        ilog(" create transaction tables");

        std::ostringstream query; 

        query << boost::format(
            "CREATE TABLE IF NOT EXISTS `transactions` ("
                "`id` VARCHAR(64) NOT NULL,"
                "`block_number` INT(10) UNSIGNED NOT NULL,"
                "`ref_block_num` BIGINT(20) NOT NULL,"
                "`ref_block_prefix` BIGINT(20) NULL DEFAULT NULL,"
                "`expiration` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "`num_actions` INT(11) NULL DEFAULT '0',"
                "PRIMARY KEY (`id`),"
                "INDEX `idx_block_number` (`block_number`),"
                "INDEX `idx_actions_created` (`created_at`)"
            ")"
            "COLLATE='utf8mb4_general_ci'"
            "ENGINE=%1%;\n"
        ) % engine;

        query << boost::format(
            "CREATE TABLE IF NOT EXISTS `transaction_trace` ("
                "`id` VARCHAR(64) NOT NULL,"
                "`block_number` INT(10) UNSIGNED NOT NULL,"
                "`cpu_usage_us` BIGINT(20) NULL DEFAULT NULL,"
                "`net_usage_words` BIGINT(20) NULL DEFAULT NULL,"
                "`scheduled` CHAR(1) NULL DEFAULT NULL,"
                "`elapsed` INT(11) NULL DEFAULT NULL,"
                "`trx_status` TINYINT(4) NOT NULL DEFAULT '-1',"
                "`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "PRIMARY KEY (`id`),"
                "INDEX `idx_block_number` (`block_number`),"
                "INDEX `idx_actions_created` (`created_at`)"
            ")"
            "COLLATE='utf8mb4_general_ci'"
            "ENGINE=%1%;"                
        ) % engine;


        con->execute(query.str());

    }
    catch(std::exception& e){
        wlog(e.what());
    }
    m_pool->release_connection(*con);

}


void transactions_table::add_trx(uint32_t block_number, const chain::transaction& transaction) {
    const auto transaction_id_str = transaction.id().str();
    const auto expiration = std::chrono::seconds{transaction.expiration.sec_since_epoch()}.count();

    if (trx_bulk_count > 0) {
        trx_bulk_sql << ", ";
    }

    trx_bulk_sql << boost::format("('%1%', '%2%', '%3%', '%4%', FROM_UNIXTIME('%5%'), '%6%')")
        % transaction_id_str
        % block_number
        % transaction.ref_block_num
        % transaction.ref_block_prefix
        % expiration
        % transaction.total_actions();

    trx_bulk_count++;
    if (!trx_bulk_insert_tick)
        trx_bulk_insert_tick = get_now_tick(); 
    if (trx_bulk_count >= _trx_bulk_max_count)
        post_trx_query(); 


}

void transactions_table::add_trace(chain::transaction_id_type transaction_id, uint32_t block_number, int32_t cpu_usage_us, int32_t net_usage_words, int elapsed, int trx_status, bool scheduled)
{
    const auto transaction_id_str = transaction_id.str();
    //const auto created_at = std::chrono::seconds(std::time(NULL));

    if (trace_bulk_count > 0) {
        trace_bulk_sql << ", ";
    }

    trace_bulk_sql << boost::format(" ('%1%','%2%','%3%','%4%','%5%','%6%','%7%') ")
        % transaction_id_str
        % block_number
        % cpu_usage_us
        % net_usage_words
        % elapsed
        % ((scheduled)?"1":"0")
        % trx_status;

    trace_bulk_count++; 
    if (!trace_bulk_insert_tick)
        trace_bulk_insert_tick = get_now_tick(); 
    if (trace_bulk_count >= _trace_bulk_max_count) 
        post_trace_query(); 

}

void transactions_table::finalize() {
    post_trx_query();
    post_trace_query();
}

void transactions_table::tick(const int64_t tick) {
    if (trx_bulk_insert_tick && ((tick - trx_bulk_insert_tick) > 5000 )) {
        /*
        std::cout << "transaction table tick ans save " 
            << tick << ", " 
            << tick - trx_bulk_insert_tick 
            << std::endl; 
        //*/

        post_trx_query(); 
    }

    if (trace_bulk_insert_tick && ((tick - trace_bulk_insert_tick) > 5000 )) {
        /*
        std::cout << "trace table tick ans save " 
            << tick << ", " 
            << tick - trace_bulk_insert_tick 
            << std::endl; 
        //*/

        post_trace_query(); 
    }
}

void transactions_table::post_trx_query() {
    if (trx_bulk_count) {
        post_query_str_to_queue(
            TRX_INSERT_STR +
            trx_bulk_sql.str()
        );

    /*
        post_query_str_to_queue(
            ARCHIVE_TRX_INSERT_STR +
            trx_bulk_sql.str()
        );
    // */

        trx_bulk_sql.str(""); trx_bulk_sql.clear(); 
        trx_bulk_count = 0;
        trx_bulk_insert_tick = 0;
    }
}

void transactions_table::post_trace_query() {
   if (trace_bulk_count) {
        post_query_str_to_queue(
            TRX_TRACE_INSERT_STR +
            trace_bulk_sql.str()
        );

        trace_bulk_sql.str(""); trace_bulk_sql.clear(); 
        trace_bulk_count = 0; 
        trace_bulk_insert_tick = 0; 
    }
}


} // namespace
