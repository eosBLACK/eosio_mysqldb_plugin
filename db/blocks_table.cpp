#include "blocks_table.h"

#include <iostream>
#include <boost/format.hpp>
#include "mysqlconn.h"

namespace eosio {

blocks_table::blocks_table(std::shared_ptr<eosio::connection_pool> pool):
        m_pool(pool)
{

}

void blocks_table::drop()
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);

    ilog("blocks_table : m_pool->get_connection succeeded");
    try {
        con->execute("DROP TABLE IF EXISTS blocks;");
    }
    catch(std::exception& e){
        wlog(e.what());
    }
    m_pool->release_connection(*con);
}

void blocks_table::create()
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    try {
        con->execute("CREATE TABLE IF NOT EXISTS blocks("
            "id VARCHAR(64) PRIMARY KEY,"
            "block_number INT NOT NULL AUTO_INCREMENT,"
            "prev_block_id VARCHAR(64),"
            "irreversible TINYINT(1) DEFAULT 0,"
            "timestamp DATETIME DEFAULT NOW(),"
            "transaction_merkle_root VARCHAR(64),"
            "action_merkle_root VARCHAR(64),"
            "producer VARCHAR(12),"
            "version INT NOT NULL DEFAULT 0,"
            "new_producers JSON DEFAULT NULL,"
            "num_transactions INT DEFAULT 0,"
            "confirmed INT, UNIQUE KEY block_number (block_number)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_general_ci;");

        con->execute("CREATE INDEX idx_blocks_producer ON blocks (producer);");
        con->execute("CREATE INDEX idx_blocks_number ON blocks (block_number);");
    }
    catch(std::exception& e){
        wlog(e.what());
    }
    m_pool->release_connection(*con);

}



void blocks_table::add(chain::signed_block_ptr block)
{
    const auto block_id_str = block->id().str();
    const auto previous_block_id_str = block->previous.str();
    const auto transaction_mroot_str = block->transaction_mroot.str();
    const auto action_mroot_str = block->action_mroot.str();
    const auto timestamp = std::chrono::seconds{block->timestamp.operator fc::time_point().sec_since_epoch()}.count();
    const auto num_transactions = (int)block->transactions.size();
    int exist;

    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    std::ostringstream sql;
    try {
        sql << boost::format("INSERT INTO blocks(id, block_number, prev_block_id, timestamp, transaction_merkle_root, action_merkle_root,"
                    "producer, version, confirmed, num_transactions) VALUES ('%1%', '%2%', '%3%', FROM_UNIXTIME('%4%'), '%5%', '%6%', '%7%', '%8%', '%9%', '%10%') ON DUPLICATE KEY UPDATE irreversible = '1' ")
        % block_id_str
        % block->block_num()
        % previous_block_id_str
        % timestamp
        % transaction_mroot_str
        % action_mroot_str
        % block->producer.to_string()
        % block->schedule_version
        % block->confirmed
        % num_transactions;

        con->execute(sql.str());

        if (block->new_producers) {
            const auto new_producers = fc::json::to_string(block->new_producers->producers);
            sql.str("");sql.clear();
            sql << boost::format("UPDATE blocks SET new_producers = '%1%' WHERE id = '%2%' ")
            % new_producers
            % block_id_str;

            con->execute(sql.str());
        }

    }
    catch(std::exception& e){
        wlog(e.what());
    }
    m_pool->release_connection(*con);
    
    
}

void blocks_table::createBlockStatement(chain::signed_block_ptr block,std::string* stmt_block)
{
    const auto block_id_str = block->id().str();
    const auto previous_block_id_str = block->previous.str();
    const auto transaction_mroot_str = block->transaction_mroot.str();
    const auto action_mroot_str = block->action_mroot.str();
    const auto timestamp = std::chrono::seconds{block->timestamp.operator fc::time_point().sec_since_epoch()}.count();
    const auto num_transactions = (int)block->transactions.size();
    int exist;
    std::ostringstream s_values;

    try {
        stmt_block->append("INSERT INTO blocks(id, block_number, prev_block_id, timestamp, transaction_merkle_root, action_merkle_root,producer, version, confirmed, num_transactions) VALUES ");
        s_values << boost::format("('%1%', '%2%', '%3%', FROM_UNIXTIME('%4%'), '%5%', '%6%', '%7%', '%8%', '%9%', '%10%') ON DUPLICATE KEY UPDATE irreversible = '1'; ") 
                    % block_id_str
                    % block->block_num()
                    % previous_block_id_str
                    % timestamp
                    % transaction_mroot_str
                    % action_mroot_str
                    % block->producer.to_string()
                    % block->schedule_version
                    % block->confirmed
                    % num_transactions;

        stmt_block->append(s_values.str());

        if (block->new_producers) {
            s_values.str("");s_values.clear();
            const auto new_producers = fc::json::to_string(block->new_producers->producers);
            s_values << boost::format("UPDATE blocks SET new_producers = '%1%' WHERE id = '%2%'; ")
            % new_producers
            % block_id_str;
            
            stmt_block->append(s_values.str());
        }
    }
    catch(std::exception& e){
        wlog(e.what());
    }

}


void blocks_table::executeBlocks(std::string sql_blocks, std::string sql_transactions)
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);

    if(sql_blocks.length() > 0)
        con->execute(sql_blocks.c_str());

    if(sql_transactions.length() > 0)
        con->execute(sql_transactions.c_str());

    m_pool->release_connection(*con);
}

void blocks_table::process_irreversible(chain::signed_block_ptr block)
{
    
}

} // namespace
