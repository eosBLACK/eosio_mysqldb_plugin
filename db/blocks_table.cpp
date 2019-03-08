#include "blocks_table.h"

#include <iostream>
#include <boost/format.hpp>
#include "mysqlconn.h"

namespace eosio {

extern void post_query_str_to_queue(const std::string query_str);
extern void post_odr_query_str_to_queue(const std::string query_str);
extern const int64_t get_now_tick();

extern bool call_sp_proc_action_accs; 
extern uint16_t call_sp_proc_action_accs_int;


//static const uint32_t BLOCK_INSERT_COUNT  = 100 * 3;

static const std::string BLOCK_INSERT_HEAD_STR =
    "INSERT INTO blocks(`block_number`,`id`,`prev_block_id`,`irreversible`,`timestamp`,`transaction_merkle_root`,`action_merkle_root`,`producer`,`version`,`confirmed`,`num_transactions`) VALUES ";
    //"INSERT INTO blocks(`id`,`block_number`,`timestamp`,`producer`,`version`,`confirmed`,`num_transactions`) VALUES ";
static const std::string BLOCK_INSERT_TAIL_STR =
    //" ON DUPLICATE KEY UPDATE irreversible = '1'";
    " ON DUPLICATE KEY UPDATE"
        "`id` = IF(`irreversible`=0 or VALUES(`irreversible`) = 1, VALUES(`id`), `id`), "
        "`timestamp` = IF(`irreversible`=0 or VALUES(`irreversible`) = 1, VALUES(`timestamp`), `timestamp`), "
        "`transaction_merkle_root` = IF(`irreversible`=0 or VALUES(`irreversible`) = 1, VALUES(`transaction_merkle_root`), `transaction_merkle_root`), "
        "`action_merkle_root` = IF(`irreversible`=0 or VALUES(`irreversible`) = 1, VALUES(`action_merkle_root`), `action_merkle_root`), "
        "`version` = IF(`irreversible`=0 or VALUES(`irreversible`) = 1, VALUES(`version`), `version`), "
        "`confirmed` = IF(`irreversible`=0 or VALUES(`irreversible`) = 1, VALUES(`confirmed`), `confirmed`), "
        "`num_transactions` = IF(`irreversible`=0 or VALUES(`irreversible`) = 1, VALUES(`num_transactions`), `num_transactions`), "
        "`irreversible` = IF(`irreversible`=0 or VALUES(`irreversible`) = 1, VALUES(`irreversible`), `irreversible`);";

static const std::string CREATE_PROC_ACTION_ACCS_STR = 
R"(
CREATE PROCEDURE `proc_action_accs`(
    IN `a_block_number` BIGINT
)
COMMENT 'Arrange actions_accounts'
proc_label: BEGIN

	START TRANSACTION;

--	SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;

	-- Find irrevesible block number.
	SET @c_block = a_block_number;
	SET @c0 = (SELECT block_number FROM blocks WHERE block_number <= a_block_number AND irreversible = 1 ORDER BY block_number DESC LIMIT 1);
	IF @c0 < @c_block THEN
		SET @c_block = @c0;
	END IF;
	
	-- Find last action.
	SET @c1 = (SELECT MAX(block_number) FROM actions);
	if @c1 <= @c_block THEN
		SET @c_block = @c1 -1;
	END IF;
	

	SET @c_prev = IFNULL((SELECT val FROM `proc_infos` WHERE `key`='#proc_block'), 0);
	
	IF @c_prev >= @c_block THEN
		SELECT CONCAT('Processing block number "', @c_block, '" is same or samller then last process.');
		ROLLBACK;
--		SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
		LEAVE proc_label;
	END IF;
	

	-- Generate temporary table.
	CREATE TEMPORARY TABLE -- TABLE -- 
	IF NOT EXISTS `TB_actions_ar` (
		`id` BIGINT(64) NOT NULL,
		`receiver` VARCHAR(12) NOT NULL,
		PRIMARY KEY (`id`),
		INDEX `idx_receiver` (`receiver`)
	)  ENGINE=MEMORY;
	-- COLLATE='utf8mb4_general_ci'
	-- ENGINE=InnoDB;
	
	CREATE TEMPORARY TABLE -- TABLE -- 
	IF NOT EXISTS `TB_action_accs_ar` (
		`action_id` BIGINT(64) NOT NULL,
		`actor` VARCHAR(12) NOT NULL,
		`permission` VARCHAR(13) NOT NULL,
		`receiver` VARCHAR(12) NOT NULL,
		INDEX `idx_action_id` (`action_id`),
		INDEX `idx_actor` (`actor`),
		INDEX `idx_receiver` (`receiver`)
	)  ENGINE=MEMORY;
	-- COLLATE='utf8mb4_general_ci'
	-- ENGINE=InnoDB;



	-- ------------------
	
	TRUNCATE TABLE `TB_actions_ar`;
	TRUNCATE TABLE `TB_action_accs_ar`;
	
	
	
	-- Move data to temporary table.
	INSERT INTO `TB_action_accs_ar`
		SELECT a.id, 
			JSON_UNQUOTE(JSON_EXTRACT(a.auth, CONCAT('$[', idx, '].actor'))) as actor,
			JSON_UNQUOTE(JSON_EXTRACT(a.auth, CONCAT('$[', idx, '].permission'))) as permission,
			a.receiver
		FROM actions a
		JOIN (
		  select 0 as idx union all
		  select 1 as idx union all
		  select 2 as idx union all
		  select 3 as idx union all
		  select 4 as idx union all
		  select 5 as idx union all
		  select 6 as idx union all
		  select 7 as idx union all
		  select 8 as idx union all
		  select 9 as idx 
		) indexes
	-- ON a.id = b.action_id and a.transaction_id = b.transaction_id
	WHERE JSON_EXTRACT(auth, CONCAT('$[', idx, ']')) IS NOT NULL
	AND a.block_number > @c_prev
	AND a.block_number <= @c_block;
	
	INSERT IGNORE INTO `TB_actions_ar`
		SELECT action_id, receiver
	FROM `TB_action_accs_ar`;

	
	-- Increase action_seq for accounts_seq 1.
	INSERT INTO accounts_seq (`name`,`action_seq`)
	  -- actor base.
	  SELECT actor, count(*)
	  FROM `TB_action_accs_ar`
	  WHERE actor != receiver 
	  GROUP BY actor
	ON DUPLICATE KEY UPDATE
	  accounts_seq.`action_seq` = accounts_seq.`action_seq` + VALUES(`action_seq`);
	
	-- Increase action_seq for accounts_seq 2.
	INSERT INTO accounts_seq (`name`,`action_seq`)
	  -- receiver base.
	  SELECT receiver, count(*)
	  FROM `TB_actions_ar`
	  GROUP BY receiver
	ON DUPLICATE KEY UPDATE
	  accounts_seq.`action_seq` = accounts_seq.`action_seq` + VALUES(`action_seq`);
	
	
	-- Increase action_seq for accounts 1.
	INSERT INTO accounts (`name`,`action_seq`)
	  -- actor base.
	  SELECT actor, count(*)
	  FROM `TB_action_accs_ar`
	  WHERE actor != receiver
	  GROUP BY actor
	ON DUPLICATE KEY UPDATE
	  accounts.`action_seq` = accounts.`action_seq` + VALUES(`action_seq`);
	
	-- Increase action_seq for accounts 2.
	INSERT INTO accounts (`name`,`action_seq`)
	  -- receiver base.
	  SELECT receiver, count(*)
	  FROM `TB_actions_ar`
	  GROUP BY receiver
	ON DUPLICATE KEY UPDATE
	  accounts.`action_seq` = accounts.`action_seq` + VALUES(`action_seq`);
	
	
	-- Send to actions_accounts 
	INSERT INTO actions_accounts (action_id, actor, permission)
	SELECT action_id, actor, permission
	FROM `TB_action_accs_ar`;

	
	TRUNCATE TABLE `TB_actions_ar`;
	TRUNCATE TABLE `TB_action_accs_ar`;
	-- DROP TEMPORARY TABLE `TB_actions_ar`;
	-- DROP TEMPORARY TABLE `TB_action_accs_ar`;
	
	REPLACE INTO `proc_infos` (`key`, `val`) VALUES ('#proc_block', @c_block);
	-- REPLACE INTO `proc_infos` (`key`, `val`) VALUES ('#processing', 0);
	
	COMMIT; 
	
--	SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
	
	SELECT 'Done', @c_block;
END
)";


blocks_table::blocks_table(std::shared_ptr<eosio::connection_pool> pool, uint32_t block_bulk_max_count): //, bool call_sp_proc_action_accs):
        m_pool(pool), _block_bulk_max_count(block_bulk_max_count)//, _call_sp_proc_action_accs(call_sp_proc_action_accs)
{
    //std::cout << "call_sp_proc_action_accs_int: " << call_sp_proc_action_accs_int << std::endl; 

}

void blocks_table::drop()
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);

    //ilog("blocks_table : m_pool->get_connection succeeded");
    ilog(" wipe block table");
    try {
        //con->execute("DROP TABLE IF EXISTS blocks;");
        con->execute("DROP TABLE IF EXISTS `blocks`");
        con->execute("DROP TABLE IF EXISTS `proc_infos`");
        con->execute("DROP PROCEDURE `proc_action_accs3`");
    }
    catch(std::exception& e){
        wlog(e.what());
    }
    m_pool->release_connection(*con);
}

void blocks_table::create(const string engine)
{
    shared_ptr<MysqlConnection> con = m_pool->get_connection();
    assert(con);
    try {
        ilog(" create block table");

        // Single line should be execute before multiline sql 
        con->execute(CREATE_PROC_ACTION_ACCS_STR);
        /*
        auto r = con->execute(CREATE_PROC_ACTION_ACCS_STR);
        if (!r) {
            std::cout << con->lastError() << std::endl; 
        }
        // */

        std::ostringstream query; 

        query << boost::format(
            "CREATE TABLE IF NOT EXISTS `blocks` ("
                "`block_number` INT(11) UNSIGNED NOT NULL,"
                "`id` VARCHAR(64) NOT NULL,"
                "`prev_block_id` VARCHAR(64) NULL DEFAULT NULL,"                // 삭제예정
                "`irreversible` TINYINT(1) NULL DEFAULT '0',"
                // ms 단위 저장. 
                // https://stackoverflow.com/questions/26299149/timestamp-with-a-millisecond-precision-how-to-save-them-in-mysql/26299379
                "`timestamp` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
                "`transaction_merkle_root` VARCHAR(64) NULL DEFAULT NULL,"      // 삭제예정
                "`action_merkle_root` VARCHAR(64) NULL DEFAULT NULL,"           // 삭제예정
                "`producer` VARCHAR(12) NULL DEFAULT NULL,"
                "`version` INT(11) NOT NULL DEFAULT '0',"
                "`num_transactions` INT(11) NULL DEFAULT '0',"
                "`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "`confirmed` INT(11) NULL DEFAULT NULL,"
                "PRIMARY KEY (`block_number`),"
                //"INDEX `idx_blocks_producer` (`producer`),"
                //"INDEX `idx_blocks_created` (`created_at`)"
                "INDEX `idx_blocks_timestamp` (`timestamp`)"
            ")"
            "COLLATE='utf8mb4_general_ci'"
            "ENGINE=%1%;\n"
        ) % engine;

        query << boost::format(
            "CREATE TABLE IF NOT EXISTS `proc_infos` (" 
                "`key`   VARCHAR(12) NOT NULL," 
                "`val`   BIGINT(64) NOT NULL," 
                "PRIMARY KEY (`key`)" 
            ") ENGINE=%1%;\n"
        ) % engine;

        con->execute(query.str());
    }
    catch(std::exception& e){
        wlog(e.what());
    }
    m_pool->release_connection(*con);

}


void blocks_table::add_block(const chain::signed_block_ptr block, bool irreversible) {
    const auto block_id_str = block->id().str();
    const auto previous_block_id_str = block->previous.str();
    const auto transaction_mroot_str = block->transaction_mroot.str();
    const auto action_mroot_str = block->action_mroot.str();
    //const auto timestamp = std::chrono::seconds{block->timestamp.operator fc::time_point().sec_since_epoch()}.count();
    const auto timestamp_milisec = std::chrono::milliseconds{block->timestamp.operator fc::time_point().time_since_epoch().count()}.count() / 1000;
    const auto num_transactions = (int)block->transactions.size();

/*
    std::cout << timestamp_milisec << ", " ;
    const auto timestamp_milisec2 = timestamp_milisec / 1000;
    std::cout << timestamp_milisec2 << std::endl; 
*/
        

    if (block_bulk_count > 0) {
        block_bulk_sql << ", ";
    }

    //*
    //block_bulk_sql << boost::format("('%1%','%2%','%3%','%4%',FROM_UNIXTIME('%5%'),'%6%','%7%','%8%','%9%','%10%','%11%')") 
    block_bulk_sql << boost::format("('%1%','%2%','%3%','%4%',FROM_UNIXTIME(%5% * 0.001),'%6%','%7%','%8%','%9%','%10%','%11%')") 
        % block->block_num()
        % block_id_str
        % previous_block_id_str
        % (irreversible? 1:0)
        //% timestamp
        % timestamp_milisec
        % transaction_mroot_str
        % action_mroot_str
        % block->producer.to_string()
        % block->schedule_version
        % block->confirmed
        % num_transactions;
    //*/

    block_bulk_count++;
    if (!block_bulk_insert_tick)
        block_bulk_insert_tick = get_now_tick(); 
    if (block_bulk_count >= _block_bulk_max_count)    
        post_query(); 


}

void blocks_table::set_irreversible(const chain::signed_block_ptr block) {
    if (_last_irreversible < block->block_num()) {
        _last_irreversible = block->block_num();
        add_block(block, true);
    }
}

void blocks_table::finalize() {
    post_query(); 
}

void blocks_table::tick(const int64_t tick) {
    if (block_bulk_insert_tick && ((tick - block_bulk_insert_tick) > 5000 )) {
        /*
        std::cout << "Block table tick ans save " 
            << tick << ", " 
            << tick - block_bulk_insert_tick 
            << std::endl; 
        //*/
        post_query(); 
    }

}

void blocks_table::post_query() {
    if (block_bulk_count) {
        post_query_str_to_queue(
            BLOCK_INSERT_HEAD_STR +
            block_bulk_sql.str() +
            BLOCK_INSERT_TAIL_STR 
        );

        int64_t now_tick = get_now_tick(); 
        //std::cout << "TimeCheck " << (now_tick - call_sp_tick) << std::endl; 
        //if (_call_sp_proc_action_accs  && _last_irreversible > 1000) {

        // Ignore first call
        if (!call_sp_tick) {
            call_sp_tick = now_tick; 
        } else
        if (call_sp_proc_action_accs && 
            call_sp_proc_action_accs_int &&
            _last_irreversible > _last_call_sp_block &&
            (now_tick - call_sp_tick) > (1000 * call_sp_proc_action_accs_int)) {

            _last_call_sp_block = _last_irreversible;
            call_sp_tick = now_tick; 

            std::ostringstream proc_action_accs_sql;
            proc_action_accs_sql << boost::format("CALL `proc_action_accs`('%1%');") 
                //% (_last_irreversible - 1000);
                % (_last_irreversible);
            //post_query_str_to_queue(
            post_odr_query_str_to_queue(
                proc_action_accs_sql.str() 
            );
            //std::cout << "CALL SP:: " << proc_action_accs_sql.str() << std::endl; 


        }




        block_bulk_sql.str(""); block_bulk_sql.clear(); 
        block_bulk_count = 0; 
        block_bulk_insert_tick = 0;
    }

}


} // namespace
