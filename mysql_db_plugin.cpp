/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#include <eosio/mysql_db_plugin/mysql_db_plugin.hpp>
#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <boost/chrono.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include <queue>
#include <sstream>

#include <future>

/**
 * table classes
 * */
#include "accounts_table.h"
#include "transactions_table.h"
#include "blocks_table.h"
#include "actions_table.h"

//#define ACTION_ACC_ONLY 

namespace fc { class variant; }

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

static appbase::abstract_plugin& _mysql_db_plugin = app().register_plugin<mysql_db_plugin>();
int queue_sleep_time = 0;

bool call_sp_proc_action_accs = true; 
uint16_t call_sp_proc_action_accs_int = 5; // sp call interval

bool use_action_reversible = true; 


const int64_t get_now_tick() {
    return fc::time_point::now().time_since_epoch().count()/1000;
}

class mysql_db_plugin_impl;

static mysql_db_plugin_impl* static_mysql_db_plugin_impl = nullptr; 

class mysql_db_plugin_impl : public std::enable_shared_from_this<mysql_db_plugin_impl> {
public:
   mysql_db_plugin_impl(boost::asio::io_service& io);
   ~mysql_db_plugin_impl();

   fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
   fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
   //fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
   fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;

   void consume_query_process(); 
   void consume_odr_query_process(); 


   void accepted_block( const chain::block_state_ptr& );
   void applied_irreversible_block(const chain::block_state_ptr&);
   void applied_transaction(const chain::transaction_trace_ptr&);

   void process_applied_transaction(const chain::transaction_trace_ptr&);
   void process_accepted_block( const chain::block_state_ptr& );
   void process_irreversible_block(const chain::block_state_ptr&);


   void process_add_action_trace( uint64_t parent_action_id, const chain::action_trace& atrace,
                          //bool executed, 
                          uint32_t act_num );

   void init(const std::string host, const std::string user, const std::string passwd, const std::string database, 
      const uint16_t port, const uint16_t max_conn, bool do_close_on_unlock,
      uint32_t block_num_start, const variables_map& options);

   void wipe_database(const string engine);
   void just_create_database(const string engine); 

   void tick_loop_process(); 

   template<typename Mutex, typename Condition, typename Queue, typename Entry> 
   void queue( Mutex& mtx, Condition& condition, Queue& queue, const Entry& e );

   
   bool configured{false};
   bool wipe_database_on_startup{false};
   uint32_t start_block_num = 0;
   uint32_t end_block_num = 0;
   //uint64_t start_action_idx = 0;
   bool start_block_reached = false;
   //bool is_producer = false;

   //bool is_replaying = false; 

/**
 * mysql db connection definition
 **/
   std::shared_ptr<connection_pool> m_connection_pool;
   std::unique_ptr<accounts_table> m_accounts_table;
   std::unique_ptr<actions_table> m_actions_table;
   std::unique_ptr<blocks_table> m_blocks_table;
   std::unique_ptr<transactions_table> m_transactions_table;
   std::string system_account;
   uint32_t m_block_num_start;

   size_t max_queue_size      = 100000; 
   size_t query_thread_count  = 4; 

   //  Parallel Processing Query Queue. Processes query_thread_count number of threads
   std::deque<std::string> query_queue; 
   boost::mutex query_mtx;
   boost::condition_variable query_condition;
   std::vector<boost::thread> consume_query_threads;

   // Sequential processing Query Queue. Process one thread at a time.
   std::deque<std::string> odr_query_queue; 
   boost::mutex odr_query_mtx;
   boost::condition_variable odr_query_condition;
   boost::thread consume_odr_query_thread;



   boost::atomic<bool> done{false};
   boost::atomic<bool> startup{true};
   fc::optional<chain::chain_id_type> chain_id;
   fc::microseconds abi_serializer_max_time;

   static const account_name newaccount;
   static const account_name setabi;

   boost::asio::deadline_timer  _timer;
};


template<typename Mutex, typename Condition, typename Queue, typename Entry> 
void mysql_db_plugin_impl::queue( Mutex& mtx, Condition& condition, Queue& queue, const Entry& e ) {
   boost::mutex::scoped_lock lock( mtx );
   auto queue_size = queue.size();
   if( queue_size > max_queue_size ) {
      lock.unlock();
      condition.notify_one();
      queue_sleep_time += 10;
      if( queue_sleep_time > 1000 )
         wlog("queue size: ${q}", ("q", queue_size));
      boost::this_thread::sleep_for( boost::chrono::milliseconds( queue_sleep_time ));
      lock.lock();
   } else {
      queue_sleep_time -= 10;
      if( queue_sleep_time < 0 ) queue_sleep_time = 0;
   }
   queue.emplace_back( e );
   lock.unlock();
   condition.notify_one();
}

void mysql_db_plugin_impl::applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      if( start_block_reached ) {
         process_applied_transaction(t); 
      }
   } catch (fc::exception& e) {
      elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_transaction");
   }
}

void mysql_db_plugin_impl::applied_irreversible_block( const chain::block_state_ptr& bs ) {
#ifdef ACTION_ACC_ONLY
    return; 
#endif

   try {
      //ilog("irreversible : ${s} ${c}",("s",bs->block_num) ("c", bs->block->confirmed) );
      if (end_block_num > 0) {
         if (bs->block_num >= start_block_num && bs->block_num <= end_block_num){
            process_irreversible_block( bs );
         }
      } else {
         if(start_block_reached) {
            process_irreversible_block( bs );
         }
      }
   } catch (fc::exception& e) {
      elog("FC Exception while applied_irreversible_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_irreversible_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_irreversible_block");
   }
}

void mysql_db_plugin_impl::accepted_block( const chain::block_state_ptr& bs ) {
   try {
      //ilog("accepted : ${s} ${c}",("s",bs->block_num) ("c", bs->block->confirmed) );
      if (end_block_num > 0 && bs->block_num > end_block_num){

         if (start_block_reached) {
            m_accounts_table->finalize();
            m_actions_table->finalize(); 
            m_transactions_table->finalize(); 
            m_blocks_table->finalize(); 

            //if (is_replaying) 
            {

                wlog( "endblock reached. I'll shutdown this process. Sorry for Dirty flag." );
                done = true;

                odr_query_condition.notify_one();
                for (size_t i=0; i< consume_query_threads.size(); i++ ) {
                    query_condition.notify_one();
                }

                consume_odr_query_thread.join(); 
                for (size_t i=0; i< consume_query_threads.size(); i++ ) {
                    consume_query_threads[i].join(); 
                }

                std::terminate();
                //throw std::runtime_error( "Stop recording l!!" );

            }

            start_block_reached = false;
         }

      }else {
         if( !start_block_reached ) {
            if( bs->block_num >= start_block_num ) {
               start_block_reached = true;
            }
         }

         if( start_block_reached ) {
            process_accepted_block( bs );
         }
      }
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_block");
   }
}

void mysql_db_plugin_impl::consume_query_process() {
      try {
            while (true) {
                  boost::mutex::scoped_lock lock(query_mtx);
                  while ( query_queue.empty() && !done ) {
                        //ilog("waiting _1");
                        query_condition.wait(lock);
                  }
                  
                  size_t query_queue_count = query_queue.size(); 
                  std::string query_str = "";
                  if (query_queue_count > 0) {
                        query_str = query_queue.front(); 
                        query_queue.pop_front(); 
                  }
                  //ilog("awake _1");
                  //std::cout << query_queue_count << std::endl; 

                  lock.unlock();

                  if (query_queue_count > 0) {
                        //ilog("do query _1");
                        //std::cout << query_str << std::endl; 
                        shared_ptr<MysqlConnection> con = m_connection_pool->get_connection();
                        assert(con);
                        try{
                              con->execute(query_str, true);

                              m_connection_pool->release_connection(*con);
                        } catch (...) {
                              m_connection_pool->release_connection(*con);
                        }
                  }
               
                  if( query_queue_count == 0 && done ) {
                        //ilog("uhahaha!!");
                        break;
                  }      
            }

            ilog("consume query thread shutdown gracefully");

      } catch (...) {
            elog("Unknown exception while consuming query");
      }

}

void mysql_db_plugin_impl::consume_odr_query_process() {
     try {
            while (true) {
                  boost::mutex::scoped_lock lock(odr_query_mtx);
                  while ( odr_query_queue.empty() && !done ) {
                        //ilog("waiting _1");
                        odr_query_condition.wait(lock);
                  }
                  
                  size_t query_queue_count = odr_query_queue.size(); 
                  std::string query_str = "";
                  if (query_queue_count > 0) {
                        query_str = odr_query_queue.front(); 
                        odr_query_queue.pop_front(); 
                  }
                  //ilog("awake _1");
                  //std::cout << query_queue_count << std::endl; 

                  lock.unlock();

                  if (query_queue_count > 0) {
                        //ilog("do query _2");
                        //std::cout << query_str << std::endl; 
                        shared_ptr<MysqlConnection> con = m_connection_pool->get_connection();
                        assert(con);
                        try{
                              con->execute(query_str, true);

                              m_connection_pool->release_connection(*con);
                        } catch (...) {
                              m_connection_pool->release_connection(*con);
                        }
                  }
               
                  if( query_queue_count == 0 && done ) {
                        //ilog("uhahaha!!");
                        break;
                  }      
            }

            ilog("consume odr query thread shutdown gracefully");

      } catch (...) {
            elog("Unknown exception while consuming odr query");
      }

}


void mysql_db_plugin_impl::process_add_action_trace( uint64_t parent_action_id, const chain::action_trace& atrace,
      //bool executed, 
      uint32_t act_num ) {

    const auto action_id = atrace.receipt.global_sequence ; 
    const auto trx_id    = atrace.trx_id;
    const auto block_id  = atrace.producer_block_id->str();
   

#ifdef ACTION_ACC_ONLY 
#else
    if( atrace.receipt.receiver == chain::config::system_account_name ) {
        m_accounts_table->update_account( atrace.act, trx_id );
    }
#endif

    /*
    // !!TEMP. To save action by INSERT IGNORE in ACTION_ACC_ONLY Mode
    m_actions_table->add_action(
       action_id, parent_action_id, atrace.receipt.receiver.to_string(), atrace.act, trx_id, block_id, atrace.block_num, act_num
    );
    //*/



//*
#ifdef ACTION_ACC_ONLY 
    m_actions_table->add_act_acc_only(action_id, atrace.act, trx_id, atrace.block_num);
#else
    m_actions_table->add_action(
       action_id, parent_action_id, atrace.receipt.receiver.to_string(), atrace.act, trx_id, block_id, atrace.block_num, act_num
    );
#endif   
//*/

    for( const auto& iline_atrace : atrace.inline_traces ) {
        process_add_action_trace( action_id, iline_atrace, act_num );
    }

}

void mysql_db_plugin_impl::process_applied_transaction(const chain::transaction_trace_ptr& t) {

   //Does not save if block number is 0
   if ( t->block_num == 0 ) return; 

   bool write_atraces = false;
   bool executed = t->receipt.valid() &&
      t->receipt->status == chain::transaction_receipt_header::executed;

/*
    //  For test purpose
    // 2018.12.11. The situaion where the action's global sequence is 0
    if (!t->receipt.valid()) {
        std::cout
         << "\nCheck trx : "
         << t->id.str()
         << " / "
         << t->block_num
         << std::endl; 
    }
*/

   uint32_t act_num = 0;

#ifdef ACTION_ACC_ONLY
#else
   m_transactions_table->add_trace(
      t->id,
      t->block_num,
      t->receipt->cpu_usage_us,
      t->receipt->net_usage_words,
      t->elapsed.count(),
      t->receipt->status, 
      t->scheduled
   );
#endif

/*
    // !TEMP. save trace on ACTION_ACC_ONLY mode.
   m_transactions_table->add_trace(
      t->id,
      t->block_num,
      t->receipt->cpu_usage_us,
      t->receipt->net_usage_words,
      t->elapsed.count(),
      t->receipt->status, 
      t->scheduled
   );
//*/
   if ( !executed ) return; 
   
   auto start_time = fc::time_point::now();

   for( const auto& atrace : t->action_traces ) {
      try {
         if (atrace.block_num) {
            process_add_action_trace( 0, atrace, act_num );
            ++act_num;
         }

      } catch(...) {
         wlog("add action traces failed.");
      }
   }

   //if( !start_block_reached ) return;
   //if( !write_atraces ) return; //< do not insert transaction_trace if all action_traces filtered out

   //if(qry_actions.length() < 1) return;
   

   auto time = fc::time_point::now() - start_time;
   if( time > fc::microseconds(500000) )
      ilog( "process actions, trans_id: ${r} time: ${t}", ("r", t->id.str())("t", time) );
}


void mysql_db_plugin_impl::process_accepted_block( const chain::block_state_ptr& block ) {
#ifdef ACTION_ACC_ONLY
    return; 
#endif

      try {      
            m_blocks_table->add_block(block->block, false);

            for (const auto &transaction : block->trxs) {
                  //m_transactions_table->add_trx(block->block_num, transaction->trx);
                  // 2019.3.6. fix for 1.6.1
                  m_transactions_table->add_trx(block->block_num, transaction->packed_trx->get_signed_transaction());
            }

            {
                  /*
                  // 2018.12.11. Timer is not good enough. Process directly here.
                  // -->> Exchange timer to 1 second interval
                  int64_t tick = get_now_tick();

                  m_accounts_table->tick(tick);
                  m_actions_table->tick(tick); 
                  m_blocks_table->tick(tick); 
                  m_transactions_table->tick(tick); 
                  //*/

            }

      } catch (const std::exception &ex) {
            elog("${e}", ("e", ex.what())); // prevent crash
      }
}


void mysql_db_plugin_impl::process_irreversible_block(const chain::block_state_ptr& block) {
      try {      
            //m_blocks_table->add_block(block->block);
            m_blocks_table->set_irreversible(block->block);
      } catch (const std::exception &ex) {
            elog("${e}", ("e", ex.what())); // prevent crash
      }
}

mysql_db_plugin_impl::mysql_db_plugin_impl(boost::asio::io_service& io): 
    _timer(io)
{
    static_mysql_db_plugin_impl = this; 
    // io parameter test 
    //ilog("mysql_plugin_impl");
    //std::cout << (u_int64_t)&io << std::endl; 
}

mysql_db_plugin_impl::~mysql_db_plugin_impl() {
   if (!startup) {
      try {
         m_actions_table->finalize(); 
         m_accounts_table->finalize(); 
         m_transactions_table->finalize();
         m_blocks_table->finalize(); 

         ilog( "shutdown in process please be patient this can take a few minutes" );
         done = true;

         odr_query_condition.notify_one();
         for (size_t i=0; i< consume_query_threads.size(); i++ ) {
            query_condition.notify_one();
         }


         consume_odr_query_thread.join(); 
         for (size_t i=0; i< consume_query_threads.size(); i++ ) {
            consume_query_threads[i].join(); 
         }

      } catch( std::exception& e ) {
         elog( "Exception on mysql_db_plugin shutdown of consume thread: ${e}", ("e", e.what()));
      }
   }


   static_mysql_db_plugin_impl = nullptr; 
}

void mysql_db_plugin_impl::wipe_database(const string engine) {
   ilog("wipe tables");

   // drop tables
   m_actions_table->drop();
   m_accounts_table->drop();
   m_transactions_table->drop();
   m_blocks_table->drop();
   
   ilog("create tables");
   m_actions_table->create(engine); 
   m_accounts_table->create(engine);
   m_transactions_table->create(engine);
   m_blocks_table->create(engine);   

   m_accounts_table->add(system_account);

   //ilog("create tables done");
   ilog("done");
}

void mysql_db_plugin_impl::just_create_database(const string engine) {
   ilog("create tables");
   m_actions_table->create(engine); 
   m_accounts_table->create(engine);
   m_transactions_table->create(engine);
   m_blocks_table->create(engine);   
   ilog("done");
}

void mysql_db_plugin_impl::tick_loop_process() {
    std::weak_ptr<mysql_db_plugin_impl> weak_this = shared_from_this();

    _timer.cancel(); 
    _timer.expires_from_now( boost::posix_time::milliseconds( 1000 )); 


    _timer.async_wait([weak_this](const boost::system::error_code& ec){
        //ilog("Timer fired! ${t}", ("t",get_now_tick()));


        auto self = weak_this.lock(); 
        int64_t tick = get_now_tick();

        self->m_accounts_table->tick(tick);
        self->m_actions_table->tick(tick); 
        self->m_blocks_table->tick(tick); 
        self->m_transactions_table->tick(tick); 

        self->tick_loop_process(); 
    });

} 


void mysql_db_plugin_impl::init(const std::string host, const std::string user, const std::string passwd, const std::string database, 
      const uint16_t port, const uint16_t max_conn, bool do_close_on_unlock, 
      uint32_t block_num_start, const variables_map& options) 
{
   m_connection_pool = std::make_shared<connection_pool>(host, user, passwd, database, port, max_conn, do_close_on_unlock);

   {
      uint32_t account_ag_count = 1; 
      if( options.count( "mysqldb-ag-account" )) {
            account_ag_count = options.at("mysqldb-ag-account").as<uint32_t>();
      }
      ilog(" aggregate account: ${n}", ("n", account_ag_count));
      m_accounts_table = std::make_unique<accounts_table>(m_connection_pool, account_ag_count);
   }

   {
      uint32_t action_raw_ag_count = 10;
      uint32_t action_acc_ag_count = 12;
      if( options.count( "mysqldb-ag-action-raw" )) {
            action_raw_ag_count = options.at("mysqldb-ag-action-raw").as<uint32_t>();
      }
      if( options.count( "mysqldb-ag-action-acc" )) {
            action_acc_ag_count = options.at("mysqldb-ag-action-acc").as<uint32_t>();
      }
      ilog(" aggregate action raw: ${n}", ("n", action_raw_ag_count));
      ilog(" aggregate action acc: ${n}", ("n", action_acc_ag_count));
      m_actions_table = std::make_unique<actions_table>(m_connection_pool, action_raw_ag_count, action_acc_ag_count);

   }

   {
      uint32_t block_ag_count = 5;
      if( options.count( "mysqldb-ag-block" )) {
            block_ag_count = options.at("mysqldb-ag-block").as<uint32_t>();
      }
      ilog(" aggregate block: ${n}", ("n", block_ag_count));

      m_blocks_table = std::make_unique<blocks_table>(m_connection_pool, block_ag_count);
   }

   {
      uint32_t trace_ag_count = 10;
      uint32_t transaction_ag_count = 10;
      if( options.count( "mysqldb-ag-trace" )) {
            trace_ag_count = options.at("mysqldb-ag-trace").as<uint32_t>();
      }
      if( options.count( "mysqldb-ag-transaction" )) {
            transaction_ag_count = options.at("mysqldb-ag-transaction").as<uint32_t>();
      }
      ilog(" aggregate trace: ${n}", ("n", trace_ag_count));
      ilog(" aggregate transaction: ${n}", ("n", transaction_ag_count));
      m_transactions_table = std::make_unique<transactions_table>(m_connection_pool, trace_ag_count, transaction_ag_count);
   }
   
   m_block_num_start = block_num_start;
   system_account = chain::name(chain::config::system_account_name).to_string();


   std::string engine_str = options.at("mysqldb-engine").as<std::string>();
   //std::cout << "Engine " << engine_str << std::endl; 
   if( wipe_database_on_startup ) {
      wipe_database(engine_str);
   } else {
      just_create_database(engine_str);
   }

   ilog("starting mysql db plugin thread");

   consume_odr_query_thread = boost::thread([this] { consume_odr_query_process(); });

   for (size_t i=0; i<query_thread_count; i++) {
      consume_query_threads.push_back( boost::thread([this] { consume_query_process(); }) );
   }

   // 2018.12.11. Timer is not good. Terminate. Processed at accepted_block  
   // -->> Modify Timer. Normal Operation. 
   tick_loop_process(); 
   
   startup = false;
}

mysql_db_plugin::mysql_db_plugin()
:my(new mysql_db_plugin_impl(app().get_io_service()))
{

}

mysql_db_plugin::~mysql_db_plugin()
{
      
}

void mysql_db_plugin::set_program_options(options_description&, options_description& cfg) {
   cfg.add_options()
         ("mysqldb-queue-size", bpo::value<uint32_t>()->default_value(100000),
         "Query queue size.")
         ("mysqldb-query-thread", bpo::value<uint32_t>()->default_value(4),
         "Query work thread count.")
         ("mysqldb-wipe", bpo::bool_switch()->default_value(false),
         "Required with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks to wipe mysql db."
         "This option required to prevent accidental wipe of mysql db.")
         ("mysqldb-host", bpo::value<std::string>(),
         "mysqlDB host address string")
         ("mysqldb-port", bpo::value<uint16_t>()->default_value(3306),
         "mysqlDB port integer")
         ("mysqldb-user", bpo::value<std::string>(),
         "mysqlDB user id string")
         ("mysqldb-passwd", bpo::value<std::string>(),
         "mysqlDB user password string")
         ("mysqldb-database", bpo::value<std::string>(),
         "mysqlDB database name string")
         ("mysqldb-engine", bpo::value<std::string>()->default_value("InnoDB"),
         "mysqlDB database engine string")

         ("mysqldb-max-connection", bpo::value<uint16_t>()->default_value(5),
         "mysqlDB max connection. " 
         "Should be one or more larger then mysqldb-query-thread value.")
         ("mysqldb-close-on-unlock", bpo::bool_switch()->default_value(false),
         "Close connection from db when release lock.")
         ("mysqldb-proc-action-accs", bpo::bool_switch()->default_value(false),
         "Call 'proc_action_accs' sp when irreversible block recorded.")
         ("mysqldb-proc-action-accs-int", bpo::value<uint16_t>()->default_value(5),
         "Call interval for 'proc_action_accs'")
         ("mysqldb-use-action-reversible", bpo::bool_switch()->default_value(false),
         "Use action reversible table 'actions_accounts_r'")


         ("mysqldb-block-start", bpo::value<uint32_t>()->default_value(0),
         "If specified then only abi data pushed to mysqldb until specified block is reached.")
         ("mysqldb-block-end", bpo::value<uint32_t>()->default_value(0),
         "stop when reached end block number.")

         // bulk aggregation count
         ("mysqldb-ag-account", bpo::value<uint32_t>(),
         "account db aggregation count")
         ("mysqldb-ag-action-raw", bpo::value<uint32_t>(),
         "action raw db aggregation count")
         ("mysqldb-ag-action-acc", bpo::value<uint32_t>(),
         "action acc db aggregation count")
         ("mysqldb-ag-block", bpo::value<uint32_t>(),
         "block aggregation count")
         ("mysqldb-ag-transaction", bpo::value<uint32_t>(),
         "transaction aggregation count")
         ("mysqldb-ag-trace", bpo::value<uint32_t>(),
         "trace aggregation count")
         ;
}

void mysql_db_plugin::plugin_initialize(const variables_map& options) {
   try {
      if( options.count( "mysqldb-host" )) {
         ilog( "initializing mysql_db_plugin" );
         my->configured = true;

         if( options.at( "replay-blockchain" ).as<bool>() || options.at( "hard-replay-blockchain" ).as<bool>() || options.at( "delete-all-blocks" ).as<bool>() ) {
            if( options.at( "mysqldb-wipe" ).as<bool>()) {
               ilog( "Wiping mysql database on startup" );
               my->wipe_database_on_startup = true;
            } else if( options.count( "mysqldb-block-start" ) == 0 ) {
               EOS_ASSERT( false, chain::plugin_config_exception, "--mysqldb-wipe required with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks"
                                 " --mysqldb-wipe will remove all EOS collections from mysqldb." );
            }
         }

        /*
         if( options.at( "replay-blockchain" ).as<bool>() || options.at( "hard-replay-blockchain" ).as<bool>() ) {
            my->is_replaying = true; 
         }
         */

         if( options.count( "mysqldb-block-end" ) ) {
            my->end_block_num = options.at("mysqldb-block-end").as<uint32_t>();
         }
         if( options.count( "abi-serializer-max-time-ms") == 0 ) {
            EOS_ASSERT(false, chain::plugin_config_exception, "--abi-serializer-max-time-ms required as default value not appropriate for parsing full blocks");
         }
         my->abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();

         if( options.count( "mysqldb-queue-size" )) {
            my->max_queue_size = options.at( "mysqldb-queue-size" ).as<uint32_t>();
         }

         if( options.count( "mysqldb-query-thread" )) {
            my->query_thread_count = options.at( "mysqldb-query-thread" ).as<uint32_t>();
         }
         
         if( options.count( "mysqldb-block-start" )) {
            my->start_block_num = options.at( "mysqldb-block-start" ).as<uint32_t>();
         }
         if( options.count( "producer-name") ) {
            wlog( "MySQL plugin not recommended on producer node" );
            //my->is_producer = true;
         }
         /*
         if( options.count( "mysqldb-action-idx")) {
            my->start_action_idx = options.at( "mysqldb-action-idx" ).as<uint64_t>();
         }
         */
         if( my->start_block_num == 0 ) {
            my->start_block_reached = true;
         }

         uint16_t port = 3306;
         uint16_t max_conn = 5;

         // create mysql db connection pool
         std::string host_str = options.at("mysqldb-host").as<std::string>();
         if( options.count( "mysqldb-port" )) {
            port = options.at("mysqldb-port").as<uint16_t>();
         }
         std::string userid = options.at("mysqldb-user").as<std::string>();
         std::string pwd = options.at("mysqldb-passwd").as<std::string>();
         std::string database = options.at("mysqldb-database").as<std::string>();
         if( options.count( "mysqldb-max-connection" )) {
            max_conn = options.at("mysqldb-max-connection").as<uint16_t>();
         }

         if ( options.count( "mysqldb-proc-action-accs" )) {
            call_sp_proc_action_accs = options.at("mysqldb-proc-action-accs").as<bool>();
         }

         if ( options.count( "mysqldb-proc-action-accs-int")) {
            call_sp_proc_action_accs_int =  options.at("mysqldb-proc-action-accs-int").as<uint16_t>();
         }
         
         if ( options.count( "mysqldb-use-action-reversible" )) {
            use_action_reversible = options.at("mysqldb-use-action-reversible").as<bool>();
         }

         
         // hook up to signals on controller
         chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
         EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, ""  );
         auto& chain = chain_plug->chain();
         my->chain_id.emplace( chain.get_chain_id());

         my->accepted_block_connection.emplace( 
            chain.accepted_block.connect( [&]( const chain::block_state_ptr& bs ) {
               my->accepted_block( bs );
            } ));
         my->irreversible_block_connection.emplace(
            chain.irreversible_block.connect( [&]( const chain::block_state_ptr& bs ) {
               my->applied_irreversible_block( bs );
            } ));
         
      //    my->accepted_transaction_connection.emplace(
      //          chain.accepted_transaction.connect( [&]( const chain::transaction_metadata_ptr& t ) {
      //             my->accepted_transaction( t );
      //          } ));
         my->applied_transaction_connection.emplace(
            chain.applied_transaction.connect( [&]( const chain::transaction_trace_ptr& t ) {
               my->applied_transaction( t );
            } ));
         
         ilog( "connect to ${h}:${p}. ${u}@${d} ", ("h", host_str)("p", port)("u", userid)("d", database));
         bool close_on_unlock = options.at("mysqldb-close-on-unlock").as<bool>();
         my->init(host_str, userid, pwd, database, port, max_conn, close_on_unlock, my->start_block_num, options);
         
      } else {
         wlog( "eosio::mysql_db_plugin configured, but no --mysqldb-uri specified." );
         wlog( "mysql_db_plugin disabled." );
      }
   } FC_LOG_AND_RETHROW()
}

void mysql_db_plugin::plugin_startup() {

}

void mysql_db_plugin::plugin_shutdown() {
   try {
      my->_timer.cancel();
   } catch(fc::exception& e) {
      edump((e.to_detail_string()));
   }

   my->accepted_block_connection.reset();
   my->irreversible_block_connection.reset();
//    my->accepted_transaction_connection.reset();
   my->applied_transaction_connection.reset();

   my.reset();
}


void post_query_str_to_queue(const std::string query_str) {
      if (!static_mysql_db_plugin_impl) return; 

      static_mysql_db_plugin_impl->queue(
            static_mysql_db_plugin_impl->query_mtx,
            static_mysql_db_plugin_impl->query_condition,
            static_mysql_db_plugin_impl->query_queue, 
            query_str
      );
}

void post_odr_query_str_to_queue(const std::string query_str) {
      if (!static_mysql_db_plugin_impl) return; 

      static_mysql_db_plugin_impl->queue(
            static_mysql_db_plugin_impl->odr_query_mtx,
            static_mysql_db_plugin_impl->odr_query_condition,
            static_mysql_db_plugin_impl->odr_query_queue, 
            query_str
      );
}

      
}
