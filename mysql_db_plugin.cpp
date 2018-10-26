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

class mysql_db_plugin_impl {
public:
   mysql_db_plugin_impl();
   ~mysql_db_plugin_impl();

   fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
   fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
   fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
   fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;

   void consume_accepted_transactions();
   void consume_applied_transactions();
   void consume_blocks();
   void consume_actions_processed();

   void accepted_block( const chain::block_state_ptr& );
   void applied_irreversible_block(const chain::block_state_ptr&);
   void accepted_transaction(const chain::transaction_metadata_ptr&);
   void applied_transaction(const chain::transaction_trace_ptr&);
   void process_accepted_transaction(const chain::transaction_metadata_ptr&);
   void _process_accepted_transaction(const chain::transaction_metadata_ptr&);
   void process_applied_transaction(const chain::transaction_trace_ptr&);
   void _process_applied_transaction(const chain::transaction_trace_ptr&);
   void process_accepted_block( const chain::block_state_ptr& );
   void _process_accepted_block( const chain::block_state_ptr& );
   void process_irreversible_block(const chain::block_state_ptr&);
   void _process_irreversible_block(const chain::block_state_ptr&);

   bool add_action_trace( uint32_t action_id, uint32_t parent_action_id, const chain::action_trace& atrace,
                          bool executed, uint32_t act_num, std::string* qry_actions, std::string* qry_actions_account );

   void init(const std::string host, const std::string user, const std::string passwd, const std::string database, const uint16_t port, const uint16_t max_conn, uint32_t block_num_start);
   void wipe_database();

   bool configured{false};
   bool wipe_database_on_startup{false};
   uint32_t start_block_num = 0;
   bool start_block_reached = false;

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

   size_t queue_size = 0;
   std::deque<chain::transaction_metadata_ptr> transaction_metadata_queue;
   std::deque<chain::transaction_metadata_ptr> transaction_metadata_process_queue;
   std::deque<chain::transaction_trace_ptr> transaction_trace_queue;
   // std::deque<chain::transaction_trace_ptr> transaction_trace_process_queue;
   std::deque<chain::block_state_ptr> block_state_queue;
   std::deque<chain::block_state_ptr> block_state_process_queue;
   std::deque<chain::block_state_ptr> irreversible_block_state_queue;
   std::deque<chain::block_state_ptr> irreversible_block_state_process_queue;
   boost::mutex mtx;
   boost::mutex mtx_applied_trans;
   boost::mutex mtx_action_id;
   boost::mutex mtx_action_processed;
   boost::condition_variable condition;
   boost::thread consume_thread_accepted_trans;
   boost::thread consume_thread_applied_trans;
//    boost::thread consume_thread_applied_trans[10];
   boost::thread consume_thread_blocks;
   boost::thread consume_thread_actions;
   boost::atomic<bool> done{false};
   boost::atomic<bool> startup{true};
   fc::optional<chain::chain_id_type> chain_id;
   fc::microseconds abi_serializer_max_time;

   static const account_name newaccount;
   static const account_name setabi;

   uint32_t m_action_id = 0;
};

namespace {

template<typename Queue, typename Entry>
void queue(boost::mutex& mtx, boost::condition_variable& condition, Queue& queue, const Entry& e, size_t max_queue_size) {
   int sleep_time = 0;
   
   boost::mutex::scoped_lock lock(mtx);
   auto queue_size = queue.size();
   if (queue_size > max_queue_size) {
      lock.unlock();
      condition.notify_one();
      sleep_time += 10;
      if( sleep_time > 1000 )
         wlog("queue size: ${q}", ("q", queue_size));
      boost::this_thread::sleep_for(boost::chrono::milliseconds(sleep_time));
      lock.lock();
   } else {
      sleep_time -= 10;
      if( sleep_time < 0 ) sleep_time = 0;
   }
   queue.emplace_back(e);
   lock.unlock();
   condition.notify_one();
}

}

void mysql_db_plugin_impl::accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   try {
      queue( mtx, condition, transaction_metadata_queue, t, queue_size );
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_transaction");
   }
}

void mysql_db_plugin_impl::applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      queue( mtx_applied_trans, condition, transaction_trace_queue, t, queue_size );
   } catch (fc::exception& e) {
      elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_transaction");
   }
}

void mysql_db_plugin_impl::applied_irreversible_block( const chain::block_state_ptr& bs ) {
   try {
      queue( mtx, condition, irreversible_block_state_queue, bs, queue_size );
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
      queue( mtx, condition, block_state_queue, bs, queue_size );
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_block");
   }
}

void mysql_db_plugin_impl::consume_actions_processed() {
   static bool b_inserted = true;
   int32_t max_raw_id = 0;
   int32_t last_action_id = 0;

   try {
      while(true) {
         boost::mutex::scoped_lock lock(mtx_action_processed);
         while ( !b_inserted &&
                 !done ) {
            condition.wait(lock);
         }

         max_raw_id = m_actions_table->get_max_id_raw();
         last_action_id = m_actions_table->get_max_id() - 1;

         lock.unlock();              

         if(last_action_id < max_raw_id) {
            b_inserted = false;
            b_inserted = m_actions_table->generate_actions_table(last_action_id);
         }

         if( done ) {
            break;
         }
      }
   } catch (...) {
      elog("Unknown exception while accepted_block");
   }
}

void mysql_db_plugin_impl::consume_accepted_transactions() {
   try {
      while (true) {
         boost::mutex::scoped_lock lock(mtx);
         while ( transaction_metadata_queue.empty() &&
                 !done ) {
            condition.wait(lock);
         }

         // capture for processing
         size_t transaction_metadata_size = transaction_metadata_queue.size();
         if (transaction_metadata_size > 0) {
            transaction_metadata_process_queue = move(transaction_metadata_queue);
            transaction_metadata_queue.clear();
         }
         
         lock.unlock();

         // warn if queue size greater than 75%
         if( transaction_metadata_size > (queue_size * 0.75)) {
            wlog("queue size: ${q}", ("q", transaction_metadata_size));
         } else if (done) {
            ilog("draining queue, size: ${q}", ("q", transaction_metadata_size));
         }

         // process transactions
         auto start_time = fc::time_point::now();
         auto size = transaction_metadata_process_queue.size();
         while (!transaction_metadata_process_queue.empty()) {
            const auto& t = transaction_metadata_process_queue.front();
            process_accepted_transaction(t);
            transaction_metadata_process_queue.pop_front();
         }
         auto time = fc::time_point::now() - start_time;
         auto per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_accepted_transaction,  time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );

         if( transaction_metadata_size == 0 &&
             done ) {
            break;
         }
      }
      ilog("mysql_db_plugin consume thread shutdown gracefully");
   } catch (fc::exception& e) {
      elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while consuming block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while consuming block");
   }
}

void mysql_db_plugin_impl::consume_applied_transactions() {
   std::deque<chain::transaction_trace_ptr> transaction_trace_process_queue;
   
   try {
      while (true) {
         boost::mutex::scoped_lock lock(mtx_applied_trans);
         while ( transaction_trace_queue.empty() &&
                 !done ) {
            condition.wait(lock);
         }

         // capture for processing
         size_t transaction_trace_size = transaction_trace_queue.size();
         if (transaction_trace_size > 0) {
            transaction_trace_process_queue = move(transaction_trace_queue);
            transaction_trace_queue.clear();
         }

         lock.unlock();

         // warn if queue size greater than 75%
         if( transaction_trace_size > (queue_size * 0.75)) {
            wlog("queue size: ${q}", ("q", transaction_trace_size));
         } else if (done) {
            ilog("draining queue, size: ${q}", ("q", transaction_trace_size));
         }

         // process transactions
         auto start_time = fc::time_point::now();
         auto size = transaction_trace_process_queue.size();
         while (!transaction_trace_process_queue.empty()) {
            const auto& t = transaction_trace_process_queue.front();
            process_applied_transaction(t);
            transaction_trace_process_queue.pop_front();
         }
         auto time = fc::time_point::now() - start_time;
         auto per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_applied_transaction, time per: ${p}, size: ${s}, time: ${t}", ("s", size)( "t", time )( "p", per ));

         if( transaction_trace_size == 0 &&
             done ) {
            break;
         }
      }
      ilog("mysql_db_plugin consume thread shutdown gracefully");
   } catch (fc::exception& e) {
      elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while consuming block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while consuming block");
   }
}

void mysql_db_plugin_impl::consume_blocks() {
   try {
      while (true) {
         boost::mutex::scoped_lock lock(mtx);
         while ( block_state_queue.empty() &&
                 irreversible_block_state_queue.empty() &&
                 !done ) {
            condition.wait(lock);
         }

         // capture for processing
         size_t block_state_size = block_state_queue.size();
         if (block_state_size > 0) {
            block_state_process_queue = move(block_state_queue);
            block_state_queue.clear();
         }

         size_t irreversible_block_size = irreversible_block_state_queue.size();
         if (irreversible_block_size > 0) {
            irreversible_block_state_process_queue = move(irreversible_block_state_queue);
            irreversible_block_state_queue.clear();
         }

         lock.unlock();

         // warn if queue size greater than 75%
         if( block_state_size > (queue_size * 0.75) ||
             irreversible_block_size > (queue_size * 0.75)) {
            wlog("queue size: ${q}", ("q", block_state_size + irreversible_block_size));
         } else if (done) {
            ilog("draining queue, size: ${q}", ("q", block_state_size + irreversible_block_size));
         }

         // process blocks
         auto start_time = fc::time_point::now();
         auto size = block_state_process_queue.size();
         while (!block_state_process_queue.empty()) {
            const auto& bs = block_state_process_queue.front();
            process_accepted_block( bs );
            block_state_process_queue.pop_front();
         }
         auto time = fc::time_point::now() - start_time;
         auto per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_accepted_block,       time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );

         // process irreversible blocks
         start_time = fc::time_point::now();
         size = irreversible_block_state_process_queue.size();
         while (!irreversible_block_state_process_queue.empty()) {
            const auto& bs = irreversible_block_state_process_queue.front();
            process_irreversible_block(bs);
            irreversible_block_state_process_queue.pop_front();
         }
         time = fc::time_point::now() - start_time;
         per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_irreversible_block,   time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );
            
         if( block_state_size == 0 &&
             irreversible_block_size == 0 &&
             done ) {
            break;
         }
      }
      ilog("mysql_db_plugin consume thread shutdown gracefully");
   } catch (fc::exception& e) {
      elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while consuming block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while consuming block");
   }
}

void mysql_db_plugin_impl::process_accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   try {
      _process_accepted_transaction(t);
   } catch (fc::exception& e) {
      elog("FC Exception while processing accepted transaction metadata: ${e}", ("e", e.to_detail_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing accepted tranasction metadata: ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing accepted transaction metadata");
   }
}

void mysql_db_plugin_impl::process_applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      if( start_block_reached ) {
         _process_applied_transaction( t );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while processing applied transaction trace: ${e}", ("e", e.to_detail_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing applied transaction trace: ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing applied transaction trace");
   }
}

void mysql_db_plugin_impl::process_irreversible_block(const chain::block_state_ptr& bs) {
  try {
     if( start_block_reached ) {
        _process_irreversible_block( bs );
     }
  } catch (fc::exception& e) {
     elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
  } catch (std::exception& e) {
     elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
  } catch (...) {
     elog("Unknown exception while processing irreversible block");
  }
}

void mysql_db_plugin_impl::process_accepted_block( const chain::block_state_ptr& bs ) {
   try {
      if( !start_block_reached ) {
         if( bs->block_num >= start_block_num ) {
            start_block_reached = true;
         }
      }
      if( start_block_reached ) {
         _process_accepted_block( bs );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while processing accepted block trace ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing accepted block trace ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing accepted block trace");
   }
}

bool mysql_db_plugin_impl::add_action_trace(uint32_t action_id, uint32_t parent_action_id, const chain::action_trace& atrace,
                                        bool executed, uint32_t act_num, std::string* qry_actions, std::string* qry_actions_account )
{
   uint32_t p_action_id = parent_action_id;
   const auto trx_id = atrace.trx_id;
   std::ostringstream oss_actions;
   std::ostringstream oss_actions_account;

   if( executed && atrace.receipt.receiver == chain::config::system_account_name ) {
      m_accounts_table->update_account( atrace.act, trx_id );
   }
   bool added = false;

   if( start_block_reached ) {
      const chain::base_action_trace& base = atrace; // without inline action traces
      std::string stmt_actions;
      std::string stmt_actions_account;

      m_actions_table->createInsertStatement_actions_raw(action_id, parent_action_id, atrace.receipt.receiver.to_string(), atrace.act, trx_id, act_num, &stmt_actions, &stmt_actions_account);

      oss_actions << stmt_actions;
      oss_actions_account << stmt_actions_account << ";";

      added = true;
   }

   qry_actions->append(oss_actions.str());
   qry_actions_account->append(oss_actions_account.str());

   // uint32_t old_action_id = action_id;
   for( const auto& iline_atrace : atrace.inline_traces ) {
      boost::mutex::scoped_lock lock(mtx_action_id);
      m_action_id++;
      lock.unlock();
      added |= add_action_trace( m_action_id, action_id, iline_atrace, executed, act_num, qry_actions, qry_actions_account );
      
   }

   return added;
}

void mysql_db_plugin_impl::_process_accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   auto now = std::chrono::seconds{fc::time_point::now().time_since_epoch().count()};

   
}

void mysql_db_plugin_impl::_process_applied_transaction( const chain::transaction_trace_ptr& t ) {
   bool write_atraces = false;
   bool executed = t->receipt.valid() && t->receipt->status == chain::transaction_receipt_header::executed;

   uint32_t act_num = 0;
   std::string qry_actions;
   std::string qry_actions_account;

   m_transactions_table->add_trace(t->id,t->receipt->cpu_usage_us,t->receipt->net_usage_words,t->elapsed.count(),t->scheduled);
   
   auto start_time = fc::time_point::now();

   for( const auto& atrace : t->action_traces ) {
      try {
         if(m_action_id == 0)
            return;
         write_atraces |= add_action_trace( m_action_id, 0, atrace, executed, act_num, &qry_actions, &qry_actions_account );
         boost::mutex::scoped_lock lock(mtx_action_id);
         m_action_id++;
         lock.unlock();
         ++act_num;
      } catch(...) {
         wlog("add action traces failed.");
      }
   }

   if( !start_block_reached ) return;
   if( !write_atraces ) return; //< do not insert transaction_trace if all action_traces filtered out

   if(qry_actions.length() < 1) return;
   
   m_actions_table->executeActions(qry_actions,qry_actions_account);

   auto time = fc::time_point::now() - start_time;
   if( time > fc::microseconds(500000) )
      ilog( "process actions, trans_id: ${r}    time: ${t}", ("r",t->id.str())("t", time) );

}

void mysql_db_plugin_impl::_process_accepted_block( const chain::block_state_ptr& block ) {
   string qry_block;
   string qry_transaction;

   try {      
      m_blocks_table->createBlockStatement(block->block, &qry_block);
      int trx_no = 0;
      for (const auto &transaction : block->trxs) {
            m_transactions_table->createTransactionStatement(block->block_num, transaction->trx, &qry_transaction);
      }
    } catch (const std::exception &ex) {
        elog("${e}", ("e", ex.what())); // prevent crash
    }
   
   m_blocks_table->executeBlocks(qry_block,qry_transaction);
}

void mysql_db_plugin_impl::_process_irreversible_block(const chain::block_state_ptr& block)
{
   const auto block_id = block->block->id();
   const auto block_id_str = block_id.str();
   const auto block_num = block->block->block_num();

   // genesis block 1 is not signaled to accepted_block
   if (block_num < 2) return;

   auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
         std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()});

   try {      
      m_blocks_table->add(block->block);

   } catch (const std::exception &ex) {
        elog("${e}", ("e", ex.what())); // prevent crash
    }
}

mysql_db_plugin_impl::mysql_db_plugin_impl(){}
mysql_db_plugin_impl::~mysql_db_plugin_impl() {
   if (!startup) {
      try {
         ilog( "mysql_db_plugin shutdown in process please be patient this can take a few minutes" );
         done = true;
         condition.notify_one();

         consume_thread_accepted_trans.join();
         consume_thread_applied_trans.join();
         consume_thread_blocks.join();
         consume_thread_actions.join();

      } catch( std::exception& e ) {
         elog( "Exception on mysql_db_plugin shutdown of consume thread: ${e}", ("e", e.what()));
      }
   }
}

void mysql_db_plugin_impl::wipe_database() {
   ilog("drop tables");

   // drop tables
   m_actions_table->drop();
   m_transactions_table->drop();
   m_blocks_table->drop();
   m_accounts_table->drop();

   ilog("create tables");
   // create Tables
   m_accounts_table->create();
   m_blocks_table->create();
   m_transactions_table->create();
   m_actions_table->create();

   m_accounts_table->add(system_account);
}

void mysql_db_plugin_impl::init(const std::string host, const std::string user, const std::string passwd, const std::string database, const uint16_t port, const uint16_t max_conn, uint32_t block_num_start) {

   m_connection_pool = std::make_shared<connection_pool>(host, user, passwd, database, port, max_conn);
   
   m_accounts_table = std::make_unique<accounts_table>(m_connection_pool);
   m_blocks_table = std::make_unique<blocks_table>(m_connection_pool);
   m_transactions_table = std::make_unique<transactions_table>(m_connection_pool);
   m_actions_table = std::make_unique<actions_table>(m_connection_pool);
   m_block_num_start = block_num_start;
   system_account = chain::name(chain::config::system_account_name).to_string();

   if( wipe_database_on_startup ) {
      wipe_database();
   } else {
      ilog("create tables");
      
      // create Tables
      m_accounts_table->create();
      m_blocks_table->create();
      m_transactions_table->create();
      m_actions_table->create();

      m_accounts_table->add(system_account);
   }

   // get last action_id from actions table
   m_action_id = m_actions_table->get_max_id();

   ilog("starting mysql db plugin thread");

//    consume_thread_accepted_trans = boost::thread([this] { consume_accepted_transactions(); });
   consume_thread_applied_trans = boost::thread([this] { consume_applied_transactions(); });
   consume_thread_blocks = boost::thread([this] { consume_blocks(); });
   // Thread for process action table
   consume_thread_actions = boost::thread([this] { consume_actions_processed(); });

   startup = false;
}

mysql_db_plugin::mysql_db_plugin()
:my(new mysql_db_plugin_impl())
{

}

mysql_db_plugin::~mysql_db_plugin()
{
      
}

void mysql_db_plugin::set_program_options(options_description&, options_description& cfg) {
   cfg.add_options()
         ("mysqldb-queue-size,q", bpo::value<uint32_t>()->default_value(256),
         "The target queue size between nodeos and mySQL DB plugin thread.")
         ("mysqldb-wipe", bpo::bool_switch()->default_value(false),
         "Required with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks to wipe mysql db."
         "This option required to prevent accidental wipe of mysql db.")
         ("mysqldb-block-start", bpo::value<uint32_t>()->default_value(0),
         "If specified then only abi data pushed to mysqldb until specified block is reached.")
         ("mysqldb-uri,m", bpo::value<std::string>(),
         "mysqlDB URI connection string."
               " If not specified then plugin is disabled. Default database 'EOS' is used if not specified in URI."
               " Example: mysql://localhost:3306/EOS?user=root&password=root")
         ("mysqldb-host", bpo::value<std::string>(),
         "mysqlDB host address string"
         " Example: mysql://localhost:3306/EOS?user=root&password=root")
         ("mysqldb-port", bpo::value<uint16_t>()->default_value(3306),
         "mysqlDB port integer")
         ("mysqldb-user", bpo::value<std::string>(),
         "mysqlDB user id string")
         ("mysqldb-passwd", bpo::value<std::string>(),
         "mysqlDB user password string")
         ("mysqldb-database", bpo::value<std::string>(),
         "mysqlDB database name string")
         ("mysqldb-max-connection", bpo::value<uint16_t>()->default_value(10),
         "mysqlDB max connection")
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

         if( options.count( "abi-serializer-max-time-ms") == 0 ) {
            EOS_ASSERT(false, chain::plugin_config_exception, "--abi-serializer-max-time-ms required as default value not appropriate for parsing full blocks");
         }
         my->abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();

         if( options.count( "mysqldb-queue-size" )) {
            my->queue_size = options.at( "mysqldb-queue-size" ).as<uint32_t>();
         }
         if( options.count( "mysqldb-block-start" )) {
            my->start_block_num = options.at( "mysqldb-block-start" ).as<uint32_t>();
         }
         if( my->start_block_num == 0 ) {
            my->start_block_reached = true;
         }

         uint16_t port = 3306;
         uint16_t max_conn = 20;

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

         ilog( "connecting to ${h} ${d} ${u} ${p}", ("h", host_str)("d", database)("u", userid)("p", port));
         
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

         my->init(host_str, userid, pwd, database, port, max_conn, my->start_block_num);
         
      } else {
         wlog( "eosio::mysql_db_plugin configured, but no --mysqldb-uri specified." );
         wlog( "mysql_db_plugin disabled." );
      }
   } FC_LOG_AND_RETHROW()
}

void mysql_db_plugin::plugin_startup() {

}

void mysql_db_plugin::plugin_shutdown() {
   my->accepted_block_connection.reset();
   my->irreversible_block_connection.reset();
//    my->accepted_transaction_connection.reset();
   my->applied_transaction_connection.reset();

   my.reset();
}

}
