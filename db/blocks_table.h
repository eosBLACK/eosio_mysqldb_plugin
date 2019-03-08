#ifndef BLOCKS_TABLE_H
#define BLOCKS_TABLE_H

#include <memory>
#include <chrono>

#include <fc/io/json.hpp>
#include <fc/variant.hpp>

#include <eosio/chain/block_state.hpp>

#include "connection_pool.h"

namespace eosio {

class blocks_table
{
public:
    //blocks_table(std::shared_ptr<connection_pool> pool, uint32_t block_bulk_max_count, bool call_sp_proc_action_accs);
    blocks_table(std::shared_ptr<connection_pool> pool, uint32_t block_bulk_max_count);

    void drop();
    void create(const string engine);

    void add_block(const chain::signed_block_ptr block, bool irreversible);
    void set_irreversible(const chain::signed_block_ptr block);
    void finalize();

    void tick(const int64_t tick);
private:
    void post_query();

    std::shared_ptr<connection_pool> m_pool;

    uint32_t _last_irreversible = 0; 
    uint32_t _last_call_sp_block = 0; 

    uint32_t _block_bulk_max_count;
    //bool _call_sp_proc_action_accs;
    int64_t call_sp_tick = 0; 

    uint32_t block_bulk_count = 0;
    int64_t block_bulk_insert_tick = 0;
    std::ostringstream block_bulk_sql;
};

} // namespace

#endif // BLOCKS_TABLE_H
