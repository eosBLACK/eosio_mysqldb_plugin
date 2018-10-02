// extern "C" {
// #include "Config.h"
// }
#include <assert.h>
#include <fc/log/logger.hpp>
#include "connection_pool.h"
#include "mysqlconn.h"

#define BSIZE 2048

namespace eosio {

    connection_pool::connection_pool(const std::string host, const std::string user, const std::string passwd, const std::string database, const uint16_t port, const uint16_t max_conn) :
    m_pool(max_conn,host,user,passwd,database,port)
    {
        std::cout << max_conn << ", " 
            << host << ", "
            << user << ", "
            << passwd << ", "
            << database << ", "
            << port << std::endl; 
            
        if(!m_pool.checkConnection())
            ilog("not connected");
    }

    connection_pool::~connection_pool()
    {
        
    }

    shared_ptr<MysqlConnection> connection_pool::get_connection() {
        return m_pool.lockConnection();
    }

    void connection_pool::release_connection(MysqlConnection& con) {
        con.unlock();
    }
}