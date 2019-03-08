// extern "C" {
// #include "Config.h"
// }
#include <assert.h>
#include <fc/log/logger.hpp>
#include "connection_pool.h"
#include "mysqlconn.h"

#define BSIZE 2048

namespace eosio {

    connection_pool::connection_pool( 
            const std::string host, const std::string user, const std::string passwd, const std::string database, 
            const uint16_t port, const uint16_t max_conn, const bool do_closeconn_on_unlock):
    m_pool(max_conn,host,user,passwd,database,port), _do_closeconn_on_unlock(do_closeconn_on_unlock)
    {
        /*
        std::cout << max_conn << ", " 
            << host << ", "
            << user << ", "
            << passwd << ", "
            << database << ", "
            << port << std::endl; 
        */    
        if(!m_pool.checkConnection()){
            //ilog("not connected");
            throw std::runtime_error( "Can't make connect to mysql!!" );
        } 
    }

    connection_pool::~connection_pool()
    {
        
    }

    shared_ptr<MysqlConnection> connection_pool::get_connection() {
        return m_pool.lockConnection();
    }

    void connection_pool::release_connection(MysqlConnection& con) {
        con.unlock(_do_closeconn_on_unlock);
    }
}