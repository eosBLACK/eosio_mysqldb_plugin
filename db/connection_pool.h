#ifndef CONNECTION_H
#define CONNECTION_H

#include <memory>
#include "mysqlconn.h"

namespace eosio {
class connection_pool
{
    public:
        explicit connection_pool(const std::string host, const std::string user, const std::string passwd, const std::string database, const uint16_t port, const uint16_t max_conn);
        ~connection_pool();
        
        shared_ptr<MysqlConnection> get_connection();
        void release_connection(MysqlConnection& con);

    private:
        MysqlConnPool m_pool;
};
}

#endif