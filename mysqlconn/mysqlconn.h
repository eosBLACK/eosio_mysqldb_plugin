#pragma once

//#include <cstdlib>
#include <cassert>
#include <vector>
#include <iostream>
#include <string>
#include <memory>
#include <mutex>
#include <thread>
#include <mysql.h>

using std::string; 
using std::shared_ptr;


// 커넥션 락 구현을 위해 mutex를 포장. 
class LockableObj {
public:
    LockableObj(); 
    virtual ~LockableObj(); 

    void unlock(); 
protected:
    virtual void lock(); 
    virtual bool trylock(); 

private:
    std::mutex _cts; 
};


// 한 줄의 정보를 저장
class MysqlRow {
public:
    const string get_value(const size_t index) const; 
    void set_value(const size_t index, const string value);
    void add_value(const string value); 
private:
    std::vector <string> _values;
};

// 한 줄씩 쿼리. 커서는 서버에
class MysqlData {
public:
    MysqlData(); 
    virtual ~MysqlData(); 

    bool store(MYSQL* conn);
    bool is_valid() const; 

    const size_t get_columnCount() const; 
    const string get_columnName(const size_t index); 

    shared_ptr<MysqlRow> next(); 
    bool more(); 

private:
    MYSQL* _conn;
    MYSQL_RES* _sqlResult;

    std::vector<string> _columns; 
};


class MysqlConnection: public LockableObj {
public:
    //bool transactionOnExecute = false; 
    bool transactionOnExecute = true; 

    MysqlConnection(); 
    virtual ~MysqlConnection(); 

    bool connect(
        const string host, 
        const string user, 
        const string passwd, 
        const string database,
        unsigned int port = 0 );
    bool disconnect();
    bool is_connected() const; 

    shared_ptr<MysqlData> open(const string query) const;
    bool exec(const string query, const bool multiline = false, my_ulonglong* affectRowsPtr = nullptr) const;
    bool execute(const string query, const bool multiline = false, my_ulonglong* affectRowsPtr = nullptr) const;

    bool ping() const; 
    my_ulonglong affectrows() const; 
    const char * lastError() const; 
    long long lastInsertID() const; 


    string escapeString(const string input) const;
    unsigned long escapeString( char *to, const char *from, unsigned long length) const;

    void transactionStart() const; 
    void transactionCommit() const; 
    void transactionRollback() const; 

    void print() const; 
public:
    void unlock(bool do_disconnect = false); 
public:
    virtual void lock() override; 
    virtual bool trylock() override; 

private:
    MYSQL* _mysql;
    MYSQL* _conn;
};

class MysqlConnPool: public LockableObj {
public:
    MysqlConnPool(
        const unsigned int poolCount,
        const string host, 
        const string user, 
        const string passwd, 
        const string database,
        unsigned int port = 0 );
    virtual ~MysqlConnPool();

    const bool checkConnection() const; 
    shared_ptr<MysqlConnection> lockConnection(); 

private:
    string _host;
    string _user;
    string _passwd;
    string _database;
    unsigned int _port;

    size_t _connIndex; 
    std::vector<shared_ptr<MysqlConnection>> _connList;  

};