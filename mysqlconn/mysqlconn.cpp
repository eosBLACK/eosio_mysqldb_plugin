#include "mysqlconn.h"

//----------------

LockableObj::LockableObj() {

} 

LockableObj::~LockableObj() {
    _cts.unlock(); 
}

void LockableObj::unlock() {
    _cts.unlock(); 
    // std::cout << "Unlock!! " << std::this_thread::get_id() << std::endl; 
}

void LockableObj::lock() {
    _cts.lock(); 
    // std::cout << "Lock!! " << std::this_thread::get_id() << std::endl; 
}

bool LockableObj::trylock() {
    return _cts.try_lock();
}

//----------------

const string MysqlRow::get_value(const size_t index) const {
  if ( index < _values.size() ) 
    return _values[index];
  else
    return ""; 
}

void MysqlRow::set_value(const size_t index, const string value) {
  if ( index < _values.size() ) 
    _values[index] = value; 
}

void MysqlRow::add_value(const string value) {
    _values.push_back( value );
}

//----------------

MysqlData::MysqlData(): _conn(nullptr), _sqlResult(nullptr) {

} 

MysqlData::~MysqlData() {
    if (_sqlResult)
        mysql_free_result(_sqlResult);
}

bool MysqlData::store(MYSQL* conn) {
    if (!conn) return false; 

    _sqlResult = mysql_use_result(conn);
    if (!_sqlResult) return false; 

    _conn = conn; 

    _columns.clear(); 
    for ( unsigned int i=0; i< mysql_num_fields(_sqlResult); i++ ) {
        MYSQL_FIELD* sqlField = mysql_fetch_field_direct(_sqlResult, i);
        _columns.push_back( sqlField->name );
    }

    return true; 
}

bool MysqlData::is_valid() const {
    return _sqlResult;
}


const size_t MysqlData::get_columnCount() const {
    return _columns.size(); 
}

const string MysqlData::get_columnName(const size_t index) {
  if ( index < _columns.size() ) 
    return _columns[index];
  else
    return ""; 

}

shared_ptr<MysqlRow> MysqlData::next() {
    MYSQL_ROW sqlRow = mysql_fetch_row(_sqlResult);
    if (!sqlRow) return nullptr; 
    

    shared_ptr<MysqlRow> retData( new MysqlRow ); 
    unsigned long *lengths = mysql_fetch_lengths(_sqlResult);
    for (size_t i=0; i<_columns.size(); i++) {
        retData->add_value( string(sqlRow[i], lengths[i]) );
    }

    return retData; 
} 

bool MysqlData::more() {
    _columns.clear(); 

    if (_sqlResult) {
        mysql_free_result(_sqlResult);
        _sqlResult = nullptr; 
    }

    if (_conn && mysql_next_result(_conn) == 0) {
        store(_conn); 
        return true; 
    }

    return false; 
}


//----------------
MysqlConnection::MysqlConnection(): _mysql(nullptr), _conn(nullptr) {

}

MysqlConnection::~MysqlConnection() {
    disconnect(); 

}


bool MysqlConnection::connect(
    const string host, 
    const string user, 
    const string passwd, 
    const string database,
    unsigned int port
) {
    disconnect(); 

    _mysql = mysql_init(nullptr);
    // 압축전송 사용.
    mysql_options(_mysql, MYSQL_OPT_COMPRESS, nullptr);

    _conn =
        mysql_real_connect(
            _mysql,
            host.c_str(), 
            user.c_str(),
            passwd.c_str(),
            database.c_str(),
            port,
            nullptr,
            CLIENT_COMPRESS | CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS
        );

    if (!_conn) {
        mysql_close(_mysql);
        _mysql = nullptr;
        return false; 
    }

    // Force server character-set
    exec("SET NAMES utf8mb4");
    // Force timezone to UTC
    exec("SET time_zone = '+00:00'");
    
    return true; 

}

bool MysqlConnection::disconnect() {
    if (_mysql) {
        mysql_close(_mysql);

        _mysql = nullptr; 
        _conn  = nullptr; 

        return true; 
    }

    return false; 
}


bool MysqlConnection::is_connected() const {
    return _conn;
}


shared_ptr<MysqlData> MysqlConnection::open(const string query) const {
    shared_ptr<MysqlData> retData( new MysqlData ); 

    if (is_connected()) {
        int retVal = mysql_real_query(_conn, query.c_str(), query.size());
        if (retVal == 0) {
            retData->store(_conn);
        }
    }

    return retData; 
}

// 커서 리턴없는 쿼리 전용. store/use 없이 단순히 mysql_next_result로 처리 가능. 
// https://dev.mysql.com/doc/refman/8.0/en/c-api-multiple-queries.html
bool MysqlConnection::exec(const string query, const bool multiline, my_ulonglong* affectRowsPtr) const {

    if (mysql_real_query(_conn, query.c_str(), query.size()) == 0) {
        if (affectRowsPtr) 
            *affectRowsPtr = mysql_affected_rows(_mysql);

        if (multiline) {
            //MYSQL_RES* sqlResult = mysql_use_result(_conn);
            while (mysql_next_result(_conn) == 0) {
                //mysql_free_result(sqlResult); 

                if (affectRowsPtr) 
                    *affectRowsPtr += mysql_affected_rows(_mysql);

                //sqlResult = mysql_use_result(_conn);
            }
            //mysql_free_result(sqlResult); 
        }
        return true; 
    } else {
        return false; 
    }

    //return mysql_real_query(_conn, query.c_str(), query.size()) == 0; 
}

bool MysqlConnection::execute(const string query, const bool multiline, my_ulonglong* affectRowsPtr) const {
    if (transactionOnExecute) transactionStart(); 
    bool retVal = exec(query, multiline, affectRowsPtr); 
    if (transactionOnExecute) transactionCommit(); 

    return retVal; 
}


bool MysqlConnection::ping() const {
    return _conn && (mysql_ping(_conn) == 0);
} 

my_ulonglong MysqlConnection::affectrows() const {
    return mysql_affected_rows(_mysql);
}

const char * MysqlConnection::lastError() const {
    return mysql_error(_mysql);
}

long long MysqlConnection::lastInsertID() const {
    long long retVal = 0; 

    auto data = open("SELECT LAST_INSERT_ID() AS ID");
    if (data->is_valid()) {
        auto rowObj = data->next(); 
        if (rowObj) {
            try {
                retVal = std::stoll(rowObj->get_value(0));
            }
            //catch (const std::invalid_argument& is) {
            catch (...) {
                retVal = 0;
            }
        }
    }

    return retVal; 
} 

void  MysqlConnection::unlock(bool do_disconnect) {
    if (do_disconnect) disconnect(); 
    LockableObj::unlock(); 
} 

void MysqlConnection::lock() {
    LockableObj::lock(); 
}

bool MysqlConnection::trylock() {
    return LockableObj::trylock(); 

}

string MysqlConnection::escapeString(const string input) const {
    /*
    char* dest = new char[input.size() * 2];

    //auto len = 
    escapeString(dest, input.c_str(), input.size()); 

    string retStr(dest); 
    delete [] dest; 
    return retStr; 
    */

    string retStr;
    retStr.resize(input.size() * 2); 

    auto len = escapeString(&(*retStr.begin()), input.c_str(), input.size()); 
    retStr.resize(len); 
    return retStr; 
}

unsigned long MysqlConnection::escapeString( char *to, const char *from, unsigned long length) const {
    //return mysql_real_escape_string(_conn, to, from, length);
    return mysql_real_escape_string_quote(_conn, to, from, length, '\'');
}


void MysqlConnection::transactionStart() const {
  exec("SET AUTOCOMMIT=0");
  exec("BEGIN");
}

void MysqlConnection::transactionCommit() const {
  exec("COMMIT");
  exec("SET AUTOCOMMIT=1");
}

void MysqlConnection::transactionRollback() const {
  exec("ROLLBACK");
  exec("SET AUTOCOMMIT=1");
}


void MysqlConnection::print() const {
    std::cout << "libmysql\t" << mysql_get_client_info() << std::endl; 
    if (_conn) {
        std::cout << "server  \t" << mysql_get_server_info(_conn) << std::endl; 
    }
}

//---------
MysqlConnPool::MysqlConnPool(
    const unsigned int poolCount,
    const string host, 
    const string user, 
    const string passwd, 
    const string database,
    unsigned int port

): _host(host), _user(user), _passwd(passwd), _database(database), _port(port), _connIndex(0) {
    for (unsigned int i=0; i< poolCount; i++) {
        _connList.push_back( shared_ptr<MysqlConnection>( new MysqlConnection ) ); 
    }
}

MysqlConnPool::~MysqlConnPool() {
    
}

const bool MysqlConnPool::checkConnection() const {
    MysqlConnection conn; 
    return conn.connect(_host, _user, _passwd, _database, _port);
}

shared_ptr<MysqlConnection> MysqlConnPool::lockConnection() {
    //if (_connList.size() == 0) return nullptr; 
    assert(_connList.size() > 0);

    shared_ptr<MysqlConnection> retConn = nullptr; 

    for ( size_t i=0; i< _connList.size(); i++ ) {
        if (_connList[i]->trylock()) {
            retConn = _connList[i]; 
            break; 
        }
    }

    if (!retConn) {
        lock(); 
        retConn = _connList[_connIndex];
        _connIndex++; 
        if (_connIndex >= _connList.size()) _connIndex =0; 
        unlock(); 

        retConn->lock(); 
    }

    if ( !retConn->is_connected()  || !retConn->ping())  
        retConn->connect(_host, _user, _passwd, _database, _port);

    while ( !retConn->is_connected()  || !retConn->ping() ) {
        //std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        retConn->connect(_host, _user, _passwd, _database, _port);
    }


/*
    do {
        retConn->connect(_host, _user, _passwd, _database, _port);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } while ( !retConn->is_connected()  || !retConn->ping() );

    if ( !retConn->is_connected()  || !retConn->ping())  
        retConn->connect(_host, _user, _passwd, _database, _port);


    if (!retConn->is_connected()) {
        retConn->unlock(); 
        return nullptr; 
    }
*/
    return retConn; 
}

