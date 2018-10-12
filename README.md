# eosio_mysql_plugin

EOSIO plugin to register blockchain data into an MySQL database.

## Requirements
- Works on any EOSIO node that runs v1.1.0 and up.
- On Ubuntu install the package libmysqlclient-dev
```
sudo apt install libmysqlclient-dev
```

- On OSX
```
brew install mysql-client
# And Change CMakeLists.txt from
include_directories(${CMAKE_CURRENT_SOURCE_DIR} include db mysqlconn /usr/include/mysql)
# to
include_directories(${CMAKE_CURRENT_SOURCE_DIR} include db mysqlconn /usr/local/opt/mysql-client/include/mysql)
```

## Building the plugin [Install on your nodeos server]
### EOSIO v1.2.0 and up
You need to statically link this plugin with nodeos. To do that, pass the following flag to cmake command when building eosio:
```
-DEOSIO_ADDITIONAL_PLUGINS=<path-to-eosio-mysqldb-plugin>
```
### EOSIO v1.1.0 and up
1. Remove or comment out this line in CMakeLists.txt:
```
eosio_additional_plugin(mysql_db_plugin)
```

2. Copy this repo to `<eosio-source-dir>/plugins/` You should now have `<eosio-source-dir>/plugins/mysql_db_plugin`
add the following param on eosio_build.sh:
```
-DBUILD_MYSQL_DB_PLUGIN=true
```
add the following phrase on programs/nodeos/CMakeLists.txt:
```
if(BUILD_MYSQL_DB_PLUGIN)
  target_link_libraries( ${NODE_EXECUTABLE_NAME} PRIVATE -Wl,${whole_archive_flag} mysql_db_plugin -Wl,${no_whole_archive_flag} )
endif()
```

add the following phrase on plugins/CMakeLists.txt:
```
add_subdirectory(mysql_db_plugin)
```

compile and run.
```
$ nodeos --help

....
Config Options for eosio::mysql_db_plugin:
    --mysqldb-wipe = true                 if true, wipe all tables from database
    --mysqldb-queue-size  arg (=256)      The queue size between nodeos and MySQL 
                                          DB plugin thread.
    --mysqldb-host = arg                  MySQL DB host address.
                                          If not specified then plugin is disabled. 
                                          e.g. 127.0.0.1
    --mysqldb-port = <port no.>           port number e.g. 3306
    --mysqldb-user = <user name>
    --mysqldb-passwd = <password>
    --mysqldb-database = <database name>
    --mysqldb-max-connection = arg (=20)  max connection pool size.
....
```
