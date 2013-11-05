/**
 * Copyright (c) 2009-2010 Beijing RYTong Information Technologies, Ltd.
 * All rights reserved.
 *
 * The contents of this file are subject to the Erlang Database Driver
 * Public License Version 1.0, (the "License"); you may not use this
 * file except in compliance with the License. You should have received
 * a copy of the Erlang Database Driver Public License along with this
 * software. If not, it can be retrieved via the world wide web at
 * http://www.rytong.com/.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * $Id: Connection.cpp 21985 2010-02-04 09:46:20Z cao.xu $
 *
 *  @file Connection.cpp
 *  @brief Abstract base class to represent a database connection.
 *  @author deng.lifen <deng.lifen@rytong.com>
 *  @version 1.0.0
 *  @date Fri Nov 13 14:36:14 CST 2009
 */

#include "Connection.h"
#ifdef USE_MYSQL
#include "../mysql/MysqlConnection.h"
#endif
#ifdef USE_ORACLE
#include "../oracle/OracleConnection.h"
#endif
#ifdef USE_SYBASE
#include "../sybase/SybConnection.h"
#endif
#ifdef USE_DB2
#include "../DB2/DB2Connection.h"
#endif
#ifdef USE_INFORMIX
#include "../informix/InformixConnection.h"
#endif

using namespace rytong;

Connection::Connection() {
}

Connection::~Connection() {
}

Connection* Connection::create(DatabaseType db_type) {
    Connection *conn = NULL;
    switch (db_type) {
#ifdef USE_MYSQL
        case MYSQL_DB:
            conn = new MysqlConnection();
            break;
#endif
#ifdef USE_ORACLE
        case ORACLE_DB:
            conn = new OracleConnection();
            break;
#endif
#ifdef USE_SYBASE
        case SYBASE_DB:
            conn = new SybConnection();
            break;
#endif
#ifdef USE_DB2
        case DB2_DB:
            conn = new DB2Connection();
            break;
#endif
#ifdef USE_INFORMIX
        case INFORMIX_DB:
            conn = new InformixConnection();
            break;
#endif
        default:
            break;
    }
    return conn;
}

void Connection::destroy(Connection* conn) {
    delete conn;
}
