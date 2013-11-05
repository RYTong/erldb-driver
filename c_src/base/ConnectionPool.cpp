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
 * $Id: ConnectionPool.cpp 22170 2010-02-23 08:54:46Z deng.lifen $
 *
 *  @file ConnectionPool.cpp
 *  @brief Connection pool class.
 *  @author deng.lifen <deng.lifen@rytong.com>
 *  @version 1.0.0
 *  @date Fri Nov 13 15:29:14 CST 2009
 */

#include "ConnectionPool.h"
#include "../util/SysLogger.h"

using namespace rytong;


void ConnectionPool::set_pool(DatabaseType type, const char* host, const char* user,
        const char* password, const char* db_name, unsigned int port, int size) {
    Connection* conn;
    for (int i = 0; i < size; i++) {
        conn = create_conn(type, host, user, password, db_name, port);

        if (conn != NULL) {
            pool_.push(conn);
        }

    }
}

Connection* ConnectionPool::create_conn(DatabaseType type, const char* host, const char* user,
        const char* password, const char* db_name, unsigned int port) {
    Connection* conn = Connection::create(type);
    // cout << "conn: " << conn << endl;
    /** @warning We should make sure the interface 'connect' is consistent
     *   in every derived class.
     */
    if (!conn->connect(host, user, password, db_name, port)) {
        SysLogger::error("invalid args to start connection");
        // cout << "\rinvalid args to start connection" << endl;
        delete conn;
        return NULL;
    }
    return conn;
}

ConnectionPool::~ConnectionPool() {
    Connection* conn;
    // delete connections in the pool
    while (!pool_.empty()) {
        conn = pool_.front();
        delete conn;
        pool_.pop();
    }
    SysLogger::debug("delete allConnection!");
}


bool ConnectionPool::push(Connection* conn) {
    if (!mutex_.lock()) {
        return false;
    }
    if (NULL != conn) {
        pool_.push(conn);
    }
    mutex_.unlock();
    return true;
}

Connection* ConnectionPool::pop() {
    Connection* conn = NULL;
    if (!mutex_.lock()) {
        return conn;
    }
    if (!pool_.empty()) {
        conn = pool_.front();
        pool_.pop();
    }
    mutex_.unlock();
    return conn;
}
