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

#include "base/ConnectionPool.h"
#include "SysLogger.h"

using namespace rytong;

ConnectionPool::ConnectionPool(DrvConf conf) {
    // cout << "here to construct coon pool " <<endl;
    Connection* conn;
        for (int i = 0; i < conf.poolsize; i++) {
            conn = create_conn(conf);
            if (conn != NULL) {
                pool_.push(conn);
            }

        }
}

Connection* ConnectionPool::create_conn(DrvConf conf) {
    Connection* conn = Connection::create(conf.db_type);
    /** @warning We should make Connection's interface 'connect' consistent in
     * every derived class.
     */
    if (!conn->connect(conf.host, conf.user, conf.password,
            conf.db_name, conf.port)) {
        SysLogger::error("invalid args to start connection");
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
        if (1 == pool_.size()) {
            cond_.broadcast();
        }
    }
    mutex_.unlock();
    return true;
}

Connection* ConnectionPool::pop() {
    Connection* conn = NULL;
    if (!mutex_.lock()) {
        return conn;
    }
    while (pool_.empty()) {
        if(ETIMEDOUT == cond_.timed_wait(mutex_, 1000)){
            mutex_.unlock();
            return conn;}
    }
    conn = pool_.front();
    pool_.pop();
    mutex_.unlock();
    return conn;
}
