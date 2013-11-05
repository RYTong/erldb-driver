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
 * $Id: ConnectionPool.h 21912 2010-02-03 05:39:15Z deng.lifen $
 *
 *  @file ConnectionPool.h
 *  @brief Connection pool class.
 *  @author deng.lifen <deng.lifen@rytong.com>
 *  @version 1.0.0
 *  @date Fri Nov 13 15:29:14 CST 2009
 */

#ifndef _RYT_CONNECTION_POOL_H
#define _RYT_CONNECTION_POOL_H

#include "Connection.h"
#include "../util/Mutex.h"
#include "DrvConf.h"
#include <queue>

namespace rytong {
typedef queue<Connection*> pool;

/** @brief Connection pool class.
 */
class ConnectionPool {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    ConnectionPool(DatabaseType type, const char* host, const char* user,
            const char* password, const char* db_name, unsigned int port, int size){
        set_pool(type, host, user, password, db_name, port, size);
    }

    /** @brief Constructor for the class.
     *  @return None.
     */
    ConnectionPool(DrvConf conf){
        set_pool(conf.db_type, conf.host, conf.user,
            conf.password, conf.db_name, conf.port, conf.poolsize);
    }

    /** @brief Destructor for the class.
     *  @return None.
     */
    ~ConnectionPool();

    /** @brief Create a connection.
     *  @param .
     *  @return The pointer to the connection.
     */
    static Connection* create_conn(DatabaseType type, const char* host, const char* user,
            const char* password, const char* db_name, unsigned int port);

    /** @brief Add connection to queue.
     *  @param conn The pointer to the connection.
     *  @return Is push successful.
     */
    bool push(Connection* conn);

    /** @brief Get connection from connection pool.
     *  @return The pointer to the connection.
     */
    Connection* pop();


private:
    ConnectionPool(const ConnectionPool&);
    ConnectionPool & operator =(const ConnectionPool&);


    void set_pool(DatabaseType type, const char* host, const char* user,
            const char* password, const char* db_name, unsigned int port, int size);


    pool pool_; // connection pool
    Mutex mutex_; // operation mutex

};
}/* end of namespace rytong */
#endif    /* _RYT_CONNECTION_POOL_H */
