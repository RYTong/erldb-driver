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

#include "base/Connection.h"
#include "Mutex.h"
#include "base/DrvConf.h"
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
    ConnectionPool(DrvConf conf);
    
    /** @brief Destructor for the class.
     *  @return None.
     */
    ~ConnectionPool();

    /** @brief Add connection to queue.
     *  @param conn The pointer to the connection.
     *  @return Is push successful.
     */
    bool push(Connection* conn);

    /** @brief Get connection from connection pool.
     *  @return The pointer to the connection.
     */
    Connection* pop();

    /** @brief Create a connection.
     *  @param conf Connect args by DrvConf struct.
     *  @return The pointer to the connection.
     */
    Connection* create_conn(DrvConf conf);

private:
    ConnectionPool(const ConnectionPool&);
    ConnectionPool & operator =(const ConnectionPool&);

    pool pool_; // connection pool
    Mutex mutex_; // operation mutex
    Condition cond_; // operation condition
};
}/* end of namespace rytong */
#endif    /* _RYT_CONNECTION_POOL_H */
