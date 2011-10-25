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
 *  @file OracleConnection.h
 *  @brief Oracle connection class.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-2-3
 */

#ifndef _RYT_ORACLE_CONNECTION_H
#define _RYT_ORACLE_CONNECTION_H
#include <occi.h>
#include <ei.h>
#include "base/Connection.h"

namespace rytong {
/** @brief Oracle connection class.
 */
class OracleConnection : public Connection {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    OracleConnection();
    
    /** @brief Destructor for the class.
     *  @return None.
     */
    ~OracleConnection();

    /** @brief Get oracle connection.
     *  @see Connection::get_connection.
     */
    void* get_connection();

    /** @brief Disconnect oracle database.
     *  @see Connection::disconnect.
     */
    bool disconnect();

    /** @brief Connect to oracle database.
     *  @see Connection::connect.
     */
    bool connect(const char *host, const char *user, const char *password,
        const char *db_name, unsigned int port);
    
    /** @brief Get the auto_commit_.
     *  @return The value of auto_commit_.
     */
    bool get_auto_commit() const;

    /** @brief Set the value of auto_commit_.
     *  @param auto_commit The value to set.
     */
    void set_auto_commit(bool auto_commit);

private:
    OracleConnection(const OracleConnection &);
    OracleConnection & operator=(const OracleConnection &);

    oracle::occi::Connection* conn_; // Oracle connection object.
    bool auto_commit_; // Auto commit flag.
};
}/* end of namespace rytong */
#endif /* _RYT_ORACLE_CONNECTION_H */
