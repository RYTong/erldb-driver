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
 * $Id: Connection.h 21889 2010-02-02 05:10:06Z deng.lifen $
 *
 *  @file Connection.h
 *  @brief Abstract base class to represent a database connection.
 *  @author deng.lifen <deng.lifen@rytong.com>
 *  @version 1.0.0
 *  @date Fri Nov 13 14:36:14 CST 2009
 */

#ifndef _RYT_CONNECTION_H
#define _RYT_CONNECTION_H

#include <string>
#include <iostream>
#include "DrvConf.h"
using namespace std;

namespace rytong {
/** @brief Abstract base class to represent a database connection.
 */
class Connection {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    Connection();

    /** @brief Destructor for the class.
     *  @return None.
     */
    virtual ~Connection() = 0;

    /** @brief Create connection object.
     *  @param dbType The database type.
     *  @return The pointer to the connection.
     */
    static Connection* create(DatabaseType db_type);

    /** @brief Destroy connection.
     *  @param conn The pointer to the connection.
     *  @return None.
     */
    static void destroy(Connection* conn);

    /** @brief Get database name.
     *  @return Database name.
     */
    const char* get_db_name() {
        return db_name_;
    }

    /** @brief Get database type.
     *  @return Database type.
     */
    const DatabaseType get_db_type() {
        return db_type_;
    }

    void set_db_type(DatabaseType db_type) {
        db_type_ = db_type;
    }

    /** @brief Connect to database.
     *  @param host The host of database.
     *  @param user The user of database.
     *  @param password The password of database.
     *  @param db_name The name of database.
     *  @param port The port of database.
     *  @return Is successful.
     *  @retval true Connect success.
     *  @retval false Connect failed.
     */
    virtual bool connect(const char *host, const char *user,
            const char *password, const char *db_name, unsigned int port) = 0;

    /** @brief Get connection.
     *  @return The pointer of connection.
     *  @retval NULL Failed to get connection.
     */
    virtual void* get_connection() = 0;

    /** @brief Disconnect database.
     *  @return Is successful.
     *  @retval true Disconnect success.
     *  @retval false Disconnect failed.
     */
    virtual bool disconnect() = 0;

protected:
    char* db_name_; // Database name.
    DatabaseType db_type_; // Database type.

private:
    // These functions are not implemented to prohibit copy construction
    // and assignment by value.
    Connection & operator=(const Connection &);
    Connection(const Connection &);
};
}/* end of namespace rytong */

#endif    /* _RYT_CONNECTION_H */
