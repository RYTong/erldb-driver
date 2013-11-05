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
 * $Id: MysqlConnection.h 21889 2010-02-02 05:10:06Z deng.lifen $
 *
 *  @file MysqlConnection.h
 *  @brief Mysql connection class.
 *  @author deng.lifen <deng.lifen@rytong.com>
 *  @version 1.0.0
 *  @date Fri Nov 13 14:36:14 CST 2009
 */

#ifndef _RYT_MYSQL_CONNECTION_H
#define _RYT_MYSQL_CONNECTION_H

#include <mysql.h>
#include "../base/Connection.h"
#include "../util/SysLogger.h"

namespace rytong{
/** @brief Mysql connection class.
 */
class MysqlConnection : public Connection {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    MysqlConnection();

    /** @brief Destructor for the class.
     *  @return None.
     */
    ~MysqlConnection();

    /** @brief Get mysql connection.
     *  @see Connection::get_connection.
     */
    void* get_connection() {
        // auto reconnect
        int state = mysql_ping(conn_);
        // error CR_COMMANDS_OUT_OF_SYNC
        // Commands were executed in an improper order.
        if (state == 1) {
            SysLogger::error("error CR_COMMANDS_OUT_OF_SYNC");
        }
        return (void*) conn_;
    }

    /** @brief Disconnect mysql database.
     *  @see Connection::disconnect.
     */
    bool disconnect() {
        if (conn_) {
            mysql_ping(conn_);
            mysql_close(conn_);
            conn_ = NULL;
        }
        return true;
    }

    /** @brief Connect to mysql database.
     *  @see Connection::connect.
     */
    bool connect(const char *host, const char *user, const char *password,
            const char *db_name, unsigned int port);

private:
    MysqlConnection(const MysqlConnection &);
    MysqlConnection & operator=(const MysqlConnection &);

    MYSQL* conn_; // Mysql connection object.

};
}/* end of namespace rytong */

#endif /* _RYT_MYSQL_CONNECTION_H */

