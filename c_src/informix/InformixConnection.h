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
 */

#ifndef _RYT_INFORMIX_CONNECTION_H
#define _RYT_INFORMIX_CONNECTION_H

#include <it.h>
#include "../base/Connection.h"
#include "../util/SysLogger.h"
#include "InformixUtils.h"

namespace rytong{
/** @brief Mysql connection class.
 */
class InformixConnection : public Connection {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    InformixConnection();

    /** @brief Destructor for the class.
     *  @return None.
     */
    ~InformixConnection();

    /** @brief Get connection.
     *  @see Connection::get_connection.
     */
    void* get_connection() ;

    /** @brief Disconnect database.
     *  @see Connection::disconnect.
     */
    bool disconnect() ;

    /** @brief Connect to mysql database.
     *  @see Connection::connect.
     */
    bool connect(const char *host, const char *user, const char *password,
            const char *db_name, unsigned int port);

    bool set_auto_commit(bool);
    bool is_auto_committed() const;

private:
    InformixConnection(const InformixConnection &);
    InformixConnection & operator=(const InformixConnection &);

    ITConnection* conn_; // connection object.

};
}/* end of namespace rytong */

#endif /* _RYT_MYSQL_CONNECTION_H */

