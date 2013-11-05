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

#ifndef _RYT_DB2_CONNECTION_H
#define _RYT_DB2_CONNECTION_H

#include "../base/Connection.h"
#include "../util/SysLogger.h"
#include "DB2Utils.h"

namespace rytong{
class DB2Connection : public Connection {
public:
    DB2Connection();
    ~DB2Connection();

    /** get connection handle */
    void* get_connection() {
        return NULL;
    }

    /** get DB2 connection handle */
    SQLHANDLE get_db2_hdbc() {
        return hdbc_;
    }

    /** disconnect database */
    bool disconnect();

    /**
     * Function: connect
     * Description: connect to DB2 database.
     * Input: host -> the host of database
     *        user -> the user of database
     *        password -> the password of database
     *        db_name -> the name of database
     *        port -> the port of database
     * Output: none
     * Returns: is success.
     */
    bool connect(const char *host, const char *user, const char *password,
            const char *db_name, unsigned int port);

     /**
     * Function: get_auto_commit
     * Description: get the atuto_commit_ flag.
     * Input: void
     * Output:
     * Returns: atuto_commit_ bool flag.
     */
    bool get_auto_commit() const;

    bool set_auto_commit(bool);

private:
    DB2Connection(const DB2Connection &);
    DB2Connection & operator=(const DB2Connection &);

    static SQLHANDLE sHenv_;  /* environment handle */
    SQLHANDLE hdbc_;          /* connection handle */
    bool auto_commit_ ;       /* auto commit flag */
};
}/* end of namespace rytong */

#endif /* _RYT_DB2_CONNECTION_H */
