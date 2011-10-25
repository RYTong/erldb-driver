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
 * $Id: MysqlConnection.cpp 21889 2010-02-02 05:10:06Z deng.lifen $
 *
 *  @file MysqlConnection.cpp
 *  @brief Mysql connection class.
 *  @author deng.lifen <deng.lifen@rytong.com>
 *  @version 1.0.0
 *  @date Fri Nov 13 14:36:14 CST 2009
 */

#include "mysql/MysqlConnection.h"

using namespace rytong;

MysqlConnection::MysqlConnection() {
    this->conn_ = NULL;
}

MysqlConnection::~MysqlConnection() {
    if (this->conn_) {
        disconnect();
    }
}

bool MysqlConnection::connect(const char *host, const char *user,
        const char *password, const char *db_name, unsigned int port) {
    conn_ = mysql_init(NULL);
    if (!conn_) {
        return false;
    }

    // set encode utf8
    if (mysql_options(conn_, MYSQL_SET_CHARSET_NAME, "utf8")) {
        SysLogger::error("set utf8 error");
    }

    // set reconnect database
    char reconnect = 1;
    if (mysql_options(conn_, MYSQL_OPT_RECONNECT, (char *) & reconnect)) {
        SysLogger::error("set reconnect error");
    }

    // connect to mysql database
    if (!mysql_real_connect(conn_, host, user, password, db_name, port, NULL,
        0)) {
        SysLogger::error(mysql_error(conn_));
        return false;
    }

    db_name_ = (char*) db_name;

    // set auto commit
    mysql_autocommit(conn_, true);

    return true;
}
