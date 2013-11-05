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
 *  @file OracleConnection.cpp
 *  @brief Oracle connection class.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-2-3
 */

#include "OracleConnection.h"
#include "OracleDBOperation.h"
#include "../util/SysLogger.h"

using namespace oracle::occi;
using namespace rytong;
using namespace std;

OracleConnection::OracleConnection() {
    env_ = NULL;
    conn_ = NULL;
    counter_ = 0;
    set_db_type(ORACLE_DB);
    set_auto_commit(true);
}

OracleConnection::~OracleConnection() {
    if (conn_) {
        disconnect();
        conn_ = NULL;
    }
}

void* OracleConnection::get_connection() {
    if (counter_ > port_ && auto_commit_ == true) {
        if (!reconnect()) {
            DBException ex("fail to get conn");
            throw ex;
        } else {
            counter_ = 0;
        }
    } else {
        ++counter_;
    }

    return conn_;
}

bool OracleConnection::disconnect() {
    try {
        env_->terminateConnection(conn_);
        Environment::terminateEnvironment(env_);
    } catch (SQLException ex) {
        SysLogger::error("Exception thrown for disconnect\n\rError number:"
            "%d\n\r%s", ex.getErrorCode(), ex.getMessage().c_str());
        return false;
    }
    return true;
}

bool OracleConnection::reconnect() {
    try {
        if (conn_ != NULL) {
            env_->terminateConnection(conn_);
            conn_ = NULL;
        }
        if (env_ != NULL) {
            Environment::terminateEnvironment(env_);
            env_ = NULL;
        }
        env_ = Environment::createEnvironment(Environment::THREADED_MUTEXED);
        conn_ = env_->createConnection(user_, password_, db_name_);
#ifdef _RYT_USE_TAF_NOTIFY
        conn_->setTAFNotify(notify, NULL);
#endif
    } catch (SQLException ex) {
        SysLogger::error("Exception thrown for connect\n\rError number:"
            "%d\n\r%s", ex.getErrorCode(), ex.getMessage().c_str());
        return false;
    }

    return true;
}

// TAF notify callback function
int notify(Environment *env,
        oracle::occi::Connection *conn, void *ctx,
        oracle::occi::Connection::FailOverType foType,
        oracle::occi::Connection::FailOverEventType foEvent) {

    SysLogger::debug("FailOverType:%d FailOverEventType:%d", foType, foEvent);
    switch(foEvent) {
        case oracle::occi::Connection::FO_BEGIN:
            SysLogger::notice(" A lost connection has been detected; "
                "Failover is starting.");
            break;
        case oracle::occi::Connection::FO_END:
            SysLogger::notice(" A failover completed successfully; The"
                " Connection is ready for use.");
            break;
        case oracle::occi::Connection::FO_ABORT:
            SysLogger::notice("The failover was unsuccessful; It will not be"
                " attempted again.");
            break;
        case oracle::occi::Connection::FO_REAUTH:
            SysLogger::notice("The user session has been reauthenticated.");
            break;
        case oracle::occi::Connection::FO_ERROR:
            SysLogger::notice(" A failover was unsuccessful, It will be"
                " attempted again.");
            return FO_RETRY;
        default:
            break;
    }
    return OCCI_SUCCESS;
}

bool OracleConnection::connect(const char *host, const char *user,
        const char *password, const char *db_name, unsigned int port) {
    if (user != user_) {
        ewp_strncpy(user_, user, 128);
        ewp_strncpy(password_, password, 128);
        ewp_strncpy(db_name_, db_name, 128);
        port_ = port;
    }
    try {
        env_ = Environment::createEnvironment(Environment::THREADED_MUTEXED);
        conn_ = env_->createConnection(user, password, db_name);
#ifdef _RYT_USE_TAF_NOTIFY
        conn_->setTAFNotify(notify, NULL);
#endif
    } catch (SQLException ex) {
        SysLogger::error("Exception thrown for connect\n\rError number:"
            "%d\n\r%s", ex.getErrorCode(), ex.getMessage().c_str());
        return false;
    }
    return true;
}

bool OracleConnection::get_auto_commit() const {
    return auto_commit_;
}

void OracleConnection::set_auto_commit(bool auto_commit) {
    auto_commit_ = auto_commit;
}
