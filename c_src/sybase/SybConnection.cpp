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
 *  @file SybConnection.cpp
 *  @brief Sybase connection class.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Wen May 4 15:06:54 CST 2011
 */

#include "SybConnection.h"

namespace rytong {
CS_CONTEXT* SybConnection::sContext_ = NULL;

SybConnection::SybConnection() {
    conn_ = NULL;
    stmt_ = NULL;
}

SybConnection::~SybConnection() {
    if (conn_) {
        disconnect();
        conn_ = NULL;
    }
    if (stmt_) {
        terminate_statement(stmt_);
    }
}

void* SybConnection::get_connection() {
    return this;
}

bool SybConnection::disconnect() {
    return con_cleanup_(CS_SUCCEED) == CS_SUCCEED;
}

bool SybConnection::connect(const char *host, const char *user,
        const char *password, const char *db_name, unsigned int port) {
    bool retcode;
    char addr[50];
    char cmd[128];

    if (strlen(host) > 30) {
        return false;
    }
    sprintf(addr, "%s %u", host, port);
    
    if (sContext_ == NULL) {
        if (init_() != CS_SUCCEED) {
            return false;
        }
    }

    if (connect_("ewp_sybase_drv", NULL, user, password, db_name)!=CS_SUCCEED) {
        return false;
    }

    if (stmt_) {
        terminate_statement(stmt_);
        stmt_ = NULL;
    }
    stmt_ = create_statement();
    if (db_name != NULL) {
        if (strlen(db_name) > 120) {
            return false;
        }
        sprintf(cmd, "use %s", db_name);
        retcode = stmt_->execute_cmd(cmd);

        if (!retcode) {
            return false;
        }
    }
    
    limit_row_count_ = 0;
    
    return true;
}

SybStatement* SybConnection::get_statement() {
    return stmt_;
}

SybStatement* SybConnection::create_statement() {
    return (new SybStatement(conn_));
}

SybStatement* SybConnection::create_statement(const char* sql) {
    return (new SybStatement(conn_, sql));
}

void SybConnection::terminate_statement(SybStatement* stmt) {
    delete stmt;
}

bool SybConnection::set_limit_row_count(int count) {
    bool retcode;
    char cmd[30];

    if (limit_row_count_ == count) {
        return true;
    }
    sprintf(cmd, "SET ROWCOUNT %d", count);
    if ((retcode = stmt_->execute_cmd(cmd))) {
        limit_row_count_ = count;
    }

    return retcode;
}

int SybConnection::get_limit_row_count() {
    return limit_row_count_;
}

CS_RETCODE SybConnection::init_()
{
    CS_RETCODE retcode;
    CS_INT netio_type = CS_SYNC_IO;

    // Get a context handle to use.
    if ((retcode = cs_ctx_alloc(EX_CTLIB_VERSION, &sContext_)) != CS_SUCCEED) {
        SysLogger::error("init_: cs_ctx_alloc() failed");
        return retcode;
    }

    // Initialize Open Client.
    if ((retcode = ct_init(sContext_, EX_CTLIB_VERSION)) != CS_SUCCEED) {
        SysLogger::error("init_: ct_init() failed");
        cs_ctx_drop(sContext_);
        sContext_ = NULL;
        return retcode;
    }

    // Install client message handlers.
    if ((retcode = ct_callback(sContext_, NULL, CS_SET, CS_CLIENTMSG_CB,
            (CS_VOID *)clientmsg_cb)) != CS_SUCCEED) {
        SysLogger::error("init_: ct_callback(CS_CLIENTMSG_CB) failed");
        ct_exit(sContext_, CS_FORCE_EXIT);
        cs_ctx_drop(sContext_);
        sContext_ = NULL;
        return retcode;
    }

    // Install server message handlers.
    if ((retcode = ct_callback(sContext_, NULL, CS_SET, CS_SERVERMSG_CB,
            (CS_VOID *)servermsg_cb)) != CS_SUCCEED) {
        SysLogger::error("init_: ct_callback(CS_SERVERMSG_CB) failed");
        ct_exit(sContext_, CS_FORCE_EXIT);
        cs_ctx_drop(sContext_);
        sContext_ = NULL;
        return retcode;
    }

    // Set the input/output type to synchronous.
    if ((retcode = ct_config(sContext_, CS_SET, CS_NETIO, &netio_type,
            CS_UNUSED, NULL)) != CS_SUCCEED) {
        SysLogger::error("init_: ct_config() failed");
        ct_exit(sContext_, CS_FORCE_EXIT);
        cs_ctx_drop(sContext_);
        sContext_ = NULL;
        return retcode;
    }

    return CS_SUCCEED;
}

CS_RETCODE SybConnection::connect_(const char *app, const char *addr,
        const char *user, const char *pwd, const char *db)
{
    CS_RETCODE retcode;
    CS_BOOL hafailover = CS_TRUE;

    // Allocate a connection structure.
    retcode = ct_con_alloc(sContext_, &conn_);
    if (retcode != CS_SUCCEED) {
        SysLogger::error("connect_: ct_con_alloc() failed");
        return retcode;
    }

    // If a appname is defined, set the CS_APPNAME property.
    if (app != NULL) {
        retcode = ct_con_props(conn_, CS_SET, CS_APPNAME,
                (CS_VOID*)app, CS_NULLTERM, NULL);
        if (retcode != CS_SUCCEED) {
            SysLogger::error("connect_: ct_con_props(CS_APPNAME) failed");
            ct_con_drop(conn_);
            conn_ = NULL;
            return retcode;
        }
    }

    // If a servername is defined, set the CS_SERVERNAME property.
    if (addr != NULL) {
        retcode = ct_con_props(conn_, CS_SET, CS_SERVERADDR,
                (CS_VOID*)addr, CS_NULLTERM, NULL);
        if (retcode != CS_SUCCEED) {
            SysLogger::error("connect_: ct_con_props(CS_SERVERADDR) failed");
            ct_con_drop(conn_);
            conn_ = NULL;
            return retcode;
        }
    }

    // If a username is defined, set the CS_USERNAME property.
    if (user != NULL) {
        retcode = ct_con_props(conn_, CS_SET, CS_USERNAME,
                (CS_VOID*)user, CS_NULLTERM, NULL);
        if (retcode != CS_SUCCEED) {
            SysLogger::error("connect_: ct_con_props(CS_USERNAME) failed");
            ct_con_drop(conn_);
            conn_ = NULL;
            return retcode;
        }
    }

    // If a password is defined, set the CS_PASSWORD property.
    if (pwd != NULL) {
        retcode = ct_con_props(conn_, CS_SET, CS_PASSWORD,
                (CS_VOID*)pwd, CS_NULLTERM, NULL);
        if (retcode != CS_SUCCEED) {
            SysLogger::error("connect_: ct_con_props(CS_PASSWORD) failed");
            ct_con_drop(conn_);
            conn_ = NULL;
            return retcode;
        }
    }

    // Set the CS_HAFAILOVER property.
    retcode = ct_con_props(conn_, CS_SET, CS_HAFAILOVER,
            &hafailover, CS_UNUSED, NULL);
    if (retcode != CS_SUCCEED) {
        SysLogger::error("connect_: ct_con_props(CS_HAFAILOVER) failed");
        ct_con_drop(conn_);
        conn_ = NULL;
        return retcode;
    }

    // Connect to the server.
    retcode = ct_connect(conn_, NULL, CS_UNUSED);
    if (retcode != CS_SUCCEED) {
        SysLogger::error("connect_: ct_connect() failed");
        ct_con_drop(conn_);
        conn_ = NULL;
        return retcode;
    }

    return CS_SUCCEED;
}

CS_RETCODE SybConnection::con_cleanup_(CS_RETCODE status)
{
    if (conn_) {
        CS_RETCODE retcode;
        CS_INT close_option;

        close_option = (status != CS_SUCCEED) ? CS_FORCE_CLOSE : CS_UNUSED;
        retcode = ct_close(conn_, close_option);
        if (retcode != CS_SUCCEED) {
            SysLogger::error("con_cleanup_: ct_close() failed");
            return retcode;
        }
        retcode = ct_con_drop(conn_);
        if (retcode != CS_SUCCEED) {
            SysLogger::error("con_cleanup_: ct_con_drop() failed");
            return retcode;
        }
        return retcode;
    } else {
        return CS_SUCCEED;
    }
}

CS_RETCODE SybConnection::ctx_cleanup_(CS_RETCODE status)
{
    if (sContext_) {
        CS_RETCODE retcode;
        CS_INT exit_option;

        exit_option = (status != CS_SUCCEED) ? CS_FORCE_EXIT : CS_UNUSED;
        retcode = ct_exit(sContext_, exit_option);
        if (retcode != CS_SUCCEED) {
            SysLogger::error("con_cleanup_: ct_exit() failed");
            return retcode;
        }
        retcode = cs_ctx_drop(sContext_);
        if (retcode != CS_SUCCEED) {
            SysLogger::error("con_cleanup_: cs_ctx_drop() failed");
            return retcode;
        }
        return retcode;
    } else {
        return CS_SUCCEED;
    }
}
}/* end of namespace rytong */
