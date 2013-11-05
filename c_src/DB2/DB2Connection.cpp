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

#include "DB2Connection.h"

using namespace rytong;

SQLHANDLE DB2Connection::sHenv_ = (SQLHANDLE)NULL;

DB2Connection::DB2Connection() {
    hdbc_ = (SQLHANDLE)NULL;
    set_db_type(DB2_DB);
}

DB2Connection::~DB2Connection() {
    if (hdbc_) {
        disconnect();
    }
}

bool DB2Connection::disconnect() {
    if (hdbc_) {
        /* disconnect from the database */
        SQLDisconnect(hdbc_);

        /* free connection handle */
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc_);
        hdbc_ = (SQLHANDLE)NULL;
    }

    return true;
}

bool DB2Connection::connect(const char *host, const char *user,
    const char *password, const char *db_name, unsigned int port) {
    SQLRETURN cliRC = SQL_SUCCESS;

    if (!sHenv_) {
        /* allocate an environment handle */
        cliRC = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &sHenv_);
        if (cliRC != SQL_SUCCESS)
        {

//            printf("\n--ERROR while allocating the environment handle.\n");
//            printf("  cliRC             = %d\n", cliRC);
//            printf("  line              = %d\n", __LINE__);
//            printf("  file              = %s\n", __FILE__);
            return false;
        }

        /* set attribute to enable application to run as ODBC 3.0 application */
        cliRC = SQLSetEnvAttr(sHenv_,
                              SQL_ATTR_ODBC_VERSION,
                              (void *)SQL_OV_ODBC3,
                              0);
        if (!SQL_ENV_SUCCESS(sHenv_, cliRC)){
            SQLFreeHandle(SQL_HANDLE_ENV, sHenv_);
            return false;
        }
    }

    /* allocate a database connection handle */
    cliRC = SQLAllocHandle(SQL_HANDLE_DBC, sHenv_, &hdbc_);
    if (!SQL_ENV_SUCCESS(sHenv_, cliRC)){
        return false;
    }

    /* set AUTOCOMMIT on */
    cliRC = SQLSetConnectAttr(hdbc_,
                              SQL_ATTR_AUTOCOMMIT,
                              (SQLPOINTER)SQL_AUTOCOMMIT_ON,
                              SQL_NTS);
    if (!SQL_DBC_SUCCESS(hdbc_, cliRC)){
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc_);
        return false;
    }
    auto_commit_ = true;

    /* connect to the database */
    cliRC = SQLConnect(hdbc_,
                       (SQLCHAR*)db_name,
                       SQL_NTS,
                       (SQLCHAR*)user,
                       SQL_NTS,
                       (SQLCHAR*)password,
                       SQL_NTS);
    if (!SQL_DBC_SUCCESS(hdbc_, cliRC)){
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc_);
        return false;
    }

    return true;
}

bool DB2Connection::get_auto_commit() const {
    return auto_commit_;
}

/* set AUTOCOMMIT on or off */
bool DB2Connection::set_auto_commit(bool auto_commit) {
    SQLRETURN cliRC = SQL_SUCCESS;

    if (auto_commit_ != auto_commit) {
        long unsigned ac = auto_commit ? SQL_AUTOCOMMIT_ON:SQL_AUTOCOMMIT_OFF;
        cliRC = SQLSetConnectAttr(hdbc_,
                                  SQL_ATTR_AUTOCOMMIT,
                                  (SQLPOINTER)ac,
                                  SQL_NTS);
        if (SQL_DBC_SUCCESS(hdbc_, cliRC)) {
            auto_commit_ = auto_commit;
            return true;
        } else {
            return false;
        }
    }

    return true;
}
