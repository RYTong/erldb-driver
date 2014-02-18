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
 * $Id: DBOperation.cpp 22235 2010-02-25 09:13:30Z deng.lifen $
 *
 *  @file DBOperation.cpp
 *  @brief Base class declaration of DBOperation.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-1-30
 */

#include "DBOperation.h"
#include "ConnectionPool.h"
#include "../util/EiEncoder.h"
#include "../util/SysLogger.h"
#ifdef USE_MYSQL
#include "../mysql/MysqlDBOperation.h"
#endif
#ifdef USE_ORACLE
#include "../oracle/OracleDBOperation.h"
#endif
#ifdef USE_SYBASE
#include "../sybase/SybDBOperation.h"
#endif
#ifdef USE_DB2
#include "../DB2/DB2Operation.h"
#endif
#ifdef USE_INFORMIX
#include "../informix/InformixOperation.h"
#endif

using namespace rytong;

DBOperation::DBOperation() : conn_(NULL), index_(0), version_(0) {
}

DBOperation::~DBOperation() {
}

DBOperation* DBOperation::create(DatabaseType db_type) {
    DBOperation* db = NULL;
    switch (db_type) {
#ifdef USE_MYSQL
        case MYSQL_DB:
            db = new MysqlDBOperation();
            break;
#endif
#ifdef USE_ORACLE
        case ORACLE_DB:
            db = new OracleDBOperation();
            break;
#endif
#ifdef USE_SYBASE
        case SYBASE_DB:
            db = new SybDBOperation();
            break;
#endif
#ifdef USE_DB2
        case DB2_DB:
            db = new DB2Operation();
            break;
#endif
#ifdef USE_INFORMIX
        case INFORMIX_DB:
            db = new InformixOperation();
            break;
#endif
        default:
            break;
    }
    return db;
}

void DBOperation::destroy(DBOperation* db) {
    delete db;
}


int DBOperation::init(DrvConf* conf, ConnectionPool* conn_pool) {
    Connection* conn;
    int success_num = 0;
    for (int i = 0; i < conf->poolsize; i++) {
        conn = create_conn(conf);
        if (conn == NULL) {
            return -1;
        }
        if (conn_pool->push(conn)) {
            success_num++;
        }
    }
    return success_num;
}

Connection* DBOperation::create_conn(DrvConf* conf) {
    Connection* conn = Connection::create(conf->db_type);
    //FIXME we should make Connection's interface 'connect' consistent in every derived class.
    if (!conn->connect(conf->host, conf->user, conf->password,
            conf->db_name, conf->port)) {
        SysLogger::error("invalid args to start connection");
        delete conn;
        return NULL;
    }
    return conn;
}

void DBOperation::release_conn(Connection* conn) {
    delete conn;
}

int DBOperation::decode_stmt_fields(FieldValue * & fields) {
    ei_get_type(buf_, &index_, &type_, &size_);
    int len = size_;
    if (len <= 0) {
        return len;
    }
    fields = new FieldValue[len];
    if (type_ == ERL_STRING_EXT) {
        char *temp = new char[len];
        ei_decode_string(buf_, &index_, temp);
        for (int i = 0; i < len; i++) {
            fields[i].value = (void*) (new long());
            if ((long) temp[i] < 0) {
                *(long*) fields[i].value = (long) temp[i] + 256;
            } else {
                *(long*) fields[i].value = (long) temp[i];
            }
            fields[i].erl_type = ERL_SMALL_INTEGER_EXT;
            fields[i].length = sizeof(long);
        }
        delete [] temp;
    } else {
        ei_decode_list_header(buf_, &index_, &type_);
        for (int i = 0; i < len; i++) {
            decode_field_value(fields[i]);
        }
        ei_decode_list_header(buf_, &index_, &type_);
    }
    return len;
}

void DBOperation::free_stmt_fields(FieldValue * & fields, int len) {
    if (len <= 0) {
        return;
    }
    for (int i = 0; i < len; i++) {
        free_field_tuple(fields[i]);
    }
    delete [] fields;
}

int DBOperation::decode_fields(FieldStruct * & field_list) {
    ei_decode_list_header(buf_, &index_, &type_);
    int len = type_;
    field_list = new FieldStruct[len];
    for (int i = 0; i < len; i++) {
        ei_decode_tuple_header(buf_, &index_, &type_);
        decode_string(field_list[i].field_name);
        decode_field_value(field_list[i].field_value);
    }
    ei_decode_list_header(buf_, &index_, &type_);
    return len;
}

void DBOperation::free_fields(FieldStruct * & field_list, int len) {
    for (int i = 0; i < len; i++) {
        free_string(field_list[i].field_name);
        free_field_tuple(field_list[i].field_value);
    }
    delete [] field_list;
}
