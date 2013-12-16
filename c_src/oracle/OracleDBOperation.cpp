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
 *  @file OracleDBOperation.cpp
 *  @brief Derived class for oracle to represent operations of database.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-2-3
 */

#include "OracleDBOperation.h"
#include "../base/ConnectionPool.h"
#include "../util/EiEncoder.h"

static const char* STMT_NULL_ERROR = "fail to get stmt";

using namespace oracle::occi;
using namespace rytong;

Environment* OracleDBOperation::env_ = NULL;

OracleDBOperation::OracleDBOperation() {
}

OracleDBOperation::~OracleDBOperation() {
}

//FIXME we need to release the env_ when it is useless

Environment* OracleDBOperation::get_env_instance() {
    if (NULL == env_) {
        env_ = Environment::createEnvironment(Environment::THREADED_MUTEXED);
    }

    return env_;
}

bool OracleDBOperation::exec(ei_x_buff * const res) {
    char* sql = NULL;
    char* param = NULL;
    Statement* stmt = NULL;
    ResultSet* rset = NULL;
    oracle::occi::Connection* conn = NULL;
    int type;

    try {
        conn = (oracle::occi::Connection*) conn_->get_connection();
        type = get_erl_type();
        if (type == ERL_SMALL_TUPLE_EXT || type == ERL_LARGE_TUPLE_EXT) {
            decode_tuple_header();
            decode_string_with_throw(sql);
            stmt = conn->createStatement(sql);
            type = get_erl_type();
            if (type == ERL_LIST_EXT) {
                long param_count = decode_list_header();
                for (long i = 0; i < param_count; i++) {
                    decode_and_set_param(stmt, (unsigned int)(i + 1));
                }
            } else {
                decode_string_with_throw(param);
                for (unsigned int i = 1; i <= strlen(param); ++i) {
                    Number number((int) param[i - 1]);
                    stmt->setNumber(i, number);
                }
                free_string(param);
            }
        } else {
            decode_string_with_throw(sql);
            stmt = conn->createStatement(sql);
        }

        if (((OracleConnection*) conn_)->get_auto_commit()) {
            stmt->setAutoCommit(true);
        }
        if (ewp_strcmp(sql, "test") == 0) {
            usleep(500);
            EiEncoder::encode_ok_number(1, res);
        } else if (is_query(sql)) {
            query(stmt, rset, res);
        } else {
            stmt->executeUpdate();
            EiEncoder::encode_ok_number(stmt->getUpdateCount(), res);
        }
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for exec\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
    } catch (exception& ex) {
        SysLogger::error("Exception thrown for exec\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
    }
    free_string(sql);
    if (rset != NULL) {
        stmt->closeResultSet(rset);
    }
    if (stmt != NULL) {
        conn->terminateStatement(stmt);
    }

    return true;
}

bool OracleDBOperation::trans_begin(ei_x_buff * const res) {
    bool retcode = true;
    try {
        rytong::Connection* conn = (rytong::Connection*) conn_;
        OracleConnection* oracle_conn = (OracleConnection*) conn;
        oracle_conn->set_auto_commit(false);
        EiEncoder::encode_ok_pointer((void*) conn, res);
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for trans_begin\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
        retcode = false;
    } catch (exception& ex) {
        SysLogger::error("Exception thrown for trans_begin\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
        retcode = false;
    }

    return retcode;
}

bool OracleDBOperation::trans_commit(ei_x_buff * const res) {
    bool retcode = true;
    try {
        oracle::occi::Connection* conn =
                (oracle::occi::Connection*) conn_->get_connection();
        conn->commit();
        ((OracleConnection*)conn_)->set_auto_commit(true);
        EiEncoder::encode_ok_msg("COMMIT", res);
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for trans_commit\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
        retcode = false;
    } catch (exception& ex) {
        SysLogger::error("Exception thrown for trans_commit\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
        retcode = false;
    }

    return retcode;
}

bool OracleDBOperation::trans_rollback(ei_x_buff * const res) {
    bool retcode = true;
    try {
        oracle::occi::Connection* conn =
                (oracle::occi::Connection*) conn_->get_connection();
        conn->rollback();
        ((OracleConnection*)conn_)->set_auto_commit(true);
        EiEncoder::encode_ok_msg("ROLLBACK", res);
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for trans_rollback\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
        retcode = false;
    } catch (exception& ex) {
        SysLogger::error("Exception thrown for trans_rollback\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
        retcode = false;
    }

    return retcode;
}

bool OracleDBOperation::prepare_statement_init(ei_x_buff * const res) {
    char *sql = NULL;
    Statement* stmt = NULL;
    oracle::occi::Connection* conn = NULL;

    try {
        decode_string_with_throw(sql);

        conn = (oracle::occi::Connection*) conn_->get_connection();
        stmt = conn->createStatement(sql);

        if (((OracleConnection*) conn_)->get_auto_commit()) {
            stmt->setAutoCommit(true);
        }

        EiEncoder::encode_ok_pointer(stmt, res);
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for prepare_stat_init\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
        if (stmt != NULL) {
            conn->terminateStatement(stmt);
        }

        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
    } catch (exception& ex) {
        if (stmt != NULL) {
            conn->terminateStatement(stmt);
        }
        SysLogger::error("Exception thrown for prepare_stat_init\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
    }

    free_string(sql);

    return true;
}

bool OracleDBOperation::prepare_statement_exec(ei_x_buff * const res) {
    // char* prepare_name = NULL;
    char* param = NULL;
    Statement* stmt = NULL;
    ResultSet* rset = NULL;

    try {
        decode_tuple_header();
        // decode_string_with_throw(prepare_name);
        ei_decode_binary(buf_, &index_, &stmt, &bin_size_);

        // stmt = (Statement*) stmt_map_->get(prepare_name);
        if (stmt == NULL) {
            DBException ex(STMT_NULL_ERROR);
            throw ex;
        }
        int type = get_erl_type();

        if (type == ERL_LIST_EXT) {
            long param_count = decode_list_header();

            for (long i = 0; i < param_count; i++) {
                decode_and_set_param(stmt, (unsigned int)(i + 1));
            }
        } else {
            decode_string_with_throw(param);
            for (unsigned int i = 1; i <= strlen(param); ++i) {
                Number number((int) param[i - 1]);
                stmt->setNumber(i, number);
            }
        }
        if (is_query(stmt->getSQL().c_str())) {
            query(stmt, rset, res);
        } else {
            if (((OracleConnection*) conn_)->get_auto_commit()) {
                stmt->setAutoCommit(true);
            }

            stmt->executeUpdate();

            EiEncoder::encode_ok_number(stmt->getUpdateCount(), res);
        }

        if (rset != NULL) {
            stmt->closeResultSet(rset);
        }
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for prepare_stat_exec\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
    } catch (exception& ex) {
        SysLogger::error("Exception thrown for prepare_stat_exec\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
    }

    // free_string(prepare_name);
    free_string(param);

    return true;
}

bool OracleDBOperation::prepare_statement_release(ei_x_buff * const res) {
    // char* prepare_name = NULL;
    Statement* stmt = NULL;
    try {
        // decode_string_with_throw(prepare_name);
        ei_decode_binary(buf_, &index_, &stmt, &bin_size_);
        // Statement* stmt = (Statement*) stmt_map_->remove(string(prepare_name));
        if (stmt == NULL) {
            DBException ex(STMT_NULL_ERROR);
            throw ex;
        }

        oracle::occi::Connection* conn = stmt->getConnection();
        conn->terminateStatement(stmt);

        EiEncoder::encode_ok_msg("close stmt", res);
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for prepare_stat_release\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
    } catch (exception& ex) {
        SysLogger::error("Exception thrown for prepare_stat_release\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
    }

    // free_string(prepare_name);

    return true;
}

bool OracleDBOperation::prepare_stat_init(ei_x_buff * const res) {
    char* prepare_name = NULL;
    char *sql = NULL;
    Statement* stmt = NULL;
    oracle::occi::Connection* conn = NULL;

    try {
        decode_tuple_header();
        decode_string_with_throw(prepare_name);
        decode_string_with_throw(sql);

        conn = (oracle::occi::Connection*) conn_->get_connection();
        stmt = conn->createStatement(sql);

        if (((OracleConnection*) conn_)->get_auto_commit()) {
            stmt->setAutoCommit(true);
        }

        if (stmt_map_->add((string) prepare_name, (void*)stmt)) {
            EiEncoder::encode_ok_msg(prepare_name, res);
        } else {
            conn->terminateStatement(stmt);
            EiEncoder::encode_error_msg("already registered prepare name", res);
        }
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for prepare_stat_init\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
        if (stmt != NULL) {
            conn->terminateStatement(stmt);
        }

        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
    } catch (exception& ex) {
        if (stmt != NULL) {
            conn->terminateStatement(stmt);
        }
        SysLogger::error("Exception thrown for prepare_stat_init\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
    }

    free_string(prepare_name);
    free_string(sql);

    return true;
}

bool OracleDBOperation::prepare_stat_exec(ei_x_buff * const res) {
    char* prepare_name = NULL;
    char* param = NULL;
    Statement* stmt = NULL;
    ResultSet* rset = NULL;

    try {
        decode_tuple_header();
        decode_string_with_throw(prepare_name);

        stmt = (Statement*) stmt_map_->get(prepare_name);
        if (stmt == NULL) {
            DBException ex(STMT_NULL_ERROR);
            throw ex;
        }
        int type = get_erl_type();

        if (type == ERL_LIST_EXT) {
            long param_count = decode_list_header();

            for (long i = 0; i < param_count; i++) {
                decode_and_set_param(stmt, (unsigned int)(i + 1));
            }
        } else {
            decode_string_with_throw(param);
            for (unsigned int i = 1; i <= strlen(param); ++i) {
                Number number((int) param[i - 1]);
                stmt->setNumber(i, number);
            }
        }
        if (is_query(stmt->getSQL().c_str())) {
            query(stmt, rset, res);
        } else {
            if (((OracleConnection*) conn_)->get_auto_commit()) {
                stmt->setAutoCommit(true);
            }

            stmt->executeUpdate();

            EiEncoder::encode_ok_number(stmt->getUpdateCount(), res);
        }

        if (rset != NULL) {
            stmt->closeResultSet(rset);
        }
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for prepare_stat_exec\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
    } catch (exception& ex) {
        SysLogger::error("Exception thrown for prepare_stat_exec\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
    }

    free_string(prepare_name);
    free_string(param);

    return true;
}

bool OracleDBOperation::prepare_stat_release(ei_x_buff * const res) {
    char* prepare_name = NULL;
    try {
        decode_string_with_throw(prepare_name);

        Statement* stmt = (Statement*) stmt_map_->remove(string(prepare_name));
        if (stmt == NULL) {
            DBException ex(STMT_NULL_ERROR);
            throw ex;
        }

        oracle::occi::Connection* conn = stmt->getConnection();
        conn->terminateStatement(stmt);

        EiEncoder::encode_ok_msg("close stmt", res);
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for prepare_stat_release\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
    } catch (exception& ex) {
        SysLogger::error("Exception thrown for prepare_stat_release\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
    }

    free_string(prepare_name);

    return true;
}

bool OracleDBOperation::insert(ei_x_buff * const res) {
    ParamIndex param_index;
    stringstream sql, tmp;
    char* table_name = NULL;
    char* field_name = NULL;
    Statement* stmt  = NULL;
    oracle::occi::Connection* conn = NULL;

    try {
        decode_tuple_header();
        decode_string_with_throw(table_name);

        sql << "INSERT INTO " << table_name << " (";
        tmp << " VALUES(";

        int num_field = decode_list_header();

        for (int i = 0; i < num_field; ++i) {
            decode_tuple_header();
            decode_string_with_throw(field_name);

            sql << field_name;
            tmp << ":" << i + 1;

            if (i < num_field - 1) {
                sql << ",";
                tmp << ",";
            }

            param_index.push_back(index_);
            skip_term();

            free_string(field_name);
        }

        sql << ")";
        tmp << ")";

        conn = (oracle::occi::Connection*) conn_->get_connection();
        stmt = conn->createStatement(sql.str() + tmp.str());
        //SysLogger::info("sql:%s\n\r", (sql.str() + tmp.str()).c_str());
        if (((OracleConnection*) conn_)->get_auto_commit()) {
            stmt->setAutoCommit(true);
        }

        for (unsigned int i = 0; i < param_index.size(); ++i) {
            decode_and_set_param(stmt, i + 1, &param_index[i]);
        }

        stmt->executeUpdate();

        EiEncoder::encode_ok_number(stmt->getUpdateCount(), res);
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for insert\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
    } catch (exception& ex) {
        SysLogger::error("Exception thrown for insert\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
    }

    free_string(table_name);
    free_string(field_name);

    if (stmt != NULL) {
        conn->terminateStatement(stmt);
    }

    return true;
}

bool OracleDBOperation::update(ei_x_buff * const res) {
    ParamIndex param_index;
    stringstream sql;
    char* table_name = NULL;
    char* field_name = NULL;
    Statement* stmt = NULL;
    oracle::occi::Connection* conn = NULL;

    try {
        decode_tuple_header();
        decode_string_with_throw(table_name);

        sql << "UPDATE " << table_name << " SET";

        int num_field = decode_list_header();

        for (int i = 0; i < num_field; ++i) {
            decode_tuple_header();
            decode_string_with_throw(field_name);

            sql << " "
                << field_name
                << "=" << ":"
                << i + 1;

            if (i < num_field - 1) {
                sql << ",";
            }

            param_index.push_back(index_);
            skip_term();

            free_string(field_name);
        }

        decode_empty_list();

        int type = get_erl_type();

        if (type == ERL_LIST_EXT) {
            sql << " WHERE";

            decode_list_header();
            make_expr(sql, param_index);
        }

        conn = (oracle::occi::Connection*) conn_->get_connection();
        stmt = conn->createStatement(sql.str());

        if (((OracleConnection*) conn_)->get_auto_commit()) {
            stmt->setAutoCommit(true);
        }

        for (unsigned int i = 0; i < param_index.size(); ++i) {
            decode_and_set_param(stmt, i + 1, &param_index[i]);
        }

        stmt->executeUpdate();

        EiEncoder::encode_ok_number(stmt->getUpdateCount(), res);
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for update\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
    } catch (exception& ex) {
        SysLogger::error("Exception thrown for update\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
    }

    free_string(table_name);
    free_string(field_name);

    if (stmt != NULL) {
        conn->terminateStatement(stmt);
    }

    return true;
}

bool OracleDBOperation::del(ei_x_buff * const res) {
    ParamIndex param_index;
    stringstream sql;
    Statement* stmt = NULL;
    oracle::occi::Connection* conn = NULL;
    char* table_name = NULL;

    try {
        decode_tuple_header();
        decode_string_with_throw(table_name);

        sql << "DELETE FROM " << table_name;

        int type = get_erl_type();

        if (type == ERL_LIST_EXT) {
            sql << " WHERE";
            decode_list_header();
            make_expr(sql, param_index);
        }

        conn = (oracle::occi::Connection*) conn_->get_connection();
        stmt = conn->createStatement(sql.str());

        if (((OracleConnection*) conn_)->get_auto_commit()) {
            stmt->setAutoCommit(true);
        }

        for (unsigned int i = 0; i < param_index.size(); ++i) {
            decode_and_set_param(stmt, i + 1, &param_index[i]);
        }

        stmt->executeUpdate();

        EiEncoder::encode_ok_number(stmt->getUpdateCount(), res);
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for del\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
    } catch (exception& ex) {
        SysLogger::error("Exception thrown for del\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
    }

    free_string(table_name);

    if (stmt != NULL) {
        conn->terminateStatement(stmt);
    }

    return true;
}

bool OracleDBOperation::select(ei_x_buff * const res) {
    ParamIndex param_index;
    stringstream sql;
    Statement* stmt = NULL;
    ResultSet* rset = NULL;
    oracle::occi::Connection* conn = NULL;

    try {
        long distinct = 0;
        decode_tuple_header();
        if (!decode_integer(distinct)) {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
        sql << ((0 == distinct) ? "SELECT" : "SELECT DISTINCT");

        /* decode select field*/
        int type = get_erl_type();
        if (type != ERL_LIST_EXT) {
            sql << " *";
            skip_term();
        } else {
            int num_field = decode_list_header();

            for (int i = 0; i < num_field; ++i) {
                make_expr(sql, param_index);
                if (i < num_field - 1) {
                    sql << " ,";
                }
            }
            decode_empty_list();
        }

        /* decode table list*/
        sql << " FROM";
        int num_table = decode_list_header();
        for (int i = 0; i < num_table; ++i) {
            make_expr(sql, param_index);
            if (i < num_table - 1) {
                sql << " ,";
            }
        }
        decode_empty_list();

        /* decode where expr*/
        type = get_erl_type();
        if (type == ERL_LIST_EXT) {
            sql << " WHERE";
            decode_list_header();
            make_expr(sql, param_index);
            decode_empty_list();
        }

        /* decode extras*/
        type = get_erl_type();
        if (type == ERL_LIST_EXT) {
            int num_extra = decode_list_header();

            for (int i = 0; i < num_extra; ++i) {
                make_expr(sql, param_index);
            }
            decode_empty_list();
        }

        conn = (oracle::occi::Connection*) conn_->get_connection();
        SysLogger::debug("sql:%s\n\r", sql.str().c_str());
        stmt = conn->createStatement(sql.str());

        if (((OracleConnection*) conn_)->get_auto_commit()) {
            stmt->setAutoCommit(true);
        }

        for (unsigned int i = 0; i < param_index.size(); ++i) {
            decode_and_set_param(stmt, i + 1, &param_index[i]);
        }

        rset = stmt->executeQuery();

        encode_select_result(res, rset);
    } catch (SQLException& ex) {
        SysLogger::error("Exception thrown for select\n\rError number:%d\n\r%s",
                ex.getErrorCode(), ex.getMessage().c_str());
        EiEncoder::encode_error_msg(ex.getMessage().c_str(), res);
    } catch (exception& ex) {
        SysLogger::error("Exception thrown for select\n\r%s", ex.what());
        EiEncoder::encode_error_msg(ex.what(), res);
    }

    if (rset != NULL) {
        stmt->closeResultSet(rset);
    }

    if (stmt != NULL) {
        conn->terminateStatement(stmt);
    }

    return true;
}

/* private Member functions*/

int OracleDBOperation::encode_column(
        int type,
        unsigned int colIndex,
        ResultSet* rset, ei_x_buff* x) {
    switch (type) {
        case SQLT_CHR:
            encode_chr(colIndex, rset, x);
            break;
        case SQLT_NUM:
            encode_num(colIndex, rset, x);
            break;
        case SQLT_LNG:
            encode_lng(colIndex, rset, x);
            break;
        case SQLT_DAT:
            encode_dat(colIndex, rset, x);
            break;
        case SQLT_IBFLOAT:
            encode_ibfloat(colIndex, rset, x);
            break;
        case SQLT_IBDOUBLE:
            encode_ibdouble(colIndex, rset, x);
            break;
        case SQLT_TIMESTAMP:
            encode_timestamp(colIndex, rset, x);
            break;
        case SQLT_TIMESTAMP_TZ:
            encode_timestamp_tz(colIndex, rset, x);
            break;
        case SQLT_TIMESTAMP_LTZ:
            encode_timestamp_ltz(colIndex, rset, x);
            break;
        case SQLT_INTERVAL_YM:
            encode_interval_ym(colIndex, rset, x);
            break;
        case SQLT_INTERVAL_DS:
            encode_interval_ds(colIndex, rset, x);
            break;
        case SQLT_BIN:
            encode_bin(colIndex, rset, x);
            break;
        case SQLT_LBI:
            encode_lbi(colIndex, rset, x);
            break;
        case SQLT_RDD:
            encode_rdd(colIndex, rset, x);
            break;
        case SQLT_AFC:
            encode_afc(colIndex, rset, x);
            break;
        case SQLT_CLOB:
            encode_clob(colIndex, rset, x);
            break;
        case SQLT_BLOB:
            encode_blob(colIndex, rset, x);
            break;
        case SQLT_BFILEE:
            encode_bfile(colIndex, rset, x);
            break;
        default:
            ei_x_encode_string(x, "unknown");
            break;
    }
    return 0;
}

void OracleDBOperation::encode_select_result(
        ei_x_buff * const result,
        ResultSet * const rset) {
    std::vector<MetaData> columnListMetaData =
            rset->getColumnListMetaData();
    unsigned int columnSize = columnListMetaData.size();
    unsigned int rowSize = 0;

    ei_x_new_with_version(result);
    ei_x_encode_tuple_header(result, 2);
    ei_x_encode_atom(result, "ok");


    int types[MAX_COLUMN_SIZE];
    unsigned int i;

    for (i = 0; i < columnSize; ++i) {
        types[i] = columnListMetaData[i]. getInt(
                MetaData::ATTR_DATA_TYPE);
        if (types[i] == SQLT_LNG) {
            rset->setMaxColumnSize(i + 1, MAX_LONG_SIZE);
        }
        if (types[i] == SQLT_LBI) {
            rset->setMaxColumnSize(i + 1, MAX_LBI_SIZE);
        }
    }

    int pos = result->index;
    ei_x_encode_list_header(result, 1);
    while (rset->next()) {
        ei_x_encode_list_header(result, columnSize);
        for (unsigned int i = 0; i < columnSize; ++i) {
            if (rset->isNull(i + 1)) {
                ei_x_encode_atom(result, "undefined");
            } else {
                encode_column(types[i], i + 1, rset, result);
            }
        }
        ei_x_encode_empty_list(result);

        rowSize++;
    }

    ei_x_encode_empty_list(result);

    ei_x_buff x;
    ei_x_new(&x);
    ei_x_encode_list_header(&x, rowSize);
    memcpy(result->buff + pos, x.buff, x.index);
    ei_x_free(&x);
}

int OracleDBOperation::get_data_type(int* p_index) {
    if (p_index == NULL) {
        p_index = &index_;
    }

    int type = -1;
    long key;
    switch(get_erl_type(p_index)) {
        case ERL_NIL_EXT:
            type = EMPTY;
            break;
        case ERL_SMALL_TUPLE_EXT:
        case ERL_LARGE_TUPLE_EXT:
          decode_tuple_header(p_index);
          if (decode_integer(key, p_index)) {
            switch (key){
            case DB_DRV_SQL_DATETIME:
              type = DATE;
              break;
            case DB_DRV_SQL_TIMESTAMP:
              type = TIMESTAMP;
              break;
            case DB_DRV_SQL_INTERVAL_YM:
              type = INTERVAL_YM;
              break;
            case DB_DRV_SQL_INTERVAL_DS:
              type = INTERVAL_DS;
              break;
            case DB_DRV_SQL_BFILE:
              type = BFILEE;
              break;
            }
          }
          /*
            if (decode_string(type_str, p_index)) {
            if (ewp_strcmp(type_str, "datetime") == 0) {
            type = DATE;
            } else if (ewp_strcmp(type_str, "timestamp") == 0) {
            type = TIMESTAMP;
            } else if (ewp_strcmp(type_str, "interval_ym") == 0) {
            type = INTERVAL_YM;
            } else if (ewp_strcmp(type_str, "interval_ds") == 0) {
            type = INTERVAL_DS;
            } else if (ewp_strcmp(type_str, "bfile") == 0) {
            type = BFILEE;
            }
            }
          */
          break;
        case ERL_SMALL_INTEGER_EXT:
        case ERL_INTEGER_EXT:
        case ERL_SMALL_BIG_EXT:
        case ERL_LARGE_BIG_EXT:
        case ERL_FLOAT_EXT:
            type = NUMBER;
            break;
        case ERL_ATOM_EXT:
        case ERL_STRING_EXT:
            type = STRING;
            break;
        case ERL_BINARY_EXT:
            type = BYTES;
            break;
        default:
            break;

    }

    if (type == -1) {
         DBException ex(BAD_ARG_ERROR);
         throw ex;
    }

    return type;
}

void OracleDBOperation::decode_and_set_param(Statement* stmt,
        unsigned int index, int* p_index) {
    if (p_index == NULL) {
        p_index = &index_;
    }

    int type = get_data_type(p_index);

    switch (type) {
        case EMPTY:
            decode_and_set_empty(stmt, index, p_index);
            break;
        case STRING:
            decode_and_set_string(stmt, index, p_index);
            break;
        case NUMBER:
            decode_and_set_number(stmt, index, p_index);
            break;
        case DATE:
            decode_and_set_date(stmt, index, p_index);
            break;
        case TIMESTAMP:
            decode_and_set_timestamp(stmt, index, p_index);
            break;
        case INTERVAL_YM:
            decode_and_set_interval_ym(stmt, index, p_index);
            break;
        case INTERVAL_DS:
            decode_and_set_interval_ds(stmt, index, p_index);
            break;
        case BYTES:
            decode_and_set_bytes(stmt, index, p_index);
            break;
        case BFILEE:
            decode_and_set_bfile(stmt, index, p_index);
            break;
        default:
            DBException ex("unkown data type");
            throw ex;
    }
}


void OracleDBOperation::make_expr(stringstream& sql,
        ParamIndex& param_index) {
    DBException ex(BAD_ARG_ERROR);
    long long lvalue;
    double dvalue;
    char* cvalue;
    long key_word;
    int type, index = index_;
    switch(get_erl_type()) {
        case ERL_SMALL_TUPLE_EXT:
        case ERL_LARGE_TUPLE_EXT:
            decode_tuple_header(&index);
            type = get_erl_type(&index);
            if (type == ERL_SMALL_INTEGER_EXT ||
                type == ERL_INTEGER_EXT ||
                type == ERL_SMALL_BIG_EXT ||
                type == ERL_LARGE_BIG_EXT) {
                if (decode_integer(key_word, &index)) {
                    index_ = index;
                    switch (key_word) {
                        case DB_DRV_SQL_AND:
                            make_and(sql, param_index);
                            break;
                        case DB_DRV_SQL_OR:
                            make_or(sql, param_index);
                            break;
                        case DB_DRV_SQL_NOT:
                            make_not(sql, param_index);
                            break;
                        case DB_DRV_SQL_LIKE:
                            make_like(sql, param_index);
                            break;
                        case DB_DRV_SQL_AS:
                            make_as(sql, param_index);
                            break;
                        case DB_DRV_SQL_EQUAL:
                            make_equal(sql, param_index);
                            break;
                        case DB_DRV_SQL_GREATER:
                            make_greater(sql, param_index);
                            break;
                        case DB_DRV_SQL_GREATER_EQUAL:
                            make_greater_equal(sql, param_index);
                            break;
                        case DB_DRV_SQL_LESS:
                            make_less(sql, param_index);
                            break;
                        case DB_DRV_SQL_LESS_EQUAL:
                            make_less_equal(sql, param_index);
                            break;
                        case DB_DRV_SQL_JOIN:
                            make_join(sql, param_index);
                            break;
                        case DB_DRV_SQL_LEFT_JOIN:
                            make_left_join(sql, param_index);
                            break;
                        case DB_DRV_SQL_RIGHT_JOIN:
                            make_right_join(sql, param_index);
                            break;
                        case DB_DRV_SQL_NOT_EQUAL:
                            make_not_equal(sql, param_index);
                            break;
                        case DB_DRV_SQL_ORDER:
                            make_order(sql, param_index);
                            break;
                        case DB_DRV_SQL_LIMIT:
                            make_limit(sql, param_index);
                            break;
                        case DB_DRV_SQL_DOT:
                            make_dot(sql, param_index);
                            break;
                        case DB_DRV_SQL_GROUP:
                            make_group(sql, param_index);
                            break;
                        case DB_DRV_SQL_HAVING:
                            make_having(sql, param_index);
                            break;
                        case DB_DRV_SQL_BETWEEN:
                            make_between(sql, param_index);
                            break;
                        case DB_DRV_SQL_ADD:
                            make_add(sql, param_index);
                            break;
                        case DB_DRV_SQL_SUB:
                            make_sub(sql, param_index);
                            break;
                        case DB_DRV_SQL_MUL:
                            make_mul(sql, param_index);
                            break;
                        case DB_DRV_SQL_DIV:
                            make_div(sql, param_index);
                            break;
                        case DB_DRV_SQL_FUN:
                            make_fun(sql, param_index);
                            break;
                        case DB_DRV_SQL_INNER_JOIN:
                            make_inner_join(sql, param_index);
                            break;
                        case DB_DRV_SQL_IS_NULL:
                            make_is_null(sql, param_index);
                            break;
                        case DB_DRV_SQL_IS_NOT_NULL:
                            make_is_not_null(sql, param_index);
                            break;
                        default:
                            throw ex;
                            break;
                    } /** end of switch (key_word) */
                } /** end of if (decode_integer(key_word)) */
            } else {
                param_index.push_back(index_);
                skip_term();
                sql << " :" << param_index.size();
            }
            break;
        case ERL_SMALL_INTEGER_EXT:
        case ERL_INTEGER_EXT:
        case ERL_SMALL_BIG_EXT:
        case ERL_LARGE_BIG_EXT:
            if (decode_integer(lvalue)) {
                sql << " " << lvalue;
            } else {
                throw ex;
            }
            break;
        case ERL_FLOAT_EXT:
            if (decode_double(dvalue)) {
                sql << " " << dvalue;
            } else {
                throw ex;
            }
            break;
        case ERL_ATOM_EXT:
            decode_string_with_throw(cvalue);
            sql << " " << cvalue;
            free_string(cvalue);
            break;
        case ERL_STRING_EXT:
        case ERL_LIST_EXT:
        case ERL_NIL_EXT:
            decode_string_with_throw(cvalue);
            sql << " \'" << cvalue << "\'";
            free_string(cvalue);
            break;
        case ERL_BINARY_EXT:
            param_index.push_back(index_);
            skip_term();
            break;
        default:
            throw ex;
    }
}
