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
 *  @file SybDBOperation.cpp
 *  @brief Derived class for sybase to represent operations of database.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Mon May 16 12:37:54 CST 2011
 */

#include "SybDBOperation.h"
#include "../base/ConnectionPool.h"
#include "../util/EiEncoder.h"

namespace rytong {
#define RETURN_ERROR(res, error) {EiEncoder::encode_error_msg(error, res); return true;}

const char* SybDBOperation::CONN_NULL_ERROR = "get connection failed";
const char* SybDBOperation::STMT_NULL_ERROR = "get statement failed";
const char* SybDBOperation::BAD_ARG_ERROR = "bad argument";
const char* SybDBOperation::EXECUTE_SQL_ERROR = "execute sql failed";

SybDBOperation::SybDBOperation() {
    limit_row_count_ = 0;
}

SybDBOperation::~SybDBOperation() {
}

bool SybDBOperation::exec(ei_x_buff * const res)
{
    bool retcode;
    char* sql = NULL;
    SybConnection* conn = NULL;
    SybStatement* stmt = NULL;

    if (conn_ == NULL) {
        RETURN_ERROR(res, CONN_NULL_ERROR)
    }
    if (!decode_string(sql)) {
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }
    conn = (SybConnection*)conn_;
    stmt = conn->get_statement();
    retcode = stmt->execute_sql(res, sql);
    free_string(sql);
    if (!retcode) {
        RETURN_ERROR(res, EXECUTE_SQL_ERROR)
    }

    return true;
}

bool SybDBOperation::trans_begin(ei_x_buff * const res)
{
    SybConnection* conn = NULL;
    SybStatement* stmt = NULL;

    if (conn_ == NULL) {
        RETURN_ERROR(res, CONN_NULL_ERROR)
    }
    conn = (SybConnection*)conn_;
    stmt = conn->get_statement();
    if (stmt->execute_cmd("BEGIN TRANSACTION")) {
        EiEncoder::encode_ok_pointer((void*) conn, res);
    } else {
        RETURN_ERROR(res, EXECUTE_SQL_ERROR)
    }

    return true;
}

bool SybDBOperation::trans_commit(ei_x_buff * const res)
{
    SybConnection* conn = NULL;
    SybStatement* stmt = NULL;

    if (conn_ == NULL) {
        RETURN_ERROR(res, CONN_NULL_ERROR)
    }
    conn = (SybConnection*)conn_;
    stmt = conn->create_statement();
    if (stmt->execute_cmd("COMMIT TRANSACTION")) {
        EiEncoder::encode_ok_msg("COMMIT", res);
    } else {
        RETURN_ERROR(res, EXECUTE_SQL_ERROR)
    }

    return true;
}

bool SybDBOperation::trans_rollback(ei_x_buff * const res)
{
    SybConnection* conn = NULL;
    SybStatement* stmt = NULL;

    if (conn_ == NULL) {
        RETURN_ERROR(res, CONN_NULL_ERROR)
    }
    conn = (SybConnection*)conn_;
    stmt = conn->create_statement();
    if (stmt->execute_cmd("ROLLBACK TRANSACTION")) {
        EiEncoder::encode_ok_msg("ROLLBACK", res);
    } else {
        RETURN_ERROR(res, EXECUTE_SQL_ERROR)
    }

    return true;
}

bool SybDBOperation::prepare_stat_init(ei_x_buff * const res)
{
    char* name = NULL;
    char *sql = NULL;
    SybConnection* conn = NULL;
    SybStatement* stmt = NULL;

    if (conn_ == NULL) {
        RETURN_ERROR(res, CONN_NULL_ERROR)
    }

    decode_tuple_header();
    if (!decode_string(name)) {
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }
    if (!decode_string(sql)) {
        free_string(name);
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }

    conn = (SybConnection*)conn_;
    stmt = conn->create_statement(sql);
    if (!stmt->prepare_init(name)) {
        free(name);
        free(sql);
        RETURN_ERROR(res, "prepare init failed")
    }

    if (stmt_map_->add((string)name, (void*)stmt)) {
        EiEncoder::encode_ok_msg(name, res);
    } else {
        stmt->prepare_release();
        conn->terminate_statement(stmt);
        delete stmt;
        EiEncoder::encode_error_msg("already registered prepare name", res);
    }

    free_string(name);
    free_string(sql);

    return true;
}

bool SybDBOperation::prepare_stat_exec(ei_x_buff * const res)
{
    bool retcode;
    char* name = NULL;
    char* param = NULL;
    SybStatement* stmt = NULL;

    decode_tuple_header();
    if (!decode_string(name)) {
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }

    stmt = (SybStatement*) stmt_map_->get(name);
    if (stmt == NULL) {
        free_string(name);
        RETURN_ERROR(res, STMT_NULL_ERROR)
    }

    if (get_erl_type() == ERL_LIST_EXT) {
        long param_count = decode_list_header();
        if (param_count != stmt->get_param_count()) {
            free_string(name);
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }

        for (long i = 0; i < param_count; i++) {
            if (!decode_and_set_param(stmt, i + 1)) {
                free_string(name);
                RETURN_ERROR(res, BAD_ARG_ERROR)
            }
        }
    } else {
        if (!decode_string(param)) {
            free_string(name);
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
        for (unsigned int i = 0; i < strlen(param); ++i) {
            if (!set_param(stmt, i + 1, (unsigned char)param[i])) {
                free_string(name);
                free_string(param);
                RETURN_ERROR(res, BAD_ARG_ERROR)
            }
        }
        free_string(param);
    }

    retcode = stmt->execute_sql(res);

    free_string(name);

    if (!retcode) {
        RETURN_ERROR(res, EXECUTE_SQL_ERROR)
    }

    return true;
}

bool SybDBOperation::prepare_stat_release(ei_x_buff * const res)
{
    char* name = NULL;
    SybStatement* stmt = NULL;

    if (!decode_string(name)) {
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }
    stmt = (SybStatement*) stmt_map_->remove(string(name));
    if (stmt == NULL) {
        free_string(name);
        RETURN_ERROR(res, STMT_NULL_ERROR)
    }
    if (!stmt->prepare_release()) {
        EiEncoder::encode_error_msg("close stmt failed", res);
    } else {
        EiEncoder::encode_ok_msg("close stmt", res);
    }
    delete stmt;
    free_string(name);

    return true;
}

bool SybDBOperation::insert(ei_x_buff * const res)
{
    bool retcode;
    int num_field;
    stringstream sqlstream, tmpstream;
    char* table_name = NULL;
    char* field_name = NULL;
    SybStatement* stmt = NULL;
    SybConnection* conn = NULL;

    if (conn_ == NULL) {
        RETURN_ERROR(res, CONN_NULL_ERROR)
    } else {
        conn = (SybConnection*)conn_;
    }

    if (decode_tuple_header() !=2 || !decode_string(table_name)) {
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }
    if ((num_field = decode_list_header()) < 0) {
        free_string(table_name);
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }

    sqlstream << "INSERT INTO " << table_name << " (";
    tmpstream << " VALUES(";
    for (int i = 0; i < num_field; ++i) {
        if (decode_tuple_header() != 2 || !decode_string(field_name)) {
            free_string(table_name);
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
        sqlstream << field_name;
        if (!decode_and_input_value(tmpstream)) {
            free_string(table_name);
            free_string(field_name);
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
        if (i < num_field - 1) {
            sqlstream << ",";
            tmpstream << ",";
        }
        free_string(field_name);
    }
    sqlstream << ")";
    tmpstream << ")";

    stmt = conn->get_statement();
    retcode = stmt->execute_sql(res, (sqlstream.str() + tmpstream.str()).c_str());
    free_string(table_name);
    if (!retcode) {
        RETURN_ERROR(res, EXECUTE_SQL_ERROR)
    }

    return true;
}

bool SybDBOperation::update(ei_x_buff * const res)
{
    bool retcode;
    int num_field;
    stringstream sqlstream;
    char* table_name = NULL;
    char* field_name = NULL;
    SybStatement* stmt = NULL;
    SybConnection* conn = NULL;

    if (conn_ == NULL) {
        RETURN_ERROR(res, CONN_NULL_ERROR)
    } else {
        conn = (SybConnection*)conn_;
    }

    if (decode_tuple_header() < 0 || !decode_string(table_name)) {
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }
    if ((num_field = decode_list_header()) < 0) {
        free_string(table_name);
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }

    sqlstream << "UPDATE " << table_name << " SET ";
    for (int i = 0; i < num_field; ++i) {
        if (decode_tuple_header() != 2 || !decode_string(field_name)) {
            free_string(table_name);
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
        sqlstream << field_name << "=";
        if (!decode_and_input_value(sqlstream)) {
            free_string(table_name);
            free_string(field_name);
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
        if (i < num_field - 1) {
            sqlstream << ",";
        }
        free_string(field_name);
    }
    if (!decode_empty_list()) {
        free(table_name);
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }
    if (decode_list_header() > 0) {
        sqlstream << " WHERE";
        if (!make_where_expr(sqlstream)) {
            free(table_name);
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
    }

    stmt = conn->get_statement();
    retcode = stmt->execute_sql(res, sqlstream.str().c_str());
    free_string(table_name);
    if (!retcode) {
        RETURN_ERROR(res, EXECUTE_SQL_ERROR)
    }

    return true;
}

bool SybDBOperation::del(ei_x_buff * const res)
{
    bool retcode;
    stringstream sqlstream;
    char* table_name = NULL;
    SybStatement* stmt = NULL;
    SybConnection* conn = NULL;

    if (conn_ == NULL) {
        RETURN_ERROR(res, CONN_NULL_ERROR)
    } else {
        conn = (SybConnection*)conn_;
    }

    if (decode_tuple_header() < 0 || !decode_string(table_name)) {
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }
    sqlstream << "DELETE FROM " << table_name;
    if (decode_list_header() > 0) {
        sqlstream << " WHERE";
        if (!make_where_expr(sqlstream)) {
            free(table_name);
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
    }

    stmt = conn->get_statement();
    retcode = stmt->execute_sql(res, sqlstream.str().c_str());
    free_string(table_name);
    if (!retcode) {
        RETURN_ERROR(res, EXECUTE_SQL_ERROR)
    }

    return true;
}

bool SybDBOperation::select(ei_x_buff * const res)
{
    bool retcode;
    stringstream sqlstream;
    long distinct;
    int num;
    SybStatement* stmt = NULL;
    SybConnection* conn = NULL;

    if (conn_ == NULL) {
        RETURN_ERROR(res, CONN_NULL_ERROR)
    } else {
        conn = (SybConnection*)conn_;
    }
    limit_row_count_ = 0;

    /* decode distinct expression */
    if (decode_tuple_header() < 0 || !decode_integer(distinct)) {
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }
    sqlstream << ((0 == distinct) ? "SELECT" : "SELECT DISTINCT");

    /* decode select field expression */
    if (get_erl_type() != ERL_LIST_EXT) {
        sqlstream << " *";
        if (!skip_term()) {
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
    } else {
        if ((num = decode_list_header()) < 0) {
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
        for (int i = 0; i < num; ++i) {
            if (!make_where_expr(sqlstream)) {
                RETURN_ERROR(res, BAD_ARG_ERROR)
            }
            if (i < num - 1) {
                sqlstream << " ,";
            }
        }
        if (!decode_empty_list()) {
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
    }

    /* decode table expression */
    sqlstream << " FROM";
    if ((num = decode_list_header()) < 0) {
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }
    for (int i = 0; i < num; ++i) {
        if (!make_where_expr(sqlstream)) {
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
        if (i < num - 1) {
            sqlstream << " ,";
        }
    }
    if (!decode_empty_list()) {
        RETURN_ERROR(res, BAD_ARG_ERROR)
    }

    /* decode where expression */
    if (decode_list_header() > 0) {
        sqlstream << " WHERE";
        if (!make_where_expr(sqlstream) || !decode_empty_list()) {
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
    } else {
        if (!skip_term()) {
            RETURN_ERROR(res, BAD_ARG_ERROR)
        }
    }

    /* decode extras expression*/
    if ((num = decode_list_header()) > 0) {
        for (int i = 0; i < num; ++i) {
            if (!make_where_expr(sqlstream)) {
                RETURN_ERROR(res, BAD_ARG_ERROR)
            }
        }
    }

    if (limit_row_count_ > 0) {
        if (!conn->set_limit_row_count(limit_row_count_)) {
            conn->terminate_statement(stmt);
            RETURN_ERROR(res, "set limit row count failed")
        }
    }
    stmt = conn->get_statement();
    retcode = stmt->execute_sql(res, sqlstream.str().c_str());
    conn->set_limit_row_count(0);

    if (!retcode) {
        RETURN_ERROR(res, EXECUTE_SQL_ERROR)
    }

    return true;
}
}/* end of namespace rytong */
