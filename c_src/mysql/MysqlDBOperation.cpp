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
 * $Id: MysqlDBOperation.cpp 22235 2010-02-25 09:13:30Z deng.lifen $
 *
 *  @file MysqlDBOperation.cpp
 *  @brief Derived class for mysql to represent operations of database.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-1-31
 */

#include "MysqlDBOperation.h"
#include "../base/ConnectionPool.h"
#include "../util/EiEncoder.h"

using namespace rytong;

static const char* CONN_NULL_ERROR = "failed to get conn";
static const int STRING_SIZE = 1000;
static const int STMT_RES_FIELD_COUNT = 100;

MysqlDBOperation::MysqlDBOperation() {
}

MysqlDBOperation::~MysqlDBOperation() {
}

bool MysqlDBOperation::exec(ei_x_buff * const res) {
    char * sql = NULL;
    if (decode_string(sql)){
        exec_sql(sql, res);
        free_string(sql);
    } else {
        EiEncoder::encode_error_msg("failed to decode sql", res);
    }
    return true;
}

bool MysqlDBOperation::insert(ei_x_buff * const res) {
    char * table = NULL;
    stringstream sql_sm;
    stringstream value_sm;
    FieldStruct * field_list = NULL;
    ei_decode_tuple_header(buf_, &index_, &type_);
    decode_string(table);
    const int len = decode_fields(field_list);

    sql_sm << "insert into " << table << " (";
    value_sm << ") values(";
    for (int i = 0; i < len; i++) {
        if (0 != i) {
            sql_sm << ",";
            value_sm << ",";
        }
        sql_sm << field_list[i].field_name;
        fill_value(value_sm, field_list[i].field_value);
    }
    sql_sm << value_sm.str() << ")";
    exec_sql(sql_sm.str().c_str(), res);
    free_string(table);
    free_fields(field_list, len);
    return true;
}

bool MysqlDBOperation::update(ei_x_buff * const res) {
    char * table = NULL;
    stringstream sql_sm;
    FieldStruct * field_list = NULL;
    ei_decode_tuple_header(buf_, &index_, &type_);
    decode_string(table);
    const int len = decode_fields(field_list);

    sql_sm << "update " << table << " set ";
    for (int i = 0; i < len; i++) {
        sql_sm << field_list[i].field_name << "=";
        fill_value(sql_sm, field_list[i].field_value);
        if (i < len - 1) {
            sql_sm << ",";
        }
    }
    make_where(sql_sm);
    exec_sql(sql_sm.str().c_str(), res);
    free_string(table);
    free_fields(field_list, len);
    return true;
}

bool MysqlDBOperation::del(ei_x_buff * const res) {
    char * table = NULL;
    stringstream sql_sm;
    ei_decode_tuple_header(buf_, &index_, &type_);
    decode_string(table);

    sql_sm << "delete from " << table;
    make_where(sql_sm);
    exec_sql(sql_sm.str().c_str(), res);
    free_string(table);
    return true;
}

bool MysqlDBOperation::select(ei_x_buff * const res) {
    stringstream sql_sm;
    ei_decode_tuple_header(buf_, &index_, &type_);
    sql_sm << ((0 == decode_int()) ? "select" : "select distinct");
    make_field(sql_sm);
    make_table(sql_sm);
    make_where(sql_sm);
    make_extras(sql_sm);
    exec_sql(sql_sm.str().c_str(), res);
    return true;
}

/** transaction begin interface  */
bool MysqlDBOperation::trans_begin(ei_x_buff * const res) {
    MYSQL* db_conn = NULL;
    if (real_query_sql("BEGIN", res, db_conn)) {
        EiEncoder::encode_ok_pointer((void*) conn_, res);
        return true;
    } else {
        return false;
    }
}

/** transaction commit interface  */
bool MysqlDBOperation::trans_commit(ei_x_buff * const res) {
    MYSQL* db_conn = NULL;
    const char * sql = "COMMIT";
    if (real_query_sql(sql, res, db_conn)) {
        EiEncoder::encode_ok_msg(sql, res);
        return true;
    }
    return false;
}

/** transaction rollback interface  */
bool MysqlDBOperation::trans_rollback(ei_x_buff * const res) {
    MYSQL* db_conn = NULL;
    const char * sql = "ROLLBACK";
    if (real_query_sql(sql, res, db_conn)) {
        EiEncoder::encode_ok_msg(sql, res);
        return true;
    }
    return false;
}

bool MysqlDBOperation::prepare_statement_init(ei_x_buff * const res) {
    char * sql = NULL;
    decode_string(sql);
    if (NULL == conn_) {
        EiEncoder::encode_error_msg(CONN_NULL_ERROR, res);
    } else {
        MYSQL* db_conn = (MYSQL*) (conn_->get_connection());
        MYSQL_STMT* mysql_stmt = mysql_stmt_init(db_conn);
        if (!mysql_stmt) {
            EiEncoder::encode_error_msg("mysql_stmt_init(), out of memory", res);
        } else {
            if (mysql_stmt_prepare(mysql_stmt, sql, strlen(sql))) {
                EiEncoder::encode_error_msg(mysql_stmt_error(mysql_stmt), res);
            } else {
                StmtData* stmt_data = new StmtData;
                stmt_data->stmt = mysql_stmt;
                stmt_data->meta_data = mysql_stmt_result_metadata(mysql_stmt);
                EiEncoder::encode_ok_pointer(stmt_data, res);
            }
        }
    }

    // free_string(prepare_name);
    free_string(sql);
    return true;
}

bool MysqlDBOperation::prepare_statement_exec(ei_x_buff * const res) {
    StmtData* stmt_data = NULL;
    FieldValue * stmt_fields = NULL;
    ei_decode_tuple_header(buf_, &index_, &type_);
    ei_decode_binary(buf_, &index_, &stmt_data, &bin_size_);
    int len = decode_stmt_fields(stmt_fields);

    if (stmt_data == NULL) {
        EiEncoder::encode_error_msg("failed to get stmt data", res);
    } else {
        exec_stmt(stmt_data, stmt_fields, res);
    }

    free_stmt_fields(stmt_fields, len);
    return true;
}

/** perpare statement release interface  */
bool MysqlDBOperation::prepare_statement_release(ei_x_buff * const res) {
    StmtData* stmt_data = NULL;
    ei_decode_binary(buf_, &index_, &stmt_data, &bin_size_);
    if (NULL == stmt_data) {
        EiEncoder::encode_error_msg("failed to get stmt data", res);
    } else {
        mysql_free_result((MYSQL_RES*)stmt_data->meta_data);
        mysql_stmt_close((MYSQL_STMT*)stmt_data->stmt);
        delete stmt_data;
        EiEncoder::encode_ok_msg("close stmt", res);
    }
    return true;
}

/** perpare statement init interface  */
bool MysqlDBOperation::prepare_stat_init(ei_x_buff * const res) {
    char * prepare_name = NULL;
    char * sql = NULL;
    ei_decode_tuple_header(buf_, &index_, &type_);
    decode_string(prepare_name);
    decode_string(sql);
    if (NULL == conn_) {
        EiEncoder::encode_error_msg(CONN_NULL_ERROR, res);
    } else {
        MYSQL* db_conn = (MYSQL*) (conn_->get_connection());
        MYSQL_STMT* mysql_stmt = mysql_stmt_init(db_conn);
        if (!mysql_stmt) {
            EiEncoder::encode_error_msg("mysql_stmt_init(), out of memory", res);
        } else {
            if (mysql_stmt_prepare(mysql_stmt, sql, strlen(sql))) {
                EiEncoder::encode_error_msg(mysql_stmt_error(mysql_stmt), res);
            } else {
                StmtData* stmt_data = new StmtData;
                stmt_data->stmt = mysql_stmt;
                stmt_data->meta_data = mysql_stmt_result_metadata(mysql_stmt);
                if (stmt_map_->add(string(prepare_name), (void*)stmt_data)) {
                    EiEncoder::encode_ok_msg(prepare_name, res);
                } else {
                    mysql_free_result((MYSQL_RES *)stmt_data->meta_data);
                    mysql_stmt_close(mysql_stmt);
                    delete stmt_data;
                    EiEncoder::encode_error_msg(
                            "mysql_stmt_init(), already registered prepare name",
                            res);
                }

            }
        }
    }

    free_string(prepare_name);
    free_string(sql);
    return true;
}

bool MysqlDBOperation::prepare_stat_exec(ei_x_buff * const res) {
    char * prepare_name = NULL;
    FieldValue * stmt_fields = NULL;
    ei_decode_tuple_header(buf_, &index_, &type_);
    decode_string(prepare_name);
    int len = decode_stmt_fields(stmt_fields);

    StmtData* stmt_data = (StmtData*)stmt_map_->get(string(prepare_name));
    if (stmt_data == NULL) {
        EiEncoder::encode_error_msg("failed to get stmt data", res);
    } else {
        exec_stmt(stmt_data, stmt_fields, res);
    }

    free_string(prepare_name);
    free_stmt_fields(stmt_fields, len);
    return true;
}

/** perpare statement release interface  */
bool MysqlDBOperation::prepare_stat_release(ei_x_buff * const res) {
    char * prepare_name = NULL;
    decode_string(prepare_name);
    StmtData* stmt_data = (StmtData*)stmt_map_->remove(string(prepare_name));
    if (NULL == stmt_data) {
        EiEncoder::encode_error_msg("failed to get stmt data", res);
    } else {
        mysql_free_result((MYSQL_RES*)stmt_data->meta_data);
        mysql_stmt_close((MYSQL_STMT*)stmt_data->stmt);
        delete stmt_data;
        EiEncoder::encode_ok_msg("close stmt", res);
    }
    free_string(prepare_name);
    return true;
}

void MysqlDBOperation::exec_stmt(StmtData* stmt_data, FieldValue* stmt_fields,
        ei_x_buff * const res) {
    MYSQL_STMT* stmt = (MYSQL_STMT*)stmt_data->stmt;
    // MYSQL* db_conn = (MYSQL*) (conn_->get_connection());
    // MYSQL_STMT* stmt = mysql_stmt_init(db_conn);
    unsigned long param_count = mysql_stmt_param_count(stmt);

    if (0 == param_count) {
        call_exec_stmt(stmt_data, res);
        return;
    }

    MYSQL_BIND* bind = new MYSQL_BIND[param_count];
    memset(bind, 0, sizeof (MYSQL_BIND) * param_count);

    for (unsigned long i = 0; i < param_count; i++) {
        bind[i].buffer_type = get_db_field_type(stmt_fields[i].erl_type);
        bind[i].is_null = new my_bool;
        bind[i].length = new unsigned long;
        switch (stmt_fields[i].erl_type) {
            case ERL_SMALL_INTEGER_EXT:
            case ERL_INTEGER_EXT:
            case ERL_SMALL_BIG_EXT:
            case ERL_LARGE_BIG_EXT:
                bind[i].buffer = new long;
                break;
            case ERL_FLOAT_EXT:
                bind[i].buffer = new double;
                break;
            case ERL_LIST_EXT:
            case ERL_STRING_EXT:
            case ERL_ATOM_EXT:
            case ERL_BINARY_EXT:
                bind[i].buffer = new char[stmt_fields[i].length];
                bind[i].buffer_length = stmt_fields[i].length;
                break;
            default:
                break;
        }
    }

    if (mysql_stmt_bind_param(stmt, bind)) {
        EiEncoder::encode_error_msg(mysql_stmt_error(stmt), res);
    } else {
        for (unsigned long i = 0; i < param_count; i++) {
            memcpy(bind[i].buffer, stmt_fields[i].value, stmt_fields[i].length);
            switch (stmt_fields[i].erl_type) {
                case ERL_SMALL_INTEGER_EXT:
                case ERL_INTEGER_EXT:
                case ERL_SMALL_BIG_EXT:
                case ERL_LARGE_BIG_EXT:
                case ERL_FLOAT_EXT:
                    *bind[i].is_null = 0;
                    break;
                case ERL_LIST_EXT:
                case ERL_STRING_EXT:
                case ERL_ATOM_EXT:
                case ERL_BINARY_EXT:
                    *bind[i].is_null = 0;
                    *bind[i].length = stmt_fields[i].length;
                    break;
                default:
                    break;
            }
        }
        call_exec_stmt(stmt_data, res);
    }

    for (unsigned long i = 0; i < param_count; i++) {
        switch (stmt_fields[i].erl_type) {
            case ERL_SMALL_INTEGER_EXT:
            case ERL_INTEGER_EXT:
            case ERL_SMALL_BIG_EXT:
            case ERL_LARGE_BIG_EXT:
                delete (long*) bind[i].buffer;
                break;
            case ERL_FLOAT_EXT:
                delete (double*) bind[i].buffer;
                break;
            case ERL_LIST_EXT:
            case ERL_STRING_EXT:
            case ERL_ATOM_EXT:
            case ERL_BINARY_EXT:
                delete (char*) bind[i].buffer;
                break;
            default:
                break;
        }
        bind[i].buffer = NULL;
    }
    delete [] bind;
    bind = NULL;
}

void MysqlDBOperation::call_exec_stmt(StmtData* stmt_data, ei_x_buff * const res) {
    MYSQL_STMT* stmt = (MYSQL_STMT*)stmt_data->stmt;
    if (mysql_stmt_execute(stmt)) {
        EiEncoder::encode_error_msg(mysql_stmt_error(stmt), res);
    } else {
        unsigned int field_count = mysql_stmt_field_count(stmt);
        if (field_count > 0) {
            encode_stmt_result(stmt_data, field_count, res);
        } else {
            EiEncoder::encode_ok_number((long) mysql_stmt_affected_rows(stmt), res);
        }
    }
}

void MysqlDBOperation::encode_stmt_result(StmtData* stmt_data,
        unsigned int field_count, ei_x_buff * const res) {
    if (field_count > (unsigned int)STMT_RES_FIELD_COUNT) {
        EiEncoder::encode_error_msg("result fields is too long", res);
    }

    unsigned long length[STMT_RES_FIELD_COUNT];
    MYSQL_RES * meta_data = (MYSQL_RES*)stmt_data->meta_data;
    MYSQL_STMT * stmt = (MYSQL_STMT*)stmt_data->stmt;
    MYSQL_BIND* bind = new MYSQL_BIND[field_count];
    memset(bind, 0, sizeof (MYSQL_BIND) * field_count);

    for (unsigned int i = 0; i < field_count; i++) {
        bind[i].buffer_type = meta_data->fields[i].type;
        bind[i].is_null = new my_bool;
        memset(bind[i].is_null, 0, sizeof(my_bool));
        bind[i].length = &length[i];
        *bind[i].length = 0;

        if (meta_data->fields[i].type == MYSQL_TYPE_TINY) {
            bind[i].buffer = new char;
            bind[i].buffer_length = sizeof(char);
        } else if (meta_data->fields[i].type == MYSQL_TYPE_FLOAT) {
            bind[i].buffer = new float;
            bind[i].buffer_length = sizeof(float);
        } else if (meta_data->fields[i].type == MYSQL_TYPE_DOUBLE) {
            bind[i].buffer = new double;
            bind[i].buffer_length = sizeof(double);
        } else if (meta_data->fields[i].type == MYSQL_TYPE_DECIMAL
                || meta_data->fields[i].type == MYSQL_TYPE_NEWDECIMAL) {
            bind[i].buffer = new char[STRING_SIZE];
            bind[i].buffer_length = STRING_SIZE;
        } else if (meta_data->fields[i].type == MYSQL_TYPE_SHORT
                || meta_data->fields[i].type == MYSQL_TYPE_YEAR) {
            bind[i].buffer = new short;
            bind[i].buffer_length = sizeof(short);
        } else if (meta_data->fields[i].type == MYSQL_TYPE_INT24
                || meta_data->fields[i].type == MYSQL_TYPE_LONG) {
            bind[i].buffer = new int;
            bind[i].buffer_length = sizeof(int);
        } else if (meta_data->fields[i].type == MYSQL_TYPE_LONGLONG) {
            bind[i].buffer = new long long int;
            bind[i].buffer_length = sizeof(long long int);
        } else if (meta_data->fields[i].type == MYSQL_TYPE_TIME
                || meta_data->fields[i].type == MYSQL_TYPE_DATE
                || meta_data->fields[i].type == MYSQL_TYPE_DATETIME
                || meta_data->fields[i].type == MYSQL_TYPE_TIMESTAMP) {
            bind[i].buffer = new MYSQL_TIME;
            bind[i].buffer_length = sizeof (MYSQL_TIME);
        } else if (IS_BLOB(meta_data->fields[i].flags)) {
            bind[i].buffer = new char[STRING_SIZE];
            bind[i].buffer_length = STRING_SIZE;
        } else {
            bind[i].buffer = new char[STRING_SIZE];
            bind[i].buffer_length = STRING_SIZE;
        }
        memset(bind[i].buffer, 0, bind[i].buffer_length);
    }

    if (mysql_stmt_bind_result(stmt, bind)) {
        EiEncoder::encode_error_msg(mysql_stmt_error(stmt), res);
        return;
    }

    if (mysql_stmt_store_result(stmt)) {
        EiEncoder::encode_error_msg(mysql_stmt_error(stmt), res);
        return;
    }
    unsigned long long row_count = mysql_stmt_num_rows(stmt);
    if (0 == row_count) {
        EiEncoder::encode_ok_msg("", res);
    } else {
        ei_x_new_with_version(res);
        ei_x_encode_tuple_header(res, 2);
        ei_x_encode_atom(res, "ok");
        ei_x_encode_list_header(res, row_count);

        while (mysql_stmt_fetch(stmt) == 0) {
            ei_x_encode_list_header(res, field_count);
            for (unsigned int i = 0; i < field_count; i++) {
                if (*bind[i].is_null) {
                    ei_x_encode_atom(res, "undefined");
                } else {
                    if (meta_data->fields[i].type == MYSQL_TYPE_TINY) {
                        ei_x_encode_char(res, *(char*)bind[i].buffer);
                    } else if (meta_data->fields[i].type == MYSQL_TYPE_FLOAT) {
                        stringstream float_sm;
                        float_sm << *(float*)bind[i].buffer;
                        double float_data;
                        float_sm >> float_data;
                        ei_x_encode_double(res, float_data);
                    } else if (meta_data->fields[i].type == MYSQL_TYPE_DOUBLE) {
                        ei_x_encode_double(res, *(double*) bind[i].buffer);
                    } else if (meta_data->fields[i].type == MYSQL_TYPE_DECIMAL
                            || meta_data->fields[i].type == MYSQL_TYPE_NEWDECIMAL) {
                        stringstream decimal_sm((char*) bind[i].buffer);
                        double decimal_data;
                        decimal_sm >> decimal_data;
                        ei_x_encode_double(res, decimal_data);
                    } else if (meta_data->fields[i].type == MYSQL_TYPE_SHORT
                            || meta_data->fields[i].type == MYSQL_TYPE_YEAR) {
                        short short_data = *(short*) bind[i].buffer;
                        ei_x_encode_long(res, (long)short_data);
                    } else if (meta_data->fields[i].type == MYSQL_TYPE_INT24
                            || meta_data->fields[i].type == MYSQL_TYPE_LONG) {
                        int int_data = *(int*) bind[i].buffer;
                        ei_x_encode_long(res, (long) int_data);
                    } else if (meta_data->fields[i].type == MYSQL_TYPE_LONGLONG) {
                        long long int long_data = *(long long int*) bind[i].buffer;
                        ei_x_encode_long(res, (long) long_data);
                    } else if (meta_data->fields[i].type == MYSQL_TYPE_DATETIME
                            || meta_data->fields[i].type == MYSQL_TYPE_TIMESTAMP) {
                        MYSQL_TIME tm_data = *(MYSQL_TIME*) bind[i].buffer;
                        ei_x_encode_tuple_header(res, 2);
                        ei_x_encode_atom(res, "datetime");
                        ei_x_encode_tuple_header(res, 2);
                        encode_date_tuple(tm_data.year, tm_data.month, tm_data.day, res);
                        encode_date_tuple(tm_data.hour, tm_data.minute, tm_data.second, res);
                    } else if (meta_data->fields[i].type == MYSQL_TYPE_DATE) {
                        MYSQL_TIME tm_data = *(MYSQL_TIME*) bind[i].buffer;
                        ei_x_encode_tuple_header(res, 2);
                        ei_x_encode_atom(res, "date");
                        encode_date_tuple(tm_data.year, tm_data.month, tm_data.day, res);
                    } else if (meta_data->fields[i].type == MYSQL_TYPE_TIME) {
                        MYSQL_TIME tm_data = *(MYSQL_TIME*) bind[i].buffer;
                        ei_x_encode_tuple_header(res, 2);
                        ei_x_encode_atom(res, "time");
                        encode_date_tuple(tm_data.hour, tm_data.minute, tm_data.second, res);
                    } else if (IS_BLOB(meta_data->fields[i].flags)
                            || meta_data->fields[i].type == MYSQL_TYPE_BIT) {
                        ei_x_encode_binary(res, (char*) bind[i].buffer, *bind[i].length);
                    } else {
                        ei_x_encode_string(res, (char*) bind[i].buffer);
                    }
                }
            }
            ei_x_encode_empty_list(res);
        }
        ei_x_encode_empty_list(res);
    }

    for(unsigned int i = 0; i < field_count; i++) {
        delete bind[i].is_null;
        if (meta_data->fields[i].type == MYSQL_TYPE_FLOAT) {
            delete (float*)bind[i].buffer;
        } else if (meta_data->fields[i].type == MYSQL_TYPE_DOUBLE) {
            delete (double*)bind[i].buffer;
        } else if (meta_data->fields[i].type == MYSQL_TYPE_SHORT
                || meta_data->fields[i].type == MYSQL_TYPE_YEAR) {
            delete (short*)bind[i].buffer;
        } else if (meta_data->fields[i].type == MYSQL_TYPE_INT24
                || meta_data->fields[i].type == MYSQL_TYPE_LONG) {
            delete (int*)bind[i].buffer;
        } else if (meta_data->fields[i].type == MYSQL_TYPE_LONGLONG) {
            delete (long long int*)bind[i].buffer;
        } else if (meta_data->fields[i].type == MYSQL_TYPE_TIME
                || meta_data->fields[i].type == MYSQL_TYPE_DATE
                || meta_data->fields[i].type == MYSQL_TYPE_DATETIME
                || meta_data->fields[i].type == MYSQL_TYPE_TIMESTAMP) {
            delete (MYSQL_TIME*)bind[i].buffer;
        } else if (meta_data->fields[i].type == MYSQL_TYPE_TINY) {
            delete (char*)bind[i].buffer;
        } else {
            delete [] (char*)bind[i].buffer;
        }
    }

    mysql_stmt_free_result(stmt);
    delete [] bind;
}

void MysqlDBOperation::exec_sql(const char* sql, ei_x_buff* res) {
    MYSQL * db_conn = NULL;
    // cout << "sql=" << sql << "\r" <<endl;
    if (real_query_sql(sql, res, db_conn)) {
        if (mysql_field_count(db_conn) > 0) {
            MYSQL_RES* record_set = mysql_store_result(db_conn);
            encode_select_result(record_set, res);
            mysql_free_result(record_set);

            // fixed procedure store results.
            while(!mysql_next_result(db_conn)) {
                record_set = mysql_store_result(db_conn);
                if (record_set)
                {
                    mysql_free_result(record_set);
                }
            }

            record_set = NULL;
        } else {
            unsigned long long rows_num = mysql_affected_rows(db_conn);
            EiEncoder::encode_ok_number((long) rows_num, res);
        }
    }
}

bool MysqlDBOperation::real_query_sql(const char* sql, ei_x_buff* res, MYSQL* & db_conn) {
    if (NULL == conn_) {
        EiEncoder::encode_error_msg(CONN_NULL_ERROR, res);
        return false;
    }
    db_conn = (MYSQL*) (conn_->get_connection());
    if (0 != mysql_real_query(db_conn, sql, strlen(sql))) {
        EiEncoder::encode_error_msg(mysql_error(db_conn), res);
        return false;
    }
    return true;
}

/**
 * helpers.
 */
void MysqlDBOperation::encode_select_result(MYSQL_RES* record_set, ei_x_buff * const res) {
    MYSQL_ROW curr_row = NULL;
    ei_x_new_with_version(res);
    ei_x_encode_tuple_header(res, 2);
    ei_x_encode_atom(res, "ok");

    unsigned long long affected_rows = mysql_num_rows(record_set);
    unsigned int field_num = mysql_num_fields(record_set);
    MYSQL_FIELD *fields = mysql_fetch_fields(record_set);

    ei_x_encode_list_header(res, affected_rows);
    while (NULL != (curr_row = mysql_fetch_row(record_set))) {
        encode_one_row(curr_row, fields, field_num, mysql_fetch_lengths(record_set), res);
    }
    ei_x_encode_empty_list(res);

    /*
    unsigned long index = (unsigned long)res->index;
    SysLogger::info("index: %lu", index);

    if (index > LARGE_RES_LEN) {
        MemInfo mem_info;
        unsigned long mem_free = mem_info.get_free() * 1024;
        SysLogger::info("mem_free: %lu", mem_free);

        if (index > mem_free / 12) {
            ei_x_free(res);
            EiEncoder::encode_error_msg("Not enough space to store the query results", res);
            SysLogger::error("Not enough space to store the query results");
        }
    }*/
}

void MysqlDBOperation::encode_one_row(MYSQL_ROW curr_row, MYSQL_FIELD* fields,
        unsigned int field_num, unsigned long * field_lengths, ei_x_buff * const res) {
    ei_x_encode_list_header(res, field_num);
    for (unsigned int col = 0; col < field_num; col++) {
        MYSQL_TIME tm;
        if (!curr_row[col]) {
            ei_x_encode_atom(res, "undefined");
        } else if (field_lengths[col] <= 0) {
            ei_x_encode_string(res, "");
        } else if (IS_BLOB(fields[col].flags)
                || fields[col].type == MYSQL_TYPE_BIT) {
            ei_x_encode_binary(res, curr_row[col], field_lengths[col]);
        } else if (fields[col].type == MYSQL_TYPE_FLOAT
                || fields[col].type == MYSQL_TYPE_DOUBLE
                || fields[col].type == MYSQL_TYPE_DECIMAL
                || fields[col].type == MYSQL_TYPE_NEWDECIMAL) {
            stringstream double_sm;
            double double_data;
            double_sm << curr_row[col];
            double_sm >> double_data;
            ei_x_encode_double(res, double_data);
        } else if (fields[col].type == MYSQL_TYPE_DATETIME
                || fields[col].type == MYSQL_TYPE_TIMESTAMP) {
            sscanf(curr_row[col], "%d-%d-%d %d:%d:%d", &tm.year, &tm.month,
                    &tm.day, &tm.hour, &tm.minute, &tm.second);
            ei_x_encode_tuple_header(res, 2);
            ei_x_encode_atom(res, "datetime");
            ei_x_encode_tuple_header(res, 2);
            encode_date_tuple(tm.year, tm.month, tm.day, res);
            encode_date_tuple(tm.hour, tm.minute, tm.second, res);
        } else if (fields[col].type == MYSQL_TYPE_DATE) {
            sscanf(curr_row[col], "%d-%d-%d", &tm.year, &tm.month, &tm.day);
            ei_x_encode_tuple_header(res, 2);
            ei_x_encode_atom(res, "date");
            encode_date_tuple(tm.year, tm.month, tm.day, res);
        } else if (fields[col].type == MYSQL_TYPE_TIME) {
            sscanf(curr_row[col], "%d:%d:%d", &tm.hour, &tm.minute, &tm.second);
            ei_x_encode_tuple_header(res, 2);
            ei_x_encode_atom(res, "time");
            encode_date_tuple(tm.hour, tm.minute, tm.second, res);
        } else if (IS_NUM(fields[col].type)) {
            stringstream long_sm;
            long long_data;
            long_sm << curr_row[col];
            long_sm >> long_data;
            ei_x_encode_long(res, long_data);
        } else {
            ei_x_encode_string(res, curr_row[col]);
        }
    }
    ei_x_encode_empty_list(res);
}

const enum_field_types MysqlDBOperation::get_db_field_type(char field_type) const {
    enum_field_types type;
    switch (field_type) {
        case ERL_BINARY_EXT:
            type = MYSQL_TYPE_BLOB;
            break;
        case ERL_SMALL_INTEGER_EXT:
        case ERL_INTEGER_EXT:
        case ERL_SMALL_BIG_EXT:
        case ERL_LARGE_BIG_EXT:
            type = MYSQL_TYPE_LONG;
            break;
        case ERL_FLOAT_EXT:
            type = MYSQL_TYPE_DOUBLE;
            break;
        case ERL_ATOM_EXT:
        case ERL_LIST_EXT:
        case ERL_STRING_EXT:
            type = MYSQL_TYPE_STRING;
            break;
        default:
            type = MYSQL_TYPE_NULL;
            break;
    }
    return type;
}

void MysqlDBOperation::fill_value(stringstream & sm, FieldValue &field) {
    switch (field.erl_type) {
        case ERL_SMALL_INTEGER_EXT:
        case ERL_INTEGER_EXT:
        case ERL_SMALL_BIG_EXT:
        case ERL_LARGE_BIG_EXT:
            sm << *(long*) field.value;
            break;
        case ERL_FLOAT_EXT:
            sm << *(double*) field.value;
            break;
        case ERL_ATOM_EXT:
            sm << (char*) field.value;
            break;
        case ERL_SMALL_TUPLE_EXT:
        case ERL_LARGE_TUPLE_EXT:
            sm << (char*) field.value;
            break;
        case ERL_STRING_EXT:
        case ERL_LIST_EXT:
            sm << "\"" << (char*) field.value << "\"";
            break;
        case ERL_BINARY_EXT:
            fill_binary_value(sm, (char*) field.value, field.length);
            break;
        default:
            sm << "\"" << (char*) field.value << "\"";
            break;
    }
}

void MysqlDBOperation::make_where(stringstream& sm) {
    ei_get_type(buf_, &index_, &type_, &size_);
    if (ERL_LIST_EXT == type_) {
        ei_decode_list_header(buf_, &index_, &type_);
        sm << " where";
        decode_expr(sm);
        ei_decode_list_header(buf_, &index_, &type_);
    } else {
        decode_long();
    }
}

void MysqlDBOperation::make_field(stringstream& sm) {
    ei_get_type(buf_, &index_, &type_, &size_);
    if (ERL_LIST_EXT == type_) {
        ei_decode_list_header(buf_, &index_, &type_);
        int len = type_;
        for (int i = 0; i < len; i++) {
            decode_expr(sm);
            if (i < len - 1) {
                sm << ",";
            }
        }
        ei_decode_list_header(buf_, &index_, &type_);
    } else {
        decode_long();
        sm << " *";
    }
}

void MysqlDBOperation::make_table(stringstream& sm) {
    ei_get_type(buf_, &index_, &type_, &size_);
    if (ERL_LIST_EXT == type_) {
        sm << " from";
        ei_decode_list_header(buf_, &index_, &type_);
        int len = type_;
        for (int i = 0; i < len; i++) {
            decode_expr(sm);
            if (i < len - 1) {
                sm << ",";
            }
        }
        ei_decode_list_header(buf_, &index_, &type_);
    } else {
        decode_long();
    }
}

void MysqlDBOperation::make_extras(stringstream& sm) {
    ei_get_type(buf_, &index_, &type_, &size_);
    if (ERL_LIST_EXT == type_) {
        ei_decode_list_header(buf_, &index_, &type_);
        int len = type_;
        for (int i = 0; i < len; i++) {
            decode_expr(sm);
        }
        ei_decode_list_header(buf_, &index_, &type_);
    } else {
        decode_long();
    }
}

void MysqlDBOperation::decode_expr(stringstream& sm) {
    char *str = NULL;
    char *bin = NULL;
    int len;
    ei_get_type(buf_, &index_, &type_, &size_);
    switch(type_) {
        case ERL_SMALL_INTEGER_EXT:
        case ERL_INTEGER_EXT:
        case ERL_SMALL_BIG_EXT:
        case ERL_LARGE_BIG_EXT:
            sm << " " << decode_long();
            break;
        case ERL_FLOAT_EXT:
            sm << " " << decode_double();
            break;
        case ERL_ATOM_EXT:
            decode_string(str);
            sm << " " << str;
            free_string(str);
            break;
        case ERL_SMALL_TUPLE_EXT:
        case ERL_LARGE_TUPLE_EXT:
            decode_expr_tuple(sm);
            break;
        case ERL_NIL_EXT:
            sm << " \"\"";
            break;
        case ERL_STRING_EXT:
        case ERL_LIST_EXT:
            decode_string(str);
            sm << " \"" << str << "\"";
            free_string(str);
            break;
        case ERL_BINARY_EXT:
            len = decode_binary(bin);
            fill_binary_value(sm, bin, len);
            free_binary(bin);
            break;
    }
}

void MysqlDBOperation::decode_expr_tuple(stringstream& sm) {
    int len;
    char *str = NULL;
    ei_decode_tuple_header(buf_, &index_, &type_);
    ei_get_type(buf_, &index_, &type_, &size_);
    SqlKeyword key = (SqlKeyword) decode_long();
    switch(key) {
        case DB_DRV_SQL_AND:
            ei_decode_list_header(buf_, &index_, &type_);
            len = type_;
            sm << " (";
            for (int i = 0; i < len; i++) {
                decode_expr(sm);
                if (i < len - 1) {
                    sm << " and";
                }
            }
            sm << ")";
            ei_decode_list_header(buf_, &index_, &type_);
            break;
        case DB_DRV_SQL_OR:
            sm << " (";
            decode_expr(sm);
            sm << " or";
            decode_expr(sm);
            sm << ")";
            break;
        case DB_DRV_SQL_NOT:
            sm << " not";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_LIKE:
            decode_expr(sm);
            sm << " like";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_AS:
            decode_expr(sm);
            sm << " as";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_EQUAL:
            decode_expr(sm);
            sm << "=";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_GREATER:
            decode_expr(sm);
            sm << ">";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_GREATER_EQUAL:
            decode_expr(sm);
            sm << ">=";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_LESS:
            decode_expr(sm);
            sm << "<";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_LESS_EQUAL:
            decode_expr(sm);
            sm << "<=";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_JOIN:
            decode_expr(sm);
            sm << " join";
            decode_expr(sm);
            sm << " on";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_LEFT_JOIN:
            decode_expr(sm);
            sm << " left join";
            decode_expr(sm);
            sm << " on";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_RIGHT_JOIN:
            decode_expr(sm);
            sm << " right join";
            decode_expr(sm);
            sm << " on";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_INNER_JOIN:
            decode_expr(sm);
            sm << " inner join";
            decode_expr(sm);
            sm << " on";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_NOT_EQUAL:
            decode_expr(sm);
            sm << "!=";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_ORDER:
            sm << " order by";
            ei_decode_list_header(buf_, &index_, &type_);
            len = type_;
            for (int i = 0; i < len; i++) {
                ei_decode_tuple_header(buf_, &index_, &type_);
                decode_expr(sm);
                sm << ((0 == decode_int()) ? " asc" : " desc");
                if (i < len - 1) {
                    sm << ",";
                }
            }
            ei_decode_list_header(buf_, &index_, &type_);
            break;
        case DB_DRV_SQL_LIMIT:
            sm << " limit " << decode_int() << ",";
            sm << decode_int();
            break;
        case DB_DRV_SQL_DOT:
            sm << " ";
            decode_string(str);
            sm << str << ".";
            free_string(str);
            decode_string(str);
            sm << str;
            free_string(str);
            break;
        case DB_DRV_SQL_GROUP:
            sm << " group by";
            ei_decode_list_header(buf_, &index_, &type_);
            len = type_;
            for (int i = 0; i < len; i++) {
                decode_expr(sm);
                if (i < len - 1) {
                    sm << ",";
                }
            }
            ei_decode_list_header(buf_, &index_, &type_);
            break;
        case DB_DRV_SQL_HAVING:
            sm << " having";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_BETWEEN:
            decode_expr(sm);
            sm << " between";
            decode_expr(sm);
            sm << " and";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_IN:
            decode_expr(sm);
            sm << " in (";
            ei_get_type(buf_, &index_, &type_, &size_);
            len = size_;
            if (type_ == ERL_STRING_EXT) {
                char *temp = new char[len];
                ei_decode_string(buf_, &index_, temp);
                for (int i = 0; i < len; i++) {
                    long temp_in_value = (long) temp[i];
                    if (temp_in_value < 0) {
                        sm << temp_in_value + 256;
                    } else {
                        sm << temp_in_value;
                    }
                    if (i < len - 1) {
                        sm << ",";
                    }
                }
                delete [] temp;
            } else {
                ei_decode_list_header(buf_, &index_, &type_);
                for (int i = 0; i < len; i++) {
                    decode_expr(sm);
                    if (i < len - 1) {
                        sm << ",";
                    }
                }
                ei_decode_list_header(buf_, &index_, &type_);
            }
            sm << ")";
            break;
        case DB_DRV_SQL_ADD:
            sm << " (";
            decode_expr(sm);
            sm << "+";
            decode_expr(sm);
            sm << ")";
            break;
        case DB_DRV_SQL_SUB:
            sm << " (";
            decode_expr(sm);
            sm << "-";
            decode_expr(sm);
            sm << ")";
            break;
       case DB_DRV_SQL_MUL:
            decode_expr(sm);
            sm << "*";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_DIV:
            decode_expr(sm);
            sm << "/";
            decode_expr(sm);
            break;
        case DB_DRV_SQL_FUN:
            decode_expr(sm);
            sm << "(";
            ei_decode_list_header(buf_, &index_, &type_);
            len = type_;
            for (int i = 0; i < len; i++) {
                decode_expr(sm);
                if (i < len - 1) {
                    sm << ",";
                }
            }
            ei_decode_list_header(buf_, &index_, &type_);
            sm << ")";
            break;
        case DB_DRV_SQL_IS_NULL:
            decode_expr(sm);
            sm << " is null";
            break;
        case DB_DRV_SQL_IS_NOT_NULL:
            decode_expr(sm);
            sm << " is not null";
            break;
        case DB_DRV_SQL_DATETIME:
            ei_decode_tuple_header(buf_, &index_, &type_);
            ei_decode_tuple_header(buf_, &index_, &type_);
            sm << " \"" << decode_int();
            sm << "-" << decode_int();
            sm << "-" << decode_int();
            ei_decode_tuple_header(buf_, &index_, &type_);
            sm << " " << decode_int();
            sm << ":" << decode_int();
            sm << ":" << decode_int() << "\"";
            break;
        case DB_DRV_SQL_DATE:
            ei_decode_tuple_header(buf_, &index_, &type_);
            sm << " \"" << decode_int();
            sm << "-" << decode_int();
            sm << "-" << decode_int() << "\"";
            break;
        case DB_DRV_SQL_TIME:
            ei_decode_tuple_header(buf_, &index_, &type_);
            sm << " \"" << decode_int();
            sm << ":" << decode_int();
            sm << ":" << decode_int() << "\"";
            break;
        default:
            break;
    }
}
