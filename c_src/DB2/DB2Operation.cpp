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

#include "../base/ConnectionPool.h"
#include "../base/StmtMap.h"
#include "../util/EiEncoder.h"
#include "DB2Connection.h"
#include "DB2Operation.h"

using namespace rytong;

const char* CONN_NULL_ERROR = "get connection failed";
const char* STMT_NULL_ERROR = "get statement failed";
const char* BAD_ARG_ERROR = "bad argument";
const char* EXECUTE_SQL_ERROR = "execute sql failed";
const char* ALLOC_COLUMN_ERROR = "alloc column failed";
const char* ENCODE_COLUMN_ERROR = "encode column failed";

#define MAX_BIND_BUFFER_LENGTH 1024

#define ENCODE_ERROR(res) EiEncoder::encode_error_msg(message_, res);
#define RETURN_BAD_ARG(res)\
    LOG_ERROR(message_, BAD_ARG_ERROR)\
    ENCODE_ERROR(res)\
    return true;

DB2Operation::DB2Operation() {
}

DB2Operation::~DB2Operation() {
}

bool DB2Operation::exec(ei_x_buff * const res)
{
    char* sql = NULL;
    DB2Connection* conn = (DB2Connection*)conn_;
    SQLHANDLE hdbc = conn->get_db2_hdbc();
    PrepStmt *prepStmt = NULL;
    message_[0] = '\0';

    /**
     * Set automatic commit true or false, if the operation in a transaction,
     * we should set it false
     */
    if (!conn->set_auto_commit(!muli_tran_flag_)) {
        LOG_ERROR(message_, "set auto commit failed")
        ENCODE_ERROR(res)
        return true;
    }

    /**
     * To execute Sql statement or parameter bind statemnet, according to
     * the erlang input data
     */
    if (decode_string(sql)) {
        if (!execSqlStmt(res, hdbc, (SQLCHAR*)sql)) {
            ENCODE_ERROR(res)
        }
    } else if (ei_decode_tuple_header(buf_, &index_, &size_) == 0 &&
            size_ == 2 &&
            decode_string(sql)) {
        if ((prepStmt = initPrepStmt(hdbc, (SQLCHAR*)sql)) != NULL) {
            if (!execPrepStmt(res, prepStmt)) {
                ENCODE_ERROR(res)
            }
            finishPrepStmt(prepStmt);
        } else {
            ENCODE_ERROR(res)
        }

    } else {
        LOG_ERROR(message_, BAD_ARG_ERROR);
        ENCODE_ERROR(res)
    }

    free_string(sql);

    return true;
}

/** transaction begin interface **/
bool DB2Operation::trans_begin(ei_x_buff * const res)
{
    DB2Connection* conn = (DB2Connection*)conn_;
    message_[0] = '\0';

    /** To begin a transaction, we must set the automatic commit false */
    if (conn->set_auto_commit(false)) {
        EiEncoder::encode_ok_pointer((void*) conn, res);
        return true;
    } else {
        LOG_ERROR(message_, "set auto commit failed")
        ENCODE_ERROR(res)
        return false;
    }
}

/** transaction commit interface **/
bool DB2Operation::trans_commit(ei_x_buff * const res)
{
    SQLRETURN cliRC = SQL_SUCCESS;
    DB2Connection* conn = (DB2Connection*)conn_;
    SQLHANDLE hdbc = conn->get_db2_hdbc();
    message_[0] = '\0';

    cliRC = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_COMMIT);
    if (SQL_DBC_SUCCESS_WITH_RETURN(message_, hdbc, cliRC)) {
        EiEncoder::encode_ok_msg("COMMIT", res);
        return true;
    } else {
        ENCODE_ERROR(res)
        return false;
    }
}

/** transaction rollback interface **/
bool DB2Operation::trans_rollback(ei_x_buff * const res) {
    SQLRETURN cliRC = SQL_SUCCESS;
    DB2Connection* conn = (DB2Connection*)conn_;
    SQLHANDLE hdbc = conn->get_db2_hdbc();
    message_[0] = '\0';

    cliRC = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_ROLLBACK);
    if (SQL_DBC_SUCCESS_WITH_RETURN(message_, hdbc, cliRC)) {
        EiEncoder::encode_ok_msg("ROLLBACK", res);
        return true;
    } else {
        ENCODE_ERROR(res)
        return false;
    }
}

/** perpare statement init interface **/
bool DB2Operation::prepare_stat_init(ei_x_buff * const res)
{
    char* name = NULL;
    char *sql = NULL;
    DB2Connection* conn = (DB2Connection*)conn_;
    SQLHANDLE hdbc = conn->get_db2_hdbc();
    PrepStmt *prepStmt = NULL;
    message_[0] = '\0';

    if (!conn->set_auto_commit(!muli_tran_flag_)) {
        LOG_ERROR(message_, "set auto commit failed")
        ENCODE_ERROR(res)
        return true;
    }

    /**
     * If decode the name and SQL success, prepare an SQL statement for later
     * execution
     */
    if (ei_decode_tuple_header(buf_, &index_, &size_) == 0 &&
            size_ == 2 &&
            decode_string(name) &&
            decode_string(sql)) {
        if ((prepStmt = initPrepStmt(hdbc, (SQLCHAR*)sql)) != NULL) {
            if (stmt_map_->add((string)name, (void*)prepStmt)) {
               EiEncoder::encode_ok_msg(name, res);
            } else {
                finishPrepStmt(prepStmt);
                LOG_ERROR(message_, "Already registered prepare name")
                ENCODE_ERROR(res)
            }
        } else {
            EiEncoder::encode_error_msg(message_, res);
        }
    } else {
        LOG_ERROR(message_, BAD_ARG_ERROR);
        ENCODE_ERROR(res)
    }

    free_string(name);
    free_string(sql);

    return true;
}

/** perpare statement exec interface **/
bool DB2Operation::prepare_stat_exec(ei_x_buff * const res)
{
    char* name = NULL;
    PrepStmt *prepStmt = NULL;
    message_[0] = '\0';

    /** If decode the name success, execute the prepare statement */
    if (ei_decode_tuple_header(buf_, &index_, &size_) == 0 &&
            size_ == 2 &&
            decode_string(name)) {
        if ((prepStmt = (PrepStmt*)(stmt_map_->get(name))) != NULL) {
            if (!execPrepStmt(res, prepStmt)) {
               ENCODE_ERROR(res)
            }
        } else {
            LOG_ERROR(message_, STMT_NULL_ERROR);
            ENCODE_ERROR(res)
        }
    } else {
        LOG_ERROR(message_, BAD_ARG_ERROR);
        ENCODE_ERROR(res)
    }

    free_string(name);
    return true;
}

/** perpare statement release interface **/
bool DB2Operation::prepare_stat_release(ei_x_buff * const res)
{
    char* name = NULL;
    PrepStmt *prepStmt = NULL;
    message_[0] = '\0';

    /** If decode the name success, finish the prepare statement */
    if (decode_string(name)) {
        if ((prepStmt = (PrepStmt*)(stmt_map_->remove(name))) != NULL) {
            finishPrepStmt(prepStmt);
            EiEncoder::encode_ok_msg("close stmt", res);
        } else {
            LOG_ERROR(message_, STMT_NULL_ERROR);
            ENCODE_ERROR(res)
        }
    } else {
        LOG_ERROR(message_, BAD_ARG_ERROR);
        ENCODE_ERROR(res)
    }

    free_string(name);
    return true;
}
/** insert interface */
bool DB2Operation::insert(ei_x_buff * const res) {
    char* table_name = NULL; //表名变量
    char* field_name = NULL; //列名变量
    int field_num; //插入列的总数
    stringstream sqlstream, tmpstream; //拼接sql的变量

    DB2Connection* conn = (DB2Connection*)conn_;
    SQLHANDLE hdbc = conn->get_db2_hdbc();//connection handle

    if (!conn->set_auto_commit(!muli_tran_flag_)) {
        LOG_ERROR(message_, "set auto commit failed")
        ENCODE_ERROR(res)
        return true;
    }


    //解析tuple长度，应为2。
    //第一个元素为表名，第二个元素为键值对的列表
    //解析tuple第一个元素，即表名
    if(decode_tuple_header() != 2 || !decode_string(table_name)) {
//        cout << "decode table name error" << endl;
        RETURN_BAD_ARG(res)
    };

    //解析第二个元素，即键值对列表
    //解析列表长度
    if ((field_num = decode_list_header()) <= 0) {
//        cout << "decode number of fields error" << endl;
        free_string(table_name);
        RETURN_BAD_ARG(res)
    };

    sqlstream << "INSERT INTO " << table_name << " (";
    tmpstream << " VALUES (";

//    cout << "field_num: " << field_num << endl;
    for (int i = 0; i < field_num; i++) {
        //获取键值对，由tuple表示。
        //第一个元素为键，第二个元素为值
        //解析键值对tuple的长度，应为2
        //解析第一个元素，即列名
        if (decode_tuple_header() != 2 || !decode_string(field_name)) {
//            cout << "decode name of a field error" << endl;
            free_string(table_name);
            RETURN_BAD_ARG(res)
        }
//        cout << "field_name: " << field_name << endl;

        //在sql拼接字符串中加入列名
        sqlstream << field_name;
        //在sql拼接字符串中加入value值
        if(!decode_and_append_value(tmpstream)) {
//            cout << "append value error" << endl;
            free_string(field_name);
            free_string(table_name);
            RETURN_BAD_ARG(res)
        }

        //在sql拼接字符串的列名和值之间加入逗号
        if (i < field_num - 1) {
            sqlstream << ",";
            tmpstream << ",";
        }

        free_string(field_name);
    } //the end of "for"

    sqlstream << ")" << tmpstream.str() << ")";
//    cout << "sql: " << sqlstream.str().c_str() << endl;

    if (!execSqlStmt(res, hdbc, (SQLCHAR*)sqlstream.str().c_str())) {
        free_string(table_name);
        ENCODE_ERROR(res)
        return true;
    }

    free_string(table_name);
    return true;
}

/** update interface */
bool DB2Operation::update(ei_x_buff * const res) {
    char* table_name;//表名变量
    char* field_name;//列名变量
    stringstream sqlstream; //拼接sql语句的变量
    int field_num; //更新的列数
    DB2Connection* conn = (DB2Connection*) conn_;
    SQLHANDLE hdbc = conn->get_db2_hdbc();

    if (!conn->set_auto_commit(!muli_tran_flag_)) {
        LOG_ERROR(message_, "set auto commit failed")
        ENCODE_ERROR(res)
        return true;
    }

    //解析tuple长度，应为3
    //解析第一个元素，即表名
    if(decode_tuple_header() != 3 || !decode_string(table_name)){
        RETURN_BAD_ARG(res)
    }

    sqlstream << "UPDATE " << table_name << " SET ";
//    cout << "table name: "<< table_name << endl;
    //解析第二个元素，即set列表
    if((field_num = decode_list_header()) <= 0){
        free_string(table_name);
        RETURN_BAD_ARG(res)
    }
    for(int i = 0; i < field_num; i++){
        //解析列名
        if(decode_tuple_header() != 2 || !decode_string(field_name)){
            free_string(table_name);
            RETURN_BAD_ARG(res)
        }
        sqlstream << field_name << "=";
        //解析修改的值
        if(!decode_and_append_value(sqlstream)){
            free_string(field_name);
            free_string(table_name);
            RETURN_BAD_ARG(res)
        }
        append_comma(sqlstream, i, field_num);//加逗号

        free_string(field_name);
    }

    //解析第三个元素，即where语句
    if(!decode_empty_list() || !decode_and_append_where(sqlstream)){
        free_string(table_name);
        RETURN_BAD_ARG(res)
    }
//    cout << "sql: " << sqlstream.str().c_str() << endl;

    if (!execSqlStmt(res, hdbc, (SQLCHAR*)sqlstream.str().c_str())) {
        free_string(table_name);
        ENCODE_ERROR(res)
        return true;
    }
//    ei_x_new_with_version(res);
//    ei_x_encode_tuple_header(res, 2);
//    ei_x_encode_atom(res, "ok");
//    ei_x_encode_long(res, 0);
    free_string(table_name);

    return true;
}

/** del interface */
bool DB2Operation::del(ei_x_buff * const res) {
    char* table_name; //表名变量
    stringstream sqlstream; //拼接sql语句的变量
    DB2Connection* conn = (DB2Connection*)conn_;
    SQLHANDLE hdbc = conn->get_db2_hdbc();

    if (!conn->set_auto_commit(!muli_tran_flag_)) {
        LOG_ERROR(message_, "set auto commit failed")
        ENCODE_ERROR(res)
        return true;
    }

    //解析tuple长度，应为2
    //解析第一个元素，即表名
    if(decode_tuple_header() != 2 || !decode_string(table_name)){
        RETURN_BAD_ARG(res)
    }

    sqlstream << "DELETE FROM " << table_name;
    //解析第二个元素，即where条件。
    if(!decode_and_append_where(sqlstream)){
        free_string(table_name);
        RETURN_BAD_ARG(res)
    }
//    cout << "sql: " << sqlstream.str().c_str() << endl;

    if (!execSqlStmt(res, hdbc, (SQLCHAR*)sqlstream.str().c_str())) {
        free_string(table_name);
        ENCODE_ERROR(res)
        return true;
    }
//    ei_x_new_with_version(res);
//    ei_x_encode_tuple_header(res, 2);
//    ei_x_encode_atom(res, "ok");
//    ei_x_encode_long(res, 0);
    free_string(table_name);
    return true;
}

/** select interface */
bool DB2Operation::select(ei_x_buff * const res) {
    int field_num;//列名列表长度
    int table_num;//表名列表长度
    int extra_num;//其他条件列表长度
    long distinct_flag;//distinct标识
    stringstream sqlstream;
    DB2Connection* conn = (DB2Connection*) conn_;
    SQLHANDLE hdbc = conn->get_db2_hdbc();

    if (!conn->set_auto_commit(!muli_tran_flag_)) {
        LOG_ERROR(message_, "set auto commit failed")
        ENCODE_ERROR(res)
        return true;
    }

    //解析tuple长度，应为5
    //第一个元素为distinct标识
    if(decode_tuple_header() != 5 || !decode_integer(distinct_flag)){
        RETURN_BAD_ARG(res)
    }
    sqlstream << "SELECT" << (distinct_flag == 1? " DISTINCT ":" ");

    //第二个元素为列名列表
    if((field_num = decode_list_header()) > 0){
        for(int i = 0; i < field_num; i++){
            //解析列名，有可能为as表达式
            if(!gen_expr(sqlstream)){
                RETURN_BAD_ARG(res)
            }
            append_comma(sqlstream, i, field_num);//加逗号
        }
        //空列表表示结束
        if(!decode_empty_list()){
            RETURN_BAD_ARG(res)
        }
    }else{
        //列名列表为空时，拼接星号
        sqlstream << "*";
        ei_skip_term(buf_, &index_);//解析整数0，0表示空列表
    }

    //第三个元素为表名列表，不能为空
    if((table_num = decode_list_header()) <= 0){
        RETURN_BAD_ARG(res)
    }
    sqlstream << " FROM ";
    for(int i = 0; i < table_num; i++){
        //解析表名
        if(!gen_expr(sqlstream)){
            RETURN_BAD_ARG(res)
        }
        append_comma(sqlstream, i, table_num);//加逗号
    }
    //空列表表示结束
    if(!decode_empty_list()){
        RETURN_BAD_ARG(res)
    }

    //第四个元素为where条件列表
    if(!decode_and_append_where(sqlstream)){
        RETURN_BAD_ARG(res)
    }

    //第五个元素为其他条件列表
    if((extra_num = decode_list_header()) > 0){
        for(int i = 0; i < extra_num; i++){
            sqlstream << " ";
            //解析表达式
            if(!gen_expr(sqlstream)){
                RETURN_BAD_ARG(res)
            }
        }
        //空列表表示结束
        if(!decode_empty_list()){
            RETURN_BAD_ARG(res)
        }
//        RETURN_BAD_ARG(res)
    }

    if (!execSqlStmt(res, hdbc, (SQLCHAR*)sqlstream.str().c_str())) {
        ENCODE_ERROR(res)
        return true;
    }

    return true;
}

/**************************************************************/
/*                     private functions                      */
/**************************************************************/

/** Execute an SQL statement and encode result */
bool DB2Operation::execSqlStmt(ei_x_buff * const result, SQLHANDLE hdbc, SQLCHAR* sql)
{
    SQLRETURN cliRC = SQL_SUCCESS;
    SQLHANDLE hstmt = SQL_NULL_HSTMT;

    /** Allocate an statement handle*/
    cliRC = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (!SQL_DBC_SUCCESS_WITH_RETURN(message_, hdbc, cliRC)){
        return false;
    }

    /** Executes a statement */
    cliRC = SQLExecDirect(hstmt, sql, SQL_NTS);
    if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)){
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        return false;
    }

    /** Encode statement result */
    if (!encodeResult(result, hstmt)) {
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        return false;
    }

    /** Free the statement handle resources */
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

    return true;
}

/** Prepare an SQL statement for later execution */
PrepStmt* DB2Operation::initPrepStmt(SQLHANDLE hdbc, SQLCHAR* sql)
{
    SQLRETURN cliRC = SQL_SUCCESS;
    SQLHANDLE hstmt = SQL_NULL_HSTMT;
    SQLSMALLINT cpar = 0;
    Param* param = NULL;

    /** Allocate an statement handle*/
    cliRC = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (!SQL_DBC_SUCCESS_WITH_RETURN(message_, hdbc, cliRC)){
        return NULL;
    }

    /** Prepare an SQL statement */
    cliRC = SQLPrepare(hstmt, sql, SQL_NTS);
    if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)){
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        return NULL;
    }

    /** Get the number of parameters in the statement*/
    cliRC = SQLNumParams(hstmt, &cpar);
    if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)){
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        return NULL;
    }

    /**
     * If the statement has parameters, allocate enough buffer for parameters
     * and bind parameter markers in an SQL statement
     */
    if (cpar > 0) {
        param = allocParam(hstmt, cpar);
        if (param == NULL) {
            SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
            return NULL;
        }
    }

    PrepStmt* prepStmt = (PrepStmt*)malloc(sizeof(PrepStmt));
    if (prepStmt == NULL) {
        LOG_ERROR(message_, "alloc memory for prepStmt failed");
        if (param) {
            freeParam(param, cpar);
        }
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        return  NULL;
    }
    prepStmt->hdbc = hdbc;
    prepStmt->hstmt = hstmt;
    prepStmt->cpar = cpar;
    prepStmt->param = param;

    return prepStmt;
}

/** Execute an parepare SQL statement */
bool DB2Operation::execPrepStmt(ei_x_buff * const result, PrepStmt* prepStmt)
{
    SQLRETURN cliRC = SQL_SUCCESS;
    SQLSMALLINT cpar = prepStmt->cpar;
    Param*  param = prepStmt->param;
    char* value = NULL;


    if (cpar && param) {
        /**
         * Decode and set the parameters buffer or bind it dynamically for large
         * data,
         */
        if (ei_decode_list_header(buf_, &index_, &size_) == 0 && size_ >= cpar) {
            for (SQLSMALLINT i = 0; i < prepStmt->cpar; ++i) {
                if (!decodeAndBindParam(prepStmt, i)) {
                    freeParamDbuff(param, cpar);
                    return false;
                }
            }
        } else if (ei_get_type(buf_, &index_, &type_, &size_) == 0 && size_ >= cpar){
            /**
             * List, for example:[1, 2, 3, 4] is an string in erlang, we deal
             * with it here
             */
            value = (char*)malloc(size_ + 1);
            if (ei_decode_string(buf_, &index_, value) == 0) {
                for (SQLSMALLINT i = 0; i < cpar; ++i) {
                    if (!decodeAndBindParam(prepStmt, i, value[i])) {
                        freeParamDbuff(param, cpar);
                        free(value);
                        return false;
                    }
                }
            } else {
                free(value);
                LOG_ERROR(message_, BAD_ARG_ERROR);
                return false;
            }
            free(value);
        } else {
            LOG_ERROR(message_, BAD_ARG_ERROR);
            return false;
        }
    } //end of if

    cliRC = SQLExecute(prepStmt->hstmt);
    if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, prepStmt->hstmt, cliRC)){
        freeParamDbuff(prepStmt->param, prepStmt->cpar);
        return false;
    }

    if (!encodeResult(result, prepStmt->hstmt)) {
        freeParamDbuff(prepStmt->param, prepStmt->cpar);
        return false;
    }

    /** Free the parameter buffer which is allocated dynamically */
    freeParamDbuff(prepStmt->param, prepStmt->cpar);

    return true;
}

/**
 * Decode and set the parameters buffer or bind it dynamically for large
 * data
 */
bool DB2Operation::decodeAndBindParam(PrepStmt* prepStmt, SQLSMALLINT i, char value)
{
    SQLRETURN cliRC = SQL_SUCCESS;
    Param* param = prepStmt->param;
    long len, year, month, day, hour, minute, second;
    char bit, type[17];
    long long integerValue;
    double doubleValue;

    if (is_null()) {
        param[i].strLenOrIndPtr = SQL_NULL_DATA;
    } else {
        if (!param[i].preAlloc) {
            if (param[i].type == SQL_C_DBCHAR) {
                param[i].bufferLen = calcDBByteLength(buf_, &index_);
            } else {
                param[i].bufferLen = calcSGByteLength(buf_, &index_);
            }
            param[i].buffer = malloc(param[i].bufferLen);
        }
        switch (param[i].type) {
            case SQL_C_BIT:
                if (value == 0) {
                    if (ei_decode_char(buf_, &index_, &bit) == 0) {
                        *(SQLCHAR*)param[i].buffer = bit;
                    } else {
                        LOG_ERROR(message_, "Can not decode SQL_C_BIT data type")
                        return false;
                    }
                } else {
                    *(SQLCHAR*)param[i].buffer = value;
                }
                break;

            case SQL_C_BINARY:
                if (value == 0) {
                    if (!decodeBinary(buf_, &index_, (char*)param[i].buffer,
                            param[i].bufferLen, &len)) {
                        LOG_ERROR(message_, "Can not decode SQL_C_BINARY data type")
                        return false;
                    }
                    param[i].strLenOrIndPtr = len;
                } else {
                    snprintf((char*)param[i].buffer, param[i].bufferLen, "%d", value);
                    param[i].strLenOrIndPtr = strlen((char*)param[i].buffer);
                }
                break;

            case SQL_C_CHAR:
                if (value == 0) {
                    if (!decodeString(buf_, &index_, (char*)param[i].buffer,
                            param[i].bufferLen, &len)) {
                        LOG_ERROR(message_, "Can not decode SQL_C_CHAR data type")
                        return false;
                    }
                    param[i].strLenOrIndPtr = len;
                } else {
                    snprintf((char*)param[i].buffer, param[i].bufferLen, "%d", value);
                    param[i].strLenOrIndPtr = strlen((char*)param[i].buffer);
                }
                break;

            case SQL_C_DBCHAR:
                if (value != 0 || !decodeDBString(buf_, &index_, (unsigned char*)param[i].buffer,
                        param[i].bufferLen, &len)) {
                        LOG_ERROR(message_, "Can not decode SQL_C_DBCHAR data type")
                        return false;
                }
                param[i].strLenOrIndPtr = len;
                break;

            case SQL_C_TINYINT:
                if (value == 0) {
                    if (decodeInteger(buf_, &index_, &integerValue)) {
                        *(SQLSCHAR*)param[i].buffer = (SQLSCHAR)integerValue;
                    } else {
                        LOG_ERROR(message_, "Can not decode SQL_C_TINYINT data type")
                        return false;
                    }
                } else {
                    *(SQLSCHAR*)param[i].buffer = (SQLSCHAR)value;
                }
                break;

            case SQL_C_SHORT:
                if (value == 0) {
                    if (decodeInteger(buf_, &index_, &integerValue)) {
                        *(SQLSMALLINT*)param[i].buffer = (SQLSMALLINT)integerValue;
                    } else {
                        LOG_ERROR(message_, "Can not decode SQL_C_SHORT data type")
                        return false;
                    }
                } else {
                    *(SQLSMALLINT*)param[i].buffer = (SQLSMALLINT)value;
                }
                break;

            case SQL_C_LONG:
                if (value == 0) {
                    if (decodeInteger(buf_, &index_, &integerValue)) {
                        *(SQLINTEGER*)param[i].buffer = (SQLINTEGER)integerValue;
                    } else {
                        LOG_ERROR(message_, "Can not decode SQL_C_LONG data type")
                        return false;
                    }
                } else {
                    *(SQLINTEGER*)param[i].buffer = (SQLINTEGER)value;
                }
                break;

            case SQL_C_SBIGINT:
                if (value == 0) {
                    if (decodeInteger(buf_, &index_, &integerValue)) {
                        *(SQLBIGINT*)param[i].buffer = (SQLBIGINT)integerValue;
                    } else {
                        LOG_ERROR(message_, "Can not decode SQL_C_SBIGINT data type")
                        return false;
                    }
                } else {
                    *(SQLBIGINT*)param[i].buffer = (SQLBIGINT)value;
                }
                break;

            case SQL_C_DOUBLE:
                if (value == 0) {
                    if (decodeDouble(buf_, &index_, &doubleValue)) {
                        *(SQLDOUBLE*)param[i].buffer = (SQLDOUBLE)doubleValue;
                    } else {
                        LOG_ERROR(message_, "Can not decode SQL_C_DOUBLE data type")
                        return false;
                    }
                } else {
                    *(SQLDOUBLE*)param[i].buffer = (SQLDOUBLE)value;
                }
                break;

            case SQL_C_FLOAT:
                if (value == 0) {
                    if (decodeDouble(buf_, &index_, &doubleValue)) {
                        *(SQLREAL*)param[i].buffer = (SQLREAL)doubleValue;
                    } else {
                        LOG_ERROR(message_, "Can not decode SQL_C_FLOAT data type")
                        return false;
                    }
                } else {
                    *(SQLREAL*)param[i].buffer = (SQLREAL)value;
                }
                break;

            case SQL_C_TYPE_DATE:
                if (ei_decode_tuple_header(buf_, &index_, &size_) == 0 &&
                        size_ == 2 &&
                        ei_get_type(buf_, &index_, &type_, &size_) == 0 &&
                        size_ == 4 &&
                        ei_decode_atom(buf_, &index_, type) == 0 &&
                        strcmp(type, "date") == 0 &&
                        ei_decode_tuple_header(buf_, &index_, &size_) == 0 &&
                        size_ == 3 &&
                        ei_decode_long(buf_, &index_, &year) == 0 &&
                        ei_decode_long(buf_, &index_, &month) == 0 &&
                        ei_decode_long(buf_, &index_, &day) == 0)
                {
                    (*(DATE_STRUCT*)param[i].buffer).year = year;
                    (*(DATE_STRUCT*)param[i].buffer).month = month;
                    (*(DATE_STRUCT*)param[i].buffer).day = day;
                } else {
                    LOG_ERROR(message_, "Can not decode SQL_C_TYPE_DATE data type")
                    return false;
                }
                break;

            case SQL_C_TYPE_TIME:
                if (ei_decode_tuple_header(buf_, &index_, &size_) == 0 &&
                        size_ == 2 &&
                        ei_get_type(buf_, &index_, &type_, &size_) == 0 &&
                        size_ == 4 &&
                        ei_decode_atom(buf_, &index_, type) == 0 &&
                        strcmp(type, "time") == 0 &&
                        ei_decode_tuple_header(buf_, &index_, &size_) == 0 &&
                        size_ == 3 &&
                        ei_decode_long(buf_, &index_, &hour) == 0 &&
                        ei_decode_long(buf_, &index_, &minute) == 0 &&
                        ei_decode_long(buf_, &index_, &second) == 0)
                {
                    (*(TIME_STRUCT*)param[i].buffer).hour = hour;
                    (*(TIME_STRUCT*)param[i].buffer).minute = minute;
                    (*(TIME_STRUCT*)param[i].buffer).second = second;
                } else {
                    LOG_ERROR(message_, "Can not decode SQL_C_TYPE_TIME data type")
                    return false;
                }
                break;

            case SQL_C_TYPE_TIMESTAMP:
                if (ei_decode_tuple_header(buf_, &index_, &size_) == 0 &&
                        size_ == 2 &&
                        ei_get_type(buf_, &index_, &type_, &size_) == 0 &&
                        size_ == 8 &&
                        ei_decode_atom(buf_, &index_, type) == 0 &&
                        strcmp(type, "datetime") == 0 &&
                        ei_decode_tuple_header(buf_, &index_, &size_) == 0 &&
                        size_ == 2 &&
                        ei_decode_tuple_header(buf_, &index_, &size_) == 0 &&
                        size_ == 3 &&
                        ei_decode_long(buf_, &index_, &year) == 0 &&
                        ei_decode_long(buf_, &index_, &month) == 0 &&
                        ei_decode_long(buf_, &index_, &day) == 0 &&
                        ei_decode_tuple_header(buf_, &index_, &size_) == 0 &&
                        size_ == 3 &&
                        ei_decode_long(buf_, &index_, &hour) == 0 &&
                        ei_decode_long(buf_, &index_, &minute) == 0 &&
                        ei_decode_long(buf_, &index_, &second) == 0)
                {
                    (*(TIMESTAMP_STRUCT*)param[i].buffer).year = year;
                    (*(TIMESTAMP_STRUCT*)param[i].buffer).month = month;
                    (*(TIMESTAMP_STRUCT*)param[i].buffer).day = day;
                    (*(TIMESTAMP_STRUCT*)param[i].buffer).hour = hour;
                    (*(TIMESTAMP_STRUCT*)param[i].buffer).minute = minute;
                    (*(TIMESTAMP_STRUCT*)param[i].buffer).second = second;
                    (*(TIMESTAMP_STRUCT*)param[i].buffer).fraction = 0;
                } else {
                    LOG_ERROR(message_, "Can not decode SQL_C_TYPE_TIMESTAMP data type")
                    return false;
                }
                break;

            default:
                LOG_ERROR(message_, "unknown data type of prepare statement's parameter");
                return false;
        } //end of switch
    }
    if (!param[i].preAlloc) {
        cliRC = SQLBindParameter(prepStmt->hstmt,
                                 i + 1,
                                 SQL_PARAM_INPUT,
                                 param[i].type,
                                 param[i].sqltype,
                                 param[i].size,
                                 param[i].scale,
                                 param[i].buffer,
                                 param[i].bufferLen,
                                 &param[i].strLenOrIndPtr);
        if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, prepStmt->hstmt, cliRC)) {
            return false;
        }
    }

    return true;
}

/** Finish the prepare SQL statement */
void DB2Operation::finishPrepStmt(PrepStmt* prepStmt)
{
    if (prepStmt->hstmt != SQL_NULL_HSTMT) {
        SQLFreeHandle(SQL_HANDLE_STMT, prepStmt->hstmt);
    }
    if (prepStmt->param) {
        freeParam(prepStmt->param, prepStmt->cpar);
    }
    free(prepStmt);
}

Param* DB2Operation::allocParam(SQLHANDLE hstmt, SQLSMALLINT cpar)
{
    Param* param = NULL;
    SQLRETURN cliRC = SQL_SUCCESS;

    param = (Param*) malloc(cpar * sizeof(Param));
    if (param == NULL) {
        LOG_ERROR(message_, "alloc memory for param failed");
        return NULL;
    }

    for(SQLSMALLINT i = 0; i < cpar; ++i) {
        param[i].buffer = NULL;
        param[i].preAlloc = true;
        param[i].strLenOrIndPtr = 0;
        cliRC = SQLDescribeParam(hstmt,
                                 i + 1,
                                 &param[i].sqltype,
                                 &param[i].size,
                                 &param[i].scale,
                                 NULL);
        if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)) {
            freeParam(param, i);
            return NULL;
        }
        switch (param[i].sqltype) {
            case SQL_BIT: //todo
                param[i].type = SQL_C_BIT;
                param[i].bufferLen = sizeof(SQLCHAR);
                param[i].buffer = malloc(param[i].bufferLen);
                break;

            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY:
            case SQL_BLOB:
                param[i].type = SQL_C_BINARY;
                if (MAX_BIND_BUFFER_LENGTH < param[i].size) {
                    param[i].preAlloc = false;
                } else {
                    param[i].bufferLen = param[i].size + 1;
                    param[i].buffer = malloc(param[i].bufferLen);
                }
                break;

            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR:
            case SQL_CLOB:
                param[i].type = SQL_C_CHAR;
                if (MAX_BIND_BUFFER_LENGTH < param[i].size) {
                    param[i].preAlloc = false;
                } else {
                    param[i].bufferLen = param[i].size + 1;
                    param[i].buffer = malloc(param[i].bufferLen);
                }
                break;

            case SQL_XML:
                param[i].type = SQL_C_BINARY;
                param[i].preAlloc = false;
                break;

            case SQL_GRAPHIC:
            case SQL_VARGRAPHIC:
            case SQL_WVARCHAR:
            case SQL_LONGVARGRAPHIC:
            case SQL_WLONGVARCHAR:
            case SQL_DBCLOB:
                param[i].type = SQL_C_DBCHAR;
                if (MAX_BIND_BUFFER_LENGTH < param[i].size * sizeof(SQLDBCHAR)) {
                    param[i].preAlloc = false;
                } else {
                    param[i].bufferLen = (param[i].size + 1) * sizeof(SQLDBCHAR);
                    param[i].buffer = malloc(param[i].bufferLen);
                }
                break;

            case SQL_TINYINT:
                param[i].type = SQL_C_TINYINT;
                param[i].bufferLen = sizeof(SQLSCHAR);
                param[i].buffer = malloc(param[i].bufferLen);
                break;

            case SQL_SMALLINT:
                param[i].type = SQL_C_SHORT;
                param[i].bufferLen = sizeof(SQLSMALLINT);
                param[i].buffer = malloc(param[i].bufferLen);
                break;

            case SQL_INTEGER:
                param[i].type = SQL_C_LONG;
                param[i].bufferLen = sizeof(SQLINTEGER);
                param[i].buffer = malloc(param[i].bufferLen);
                break;

            case SQL_BIGINT:
                param[i].type = SQL_C_SBIGINT;
                param[i].bufferLen = sizeof(SQLBIGINT);
                param[i].buffer = malloc(param[i].bufferLen);
                break;

            case SQL_DOUBLE:
            case SQL_FLOAT:
                param[i].type = SQL_C_DOUBLE;
                param[i].bufferLen = sizeof(SQLDOUBLE);
                param[i].buffer = malloc(param[i].bufferLen);
                break;

            case SQL_REAL:
                param[i].type = SQL_C_FLOAT;
                param[i].bufferLen = sizeof(SQLREAL);
                param[i].buffer = malloc(param[i].bufferLen);
                break;

            case SQL_DECIMAL:
            case SQL_NUMERIC:
            case SQL_DECFLOAT:
                param[i].type = SQL_C_CHAR;
                param[i].bufferLen = 50;
                param[i].buffer = malloc(param[i].bufferLen);
                break;

            case SQL_TYPE_DATE:
                param[i].type = SQL_C_TYPE_DATE;
                param[i].bufferLen = sizeof(DATE_STRUCT);
                param[i].buffer = malloc(param[i].bufferLen);
                break;

            case SQL_TYPE_TIME:
                param[i].type = SQL_C_TYPE_TIME;
                param[i].bufferLen = sizeof(TIME_STRUCT);
                param[i].buffer = malloc(param[i].bufferLen);
                break;

            case SQL_TYPE_TIMESTAMP:
                param[i].type = SQL_C_TYPE_TIMESTAMP;
                param[i].bufferLen = sizeof(TIMESTAMP_STRUCT);
                param[i].buffer = malloc(param[i].bufferLen);
                break;

            case SQL_BLOB_LOCATOR:
            case SQL_CLOB_LOCATOR:
            case SQL_DBCLOB_LOCATOR:
            case SQL_BOOLEAN:
            case SQL_CURSORHANDLE:
            case SQL_ROW:
            case SQL_WCHAR:
            default:
                LOG_ERROR(message_, "unknown data type:%d", param[i].sqltype)
                freeParam(param, i);
                return NULL;
        } //end switch
        if (param[i].preAlloc) {
            cliRC = SQLBindParameter(hstmt,
                                     i + 1,
                                     SQL_PARAM_INPUT,
                                     param[i].type,
                                     param[i].sqltype,
                                     param[i].size,
                                     param[i].scale,
                                     param[i].buffer,
                                     param[i].bufferLen,
                                     &param[i].strLenOrIndPtr);
            if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)) {
                freeParam(param, i);
                return NULL;
            }
        }
    }

    return param;
}

void DB2Operation::freeParam(Param* param, SQLSMALLINT cpar)
{
    if (param == NULL) return;
    for (SQLSMALLINT i = 0; i < cpar; ++i) {
        if (param[i].preAlloc) {
            free(param[i].buffer);
        }
    }
    free(param);
}


void DB2Operation::freeParamDbuff(Param* param, SQLSMALLINT cpar)
{
    if (param == NULL) return;
    for (SQLSMALLINT i = 0; i < cpar; ++i) {
        if (!param[i].preAlloc && param[i].buffer) {
            free(param[i].buffer);
            param[i].buffer = NULL;
        }
    }
}

Column* DB2Operation::allocColumn(SQLHANDLE hstmt, SQLSMALLINT ccol)
{
    Column* column = NULL;
    SQLRETURN cliRC = SQL_SUCCESS;
    SQLSMALLINT sqlType = SQL_UNKNOWN_TYPE;

    column = (Column*) malloc(ccol * sizeof(Column));
    if (column == NULL) {
        LOG_ERROR(message_, "alloc memory for column failed");
        return NULL;
    }

    for (SQLSMALLINT i = 0; i < ccol; ++i) {
        column[i].buffer = NULL;
        column[i].useBind = true;
        cliRC = SQLDescribeCol(hstmt,
                               (SQLSMALLINT)(i + 1),
                               column[i].name,
                               sizeof(column[i].name),
                               &column[i].nameLen,
                               &sqlType,
                               &column[i].size,
                               &column[i].scale,
                               NULL);
        if (SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)) {
            switch (sqlType) {
                case SQL_BIT: //todo
                    column[i].type = SQL_C_BIT;
                    column[i].bufferLen = sizeof(SQLCHAR);
                    column[i].buffer = malloc(column[i].bufferLen);
                    break;

                case SQL_BINARY:
                case SQL_VARBINARY:
                case SQL_LONGVARBINARY:
                case SQL_BLOB:
                    column[i].type = SQL_C_BINARY;
                    if (MAX_BIND_BUFFER_LENGTH < column[i].size) {
                        column[i].useBind = false;
                    } else {
                        column[i].bufferLen = column[i].size;
                        column[i].buffer = malloc(column[i].bufferLen);
                    }
                    break;

                case SQL_CHAR:
                case SQL_VARCHAR:
                case SQL_LONGVARCHAR:
                case SQL_CLOB:
                    column[i].type = SQL_C_CHAR;
                    if (MAX_BIND_BUFFER_LENGTH < column[i].size) {
                        column[i].useBind = false;
                    } else {
                        column[i].bufferLen = column[i].size + 1;
                        column[i].buffer = malloc(column[i].bufferLen);
                    }
                    break;

                case SQL_XML:
                    column[i].type = SQL_C_BINARY;
                    column[i].useBind = false;
                    break;

                case SQL_GRAPHIC:
                case SQL_VARGRAPHIC:
                case SQL_WVARCHAR:
                case SQL_LONGVARGRAPHIC:
                case SQL_WLONGVARCHAR:
                case SQL_DBCLOB:
                    column[i].type = SQL_C_DBCHAR;
                    if (MAX_BIND_BUFFER_LENGTH < column[i].size * sizeof(SQLDBCHAR)) {
                        column[i].useBind = false;
                    } else {
                        column[i].bufferLen = (column[i].size + 1) * sizeof(SQLDBCHAR);
                        column[i].buffer = malloc(column[i].bufferLen);
                    }
                    break;

                case SQL_TINYINT:
                    column[i].type = SQL_C_TINYINT;
                    column[i].bufferLen = sizeof(SQLSCHAR);
                    column[i].buffer = malloc(column[i].bufferLen);
                    break;

                case SQL_SMALLINT:
                    column[i].type = SQL_C_SHORT;
                    column[i].bufferLen = sizeof(SQLSMALLINT);
                    column[i].buffer = malloc(column[i].bufferLen);
                    break;

                case SQL_INTEGER:
                    column[i].type = SQL_C_LONG;
                    column[i].bufferLen = sizeof(SQLINTEGER);
                    column[i].buffer = malloc(column[i].bufferLen);
                    break;

                case SQL_BIGINT:
                    column[i].type = SQL_C_SBIGINT;
                    column[i].bufferLen = sizeof(SQLBIGINT);
                    column[i].buffer = malloc(column[i].bufferLen);
                    break;

                case SQL_DOUBLE:
                case SQL_FLOAT:
                    column[i].type = SQL_C_DOUBLE;
                    column[i].bufferLen = sizeof(SQLDOUBLE);
                    column[i].buffer = malloc(column[i].bufferLen);
                    break;

                case SQL_REAL:
                    column[i].type = SQL_C_FLOAT;
                    column[i].bufferLen = sizeof(SQLREAL);
                    column[i].buffer = malloc(column[i].bufferLen);
                    break;

                case SQL_DECIMAL:
                case SQL_NUMERIC:
                    if (column[i].size < 19 && column[i].scale == 0) {
                        column[i].type = SQL_C_SBIGINT;
                        column[i].bufferLen = sizeof(SQLBIGINT);
                        column[i].buffer = malloc(column[i].bufferLen);
                    } else {
                        column[i].type = SQL_C_CHAR;
                        column[i].bufferLen = 50;
                        column[i].buffer = malloc(column[i].bufferLen);
                    }
                    break;
                case SQL_DECFLOAT://todo
                    column[i].type = SQL_C_CHAR;
                    column[i].bufferLen = 50;
                    column[i].buffer = malloc(column[i].bufferLen);
                    break;

                case SQL_TYPE_DATE:
                    column[i].type = SQL_C_TYPE_DATE;
                    column[i].bufferLen = sizeof(DATE_STRUCT);
                    column[i].buffer = malloc(column[i].bufferLen);
                    break;

                case SQL_TYPE_TIME:
                    column[i].type = SQL_C_TYPE_TIME;
                    column[i].bufferLen = sizeof(TIME_STRUCT);
                    column[i].buffer = malloc(column[i].bufferLen);
                    break;

                case SQL_TYPE_TIMESTAMP:
                    column[i].type = SQL_C_TYPE_TIMESTAMP;
                    column[i].bufferLen = sizeof(TIMESTAMP_STRUCT);
                    column[i].buffer = malloc(column[i].bufferLen);
                    break;

                case SQL_BLOB_LOCATOR:
                case SQL_CLOB_LOCATOR:
                case SQL_DBCLOB_LOCATOR:
                case SQL_BOOLEAN:
                case SQL_CURSORHANDLE:
                case SQL_ROW:
                case SQL_WCHAR:
                default:
                    LOG_ERROR(message_, "unknown data type:%d", sqlType)
                    freeColumn(column, i);
                    return NULL;
            } //end switch
        } else {
            freeColumn(column, i);
            return NULL;
        }
    }

    return column;
}

void DB2Operation::freeColumn(Column* column, SQLSMALLINT ccol)
{
    if (column == NULL) return;
    for (SQLSMALLINT i = 0; i < ccol; ++i) {
        free(column[i].buffer);
    }
    free(column);
}

bool DB2Operation::encodeResult (ei_x_buff * const result, SQLHANDLE hstmt)
{
    SQLRETURN cliRC = SQL_SUCCESS;
    SQLSMALLINT ccol = 0;
    Column* column = NULL;
    SQLLEN rowNum = 0;

    cliRC = SQLNumResultCols(hstmt, &ccol);
    if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)){
        return false;
    }

    if (ccol == 0) {
        cliRC = SQLRowCount(hstmt, &rowNum);
        if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)){
            return false;
        }

        ei_x_new_with_version(result);
        ei_x_encode_tuple_header(result, 2);
        ei_x_encode_atom(result, "ok");
        ei_x_encode_long(result, rowNum);
    } else {
        column = allocColumn(hstmt, ccol);
        if (column == NULL){
            return false;
        }

        for (SQLSMALLINT i = 0; i < ccol; ++i) {
            if (column[i].useBind) {
                cliRC = SQLBindCol(hstmt, i + 1, column[i].type, column[i].buffer,
                                   column[i].bufferLen, &column[i].strLenOrIndPtr);
                if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)) {
                    freeColumn(column, ccol);
                    return false;
                }
            }
        }

        /* fetch each row and display */
        cliRC = SQLFetch(hstmt);
        if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)) {
            freeColumn(column, ccol);
            return false;
        }

        ei_x_new_with_version(result);
        ei_x_encode_tuple_header(result, 2);
        ei_x_encode_atom(result, "ok");

        int pos = result->index;
        ei_x_encode_list_header(result, 1);

        while (cliRC != SQL_NO_DATA_FOUND)
        {
            rowNum++;
            ei_x_encode_list_header(result, ccol);
            if (!encodeColumn(result, hstmt, column, ccol)) {
                freeColumn(column, ccol);
                return false;
            }
            cliRC = SQLFetch(hstmt);
            if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)) {
                freeColumn(column, ccol);
                return false;
            }
            ei_x_encode_empty_list(result);
        }
        ei_x_encode_empty_list(result);

        ei_x_buff x;
        ei_x_new(&x);
        ei_x_encode_list_header(&x, rowNum);
        memcpy(result->buff + pos, x.buff, x.index);
        ei_x_free(&x);

        freeColumn(column, ccol);
    }

    return true;
}

bool DB2Operation::encodeColumn(ei_x_buff * const result, SQLHANDLE hstmt,
        Column* column, SQLSMALLINT ccol)
{
    int len = 0;
    unsigned char tmp;

    for (SQLSMALLINT i = 0; i < ccol; ++i) {
        if (!column[i].useBind && !getLargeData(hstmt, column, i)) {
            return false;
        }
        if (column[i].buffer == NULL ||
                column[i].strLenOrIndPtr == SQL_NULL_DATA) {
            ei_x_encode_atom(result, "undefined");
        } else {
            switch (column[i].type) {
                case SQL_C_BIT:
                    ei_x_encode_char(result, (*(SQLCHAR*)(column[i].buffer)));
                    break;

                case SQL_C_BINARY:
                    ei_x_encode_binary(result, (const void*)column[i].buffer,
                            (long)column[i].strLenOrIndPtr);
                    break;

                case SQL_C_CHAR:
                    ei_x_encode_string(result, (const char*)column[i].buffer);
                    break;

                case SQL_C_DBCHAR:
                    len = column[i].strLenOrIndPtr / sizeof(SQLDBCHAR);
                    if (len > 0) {
                        ei_x_encode_list_header(result, (long)len);
                        for (int j = 0; j < len; ++j) {
                            tmp = ((unsigned char*)column[i].buffer)[j*2];
                            ((unsigned char*)column[i].buffer)[j*2] =
                                    ((unsigned char*)column[i].buffer)[j*2 + 1];
                            ((unsigned char*)column[i].buffer)[j*2 + 1] = tmp;
                            ei_x_encode_long(result, ((SQLDBCHAR*)column[i].buffer)[j]);
                        }
                    }
                    ei_x_encode_empty_list(result);
                    break;

                case SQL_C_TINYINT:
                    ei_x_encode_long(result, *(SQLSCHAR*)(column[i].buffer));
                    break;

                case SQL_C_SHORT:
                    ei_x_encode_long(result, *(SQLSMALLINT*)(column[i].buffer));
                    break;

                case SQL_C_LONG:
                    ei_x_encode_long(result, *(SQLINTEGER*)(column[i].buffer));
                    break;

                case SQL_C_SBIGINT:
                    ei_x_encode_long(result, *(SQLBIGINT*)(column[i].buffer));
                    break;

                case SQL_C_DOUBLE:
                    ei_x_encode_double(result, *(SQLDOUBLE*)(column[i].buffer));
                    break;

                case SQL_C_FLOAT:
                    ei_x_encode_double(result, *(SQLREAL*)(column[i].buffer));
                    break;

                case SQL_C_TYPE_DATE:
                    ei_x_encode_tuple_header(result, 2);
                    ei_x_encode_atom(result, "date");

                    ei_x_encode_tuple_header(result, 3);
                    ei_x_encode_long(result, (*(DATE_STRUCT*)column[i].buffer).year);
                    ei_x_encode_long(result, (*(DATE_STRUCT*)column[i].buffer).month);
                    ei_x_encode_long(result, (*(DATE_STRUCT*)column[i].buffer).day);
                    break;

                case SQL_C_TYPE_TIME:
                    ei_x_encode_tuple_header(result, 2);
                    ei_x_encode_atom(result, "time");

                    ei_x_encode_tuple_header(result, 3);
                    ei_x_encode_long(result, (*(TIME_STRUCT*)column[i].buffer).hour);
                    ei_x_encode_long(result, (*(TIME_STRUCT*)column[i].buffer).minute);
                    ei_x_encode_long(result, (*(TIME_STRUCT*)column[i].buffer).second);
                    break;

                case SQL_C_TYPE_TIMESTAMP:
                    ei_x_encode_tuple_header(result, 2);
                    ei_x_encode_atom(result, "datetime");

                    ei_x_encode_tuple_header(result, 2);

                    ei_x_encode_tuple_header(result, 3);
                    ei_x_encode_long(result, (*(TIMESTAMP_STRUCT*)column[i].buffer).year);
                    ei_x_encode_long(result, (*(TIMESTAMP_STRUCT*)column[i].buffer).month);
                    ei_x_encode_long(result, (*(TIMESTAMP_STRUCT*)column[i].buffer).day);

                    ei_x_encode_tuple_header(result, 3);
                    ei_x_encode_long(result, (*(TIMESTAMP_STRUCT*)column[i].buffer).hour);
                    ei_x_encode_long(result, (*(TIMESTAMP_STRUCT*)column[i].buffer).minute);
                    ei_x_encode_long(result, (*(TIMESTAMP_STRUCT*)column[i].buffer).second);
                    break;

                default:
                    ei_x_encode_atom(result, "unknown");
                    break;
            }
        }
    }

    return true;
}

bool DB2Operation::getLargeData(SQLHANDLE hstmt, Column* column, SQLSMALLINT index)
{
    SQLRETURN cliRC = SQL_SUCCESS;
//    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH + 1];
//    SQLCHAR sqlstate[SQL_SQLSTATE_SIZE + 1] = {0};
//    SQLINTEGER sqlcode;
//    SQLSMALLINT length, i;
    void* tmpBuffer = NULL;
    SQLLEN tmpBufferLen = 0, len = 0, offset = 0;

    switch (column[index].type) {
        case SQL_C_CHAR:
            offset = 1;
            break;
        case SQL_C_DBCHAR:
            offset = 2;
            break;
        default:
            offset = 0;
    }


    column[index].buffer = malloc(MAX_BIND_BUFFER_LENGTH + offset);
    if (column[index].buffer == NULL) {
        LOG_ERROR(message_, "alloc memory for column buffer failed")
        return false;
    }
    column[index].bufferLen = MAX_BIND_BUFFER_LENGTH + offset;
    cliRC = SQLGetData(hstmt,
                       index + 1,
                       column[index].type,
                       (SQLPOINTER)column[index].buffer,
                       MAX_BIND_BUFFER_LENGTH + offset,
                       &column[index].strLenOrIndPtr);
    if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)) {
        free(column[index].buffer);
        return false;
    }

    while(cliRC == SQL_SUCCESS_WITH_INFO) {
        tmpBufferLen = column[index].bufferLen;
        column[index].bufferLen = tmpBufferLen * 2 - offset;
        tmpBuffer = realloc(column[index].buffer, column[index].bufferLen);
        if (tmpBuffer == NULL) {
            LOG_ERROR(message_, "realloc memory for column buffer failed")
            free(column[index].buffer);
            return false;
        }
        column[index].buffer = tmpBuffer;
        tmpBuffer = (void*) ((char*)column[index].buffer + tmpBufferLen - offset);
        cliRC = SQLGetData(hstmt,
                           index + 1,
                           column[index].type,
                           (SQLPOINTER)tmpBuffer,
                           tmpBufferLen,
                           &len);
        if (!SQL_STMT_SUCCESS_WITH_RETURN(message_, hstmt, cliRC)) {
            free(column[index].buffer);
            return false;
        }
//        i = 1;
//        /* get multiple field settings of diagnostic record */
//        while (SQLGetDiagRec(SQL_HANDLE_STMT,
//                             hstmt,
//                             i,
//                             sqlstate,
//                             &sqlcode,
//                             message,
//                             SQL_MAX_MESSAGE_LENGTH + 1,
//                             &length) == SQL_SUCCESS){
//            i++;
//        }
//        if (strcmp((const char*)sqlstate, "01004") != 0) {
//            break;
//        }
    }

    return true;
}
