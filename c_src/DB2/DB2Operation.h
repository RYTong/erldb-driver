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

#ifndef _RYT_DB2_OPERATION_H
#define _RYT_DB2_OPERATION_H

#include "DB2Connection.h"
#include "../util/SysLogger.h"
#include "../base/DBOperation.h"
#include "../base/DBException.h"


using namespace std;

namespace rytong {

typedef struct {
    SQLCHAR name[32];
    SQLSMALLINT nameLen;
    SQLSMALLINT type;
    SQLUINTEGER size;
    SQLSMALLINT scale;
    void* buffer;
    SQLLEN bufferLen;
    SQLLEN strLenOrIndPtr;
    bool useBind;
} Column;

typedef struct {
    SQLSMALLINT sqltype;
    SQLSMALLINT type;
    SQLUINTEGER size;
    SQLSMALLINT scale;
    void* buffer;
    SQLLEN bufferLen;
    SQLLEN strLenOrIndPtr;
    bool preAlloc;
} Param;

typedef struct {
    SQLHANDLE hdbc;
    SQLHANDLE hstmt;
    SQLSMALLINT cpar;
    Param* param;
} PrepStmt;

typedef struct {
    char* value; ///< Binary data.
    long len; ///< Binary length.
} MyBinary;

typedef struct {
    int year;
    int month;
    int day;
    int hour;
    int minute;
    int second;
} DateTime;

#define CUSTOM_UNDEFINED       '0'
#define CUSTOM_NUMERIC_STRING  '1'
#define CUSTOM_DATE            '2'
#define CUSTOM_TIME            '3'
#define CUSTOM_TIMESTAMP       '4'

class DB2Operation : public DBOperation {
public:

    DB2Operation();

    ~DB2Operation();

    static bool release_stmt(void* data) {
        /** todo **/
        return true;
    }

    /** execute interface **/
    bool exec(ei_x_buff * const res);

    /** transaction begin interface **/
    bool trans_begin(ei_x_buff * const res);

    /** transaction commit interface **/
    bool trans_commit(ei_x_buff * const res);

    /** transaction rollback interface **/
    bool trans_rollback(ei_x_buff * const res);

    /** perpare statement init interface **/
    bool prepare_stat_init(ei_x_buff * const res);

    /** perpare statement exec interface **/
    bool prepare_stat_exec(ei_x_buff * const res);

    /** perpare statement release interface **/
    bool prepare_stat_release(ei_x_buff * const res);

    /** insert interface */
    bool insert(ei_x_buff * const res);

    /** update interface */
    bool update(ei_x_buff * const res);

    /** del interface */
    bool del(ei_x_buff * const res);

    /** select interface */
    bool select(ei_x_buff * const res);

private:
    DB2Operation & operator =(const DB2Operation&);

    DB2Operation(DB2Operation&);

    char message_[SQL_MAX_MESSAGE_LENGTH + 1];

    bool execSqlStmt(ei_x_buff * const, SQLHANDLE, SQLCHAR*);

    PrepStmt* initPrepStmt(SQLHANDLE hdbc, SQLCHAR* sql);

    bool execPrepStmt(ei_x_buff * const, PrepStmt*);

    bool decodeAndBindParam(PrepStmt*, SQLSMALLINT, char value = 0);

    void finishPrepStmt(PrepStmt* prepStmt);

    Param* allocParam(SQLHANDLE hstmt, SQLSMALLINT cpar);

    void freeParam(Param* param, SQLSMALLINT cpar);

    void freeParamDbuff(Param* param, SQLSMALLINT cpar);

    Column* allocColumn(SQLHANDLE, SQLSMALLINT);

    void freeColumn(Column*, SQLSMALLINT);

    bool encodeResult (ei_x_buff * const, SQLHANDLE);

    bool encodeColumn(ei_x_buff * const, SQLHANDLE, Column*, SQLSMALLINT);

    bool getLargeData(SQLHANDLE, Column*, SQLSMALLINT);

    /** is null */
    inline bool is_null()
    {
        int index = index_;
        char type[10];

        if (ei_get_type(buf_, &index, &type_, &size_) == 0 &&
                size_ == 9 && ei_decode_atom(buf_, &index, type) == 0 &&
                strcmp(type, "undefined") == 0) {
            index_ = index;
            return true;
        }

        return false;
    }

    /**
     * 对更新操作结果编码
     * @params[in] update_count
     *     更新操作影响的列数
     * @params[out] res
     *     编码后的结果
     */
    inline void encode_update_res(ei_x_buff * const res, SQLLEN update_count){
        ei_x_new_with_version(res);
        ei_x_encode_tuple_header(res, 2);
        ei_x_encode_atom(res, "ok");
        ei_x_encode_long(res, update_count);
    }

    /**
     * 在指定的p_index位置解析buf_中的tuple长度。
     * 如未指定则在上一次完成解析动作的位置进行解析，即在index_位置。
     * @params[in] p_index
     *     解析起始位置，如未指定，则默认为空
     * @params[out] p_index
     *     解析完成的位置
     * @return 解析成功，返回tuple长度。否则返回-1.
     */
    inline int decode_tuple_header(int* p_index = NULL){
        if (p_index == NULL) {
            p_index = &index_;
        }

        if (ei_decode_tuple_header(buf_, p_index, &size_) == 0) {
            return size_;
        } else {
            return -1;
        }
    }

    /**
     * 在指定的p_index位置解析buf_中的list长度。
     * 如未指定则在上一次完成解析动作的位置进行解析，即在index_位置。
     * @params[in] p_index
     *     解析起始位置，如未指定，则默认为空
     * @params[out] p_index
     *     解析完成的位置
     * @return 解析成功，返回list长度。否则返回-1.
     */
    inline int decode_list_header(int* p_index = NULL){
        if (p_index == NULL) {
            p_index = &index_;
        }

        if (ei_decode_list_header(buf_, p_index, &size_) == 0) {
            return size_;
        } else {
            return -1;
        }
    }


    /**
     * 在指定的p_index位置解析buf_中的Erlang数据的类型。
     * 如未指定则在上一次完成解析动作的位置进行解析，即在index_位置。
     * @params[in] p_index
     *     解析起始位置，如未指定，则默认为空
     * @params[out] p_index
     *     解析完成的位置
     * @return 解析成功返回对应的类型，否则返回-1
     */
    inline int get_erl_type(int* p_index = NULL) {
        if(p_index == NULL) {
            p_index = &index_;
        }

        return ei_get_type(buf_, p_index, &type_, &size_)
                == 0 ? type_:-1;
    }

    /**
     * 在指定的p_index位置解析buf_中的二进制类型数据。
     * 如未指定则在上一次完成解析动作的位置进行解析，即在index_位置。
     * @params[in] p_index
     *     解析起始位置，如未指定，则默认为空
     * @params[out] p_index
     *     解析完成的位置
     * @params[out] dest
     *     解析完成的二进制数据
     * @return 解析成功，返回true。否则返回false.
     */
    inline bool decode_binary(MyBinary* dest, int* p_index = NULL) {
        int retcode;

        if(p_index == NULL) {
            p_index = &index_;
        }

        if(ei_get_type(buf_, p_index, &type_, &size_) != 0) {
            return false;
        }

        if(!alloc_binary(dest, size_)){
            return false;
        }

        switch(type_){
            case ERL_STRING_EXT:
                retcode = ei_decode_string(buf_, p_index, dest->value);
                break;
            case ERL_BINARY_EXT:
                retcode = ei_decode_binary(buf_, p_index, dest->value, &dest->len);
                break;
            default:
                retcode = -1;
        }

        if(retcode != 0){
            free_binary(dest);
            return false;
        }

        return true;
    }

    /**
     * 在指定的p_index位置解析buf_中的日期类型数据。
     * 如未指定则在上一次完成解析动作的位置进行解析，即在index_位置。
     * @params[in] p_index
     *     解析起始位置，如未指定，则默认为空
     * @params[out] p_index
     *     解析完成的位置
     * @params[out] date
     *     解析完成的日期数据
     * @return 解析成功，返回true。否则返回false.
     */
    inline bool decode_date(DateTime* date, int* p_index = NULL){
        long year, month, day;
        if(p_index == NULL){
            p_index = &index_;
        }

        //解析数据类型
        //应为tuple且长度为3
        if(get_erl_type() != ERL_SMALL_TUPLE_EXT
                || decode_tuple_header() != 3) {
            return false;
        }

        //解析tuple的三个元素
        //依次为年、月、日
        if(decode_integer(year) && decode_integer(month)
                && decode_integer(day)){
            date->year = year;
            date->month = month;
            date->day = day;
            return true;
        }

        return false;
    }

    /**
     * 在指定的p_index位置解析buf_中的时间类型数据。
     * 如未指定则在上一次完成解析动作的位置进行解析，即在index_位置。
     * @params[in] p_index
     *     解析起始位置，如未指定，则默认为空
     * @params[out] p_index
     *     解析完成的位置
     * @params[out] time
     *     解析完成的时间数据
     * @return 解析成功，返回true。否则返回false.
     */
    inline bool decode_time(DateTime* time, int* p_index = NULL){
        long hour, minute, second;
        if(p_index == NULL){
            p_index = &index_;
        }

        //解析数据类型
        //应为tuple且长度为3
        if(get_erl_type() != ERL_SMALL_TUPLE_EXT
                || decode_tuple_header() != 3) {
            return false;
        }

        //解析tuple的三个元素
        //依次为时、分、秒
        if(decode_integer(hour) && decode_integer(minute)
                && decode_integer(second)){
//            cout << "hour: " << hour << endl;
            time->hour = hour;
            time->minute = minute;
            time->second = second;
            return true;
        }

        return false;
    }

    /**
     * 在指定的p_index位置解析buf_中的日期和时间类型数据。
     * 如未指定则在上一次完成解析动作的位置进行解析，即在index_位置。
     * @params[in] p_index
     *     解析起始位置，如未指定，则默认为空
     * @params[out] p_index
     *     解析完成的位置
     * @params[out] timestamp
     *     解析完成的日期和时间数据
     * @return 解析成功，返回true。否则返回false.
     */
    inline bool decode_timestamp(DateTime* timestamp, int* p_index = NULL){
        if(p_index == NULL){
            p_index = &index_;
        }

        //解析数据类型
        //应为tuple且长度为2
        if(get_erl_type() != ERL_SMALL_TUPLE_EXT
                || decode_tuple_header() != 2) {
            return false;
        }

        //解析tuple的三个元素
        //依次为日期tuple、时间tuple
        if(decode_date(timestamp) && decode_time(timestamp)){
            return true;
        }

        return false;
    }

    /**
     * 在指定的p_index位置解析buf_中的自定义类型
     * @params[in] p_index
     *     解析起始位置，如未指定，则默认为空
     * @params[out] p_index
     *     解析完成的位置
     * @params[out] timestamp
     *     解析完成的日期和时间数据
     * @return 解析成功，返回自定义类型。否则返回CUSTOM_UNDEFINED。
     */
    inline char decode_custom_type(int* p_index = NULL){
        //存储解析的类型字符串
        //最长为datetime，8位
        char type[9];

        if(p_index == NULL){
            p_index = &index_;
        }

        //解析tuple，长度应为2
        //第一个元素为自定义类型，第二个元素为数值
        if(decode_tuple_header() != 2) {
            return CUSTOM_UNDEFINED;
        }

        //解析第一个元素，并存储在type变量中
        //数据类型应为Erlang atom，且长度不大于8
        if(ei_get_type(buf_, p_index, &type_, &size_) != 0
                || type_ != ERL_ATOM_EXT || size_ > 8) {
            return CUSTOM_UNDEFINED;
        }
        if(ei_decode_atom(buf_, p_index, type) != 0){
            return CUSTOM_UNDEFINED;
        }

//        cout << "type: " << type << endl;

        //判断并返回类型
        if(strcmp(type, "number") == 0){
            return CUSTOM_NUMERIC_STRING;
        } else if(strcmp(type, "date") == 0) {
            return CUSTOM_DATE;
        } else if(strcmp(type, "time") == 0) {
            return CUSTOM_TIME;
        } else if(strcmp(type, "datetime") == 0) {
            return CUSTOM_TIMESTAMP;
        } else {
            return CUSTOM_UNDEFINED;
        }

    }


    /**
     * 向指定的stringsream后拼接逗号
     * @params[in] index
     *     元素所在位置
     * @params[in] size
     *     列表所含元素个数
     * @params[out] stream
     *     拼接后的stringstream
     */
    inline void append_comma(stringstream& stream, int index, int size){
        if(index < size -1){
            stream << ", ";
        }
    }

    /**
     * 解析buf_并向指定的stringsream后拼接解析的值
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    inline bool decode_and_append_value(stringstream& stream) {
        int type = get_erl_type();
//        cout << "erl type: " << (char)type << endl;

        if(type == ERL_SMALL_TUPLE_EXT) {
            return decode_and_append_custom(stream);
        }else{
            return decode_and_append_normal(stream, type);
        }
    }

    /**
     * 解析buf_并向指定的stringsream后拼接解析的where语句
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    inline bool decode_and_append_where(stringstream& sqlstream){
    //若为空，则无where条件。
    if(decode_list_header() >0){
        sqlstream << " WHERE ";
        return gen_expr(sqlstream) && decode_empty_list();
    }

    return ei_skip_term(buf_, &index_) == 0;// 无where条件时，要解析整数0
    }

    /**
     * 解析buf_并向指定的stringsream后拼接解析的表达式语句
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    inline bool gen_expr(stringstream& stream){
        int type = get_erl_type();
        long expr_key;
        int index_tmp;
        bool result;

//        cout << "gen_expr : type = " << (char)type << endl;
        if(type == ERL_SMALL_TUPLE_EXT || type == ERL_LARGE_TUPLE_EXT) {
            //由于暂不知道tuple是数据还是表达式，因此要暂存index_
            index_tmp = index_;

            //解析tuple的第一个元素
            if(decode_tuple_header() <= 0){
                return false;
            }
            if(decode_integer(expr_key)){
                //tuple第一个元素为整型，则tuple表示表达式
//                cout << "decode expr" << endl;
//                cout << "expr_key = " << expr_key << endl;
                result = decode_expr(stream, expr_key);
            }else{
                //第一个元素不为tuple，则tuple表示自定义类型数据或格式错误
//                cout << "decode custom" << endl;
                //index_还原为解析tuple前的位置，以便正常解析自定义类型的tuple
                index_ = index_tmp;
                result = decode_and_append_custom(stream);
            }
        }else if(type == ERL_ATOM_EXT){
            char* value;

            index_tmp = index_;
            if (result = is_null()) {
                stream << "NULL";
            }else{
                index_ = index_tmp;
                if(result = decode_string(value)){
                    stream << value;
                    free_string(value);
                }
            }
            return result;
        }
        else{
//            cout << "decode normal" << endl;
            result = decode_and_append_normal(stream, type);
        }

        return result;
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的表达式
     * @params[in] expr_key
     *     表达式类型
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_expr(stringstream& stream, long expr_key){
        bool result;

        switch(expr_key) {
            case DB_DRV_SQL_AND:
                result = decode_and_expr(stream);
                break;
            case DB_DRV_SQL_OR:
                result = decode_or_expr(stream);
                break;
            case DB_DRV_SQL_NOT:
                result = decode_not_expr(stream);
                break;

            case DB_DRV_SQL_LIKE:
                result = decode_binary_operator_expr(stream, "like\0");
                break;
            case DB_DRV_SQL_AS:
                result = decode_binary_operator_expr(stream, "as\0");
                break;
            case DB_DRV_SQL_EQUAL:
                result = decode_binary_operator_expr(stream, "=\0");
                break;
            case DB_DRV_SQL_NOT_EQUAL:
                result = decode_binary_operator_expr(stream, "!=\0");
                break;
            case DB_DRV_SQL_GREATER:
                result = decode_binary_operator_expr(stream, ">\0");
                break;
            case DB_DRV_SQL_GREATER_EQUAL:
                result = decode_binary_operator_expr(stream, ">=\0");
                break;
            case DB_DRV_SQL_LESS:
                result = decode_binary_operator_expr(stream, "<\0");
                break;
            case DB_DRV_SQL_LESS_EQUAL:
                result = decode_binary_operator_expr(stream, "<=\0");
                break;
            case DB_DRV_SQL_DOT:
                result = decode_binary_operator_expr(stream, ".\0");
                break;
            case DB_DRV_SQL_ADD:
                stream << " (";
                result = decode_binary_operator_expr(stream, "+\0");
                stream << ") ";
                break;
            case DB_DRV_SQL_SUB:
                stream << " (";
                result = decode_binary_operator_expr(stream, "-\0");
                stream << ") ";
                break;
            case DB_DRV_SQL_MUL:
                stream << " (";
                result = decode_binary_operator_expr(stream, "*\0");
                stream << ") ";
                break;
            case DB_DRV_SQL_DIV:
                stream << " (";
                result = decode_binary_operator_expr(stream, "/\0");
                stream << ") ";
                break;

            case DB_DRV_SQL_JOIN:
                result = decode_join_expr(stream, "JOIN\0");
                break;
            case DB_DRV_SQL_LEFT_JOIN:
                result = decode_join_expr(stream, "LEFT JOIN\0");
                break;
            case DB_DRV_SQL_RIGHT_JOIN:
                result = decode_join_expr(stream, "RIGHT JOIN\0");
                break;
            case DB_DRV_SQL_INNER_JOIN:
                result = decode_join_expr(stream, "INNER JOIN\0");
                break;

            case DB_DRV_SQL_ORDER:
                result = decode_order_expr(stream);
                break;

            case DB_DRV_SQL_GROUP:
                result = decode_group_expr(stream);
                break;

            case DB_DRV_SQL_HAVING:
                result = decode_having_expr(stream);
                break;

            case DB_DRV_SQL_BETWEEN:
                result = decode_between_expr(stream);
                break;

            case DB_DRV_SQL_FUN:
                result = decode_function_expr(stream);
                break;

            case DB_DRV_SQL_IS_NULL:
                result = decode_null_expr(stream, true);
                break;
            case DB_DRV_SQL_IS_NOT_NULL:
                result = decode_null_expr(stream, false);
                break;

            case DB_DRV_SQL_LIMIT: //db2不支持
            default:
                result = false;
        }

//        cout << "decode normal result: " << stream.str().c_str() << endl;
        return result;
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的是否为空的表达式
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_null_expr(stringstream& stream, bool is_null) {
        if(!gen_expr(stream)) {
            return false;
        }
        if(is_null){
            stream << " IS NULL ";
        }else{
            stream << " IS NOT NULL";
        }

        return true;
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的函数表达式
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_function_expr(stringstream& stream){
        int type;
        bool result;

        //解析并拼接函数名
        if(!gen_expr(stream)){
            return false;
        }

        type = get_erl_type();//获取Erlang数据类型
        stream << "(";
        if(type == ERL_LIST_EXT){
            //若为列表，则解析参数列表
            result = decode_term_params(stream);
        }else{
            //元素全为整型且小于256的列表会被当做字符串类型
            //若为字符串，则解析整型参数列表
            result = decode_integer_params(stream);
        }
        stream << ") ";

        return result;
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的between操作表达式
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_between_expr(stringstream& stream){
        //解析并拼接列名
        if(!gen_expr(stream)){
            return false;
        }
        stream << " BETWEEN ";
        //解析第一个参数
        if(!gen_expr(stream)){
            return false;
        }
        stream << " AND ";
        //解析第二个参数
        return gen_expr(stream);
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的having操作表达式
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_having_expr(stringstream& stream){
        stream << " HAVING ";
        return gen_expr(stream); //解析并拼接表达式
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的group by操作表达式
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_group_expr(stringstream& stream){
        int size; //group by列表长度

        stream << " GROUP BY ";

        //解析列表长
        if((size = decode_list_header()) <= 0) {
            return false;
        }
        //解析列表
        for(int i = 0; i < size; i++){
            if(!gen_expr(stream)){
                return false;
            }
            append_comma(stream, i, size);
        }

        return decode_empty_list();//空列表表示group by列表的结束
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的order by操作表达式
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_order_expr(stringstream& stream){
        int size;//order by列表长度
        long sort_flag;//排序标识，1 - desc，0 - asc

        stream << " ORDER BY ";

        //解析order by列表长度
        if((size = decode_list_header()) <= 0){
            return false;
        }
        // !!
        // Erlang端order by列表排序标识转换有缺陷，版本4.3，日期20120911
        for(int i = 0; i < size; i++){
            //每个元素应为2个元素的tuple
            //第一个元素为列名，第二个元素为排序方式
            if(decode_tuple_header() != 2 || !gen_expr(stream)
                    || !decode_integer(sort_flag)){
                return false;
            }

            stream << (sort_flag == 1? " DESC":" ASC");
            append_comma(stream, i, size);
        }

        return decode_empty_list();//空列表表示order by列表长度
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的join操作表达式
     * @params[in] join_type
     *     join类型字符串
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_join_expr(stringstream& stream, char* join_type){
        //解析第一个表名
        if(!gen_expr(stream)){
            return false;
        }
        stream << " " << join_type << " ";
        //解析第二个表名
        if(!gen_expr(stream)){
            return false;
        }
        stream << " ON ";

        return gen_expr(stream);//解析表达式
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的二元操作表达式
     * @params[in] oprt
     *     表达式字符串
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_binary_operator_expr(stringstream& stream, char* oprt){
        if(!gen_expr(stream)){
            return false;
        }
        stream << " " << oprt << " ";
        return gen_expr(stream);
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的not表达式
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_not_expr(stringstream& stream){
        stream << "NOT (";
        if(!gen_expr(stream)){
            return false;
        }
        stream << ") ";

        return true;
    }

    /**
     * 解析buf_并向指定的stringsream后拼接解析的or表达式
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_or_expr(stringstream& stream){
        stream << " (";
        if(!gen_expr(stream)) {
            return false;
        }
        stream << ") OR (";
        if(!gen_expr(stream)){
            return false;
        }
        stream << ") ";
        return true;
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的and表达式
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_and_expr(stringstream& stream) {
        int size;//and列表长度

        if((size = decode_list_header()) <= 0) {
            return false;
        }

//        cout << "size = " << size << endl;
        for(int i = 0; i < size; i++) {
            stream << " (";
            if(!gen_expr(stream)){
                return false;
            }
            stream << ") ";
            if(i < size - 1) {
                stream << "AND";
            }
        }

        return decode_empty_list();// 空列表表示and列表的结束
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的常规类型
     * 包括NULL、字符串、双字节数据、二进制、整型、浮点型
     * @params[int] type
     *     数据类型
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_and_append_normal(stringstream& stream, int type) {
        bool result = true;

        switch(type){
            case ERL_ATOM_EXT:
//                cout << "decode atom: " << endl;
                if (result = is_null()) {
                    stream << "NULL";
                }
                break;
            case ERL_NIL_EXT:
//                cout << "decode nil: " << endl;
                if (result = decode_empty_list()) {
                    stream << "''";
                }
                break;
            case ERL_STRING_EXT:
//                cout << "decode string: " << endl;
                result = decode_and_append_string(stream);
                break;
            case ERL_LIST_EXT:
//                cout << "decode list: " << endl;
                result = decode_and_append_list(stream);
                break;
            case ERL_BINARY_EXT:
//                cout << "decode binary: " << endl;
                result = decode_and_append_binary(stream);
                break;
            case ERL_SMALL_INTEGER_EXT:
            case ERL_INTEGER_EXT:
            case ERL_SMALL_BIG_EXT:
            case ERL_LARGE_BIG_EXT:
                result = decode_and_append_integer(stream);
                break;
            case ERL_FLOAT_EXT:
                result = decode_and_append_float(stream);
                break;
            default:
                result = false;
        }

//        cout << "decode normal result: " << stream.str().c_str() << endl;
        return result;

    }

    /**
     * 解析buf_并向指定的stringsream后拼接解析的自定义类型
     * 包括number、date、time和timestamp
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_and_append_custom(stringstream& stream){
        bool result;
        // 解析自定义类型
        switch(decode_custom_type()){
            case CUSTOM_NUMERIC_STRING:
                // 数字字符串
//                cout << "decode custom number " << endl;
                result = decode_and_append_numstr(stream);
                break;
            case CUSTOM_DATE:
                // 日期
//                cout << "decode custom date " << endl;
                result = decode_and_append_date(stream);
                break;
            case CUSTOM_TIME:
                // 时间
//                cout << "decode custom time " << endl;
                result = decode_and_append_time(stream);
                break;
            case CUSTOM_TIMESTAMP:
                // 日期和时间
//                cout << "decode custom timestamp " << endl;
                result = decode_and_append_timestamp(stream);
                break;
            default:
                return false;
        }

        return result;
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的日期和时间
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_and_append_timestamp(stringstream& stream) {
        DateTime timestamp;
        //解析日期和时间
        if(!decode_timestamp(&timestamp)) {
            return false;
        }

        //拼接stringstream
        //使用db2提供的TIMESTAMP函数
        stream << "TIMESTAMP('";
        append_date(stream, timestamp);
        stream << " ";
        append_time(stream, timestamp);
        stream << "')";

        return true;
    }

    /**
     * 解析buf_并向指定的stringsream后拼接解析的时间数据
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_and_append_time(stringstream& stream){
        DateTime time;
        //解析时间
        if(!decode_time(&time)) {
            return false;
        }

        //拼接stream
        //使用db2提供的TIME函数
        stream << "TIME('";
        append_time(stream, time);
        stream << "')";

        return true;
    }

    /**
     * 解析buf_并向指定的stringsream后拼接解析的时间数据
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_and_append_date(stringstream& stream) {
        DateTime date;
        //解析日期
        if(!decode_date(&date)) {
            return false;
        }

        //拼接stringstream
        //使用db2提供的DATE函数
        stream << "DATE('";
        append_date(stream, date);
        stream << "')";

        return true;
    }


    /**
     * 向指定的stringsream后拼接数字字符串表示的数字
     * @params[out] stream
     *     拼接后的stringstream
     */
    bool decode_and_append_numstr(stringstream& stream){
        char* value;
        if(!decode_string(value)){
            return false;
        }

        stream << value;
        free_string(value);

        return true;
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的浮点数
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_and_append_float(stringstream& stream){
        double value;
        if(ei_decode_double(buf_, &index_, &value) != 0){
            return false;
        }

        stream << value;

        return true;
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的整数
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_and_append_integer(stringstream& stream){
        long long value;
        if(ei_decode_longlong(buf_, &index_, &value) != 0) {
            return false;
        }

        stream << value;

        return true;
    }
    /**
     * 解析buf_并向指定的stringsream后拼接解析的二进制数据
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_and_append_binary(stringstream& stream) {
        MyBinary bin;
        char* value;

        if(!decode_binary(&bin)){
            return false;
        }

        value = (char*)malloc(2 * bin.len + 1);
        if(value == NULL) {
            return false;
        }

        for(int i = 0; i < bin.len; i++) {
            long_to_hex_string((unsigned long)bin.value[i], 1, value + i * 2);
        }
        value[2 * bin.len] = 0;

        stream << "x'" << value << "'";
        free(value);

        return true;
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的双字节字符串值
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_and_append_list(stringstream& stream) {
        long len;
        char* value;
        unsigned long data;

        if((len = decode_list_header()) <= 0) {
            return false;
        } // 解析长度

        /* 给value分配空间，用两个十六位字符表示一个字节
         * 每个字符占两个字节，因此分配4倍len的空间
         */
        value = (char*)malloc(len * 4 + 1);
        if(value == NULL) {
            return false;
        }

        // value赋值
        for(int i = 0; i < len; i++) {
            //解析long类型数据，数据值大于两个字节表示的最大值，则返回false
            if(ei_decode_ulong(buf_, &index_, &data) != 0 || data > 65535) {
                free(value);
                return false;
            }
            //将long类型数据转成16位字符串
            long_to_hex_string(data, 2, value + 4 * i);
        }
        if (!decode_empty_list()) {
            free(value);
            return false;
        }// 空list用于表示list的结束
        value[4 * len] = 0;// value最后一位加入\0字符

        stream << "gx'" << value << "'";
        free(value);

        return true;
    }


    /**
     * 解析buf_并向指定的stringsream后拼接解析的字符串值
     * @params[out] stream
     *     拼接后的stringstream
     * @return 解析并拼接成功，返回true。否则返回false.
     */
    bool decode_and_append_string(stringstream& stream) {
        char* tmp;     //在buf_中解析的字符串
        char* value;   //拼接到stringstream中的字符串
        int i = 0, j = 0; //i为tmp的index，j为value的index

        //解析buf_中的字符串
        if(!decode_string(tmp)) {
            return false;
        }
        //根据tmp长度分配value长度
        //由于需要将一个单引号转成两个单引号，因此最多需要strlen(tmp) * 2 + 1长度的内存
        value = (char*)malloc(strlen(tmp) * 2 + 1);
        if(value == NULL) {
            return false;
        }

        /* value赋值，将tmp中的单引号转成两个单引号赋值给value
         * 因为我们在sql语句中使用单引号表示字符串，所以要用两个单引号来表示
         * 一个单引号字符。
         */
        while(tmp[i] != 0) {
            value[j] = tmp[i];
            if(tmp[i] == '\''){
                j++;
                value[j] = tmp[i];
            }
            i++;
            j++;
        }// end of 'while'
        value[j] = 0; //最后一位加入\0表示字符串变量。
        stream << "'" << value << "'";//拼接stringstream

        //释放字符串变量
        free_string(tmp);
        free(value);

        return true;
    }
    /**
     * 在指定的p_index位置解析buf_中的空字符串
     * 如未指定则在上一次完成解析动作的位置进行解析，即在index_位置。
     * @params[in] p_index
     *     解析起始位置，如未指定，则默认为空
     * @params[out] p_index
     *     解析完成的位置
     * @return 解析成功则返回true，否则返回false
     */
    bool decode_empty_list(int* p_index = NULL) {
        if(p_index == NULL) {
            p_index = &index_;
        }

        return ei_decode_list_header(buf_, p_index, &size_) == 0;
    }

    /**
     * 长整数据转成16进制字符串
     * @params[in] src
     *     长整数据
     * @params[in] len
     *     一个长整数据所占的字节数
     * @params[out] dest
     *     转换结果
     * @return 成功则返回true，否则返回false
     */
    bool long_to_hex_string(unsigned long src, int len, char* dest) {
         for(int i = len - 1; i >= 0; i--) {
             dest[2 * i + 1] = src % 16;
             src = src >> 4;
             dest[2 * i] = src % 16;
             src = src >> 4;

             to_hex_char(dest + 2 * i + 1);
             to_hex_char(dest + 2 * i);
         }
         return true;
     }

    /**
     * 将字符的整型值转成16进制字符
     * @params[in] c
     *     字符
     * @params[out] c
     *     转换结果
     * @return 成功则返回true，否则返回false
     */
     bool to_hex_char(char* c) {
         if(*c < 0 || *c > 15) {
             return false;
         }

         if(*c < 10) {
             *c = *c + '0';
         } else {
             *c = *c - 10 + 'a';
         }

         return true;
     }

    /**
     * 根据指定长度，分配binary变量所需空间
     * @params[in] size
     *     二进制数据长度
     * @params[out] bin
     *     分配空间后的MyBinary指针
     * @return 分配成功，返回true。否则返回false.
     */
    bool alloc_binary(MyBinary* bin, long size) {
        bin->value = (char*)malloc((size + 1) * sizeof(char));
        bin->len = size;
        return bin->value != NULL;
    }

    /**
     * 释放指定MyBinary变量分配的空间
     * @params[out] bin
     *     释放空间后的MyBinary指针
     */
    void free_binary(MyBinary* bin) {
        free(bin->value);
        bin->value = NULL;
    }


    /**
     * 将DateTime数据按日期格式向指定的stringsream后拼接数据
     * 日期格式为year-month-day
     * @params[in] date
     *     日期
     * @params[out] stream
     *     拼接后的stringstream
     */
    void append_date(stringstream& stream, DateTime date){
        stream << date.year << "-" << date.month
                << "-" << date.day;
    }

    /**
     * 将DateTime数据按时间格式向指定的stringsream后拼接数据
     * 时间格式为hour:mm:ss
     * @params[in] time
     *     时间
     * @params[out] stream
     *     拼接后的stringstream
     */
    void append_time(stringstream& stream, DateTime time){
        stream << time.hour << ":";
        if(time.minute < 10) {
            stream << "0";//0补齐十位
        }
        stream << time.minute << ":";
        if(time.second < 10) {
            stream << "0";//0补齐十位
        }
        stream << time.second;
    }

    /**
     * 解析全部为整数，且小于256的参数列表，向指定的stringsream后拼接数据
     * @params[out] stream
     *     拼接后的stringstream
     * @return 成功返回true，否则返回false
     */
    bool decode_integer_params(stringstream& stream){
        int size;//参数列表长度
        char* params;//参数列表

        //元素全为整型且小于256的列表会被当做字符串类型
        if(!decode_string(params)){
            return false;
        }
        size = strlen(params);
        //拼接参数
        for(int i = 0; i < size; i++){
            stream << (int)params[i];
            append_comma(stream, i, size);
        }
        free_string(params);

        return true;
    }

    /**
     * 解析参数列表，向指定的stringsream后拼接数据
     * @params[out] stream
     *     拼接后的stringstream
     * @return 成功返回true，否则返回false
     */
    bool decode_term_params(stringstream& stream){
        int size; //参数列表长度
        if((size = decode_list_header()) <= 0) {
            return false;
        }
        for(int i = 0; i < size; i++) {
            if(!gen_expr(stream)){
                return false;
            }
            append_comma(stream, i, size);
        }

        return decode_empty_list();
    }

};
} //end of namespace rytong

#endif  // end of _RYT_DB2_OPERATION_H
