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

#ifndef _RYT_INFORMIX_OPERATION_H
#define _RYT_INFORMIX_OPERATION_H

#include "InformixConnection.h"
#include "it.h"

#include "../util/SysLogger.h"
#include "../util/EiEncoder.h"
#include "../base/DBOperation.h"
#include "../base/DBException.h"
#include "InformixUtils.h"
#include "InformixStatement.h"
#include <stdexcept>

#define ENCODE_ERROR(msg, res) \
    LOG_INFM_ERROR(msg) \
    EiEncoder::encode_error_msg(msg, res);

#define BAD_ARG "bad argument"
#define BAD_CONVERTER "cannot get a converter"
#define BAD_DATUM "cannot get interface of datum"
#define BAD_LARGEOBJ "cannot get interface of large object"
#define BAD_INTEGER_DATA "cannot encode as integral number"
#define BAD_STRING_DATA "cannot encode as string"
#define BAD_BYTE_DATA "cannot encode as byte"
#define BAD_FLOAT_DATA "cannot encode as floating-point number"
#define STMT_NULL "cannot get the statement"

#define MAX_BUF_LENG 1024

using namespace std;

namespace rytong {

enum DateTime_Index{
    INF_Year     = 0,
    INF_Month    = 1,
    INF_Day      = 2,
    INF_Hour     = 3,
    INF_Minute   = 4,
    INF_Second   = 5
};


enum LO_Type{
    INF_TEXT = 0,
    INF_BYTE = 1,
    INF_CLOB = 2,
    INF_BLOB = 3
};

typedef struct INFLOParam{
    int index;
    LO_Type type;
    INFLOParam *next;
    INFLOParam *last;
} INFLOParam;

typedef struct{
    int data[5];
    string second_data;
    int qualifier[2];
} INFDateTime;


class InformixOperation : public DBOperation {
public:

    InformixOperation();

    ~InformixOperation();

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
    bool prepare_statement_init(ei_x_buff * const res);

    /** perpare statement exec interface **/
    bool prepare_stat_exec(ei_x_buff * const res);
    bool prepare_statement_exec(ei_x_buff * const res);

    /** perpare statement release interface **/
    bool prepare_stat_release(ei_x_buff * const res);
    bool prepare_statement_release(ei_x_buff * const res);

    /** insert interface */
    bool insert(ei_x_buff * const res);

    /** update interface */
    bool update(ei_x_buff * const res);

    /** del interface */
    bool del(ei_x_buff * const res);

    /** select interface */
    bool select(ei_x_buff * const res);

    void decode_expr_tuple(stringstream & sm){}

private:
    string err_msg_;

    InformixOperation & operator =(const InformixOperation&);

    InformixOperation(InformixOperation&);

//    char message_[SQL_MAX_MESSAGE_LENGTH + 1];
    bool execute_sql(ei_x_buff * const, ITConnection, const char*);
    bool execute_sql_with_params(ei_x_buff * const, ITConnection*, const char*, INFLOParam*);
    bool decode_and_execute_stmt(ei_x_buff * const, InformixStatement*);
    bool stmt_exec_and_encode_res(ei_x_buff * const, InformixStatement*);

    inline bool create_param(INFLOParam*&,LO_Type, int* p_index = NULL);
    inline bool add_param(INFLOParam*&,LO_Type, int* p_index = NULL);
    inline void free_params(INFLOParam*&);

    bool encode_column(ei_x_buff * const, ITValue *);
    bool encode_as_string(ei_x_buff * const, ITValue *);
    bool encode_as_byte(ei_x_buff * const, ITValue *);
    bool encode_as_lob(ei_x_buff * const, ITValue *);
    bool encode_as_integer(ei_x_buff * const, ITValue *);
    bool encode_as_float(ei_x_buff * const, ITValue *);
    bool encode_as_datetime(ei_x_buff * const, ITValue *);
    bool encode_as_date(ei_x_buff * const, ITValue *);

    bool encode_number_tuple(ei_x_buff * const, const char *);
    bool encode_datetime_tuple(ei_x_buff * const, ITDateTime *);
    bool encode_date_tuple(ei_x_buff * const, ITDateTime *);

    inline bool decode_and_append_value(stringstream& stream, INFLOParam*&);
    inline bool decode_and_append_where(stringstream& stream, INFLOParam*&);
    inline bool decode_and_bind_param(ITValue *);
    inline bool decode_and_bind_lob(ITValue *, LO_Type, int* p_index = NULL);

    inline bool decode_and_bind_byte(ITValue *, int* p_index = NULL);
    inline bool decode_and_bind_text(ITValue *, int* p_index = NULL);
    inline bool decode_and_bind_clob(ITValue *, int* p_index = NULL);
    inline bool decode_and_bind_blob(ITValue *, int* p_index = NULL);

    inline bool decode_and_bind_string(ITValue *);
    inline bool decode_and_bind_integer(ITValue *);
    inline bool decode_and_bind_float(ITValue *);
    inline bool decode_and_bind_custom(ITValue *);
    inline bool decode_and_bind_numstr(ITValue *);
    inline bool decode_and_bind_date(ITValue *);
    inline bool decode_and_bind_datetime(ITValue *);

    inline bool gen_expr(stringstream& stream, INFLOParam*&);
    inline bool decode_expr(stringstream& stream, long expr_key, INFLOParam*&);
    inline bool decode_null_expr(stringstream& stream, bool is_null, INFLOParam*&);
    inline bool decode_function_expr(stringstream& stream, INFLOParam*&);
    inline bool decode_between_expr(stringstream& stream, INFLOParam*&);
    inline bool decode_having_expr(stringstream& stream, INFLOParam*&);
    inline bool decode_group_expr(stringstream& stream, INFLOParam*&);
    inline bool decode_order_expr(stringstream& stream, INFLOParam*&);
    inline bool decode_join_expr(stringstream& stream, char* join_type, INFLOParam*&);
    inline bool decode_binary_operator_expr(stringstream& stream, char* oprt, INFLOParam*&);
    inline bool decode_not_expr(stringstream& stream, INFLOParam*&);
    inline bool decode_or_expr(stringstream& stream, INFLOParam*&);
    inline bool decode_and_expr(stringstream& stream, INFLOParam*&);
    inline bool decode_in_expr(stringstream& stream, INFLOParam*&);

    inline bool decode_term_params(stringstream& stream, INFLOParam*&);
    inline bool decode_integer_params(stringstream& stream);

    inline bool decode_and_append_custom(stringstream& stream, INFLOParam*&);
    inline bool decode_and_append_normal(stringstream& stream, int type, INFLOParam*&);

    inline bool decode_and_append_string(stringstream& stream);
    inline bool decode_and_append_date(stringstream& stream);
    inline bool decode_and_append_datetime(stringstream& stream);
    inline bool decode_and_append_numstr(stringstream& stream);
    inline bool decode_and_append_lob(stringstream&, LO_Type, INFLOParam*&);

    inline bool decode_inf_datetime(INFDateTime *datetime);
//    inline bool get_inf_datetime_index(char *qualifier_str, int& index);
    inline bool decode_datetime_field(long &field, DateTime_Index dt_index, int &first, int &last);
    inline bool decode_datetime_second(string &field, int &first, int &last);
    inline void append_inf_datetime(stringstream& stream, INFDateTime datetime);
    inline void append_inf_date(stringstream& stream, INFDateTime datetime);

//    bool execSqlStmt(ei_x_buff * const, SQLHANDLE, SQLCHAR*)

};
} //end of namespace rytong

#endif  // end of _RYT_INFORMIX_OPERATION_H
