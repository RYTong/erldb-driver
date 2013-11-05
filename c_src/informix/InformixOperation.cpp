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
#include "InformixOperation.h"



using namespace rytong;



InformixOperation::InformixOperation(){
}

InformixOperation::~InformixOperation(){
}

bool InformixOperation::exec(ei_x_buff* res){
    char *sql = NULL;
    InformixConnection *if_conn = (InformixConnection*)conn_;
    ITConnection *conn = (ITConnection*)if_conn->get_connection();

//    if(!if_conn->set_auto_commit(!muli_tran_flag_)){
//        ENCODE_ERROR("set auto commit failed", res)
//        return true;
//    }

    if(!decode_string(sql)){
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }

    execute_sql(res, *conn, (const char *)sql);
    free_string(sql);

    return true;
}

bool InformixOperation::insert(ei_x_buff* res){
    InformixConnection *if_conn = (InformixConnection*)conn_;
    ITConnection *conn = (ITConnection*)if_conn->get_connection();

//    if(!if_conn->set_auto_commit(!muli_tran_flag_)){
//        ENCODE_ERROR("set auto commit failed", res)
//        return true;
//    }

    char *table_name = NULL;
    if((decode_tuple_length() != 2) || !decode_string(table_name)){
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }
//    LOG_INFM_DEBUG("table_name: %s\n\r", table_name)

    int fields_num;
    if((fields_num = decode_list_length()) <= 0){
        free_string(table_name);
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }
//    LOG_INFM_DEBUG("fields_num: %d\n\r", fields_num)

    stringstream sqlstream, tmpstream;
    sqlstream << "INSERT INTO " << table_name << " (";
    tmpstream << " VALUES (";
    free_string(table_name);

    char* field_name;
    INFLOParam *params = NULL;
    for(int i = 0; i < fields_num; i++) {
        if((decode_tuple_length() != 2) || !decode_string(field_name)){
            ENCODE_ERROR(BAD_ARG, res)
            return true;
        }
        sqlstream << field_name;
        free_string(field_name);

        if(!decode_and_append_value(tmpstream, params)){
            ENCODE_ERROR(BAD_ARG, res)
            return true;
        }

        if (i < fields_num - 1) {
            sqlstream << ",";
            tmpstream << ",";
        }
    }
    sqlstream << ")" << tmpstream.str() << ")";

    string sql = sqlstream.str();
    if(params == NULL) {
        execute_sql(res, *conn, sql.c_str());
    }else {
        execute_sql_with_params(res, conn, sql.c_str(), params);
        free_params(params);
    }

    return true;
}

bool InformixOperation::del(ei_x_buff* res){
    InformixConnection *if_conn = (InformixConnection*)conn_;
    ITConnection *conn = (ITConnection*)if_conn->get_connection();

//    if(!if_conn->set_auto_commit(!muli_tran_flag_)){
//        ENCODE_ERROR("set auto commit failed", res)
//        return true;
//    }

    char *table_name = NULL;
    if((decode_tuple_length() != 2) || !decode_string(table_name)){
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }
//    LOG_INFM_DEBUG("table_name: %s\n\r", table_name)

    stringstream sqlstream;
    sqlstream << "DELETE FROM " << table_name ;
    free_string(table_name);
    INFLOParam *params = NULL;
    if(!decode_and_append_where(sqlstream, params)){
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }

    string sql = sqlstream.str();
    if(params == NULL) {
        execute_sql(res, *conn, sql.c_str());
    }else {
        execute_sql_with_params(res, conn, sql.c_str(), params);
        free_params(params);
    }

    return true;
}

bool InformixOperation::update(ei_x_buff* res){
    InformixConnection *if_conn = (InformixConnection*)conn_;
    ITConnection *conn = (ITConnection*)if_conn->get_connection();

//    if(!if_conn->set_auto_commit(!muli_tran_flag_)){
//        ENCODE_ERROR("set auto commit failed", res)
//        return true;
//    }

    char *table_name = NULL;
    if((decode_tuple_length() != 3) || !decode_string(table_name)){
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }
//    LOG_INFM_DEBUG("table_name: %s\n\r", table_name)

    stringstream sqlstream;
    sqlstream << "UPDATE " << table_name << " SET ";
    free_string(table_name);
    int field_num;

    if ((field_num = decode_list_length()) <= 0) {
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }

    char *field_name;
    INFLOParam *params = NULL;
    for(int i = 0; i < field_num; i++){
        if(decode_tuple_length() != 2 || !decode_string(field_name)){
            ENCODE_ERROR(BAD_ARG, res)
            return true;
        }
        sqlstream << field_name << "=";
        free_string(field_name);

        if(!gen_expr(sqlstream, params)){
            ENCODE_ERROR(BAD_ARG, res)
            return true;
        }
        if (i < field_num - 1) {
            sqlstream << ",";
        }
    }

    if (!decode_list_length() == 0 || !decode_and_append_where(sqlstream, params)) {
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }

    string sql = sqlstream.str();
    if(params == NULL) {
        execute_sql(res, *conn, sql.c_str());
    }else {
        execute_sql_with_params(res, conn, sql.c_str(), params);
        free_params(params);
    }

    return true;
}

bool InformixOperation::select(ei_x_buff* res){
    InformixConnection *if_conn = (InformixConnection*)conn_;
    ITConnection *conn = (ITConnection*)if_conn->get_connection();

//    if(!if_conn->set_auto_commit(!muli_tran_flag_)){
//        ENCODE_ERROR("set auto commit failed", res)
//        return true;
//    }

    long distinct_flag;
    if((decode_tuple_length() != 5) || !decode_integer(distinct_flag)){
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }

    stringstream sqlstream;
    sqlstream << "SELECT " << (distinct_flag == 1?"DISTINCT ":"");

    int field_num;
    INFLOParam *params = NULL;
    if ((field_num = decode_list_length()) > 0) {
        for (int i = 0; i < field_num; i++) {
            if (!gen_expr(sqlstream, params)) {
                ENCODE_ERROR(BAD_ARG, res)
                return true;
            }
            if (i < field_num - 1) {
                sqlstream << ",";
            }
        }
        if (decode_list_length() != 0) {
            ENCODE_ERROR(BAD_ARG, res)
            return true;
        }
    } else {
        sqlstream << "*";
        ei_skip_term(buf_, &index_);
    }

    int table_num;
    if ((table_num = decode_list_length()) <= 0) {
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }
    sqlstream << " FROM ";
    for (int i = 0; i < table_num; i++) {
        if (!gen_expr(sqlstream, params)) {
            ENCODE_ERROR(BAD_ARG, res)
            return true;
        }
        if (i < table_num - 1) {
            sqlstream << ",";
        }
    }

    if (decode_list_length() != 0 || !decode_and_append_where(sqlstream, params)) {
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }

    int extra_num;
    if ((extra_num = decode_list_length()) > 0) {
        for (int i = 0; i < extra_num; i++) {
            sqlstream << " ";
            if (!gen_expr(sqlstream, params)) {
                ENCODE_ERROR(BAD_ARG, res)
                return true;
            }
        }
        if (decode_list_length() != 0) {
            ENCODE_ERROR(BAD_ARG, res)
            return true;
        }
    }

    string sql = sqlstream.str();
    if(params == NULL) {
        execute_sql(res, *conn, sql.c_str());
    }else {
        execute_sql_with_params(res, conn, sql.c_str(), params);
        free_params(params);
    }
//    if(params){
//        free_params(params);
//        ENCODE_ERROR("could not execute select with byte, text, clob or blob parameters", res)
//        return true;
//    }
//    execute_sql(res, *conn, sqlstream.str().c_str());

//    LOG_INFM_DEBUG("table_name: %s\n\r", table_name)
    return true;
}

bool InformixOperation::prepare_statement_init(ei_x_buff* res) {
    char *sql;
    if(!decode_string(sql)){
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }
    ITConnection *conn = (ITConnection*)((InformixConnection*) conn_)->get_connection();
    InformixStatement *inf_stmt = new InformixStatement(conn);
    bool flag = inf_stmt->Prepare(sql);
    free_string(sql);
    if (!flag || inf_stmt->Error()) {
        delete inf_stmt;
        inf_stmt = NULL;
        ENCODE_ERROR((const char*) inf_stmt->ErrorText(), res)
        return true;
    }

//    ITStatement *stmt = new ITStatement(*conn);
//    bool flag = stmt->Prepare(sql);
//    if (!flag || stmt->Error()) {
//        ENCODE_ERROR((const char*) stmt->ErrorText(), res)
//        return true;
//    }
    EiEncoder::encode_ok_pointer(inf_stmt, res);
    return true;
}

bool InformixOperation::prepare_stat_init(ei_x_buff* res) {
    InformixConnection *if_conn = (InformixConnection*) conn_;
    ITConnection *conn = (ITConnection*) if_conn->get_connection();

//    if (!if_conn->set_auto_commit(!muli_tran_flag_)) {
//        ENCODE_ERROR("set auto commit failed", res)
//        return true;
//    }

    char *name, *sql;
    if (decode_tuple_length() != 2 ||
            !decode_string(name) ||
            !decode_string(sql)){
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }

    InformixStatement *inf_stmt = new InformixStatement(conn);
    bool flag = inf_stmt->Prepare(sql);
    free_string(sql);
    if (!flag || inf_stmt->Error()) {
        free_string(name);
        delete inf_stmt;
        inf_stmt = NULL;
        ENCODE_ERROR((const char*) inf_stmt->ErrorText(), res)
        return true;
    }

//    ITStatement *stmt = new ITStatement(*conn);
//    if (!stmt->Prepare(sql) || stmt->Error()) {
//        ENCODE_ERROR((const char*)stmt->ErrorText(), res)
//        free_string(name);
//        free_string(sql);
//        return true;
//    }

    if (stmt_map_->add((string) name, (void*) inf_stmt)) {
        EiEncoder::encode_ok_msg(name, res);
    } else {
        inf_stmt->Drop();
        delete inf_stmt;
        inf_stmt = NULL;
        ENCODE_ERROR("Already registered prepare name", res)
    }

//    LOG_INFM_DEBUG("init a statement, name is: %s\n\r", name)
    free_string(name);
    return true;
}

bool InformixOperation::prepare_statement_exec(ei_x_buff* res) {
    InformixStatement *inf_stmt;
    if(decode_tuple_length() != 2 ||
            ei_decode_binary(buf_, &index_, &inf_stmt, &bin_size_) != 0){
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }

    decode_and_execute_stmt(res, inf_stmt);
    return true;
}

bool InformixOperation::prepare_stat_exec(ei_x_buff* res){
    char *name;
    InformixStatement *inf_stmt = NULL;

    if(decode_tuple_length() != 2 || !decode_string(name)){
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }

    if((inf_stmt = (InformixStatement*)(stmt_map_->get(name))) == NULL){
        ENCODE_ERROR(STMT_NULL, res)
        free_string(name);
        return true;
    }
    free_string(name);

    decode_and_execute_stmt(res, inf_stmt);
    return true;
}

bool InformixOperation::prepare_statement_release(ei_x_buff* res) {
    InformixStatement *stmt;
    if(ei_decode_binary(buf_, &index_, &stmt, &bin_size_) != 0){
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }
    if(stmt != NULL) {
        stmt->Drop();
        delete stmt;
        stmt = NULL;
    }
    EiEncoder::encode_ok_msg("close stmt", res);
    return true;
}

bool InformixOperation::prepare_stat_release(ei_x_buff* res){
    char *name;
    if(!decode_string(name)){
        ENCODE_ERROR(BAD_ARG, res)
        return true;
    }

    InformixStatement *stmt = NULL;
    if((stmt = (InformixStatement*)(stmt_map_->get(name))) != NULL){
        if(!stmt->Drop()){
            ENCODE_ERROR("could not drop the statement", res)
            return true;
        }
        delete stmt;
        stmt = NULL;
        stmt_map_->remove(name);
        EiEncoder::encode_ok_msg("close stmt", res);
    } else {
        ENCODE_ERROR(STMT_NULL, res)
    }

    free_string(name);
    return true;
}

bool InformixOperation::trans_begin(ei_x_buff* res) {
    InformixConnection *if_conn = (InformixConnection*) conn_;

    if (!if_conn->set_auto_commit(false)) {
        ENCODE_ERROR("set auto commit failed", res)
        return false;
    }

    EiEncoder::encode_ok_pointer((void*)if_conn, res);
    return true;
}

bool InformixOperation::trans_commit(ei_x_buff* res){
    InformixConnection *if_conn = (InformixConnection*) conn_;
    ITConnection *conn = (ITConnection*) if_conn->get_connection();

    if(!conn->SetTransaction(ITConnection::COMMIT)){
        ENCODE_ERROR("failed to commit the transaction", res)
        return false;
    }
    EiEncoder::encode_ok_msg("COMMIT", res);
    return true;
}

bool InformixOperation::trans_rollback(ei_x_buff* res){
    InformixConnection *if_conn = (InformixConnection*) conn_;
    ITConnection *conn = (ITConnection*) if_conn->get_connection();

    if(!conn->SetTransaction(ITConnection::ABORT)){
        ENCODE_ERROR("failed to rollback the transaction", res)
        return false;
    }
    EiEncoder::encode_ok_msg("ROLLBACK", res);
    return true;
}

//void InformixOperation::decode_expr_tuple(stringstream& sm){
//}

bool InformixOperation::decode_and_execute_stmt(ei_x_buff * const res, InformixStatement* inf_stmt){
    inf_stmt->StartTransaction();
    int num_params = inf_stmt->NumParams();
    int erl_type = erl_data_type();
    if (erl_type == ERL_LIST_EXT) {
        if (decode_list_length() < num_params) {
            inf_stmt->AbortTransaction();
            ENCODE_ERROR(BAD_ARG, res)
            return false;
        }
        for (int i = 0; i < num_params; i++) {
            ITValue *param = inf_stmt->Param(i);
//            LOG_INFM_DEBUG("name of param's type: %s\n\r", (const char *) (param->TypeOf().Name()))
            if (!decode_and_bind_param(param)) {
                inf_stmt->AbortTransaction();
                param->Release();
                ENCODE_ERROR(err_msg_.c_str(), res)
                return false;
            }
            param->Release();
        }
    }else {
        int size;
        char *params;
        if (!decode_string(params)) {
            inf_stmt->AbortTransaction();
            ENCODE_ERROR(BAD_ARG, res)
            return false;
        }
        if((size = strlen(params)) < num_params){
            inf_stmt->AbortTransaction();
            ENCODE_ERROR(BAD_ARG, res)
            return false;
        }
        for (int i = 0; i < size; i++) {
            ITValue *param = inf_stmt->Param(i);
            ITConversions *conv = NULL;
            if (param->QueryInterface(ITConversionsIID, (void **) & conv)
                    != IT_QUERYINTERFACE_SUCCESS) {
                inf_stmt->AbortTransaction();
                ENCODE_ERROR(BAD_CONVERTER, res)
                return false;
            }
            if (!conv->ConvertFrom((int)params[i])) {
                inf_stmt->AbortTransaction();
                param->Release();
                ENCODE_ERROR("cannot convert from integer", res)
                return false;
            }
            param->Release();
        }
        free_string(params);
    }

    if(!stmt_exec_and_encode_res(res, inf_stmt)){
        inf_stmt->AbortTransaction();
        return false;
    }
    inf_stmt->CommitTransaction();

    return true;
}

bool InformixOperation::stmt_exec_and_encode_res(ei_x_buff * const res, InformixStatement* inf_stmt){
    if(!inf_stmt->Exec()){
        err_msg_ = inf_stmt->Error()? inf_stmt->ErrorText(): "failed to execute statement";
        ENCODE_ERROR(err_msg_.c_str(), res)
        return false;
    }
//    LOG_INFM_DEBUG("statement executed: %s\n\r", "")

    ei_x_new_with_version(res);
    ei_x_encode_tuple_header(res, 2);
    ei_x_encode_atom(res, "ok");

    ITRow *row = inf_stmt->NextRow();
//    LOG_INFM_DEBUG("first row is null: %d\n\r", row == NULL)
    if (row == NULL){
//        LOG_INFM_DEBUG("no result set for command: %s\n\r", (const char *)inf_stmt->Command())
        if(strcmp(inf_stmt->Command(), "select") == 0){
            ei_x_encode_list_header(res, 0);
            ei_x_encode_empty_list(res);
        }else{
            ei_x_encode_long(res, inf_stmt->RowCount());
        }
        //result set is empty or not 'select' sql
    } else {
        int pos = res->index;
        ei_x_encode_list_header(res, 1);

        do {//encode every column's data of each row
            ITValue *col = NULL;
            long columns_count = row->NumColumns();
            ei_x_encode_list_header(res, columns_count);
            for (int i = 0; i < columns_count; i++){
                col = row->Column(i);
                if(!encode_column(res, col)){
                    LOG_INFM_DEBUG("failed to encode a column: %s\n\r", "")
                    col->Release();
                    row->Release();
                    return false;
                }
                col->Release();
            }

            ei_x_encode_empty_list(res);
            row->Release();
        } while((row = inf_stmt->NextRow()) != NULL);

        ei_x_encode_empty_list(res);

        ei_x_buff x;
        ei_x_new(&x);
//        LOG_INFM_DEBUG("RowCount: %d\n\r", inf_stmt->RowCount())
        ei_x_encode_list_header(&x, inf_stmt->RowCount());
        memcpy(res->buff + pos, x.buff, x.index);
        ei_x_free(&x);
    }// if ((row = inf_stmt->.NextRow()) == NULL)
    
    return true;
}

bool InformixOperation::execute_sql_with_params(ei_x_buff* const res, 
        ITConnection *conn, const char* sql, INFLOParam* params){
    LOG_INFM_DEBUG("execute sql: %s\n\r", sql)

    InformixStatement stmt(conn);
    //start a transaction
    if (!stmt.StartTransaction()) {
        ENCODE_ERROR("could not start a transaction", res)
        return false;
    }
    //prepare a statement
//    ITStatement stmt(conn);
    if (!stmt.Prepare(sql) || stmt.Error()) {
        stmt.AbortTransaction();
//        conn.SetTransaction(ITConnection::Abort);
        ENCODE_ERROR(stmt.ErrorText().Data(), res)
        return false;
    }

    //bind parameters
    int num_params = stmt.NumParams();
    for(int i = 0; i < num_params; i++){
        if(params == NULL){
            stmt.Drop();
            stmt.AbortTransaction();
            ENCODE_ERROR(BAD_ARG, res)
            return false;
        }
        ITValue *param = stmt.Param(i);
//            LOG_INFM_DEBUG("name of param's type: %s\n\r", (const char *) (param->TypeOf().Name()))
        if (!decode_and_bind_lob(param, params->type, &(params->index))) {
            param->Release();
            stmt.Drop();
            stmt.AbortTransaction();
            ENCODE_ERROR(err_msg_.c_str(), res)
            return false;
        }
        params = params->next;
        param->Release();
    }

    if(!stmt_exec_and_encode_res(res, &stmt)){
        stmt.AbortTransaction();
        stmt.Drop();
        return false;
    }
    
    stmt.CommitTransaction();
    stmt.Drop();

    return true;
}

bool InformixOperation::execute_sql(ei_x_buff * const res, ITConnection conn, const char* sql){
    LOG_INFM_DEBUG("execute sql: %s\n\r", sql)

    ITQuery query(conn);
    if(!query.ExecForIteration(sql)){
        if(query.Error()){
            ENCODE_ERROR(query.ErrorText().Data(), res)
        } else {
            ENCODE_ERROR("failed to execute sql", res)
        }
        return false;
    }

    ei_x_new_with_version(res);
    ei_x_encode_tuple_header(res, 2);
    ei_x_encode_atom(res, "ok");

    ITRow *row;
    if ((row = query.NextRow()) == NULL){
//        LOG_INFM_DEBUG("no result set for command: %s\n\r", (const char *)query.Command())
        if(strcmp(query.Command(), "select") == 0){
            ei_x_encode_list_header(res, 0);
            ei_x_encode_empty_list(res);
        }else{
            ei_x_encode_long(res, query.RowCount());
        }
        //result set is empty or not 'select' sql
    } else {
        int pos = res->index;
        ei_x_encode_list_header(res, 1);

        do {//encode every column's data of each row
            ITValue *col = NULL;
            long columns_count = row->NumColumns();
            ei_x_encode_list_header(res, columns_count);
            for (int i = 0; i < columns_count; i++){
                col = row->Column(i);
                if(!encode_column(res, col)){
                    LOG_INFM_DEBUG("failed to encode a column: %s\n\r", "")
                    col->Release();
                    row->Release();
                    return false;
                }
                col->Release();
            }

            ei_x_encode_empty_list(res);
            row->Release();
        } while((row = query.NextRow()) != NULL);

        ei_x_encode_empty_list(res);

        ei_x_buff x;
        ei_x_new(&x);
        ei_x_encode_list_header(&x, query.RowCount());
        memcpy(res->buff + pos, x.buff, x.index);
        ei_x_free(&x);
    }// if ((row = query.NextRow()) == NULL)

    return true;
}

bool InformixOperation::encode_column(ei_x_buff * const res, ITValue* col){
    if(col->IsNull()){
//        LOG_INFM_DEBUG("column is null: %s\n\r", "")
        ei_x_encode_atom(res, "undefined");
        return true;
    }

    const char *col_type = col->TypeOf().Name();
//    LOG_INFM_DEBUG("column type is : %s\n\r", col_type)
    bool encode_flag;
    if (strcmp(col_type, "char") == 0 ||
            strcmp(col_type, "nchar") == 0 ||
            strcmp(col_type, "varchar") == 0 ||
            strcmp(col_type, "nvarchar") == 0 ||
            strcmp(col_type, "text") == 0) {
        encode_flag = encode_as_string(res, col);
    } else if (strcmp(col_type, "byte") == 0) {
        encode_flag = encode_as_byte(res, col);
    } else if (strcmp(col_type, "clob") == 0 ||
            strcmp(col_type, "blob") == 0) {
        encode_flag = encode_as_lob(res, col);
    } else if (strcmp(col_type, "integer") == 0 ||
            strcmp(col_type, "int8") == 0 ||
            strcmp(col_type, "smallint") == 0 ||
            strcmp(col_type, "serial") == 0 ||
            strcmp(col_type, "serial8") == 0) {

        encode_flag = encode_as_integer(res, col);
    } else if (strcmp(col_type, "smallfloat") == 0 ||
            strcmp(col_type, "float") == 0 ||
            strcmp(col_type, "decimal") == 0 ||
            strcmp(col_type, "money") == 0) {

        encode_flag = encode_as_float(res, col);
    } else if (strcmp(col_type, "datetime") == 0 ||
            strcmp(col_type, "interval") == 0 ){

        encode_flag = encode_as_datetime(res, col);
    } else if (strcmp(col_type, "date") == 0){

        encode_flag = encode_as_date(res, col);
    } else {
//        ei_x_encode_tuple_header(res, 2);
        ei_x_encode_atom(res, "not_support");
//        ei_x_encode_string(res, col->Printable());
        encode_flag = true;
    }

    col_type = NULL;
    return encode_flag;
}

bool InformixOperation::encode_as_string(ei_x_buff * const res, ITValue* col) {
    ITConversions *conv;
    if (col->QueryInterface(ITConversionsIID, (void **) & conv)
            != IT_QUERYINTERFACE_SUCCESS) {
        ENCODE_ERROR(BAD_CONVERTER, res)
        return false;
    }

    const char *cstring_value;
    if (!conv->ConvertTo(cstring_value)) {
        ENCODE_ERROR("cannot convert to c string", res)
        conv->Release();
        return false;
    }
    conv->Release();
    
//    LOG_INFM_DEBUG("string value: %s\n\r", cstring_value);
    if(ei_x_encode_string(res, cstring_value) != 0) {
        ENCODE_ERROR(BAD_STRING_DATA, res)
        cstring_value = NULL;
        return false;
    };

    cstring_value = NULL;
    return true;
}

bool InformixOperation::encode_as_byte(ei_x_buff* const res, ITValue* col) {
    ITDatum *datum;
    if (col->QueryInterface(ITDatumIID, (void **) & datum)
            != IT_QUERYINTERFACE_SUCCESS) {
        ENCODE_ERROR(BAD_DATUM, res)
        return false;
    }
    bool flag = true;
    if(ei_x_encode_binary(res, (char *)datum->Data(), datum->DataLength()) != 0){
        ENCODE_ERROR(BAD_BYTE_DATA, res)
        flag = false;
    }
    datum->Release();
    return flag;
}

bool InformixOperation::encode_as_lob(ei_x_buff* const res, ITValue* col) {
    ITLargeObject *lo;
    if (col->QueryInterface(ITLargeObjectIID, (void **) &lo)
            != IT_QUERYINTERFACE_SUCCESS) {
        ENCODE_ERROR(BAD_LARGEOBJ, res)
        return false;
    }

    string value;
    string::size_type length = 0;
    int num;
    char *buf = new char[MAX_BUF_LENG + 1];

    try{
        while ((num = lo->Read(buf, MAX_BUF_LENG)) > 0) {
            value.insert(length, buf, num);
            length += num;
            delete [] buf;
            buf = new char[MAX_BUF_LENG + 1];
        }
    }
    catch(bad_alloc &err_ba) {
        stringstream err_str;
        err_str << "failed to alloc memory to handle lob data, reason: "
                << err_ba.what() << endl;
        ENCODE_ERROR(err_str.str().c_str(), res)
        return false;
    }
    catch(length_error &err_len) {
        stringstream err_str;
        err_str << "the length of lob data is out of range, reason:"
                << err_len.what() << endl;
        ENCODE_ERROR(err_str.str().c_str(), res)
        return false;
    }

    delete [] buf;
    lo->Release();
    
    if(col->TypeOf().Name().Equal("clob")){
        if(ei_x_encode_string(res, value.c_str()) != 0) {
            ENCODE_ERROR(BAD_STRING_DATA, res)
            return false;
        }
    }else {
        if (ei_x_encode_binary(res, value.c_str(), length) != 0) {
            ENCODE_ERROR(BAD_BYTE_DATA, res)
            return false;
        }
    }

    return true;
}

bool InformixOperation::encode_as_integer(ei_x_buff * const res, ITValue* col){
    ITConversions *conv;
    if (col->QueryInterface(ITConversionsIID, (void **) & conv)
            != IT_QUERYINTERFACE_SUCCESS) {
        ENCODE_ERROR(BAD_CONVERTER, res)
        return false;
    }
    int size = col->TypeOf().Size();

    if(size == sizeof(short)){ // encode as short
        short cshort_value;
        if(!conv->ConvertTo(cshort_value)){
            ENCODE_ERROR("cannot convert to c short", res)
            conv->Release();
            return false;
        }
//        LOG_INFM_DEBUG("short value: %d\n\r", cshort_value)
        if(ei_x_encode_long(res, cshort_value) != 0) {
            ENCODE_ERROR(BAD_INTEGER_DATA, res)
            conv->Release();
            return false;
        };
    }else if (size == sizeof(int)) {//encode as int
        int cint_value;
        if(!conv->ConvertTo(cint_value)){
            ENCODE_ERROR("cannot convert to c int", res)
            conv->Release();
            return false;
        }
//        LOG_INFM_DEBUG("int value: %d\n\r", cint_value)
        if(ei_x_encode_long(res, cint_value) != 0) {
            ENCODE_ERROR(BAD_INTEGER_DATA, res)
            conv->Release();
            return false;
        };
    }else if (size == sizeof(long)) {//encode as long
        long clong_value;
        if(!conv->ConvertTo(clong_value)){
            ENCODE_ERROR("cannot convert to c long", res)
            conv->Release();
            return false;
        }
//        LOG_INFM_DEBUG("long value: %d\n\r", clong_value)
        if(ei_x_encode_long(res, clong_value) != 0){
            ENCODE_ERROR(BAD_INTEGER_DATA, res)
            conv->Release();
            return false;
        };
    }else { //encode as ITInt8
        ITInt8 itint8_value;
        if(!conv->ConvertTo(itint8_value)){
            conv->Release();
            ENCODE_ERROR("cannot convert to ITInt8", res)
            return false;
        }
//        LOG_INFM_DEBUG("ITInt8 value: %s\n\r", itint8_value)
        if(!encode_number_tuple(res, (ITString)itint8_value)){
            ENCODE_ERROR(BAD_INTEGER_DATA, res)
            conv->Release();
            return false;
        }
    }

    conv->Release();
    return true;
}

bool InformixOperation::encode_as_float(ei_x_buff * const res, ITValue* col){
//    LOG_INFM_DEBUG("encode_as_float: %s\n\r", "")
    ITConversions *conv;
    if (col->QueryInterface(ITConversionsIID, (void **) & conv)
            != IT_QUERYINTERFACE_SUCCESS) {
        ENCODE_ERROR(BAD_CONVERTER, res)
        return false;
    }
    int size = col->TypeOf().Size();

    if(size == sizeof(float)){
        float cfloat_value;
        if (!conv->ConvertTo(cfloat_value)) {
            ENCODE_ERROR("cannot convert to c float", res)
            return false;
        }
//        LOG_INFM_DEBUG("float value: %f\n\r", cfloat_value)
        if(ei_x_encode_double(res, cfloat_value) != 0) {
            ENCODE_ERROR(BAD_FLOAT_DATA, res)
            conv->Release();
            return false;
        }
    }else if (size == sizeof(double)){
        double cdouble_value;
        if (!conv->ConvertTo(cdouble_value)) {
            ENCODE_ERROR("cannot convert to c double", res)
            return false;
        }
//        LOG_INFM_DEBUG("float value: %f\n\r", cdouble_value)
        if(ei_x_encode_double(res, cdouble_value) != 0) {
            ENCODE_ERROR(BAD_FLOAT_DATA, res)
            conv->Release();
            return false;
        }
    }else {
        const char* decimal_str = col->Printable();
        if (strcmp(col->TypeOf().Name(), "money") == 0) {
//            LOG_INFM_DEBUG("money value: %s\n\r", decimal_str)
            if((ei_x_encode_tuple_header(res, 2) != 0) ||
                    (ei_x_encode_string(res, "money") != 0) ||
                    (ei_x_encode_string(res, decimal_str) != 0)) {
                ENCODE_ERROR(BAD_FLOAT_DATA, res)
                conv->Release();
                decimal_str = NULL;
                return false;
            }
        }else{
//            LOG_INFM_DEBUG("decimal value: %s\n\r", decimal_str)
            if(!encode_number_tuple(res, decimal_str)){
                ENCODE_ERROR(BAD_FLOAT_DATA, res)
                conv->Release();
                decimal_str = NULL;
                return false;
            }
        }// if (strcmp(col->TypeOf().Name(), "money") == 0)
        decimal_str = NULL;
    }//size == sizeof(float)

    conv->Release();
    return true;
}

bool InformixOperation::encode_as_datetime(ei_x_buff * const res, ITValue* col){
    ITDateTime *datetime;
    if (col->QueryInterface(ITDateTimeIID, (void **) & datetime)
            != IT_QUERYINTERFACE_SUCCESS) {
        ENCODE_ERROR("cannot get an interface of ITDateTime", res)
        return false;
    }
    if(!encode_datetime_tuple(res, datetime)){
        ENCODE_ERROR("cannot encode as datetime", res)
        datetime->Release();
        return false;
    }
    datetime->Release();
    return true;
}

bool InformixOperation::encode_as_date(ei_x_buff * const res, ITValue* col){
    ITDateTime *datetime;
    if (col->QueryInterface(ITDateTimeIID, (void **) & datetime)
            != IT_QUERYINTERFACE_SUCCESS) {
        ENCODE_ERROR("cannot get an interface of ITDateTime", res)
        return false;
    }
    if(!encode_date_tuple(res, datetime)){
        ENCODE_ERROR("cannot encode as datetime", res)
        datetime->Release();
        return false;
    }
    datetime->Release();
    return true;
}

bool InformixOperation::encode_number_tuple(ei_x_buff * const res, const char* number_str){
    return (ei_x_encode_tuple_header(res, 2) == 0) &&
            (ei_x_encode_string(res, "number") == 0) &&
            (ei_x_encode_string(res, number_str) == 0);
}

bool InformixOperation::encode_datetime_tuple(ei_x_buff * const res, ITDateTime* datetime) {
    //format: {datetime, {{Year,Month,Day}, {Hour, Minute, Second}}}

    if((ei_x_encode_tuple_header(res, 2) != 0) ||
            (ei_x_encode_atom(res, "datetime") != 0) ||
            (ei_x_encode_tuple_header(res, 2) != 0) ||
            (ei_x_encode_tuple_header(res, 3) != 0) ||
            (ei_x_encode_long(res, datetime->Year()) != 0) ||
            (ei_x_encode_long(res, datetime->Month()) != 0) ||
            (ei_x_encode_long(res, datetime->Day()) != 0) ||
            (ei_x_encode_tuple_header(res, 3) != 0) ||
            (ei_x_encode_long(res, datetime->Hour()) != 0) ||
            (ei_x_encode_long(res, datetime->Minute()) != 0)){
        return false;
    }
    
    stringstream ss;
    ss << datetime->Second();
    string second_str = ss.str();
    string::size_type pos = second_str.find('.');
//    long second, fraction;
    if((pos = second_str.find('.')) == string::npos){
        long second = atol(second_str.c_str());
        return ei_x_encode_long(res, second) == 0;
    }else {
        return (ei_x_encode_tuple_header(res, 2) == 0) &&
                (ei_x_encode_string(res, "number") == 0) &&
                (ei_x_encode_string(res, second_str.c_str()) == 0) ;
    }
}

bool InformixOperation::encode_date_tuple(ei_x_buff * const res, ITDateTime* datetime) {
    return (ei_x_encode_tuple_header(res, 2) == 0) &&
            (ei_x_encode_atom(res, "date") == 0) &&
            (ei_x_encode_tuple_header(res, 3) == 0) &&
            (ei_x_encode_long(res, datetime->Year()) == 0) &&
            (ei_x_encode_long(res, datetime->Month()) == 0) &&
            (ei_x_encode_long(res, datetime->Day()) == 0);
}

inline bool InformixOperation::decode_and_append_value(stringstream& stream, INFLOParam*& params) {
    int type = erl_data_type();
//        cout << "erl type: " << (char)type << endl;

    if (type == ERL_SMALL_TUPLE_EXT) {
        return decode_and_append_custom(stream, params);
    } else {
        return decode_and_append_normal(stream, type, params);
    }
}

inline bool InformixOperation::decode_and_append_custom(stringstream& stream, INFLOParam*& params) {
    if(decode_tuple_length() != 2) {
        return false;
    }

    int erl_type = erl_data_type();
    bool flag = true;
    if(erl_type == ERL_STRING_EXT){
        char *type;
        if(!decode_string(type)){
            return false;
        }
//        LOG_INFM_DEBUG("type value: %s\n\r", type)

        if(strcmp(type, "number") == 0 ||
                strcmp(type, "money") == 0){
            flag = decode_and_append_numstr(stream);
        } else if(strcmp(type, "text") == 0){
            flag = decode_and_append_lob(stream, INF_TEXT, params);
        } else if(strcmp(type, "clob") == 0){
            flag = decode_and_append_lob(stream, INF_CLOB, params);
        } else if(strcmp(type, "blob") == 0){
            flag = decode_and_append_lob(stream, INF_BLOB, params);
        } else {
            flag = false;
        }
        free_string(type);
    }else {
        long type;
        if(!decode_integer(type)){
            return false;
        }
//        LOG_INFM_DEBUG("type value: %d\n\r", type)

        switch(type){
            case DB_DRV_SQL_DATE:
                flag = decode_and_append_date(stream);
                break;
            case DB_DRV_SQL_DATETIME:
                flag = decode_and_append_datetime(stream);
                break;
            default:
                flag = false;
        }
    }

    return flag;
}

inline bool InformixOperation::decode_and_append_normal(stringstream& stream, int type, INFLOParam*& params) {
    bool flag = true;
    switch (type) {
        case ERL_ATOM_EXT:
            if (flag = decode_null()) {
                stream << "NULL";
            }
            break;
        case ERL_NIL_EXT:
            if (flag = (decode_list_length() == 0)) {
                stream << "''";
            }
            break;
        case ERL_LIST_EXT:
        case ERL_STRING_EXT:
            flag = decode_and_append_string(stream);
            break;
//        case ERL_LIST_EXT:
//            result = decode_and_append_list(stream);
//            break;
        case ERL_BINARY_EXT:
            flag = decode_and_append_lob(stream, INF_BYTE, params);
            break;
        case ERL_SMALL_INTEGER_EXT:
        case ERL_INTEGER_EXT:
        case ERL_SMALL_BIG_EXT:
        case ERL_LARGE_BIG_EXT:
            long long int_val;
            if(!decode_integer(int_val)) {
                flag = false;
                break;
            }
            stream << int_val;
            break;
        case ERL_FLOAT_EXT:
            double double_val;
            if(!decode_double(double_val)){
                flag = false;
                break;
            }
            stream << double_val;
            break;
        default:
            flag = false;
    }

    return flag;
}

inline bool InformixOperation::decode_and_append_lob(stringstream& stream, LO_Type type, INFLOParam*& params){
    stream << "?";
    if(!add_param(params, type)) {
        return false;
    }
//    LOG_INFM_DEBUG("position index_: %d\n\r", index_)
    return ei_skip_term(buf_, &index_) == 0;
}

inline bool InformixOperation::decode_and_append_string(stringstream& stream){
    char* tmp;
    char* value;
    int i = 0, j = 0;

    if (!decode_string(tmp)) {
        return false;
    }

    value = (char*) malloc(strlen(tmp) * 2 + 1);
    if (value == NULL) {
        return false;
    }

    while (tmp[i] != 0) {
        value[j] = tmp[i];
        if (tmp[i] == '\'') {
            j++;
            value[j] = tmp[i];
        }
        i++;
        j++;
    }// end of 'while'
    value[j] = 0;
    stream << "'" << value << "'";

    free_string(tmp);
    free(value);

    return true;
}

inline bool InformixOperation::decode_and_append_date(stringstream& stream){
    //{date, {year, month, day}}
    INFDateTime datetime;

    long year, month, day;
    if(decode_tuple_length() != 3 ||
            !decode_integer(year) ||
            !decode_integer(month) ||
            !decode_integer(day)) {
        return false;
    }
    datetime.data[0] = year;
    datetime.data[1] = month;
    datetime.data[2] = day;
    datetime.qualifier[0] = (int)INF_Year;
    datetime.qualifier[1] = (int)INF_Day;

    stream << "'";
    append_inf_date(stream, datetime);
    stream << "'";
    return true;
}

inline bool InformixOperation::decode_and_append_datetime(stringstream& stream){
    INFDateTime datetime;
    if(!decode_inf_datetime(&datetime)){
        return false;
    }
//    LOG_INFM_DEBUG("year value: %d\n\r", datetime.data[0])

    stream << "'";
    append_inf_datetime(stream, datetime);
    stream << "'";
    return true;
}

inline bool InformixOperation::decode_and_append_numstr(stringstream& stream){
    char *numstr;
    if(!decode_string(numstr)){
        return false;
    }

    stream << " " << numstr;
    free_string(numstr);
    return true;
}

inline bool InformixOperation::decode_datetime_second(string &field, int &first, int &last){
//    LOG_INFM_DEBUG("decode_datetime_second: %s\n\r", "ok")
    DateTime_Index dt_index = INF_Second;
    int type = erl_data_type();
    if (type == ERL_ATOM_EXT) {
//        LOG_INFM_DEBUG("type is ERL_ATOM_EXT: %s\n\r", "ok")
        if (first > -1 && last == -1) {
            last = dt_index - 1;
        }
        char *nil;
        if (!decode_string(nil)) {
            return false;
        }
        bool flag = (strcmp(nil, "undefined") != 0);
        free_string(nil);
        if (flag) {
            return false;
        }
    } else {
        if (last > -1) {
            return false;
        }
        if (first == -1) {
            first = dt_index;
        }
        last = INF_Second;
        if (type == ERL_SMALL_TUPLE_EXT || type == ERL_LARGE_TUPLE_EXT){
//            LOG_INFM_DEBUG("type is TUPLE: %s\n\r", "ok")
            char *key_word;
            if(decode_tuple_length() != 2 || !decode_string(key_word)){
                return false;
            }
            bool flag = (strcmp(key_word, "number") != 0);
            free_string(key_word);
            if(flag){
                return false;
            }

            char *second_value;
            if(!decode_string(second_value)){
                return false;
            }
            field = second_value;
            free_string(second_value);
        } else {
//            LOG_INFM_DEBUG("second is long: %s\n\r", "ok")
            long second_value;
            if(!decode_integer(second_value)){
                return false;
            }
//            LOG_INFM_DEBUG("second_value is long: %d\n\r", second_value)
            stringstream ss;
            ss << second_value;
            field = ss.str();
        }
    }
    return true;
}
inline bool InformixOperation::decode_datetime_field(long &field,
        DateTime_Index dt_index, int &first, int &last){
    int tmp_index = index_;
    if(decode_integer(field)){
//        LOG_INFM_DEBUG("field: %d\n\r", field)
        if(last > -1){
            return false;
        }
        if(first == -1){
            first = dt_index;
        }
    }else{
        if(first > -1 && last == -1){
            last = dt_index - 1;
        }
        index_ = tmp_index;
        char *nil;
        if(!decode_string(nil, &index_)){
            return false;
        }
        bool flag = (strcmp(nil, "undefined") != 0);
        free_string(nil);
        if(flag){
            return false;
        }
    }
    
//    LOG_INFM_DEBUG("first: %d\n\r", first)
//    LOG_INFM_DEBUG("last: %d\n\r", last)
    return true;
}

inline bool InformixOperation::decode_inf_datetime(INFDateTime *datetime){
//    LOG_INFM_DEBUG("decode_inf_datetime: %s\n\r", "ok")
    //{{year,month,day}, {hour, minute, second, fraction}, {qualifier first, qualifier last}}
    if(decode_tuple_length() != 2) {
        return false;
    }

    long year, month, day, hour, minute;
    year = month = day = hour = minute = 0;

    int first, last;
    first = last = -1;

    if(decode_tuple_length() != 3 ||
            !decode_datetime_field(year, INF_Year, first, last) ||
            !decode_datetime_field(month, INF_Month, first, last) ||
            !decode_datetime_field(day, INF_Day, first, last) ){
        return false;
    }

    datetime->data[0] = year;
    datetime->data[1] = month;
    datetime->data[2] = day;
//    LOG_INFM_DEBUG("decode date: %s\n\r", "ok")


    if(decode_tuple_length() != 3 ||
            !decode_datetime_field(hour, INF_Hour, first, last) ||
            !decode_datetime_field(minute, INF_Minute, first, last) ||
            !decode_datetime_second(datetime->second_data, first, last)) {
        return false;
    }
    datetime->data[3] = hour;
    datetime->data[4] = minute;
//    LOG_INFM_DEBUG("decode time: %s\n\r", "ok")

    datetime->qualifier[0] = first;
    datetime->qualifier[1] = last;

    return true;
}

//inline bool InformixOperation::get_inf_datetime_index(char *qualifier_str, int& index){
//    DateTime_Index dt_index;
//    if(strcmp(qualifier_str, "year") == 0){
//        dt_index = INF_Year;
//    }else if(strcmp(qualifier_str, "month") == 0){
//        dt_index = INF_Month;
//    }else if(strcmp(qualifier_str, "day") == 0){
//        dt_index = INF_Day;
//    }else if(strcmp(qualifier_str, "hour") == 0){
//        dt_index = INF_Hour;
//    }else if(strcmp(qualifier_str, "minute") == 0){
//        dt_index = INF_Minute;
//    }else if(strcmp(qualifier_str, "second") == 0){
//        dt_index = INF_Second;
//    }else if(strcmp(qualifier_str, "fraction") == 0){
//        dt_index = INF_Fraction;
//    }
//
//    index = (int)dt_index;
//    return true;
//}

inline void InformixOperation::append_inf_datetime(stringstream& stream, INFDateTime datetime){
    for(int i = datetime.qualifier[0]; i <= datetime.qualifier[1]; i++){
        if(i < 5){
            stream << datetime.data[i];
            if (i == datetime.qualifier[1]) {
                break;
            }
            if (i < 2) {
                stream << "-";
            } else if (i == 2) {
                stream << " ";
            } else {
                stream << ":";
            }
        } else {
            stream << datetime.second_data;
        }

    }
}

inline void InformixOperation::append_inf_date(stringstream& stream, INFDateTime datetime){
    stream << datetime.data[1] << "/" << datetime.data[2] << "/"
           << datetime.data[0];
}

inline bool InformixOperation::decode_and_append_where(stringstream& stream, INFLOParam*& params){
//    LOG_INFM_DEBUG("decode_and_append_where: %s\n\r", "")
    if(decode_list_length() >0){
        stream << " WHERE ";
        return gen_expr(stream, params) && (decode_list_length() == 0);
    }

    return ei_skip_term(buf_, &index_) == 0;
}

inline bool InformixOperation::gen_expr(stringstream& stream, INFLOParam*& params) {
    int type = erl_data_type();
    long expr_key;
    int index_tmp;
    bool result;
//    LOG_INFM_DEBUG("gen_expr: %s\n\r", "")
    if (type == ERL_SMALL_TUPLE_EXT || type == ERL_LARGE_TUPLE_EXT) {
        index_tmp = index_;

        if (decode_tuple_length() <= 0) {
            return false;
        }
        if (decode_integer(expr_key)) {
            result = decode_expr(stream, expr_key, params);
        } else {
            index_ = index_tmp;
            result = decode_and_append_custom(stream, params);
        }
    } else if (type == ERL_ATOM_EXT) {
        char* value;

        index_tmp = index_;
        if (result = decode_null()) {
            stream << "NULL";
        } else {
            index_ = index_tmp;
            if (result = decode_string(value)) {
                stream << value;
                free_string(value);
            }
        }
        return result;
    } else {
        result = decode_and_append_normal(stream, type, params);
    }

    return result;
}

inline bool InformixOperation::decode_expr(stringstream& stream, long expr_key, INFLOParam*& params) {
    bool result;

    switch (expr_key) {
        case DB_DRV_SQL_AND:
            result = decode_and_expr(stream, params);
            break;
        case DB_DRV_SQL_OR:
            result = decode_or_expr(stream, params);
            break;
        case DB_DRV_SQL_NOT:
            result = decode_not_expr(stream, params);
            break;

        case DB_DRV_SQL_LIKE:
            result = decode_binary_operator_expr(stream, "like\0", params);
            break;
        case DB_DRV_SQL_AS:
            result = decode_binary_operator_expr(stream, "as\0", params);
            break;
        case DB_DRV_SQL_EQUAL:
            result = decode_binary_operator_expr(stream, "=\0", params);
            break;
        case DB_DRV_SQL_NOT_EQUAL:
            result = decode_binary_operator_expr(stream, "!=\0", params);
            break;
        case DB_DRV_SQL_GREATER:
            result = decode_binary_operator_expr(stream, ">\0", params);
            break;
        case DB_DRV_SQL_GREATER_EQUAL:
            result = decode_binary_operator_expr(stream, ">=\0", params);
            break;
        case DB_DRV_SQL_LESS:
            result = decode_binary_operator_expr(stream, "<\0", params);
            break;
        case DB_DRV_SQL_LESS_EQUAL:
            result = decode_binary_operator_expr(stream, "<=\0", params);
            break;
        case DB_DRV_SQL_DOT:
            if (!gen_expr(stream, params)) {
                return false;
            }
            stream << ".";
            result = gen_expr(stream, params);
            break;
        case DB_DRV_SQL_ADD:
            stream << " (";
            result = decode_binary_operator_expr(stream, "+\0", params);
            stream << ") ";
            break;
        case DB_DRV_SQL_SUB:
            stream << " (";
            result = decode_binary_operator_expr(stream, "-\0", params);
            stream << ") ";
            break;
        case DB_DRV_SQL_MUL:
            stream << " (";
            result = decode_binary_operator_expr(stream, "*\0", params);
            stream << ") ";
            break;
        case DB_DRV_SQL_DIV:
            stream << " (";
            result = decode_binary_operator_expr(stream, "/\0", params);
            stream << ") ";
            break;

        case DB_DRV_SQL_JOIN:
            result = decode_join_expr(stream, "JOIN\0", params);
            break;
        case DB_DRV_SQL_LEFT_JOIN:
            result = decode_join_expr(stream, "LEFT JOIN\0", params);
            break;
        case DB_DRV_SQL_RIGHT_JOIN:
            result = decode_join_expr(stream, "RIGHT JOIN\0", params);
            break;
        case DB_DRV_SQL_INNER_JOIN:
            result = decode_join_expr(stream, "INNER JOIN\0", params);
            break;

        case DB_DRV_SQL_ORDER:
            result = decode_order_expr(stream, params);
            break;

        case DB_DRV_SQL_GROUP:
            result = decode_group_expr(stream, params);
            break;

        case DB_DRV_SQL_HAVING:
            result = decode_having_expr(stream, params);
            break;

        case DB_DRV_SQL_BETWEEN:
            result = decode_between_expr(stream, params);
            break;

        case DB_DRV_SQL_FUN:
            result = decode_function_expr(stream, params);
            break;

        case DB_DRV_SQL_IN:
            result = decode_in_expr(stream, params);
            break;

        case DB_DRV_SQL_IS_NULL:
            result = decode_null_expr(stream, true, params);
            break;
        case DB_DRV_SQL_IS_NOT_NULL:
            result = decode_null_expr(stream, false, params);
            break;

        case DB_DRV_SQL_DATETIME:
            result = decode_and_append_datetime(stream);
            break;
        case DB_DRV_SQL_DATE:
            result = decode_and_append_date(stream);
            break;
        case DB_DRV_SQL_LIMIT:
        default:
            result = false;
    }

    return result;
}

inline bool InformixOperation::decode_null_expr(stringstream& stream, bool is_null, INFLOParam*& params) {
    if (!gen_expr(stream, params)) {
        return false;
    }
    if (is_null) {
        stream << " IS NULL";
    } else {
        stream << " IS NOT NULL";
    }

    return true;
}

inline bool InformixOperation::decode_function_expr(stringstream& stream, INFLOParam*& params) {
    int type;
    bool result;

    if (!gen_expr(stream, params)) {
        return false;
    }

    type = erl_data_type();
    stream << "(";
    if(type == ERL_NIL_EXT){
        result = (decode_list_length() == 0);
    }else if (type == ERL_LIST_EXT) {
        //若为列表，则解析参数列表
        result = decode_term_params(stream, params);
    } else {
        //元素全为整型且小于256的列表会被当做字符串类型
        //若为字符串，则解析整型参数列表
        result = decode_integer_params(stream);
    }
    stream << ") ";

    return result;
}

inline bool InformixOperation::decode_between_expr(stringstream& stream, INFLOParam*& params) {
    if (!gen_expr(stream, params)) {
        return false;
    }
    stream << " BETWEEN ";
    if (!gen_expr(stream, params)) {
        return false;
    }
    stream << " AND ";
    return gen_expr(stream, params);
}

inline bool InformixOperation::decode_having_expr(stringstream& stream, INFLOParam*& params) {
    stream << " HAVING ";
    return gen_expr(stream, params);
}

inline bool InformixOperation::decode_group_expr(stringstream& stream, INFLOParam*& params) {
    int size;
    stream << " GROUP BY ";

    if ((size = decode_list_length()) <= 0) {
        return false;
    }
    for (int i = 0; i < size; i++) {
        if (!gen_expr(stream, params)) {
            return false;
        }
        if(i < size -1){
            stream << ", ";
        }
    }

    return decode_list_length() == 0;
}

inline bool InformixOperation::decode_order_expr(stringstream& stream, INFLOParam*& params) {
    int size;
    long sort_flag;

    stream << " ORDER BY ";
    if ((size = decode_list_length()) <= 0) {
        return false;
    }
    for (int i = 0; i < size; i++) {
        if (decode_tuple_length() != 2 || !gen_expr(stream, params)
                || !decode_integer(sort_flag)) {
            return false;
        }

        stream << (sort_flag == 1 ? " DESC" : " ASC");
        if(i < size -1){
            stream << ", ";
        }
    }

    return decode_list_length() == 0;
}

inline bool InformixOperation::decode_join_expr(stringstream& stream, char* join_type, INFLOParam*& params) {
    if (!gen_expr(stream, params)) {
        return false;
    }
    stream << " " << join_type << " ";
    if (!gen_expr(stream, params)) {
        return false;
    }
    stream << " ON ";

    return gen_expr(stream, params);
}

inline bool InformixOperation::decode_binary_operator_expr(stringstream& stream, char* oprt, INFLOParam*& params) {
    if (!gen_expr(stream, params)) {
        return false;
    }
    stream << " " << oprt << " ";
    return gen_expr(stream, params);
}

inline bool InformixOperation::decode_not_expr(stringstream& stream, INFLOParam*& params) {
    stream << "NOT (";
    if (!gen_expr(stream, params)) {
        return false;
    }
    stream << ") ";

    return true;
}

inline bool InformixOperation::decode_or_expr(stringstream& stream, INFLOParam*& params) {
    stream << " (";
    if (!gen_expr(stream, params)) {
        return false;
    }
    stream << ") OR (";
    if (!gen_expr(stream, params)) {
        return false;
    }
    stream << ") ";
    return true;
}

inline bool InformixOperation::decode_and_expr(stringstream& stream, INFLOParam*& params) {
    int size;

    if ((size = decode_list_length()) <= 0) {
        return false;
    }

    for (int i = 0; i < size; i++) {
        stream << " (";
        if (!gen_expr(stream, params)) {
            return false;
        }
        stream << ") ";
        if (i < size - 1) {
            stream << "AND";
        }
    }

    return decode_list_length() == 0;
}

inline bool InformixOperation::decode_in_expr(stringstream& stream, INFLOParam*& params) {
    if (!gen_expr(stream, params)) {
        return false;
    }

    int type = erl_data_type();
    bool result = true;
    
    stream << " IN (";
    if (type == ERL_LIST_EXT) {
        result = decode_term_params(stream, params);
    } else {
        result = decode_integer_params(stream);
    }
    stream << ") ";

    return result;
}


inline bool InformixOperation::decode_term_params(stringstream& stream, INFLOParam*& params) {
    int size;
    if ((size = decode_list_length()) <= 0) {
        return false;
    }
    for (int i = 0; i < size; i++) {
        if (!gen_expr(stream, params)) {
            return false;
        }
        if(i < size -1){
            stream << ", ";
        }
    }

    return decode_list_length() == 0;
}

inline bool InformixOperation::decode_integer_params(stringstream& stream) {
    int size; //参数列表长度
    char* params; //参数列表

    //元素全为整型且小于256的列表会被当做字符串类型
    if (!decode_string(params)) {
        return false;
    }
    size = strlen(params);
    //拼接参数
    for (int i = 0; i < size; i++) {
        stream << (int) params[i];
        if(i < size -1){
            stream << ", ";
        }
    }
    free_string(params);

    return true;
}

inline bool InformixOperation::decode_and_bind_lob(ITValue *param, LO_Type type, int* p_index) {
    if(p_index == NULL){
        p_index = &index_;
    }
//    LOG_INFM_DEBUG("type: %d\n\r", type)
    bool flag = true;
    switch(type){
        case INF_BYTE:
            flag = decode_and_bind_byte(param, p_index);
            break;
        case INF_TEXT:
            flag = decode_and_bind_text(param, p_index);
            break;
        case INF_CLOB:
            flag = decode_and_bind_clob(param, p_index);
            break;
        case INF_BLOB:
            flag = decode_and_bind_blob(param, p_index);
            break;
        default:
            err_msg_ = BAD_ARG;
            flag = false;
    }
    return flag;
}

inline bool InformixOperation::decode_and_bind_param(ITValue *param) {
    bool flag = true;
    int type = erl_data_type();
    
    switch (type) {
        case ERL_ATOM_EXT:
            if (flag = decode_null()) {
                param->SetNull();
            }
            break;
        case ERL_NIL_EXT:
            if (flag = (decode_list_length() == 0)) {
                ITConversions *conv = NULL;
                if (param->QueryInterface(ITConversionsIID, (void **) & conv)
                        != IT_QUERYINTERFACE_SUCCESS) {
                    err_msg_ = BAD_CONVERTER;
                    return false;
                }
                if (!conv->ConvertFrom("''")) {
                    err_msg_ = "cannot convert from string";
                    return false;
                }
            }
            break;
        case ERL_LIST_EXT:
        case ERL_STRING_EXT:
            flag = decode_and_bind_string(param);
            break;
        case ERL_BINARY_EXT:
            flag = decode_and_bind_lob(param, INF_BYTE);
            break;
        case ERL_SMALL_INTEGER_EXT:
        case ERL_INTEGER_EXT:
        case ERL_SMALL_BIG_EXT:
        case ERL_LARGE_BIG_EXT:
            flag = decode_and_bind_integer(param);
            break;
        case ERL_FLOAT_EXT:
            flag = decode_and_bind_float(param);
            break;
        case ERL_SMALL_TUPLE_EXT:
            flag = decode_and_bind_custom(param);
            break;
        default:
            flag = false;
    }

    return flag;
}

inline bool InformixOperation::decode_and_bind_blob(ITValue *param, int* p_index) {
    if (p_index == NULL) {
        p_index = &index_;
    }

    char* blob;
    int length;
    if((length = decode_binary(blob, p_index)) == -1){
        err_msg_ = BAD_ARG;
        return false;
    }
    
    ITConnection *conn = (ITConnection*)((InformixConnection*)conn_)->get_connection();
    ITLargeObjectManager lobMgr(*conn);
    if(!lobMgr.CreateLO()){
        err_msg_ = "could not create a large object";
        lobMgr.Close();
        free_binary(blob);
        return false;
    }
    lobMgr.Write(blob, length);
    free_binary(blob);

    bool flag = true;
    if(!param->FromPrintable(lobMgr.HandleText())){
        err_msg_ = "could not bind the parameter with blob";
        flag = false;
    }

    lobMgr.Close();
    return flag;
}

inline bool InformixOperation::decode_and_bind_clob(ITValue *param, int* p_index) {
    if (p_index == NULL) {
        p_index = &index_;
    }

    char* clob;
    if(!decode_string(clob, p_index)){
        err_msg_ = BAD_ARG;
        return false;
    }
    ITConnection *conn = (ITConnection*)((InformixConnection*)conn_)->get_connection();
    ITLargeObjectManager lobMgr(*conn);
    if(!lobMgr.CreateLO()){
        err_msg_ = "could not create a large object";
        lobMgr.Close();
        free_string(clob);
        return false;
    }
    lobMgr.Write(clob, strlen(clob));
    free_string(clob);

    bool flag = true;
    if(!param->FromPrintable(lobMgr.HandleText())){
        err_msg_ = "could not bind the parameter with clob";
        flag = false;
    }
    
    lobMgr.Close();
    return flag;
}

inline bool InformixOperation::decode_and_bind_text(ITValue *param, int* p_index){
    if(p_index == NULL){
        p_index = &index_;
    }
//    LOG_INFM_DEBUG("p_index: %d\n\r", *p_index)
    char* text;
    if(!decode_string(text, p_index)){
        err_msg_ = BAD_ARG;
        return false;
    }
    bool flag = true;
    if(!param->FromPrintable(text)){
        err_msg_ = "could not bind the parameter with text";
        flag = false;
    }

    free_string(text);
    return flag;
}

inline bool InformixOperation::decode_and_bind_byte(ITValue *param, int* p_index){
    if(p_index == NULL){
        p_index = &index_;
    }
    char* bin;
    int length;
    if((length = decode_binary(bin, p_index)) == -1){
        err_msg_ = BAD_ARG;
        return false;
    }
    ITDatum *pdatum;
    if (param->QueryInterface( ITDatumIID, (void **)&pdatum )
            != IT_QUERYINTERFACE_SUCCESS){
        err_msg_ = BAD_DATUM;
        free_binary(bin);
        return false;
    }

    bool flag = true;
    if(!pdatum->SetData((void *)bin, length)){
        err_msg_ = "could not bind the parameter with byte";
        flag = false;
    }
    
    free_binary(bin);
    pdatum->Release();
    return flag;
}

inline bool InformixOperation::decode_and_bind_string(ITValue *param) {
    char * string_value;
    if (!decode_string(string_value)) {
        err_msg_ = BAD_ARG;
        return false;
    }

    ITConversions *conv = NULL;
    if (param->QueryInterface(ITConversionsIID, (void **) & conv)
            != IT_QUERYINTERFACE_SUCCESS) {
        free_string(string_value);
        err_msg_ = BAD_CONVERTER;
        return false;
    }
    if (!conv->ConvertFrom(string_value)) {
        free_string(string_value);
        err_msg_ = "cannot convert from string";
        return false;
    }

    free_string(string_value);
    return true;
}

inline bool InformixOperation::decode_and_bind_integer(ITValue *param){
    ITConversions *conv = NULL;
    if (param->QueryInterface(ITConversionsIID, (void **) & conv)
            != IT_QUERYINTERFACE_SUCCESS) {
        err_msg_ = BAD_CONVERTER;
        return false;
    }

    long int_value;
    if (!decode_integer(int_value)) {
        err_msg_ = BAD_ARG;
        return false;
    }
    if (!conv->ConvertFrom(int_value)) {
        err_msg_ = "cannot convert from integer";
        return false;
    }

    return true;
}

inline bool InformixOperation::decode_and_bind_float(ITValue *param) {
    ITConversions *conv = NULL;
    if (param->QueryInterface(ITConversionsIID, (void **) & conv)
            != IT_QUERYINTERFACE_SUCCESS) {
        err_msg_ = BAD_CONVERTER;
        return false;
    }
    
    double float_value;
    if (!decode_double(float_value)) {
        err_msg_ = BAD_ARG;
        return false;
    }
    if (!conv->ConvertFrom(float_value)) {
        err_msg_ = "cannot convert from floating-point";
        return false;
    }

    return true;
}

inline bool InformixOperation::decode_and_bind_custom(ITValue *param){
    if(decode_tuple_length() != 2) {
        err_msg_ = BAD_ARG;
        return false;
    }

    bool flag = true;
    char *type;
    if (!decode_string(type)) {
        err_msg_ = BAD_ARG;
        return false;
    }
//        LOG_INFM_DEBUG("type value: %s\n\r", type)

    if (strcmp(type, "number") == 0 ||
            strcmp(type, "money") == 0) {
        flag = decode_and_bind_numstr(param);
    } else if (strcmp(type, "date") == 0) {
        flag = decode_and_bind_date(param);
    } else if (strcmp(type, "datetime") == 0) {
        flag = decode_and_bind_datetime(param);
    } else if (strcmp(type, "text") == 0) {
        flag = decode_and_bind_lob(param, INF_TEXT);
    } else if (strcmp(type, "clob") == 0) {
        flag = decode_and_bind_lob(param, INF_CLOB);
    } else if (strcmp(type, "blob") == 0) {
        flag = decode_and_bind_lob(param, INF_BLOB);
    } else {
        err_msg_ = BAD_ARG;
        flag = false;
    }
    free_string(type);

    return flag;
}

inline bool InformixOperation::decode_and_bind_numstr(ITValue *param){
    ITConversions *conv = NULL;
    if (param->QueryInterface(ITConversionsIID, (void **) & conv)
            != IT_QUERYINTERFACE_SUCCESS) {
        err_msg_ = BAD_CONVERTER;
        return false;
    }

    char *number_value;
    if (!decode_string(number_value)) {
        err_msg_ = BAD_ARG;
        return false;
    }
    if (!conv->ConvertFrom(number_value)) {
        free_string(number_value);
        err_msg_ = "cannot convert from floating-point";
        return false;
    }

    free_string(number_value);
    return true;
}

inline bool InformixOperation::decode_and_bind_date(ITValue *param) {
    long year, month, day;
    if (decode_tuple_length() != 3 ||
            !decode_integer(year) ||
            !decode_integer(month) ||
            !decode_integer(day)) {
        err_msg_ = BAD_ARG;
        return false;
    }
    
    if (param->TypeOf().Name().Equal("date")){
        ITDateTime *datetime;
        if (param->QueryInterface(ITDateTimeIID, (void **) & datetime)
                != IT_QUERYINTERFACE_SUCCESS) {
            err_msg_ = "cannot get an interface of ITDateTime";
            return false;
        }
        if (!datetime->FromDate(year, month, day)) {
            err_msg_ = "cannot convert from datetime";
            return false;
        }
    }else {
        ITConversions *conv = NULL;
        if (param->QueryInterface(ITConversionsIID, (void **) & conv)
                != IT_QUERYINTERFACE_SUCCESS) {
            err_msg_ = BAD_CONVERTER;
            return false;
        }

        stringstream stream;
        stream << month << "/" << day << "/" << year;
        if (!conv->ConvertFrom(stream.str().c_str())) {
            err_msg_ = "cannot convert from date";
            return false;
        }
    }

    return true;
}

inline bool InformixOperation::decode_and_bind_datetime(ITValue *param) {
    INFDateTime datetime;
    if(!decode_inf_datetime(&datetime)){
        err_msg_ = BAD_ARG;
        return false;
    }

    if (param->TypeOf().Name().Equal("datetime") ||
            param->TypeOf().Name().Equal("interval")){
        ITDateTime *itDateTime;
        if (param->QueryInterface(ITDateTimeIID, (void **) & itDateTime)
                != IT_QUERYINTERFACE_SUCCESS) {
            err_msg_ = "cannot get an interface of ITDateTime";
            return false;
        }
        double second_value = atof(datetime.second_data.c_str());
        if (!itDateTime->FromDate(datetime.data[0], datetime.data[1], datetime.data[2]) ||
                !itDateTime->FromTime(datetime.data[3], datetime.data[4], second_value)) {
            err_msg_ = "cannot convert from datetime";
            return false;
        }
    }else{
        ITConversions *conv = NULL;
        if (param->QueryInterface(ITConversionsIID, (void **) & conv)
                != IT_QUERYINTERFACE_SUCCESS) {
            err_msg_ = BAD_CONVERTER;
            return false;
        }

        stringstream stream;
        append_inf_datetime(stream, datetime);
        if (!conv->ConvertFrom(stream.str().c_str())) {
            err_msg_ = "cannot convert from date";
            return false;
        }
    }

    return true;
}


inline bool InformixOperation::create_param(INFLOParam*& param, LO_Type type, int* p_index) {
    if(p_index == NULL){
        p_index = &index_;
    }
    param = (INFLOParam*)malloc(sizeof(INFLOParam));
    if(param == NULL){
        return false;
    }
    param->index = *p_index;
    param->type = type;
    param->next = NULL;
    
    return true;
}

inline bool InformixOperation::add_param(INFLOParam*& params, LO_Type type, int* p_index){
    if(p_index == NULL){
        p_index = &index_;
    }

    INFLOParam* param = NULL;
    if(!create_param(param, type, p_index)) {
        return false;
    }

    if(params == NULL){
        params = param;
    }else{
        INFLOParam* last = params->last;
        last->next = param;
    }
    params->last = param;

    return true;
}

inline void InformixOperation::free_params(INFLOParam*& params){
    while (params) {
        INFLOParam* next = params->next;
        free(params);
        params = next;
    }
}

