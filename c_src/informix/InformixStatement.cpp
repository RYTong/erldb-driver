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

#include "InformixStatement.h"

using namespace rytong;


InformixStatement::InformixStatement(ITConnection *conn){
    this->conn_ = conn;
    this->row_count_ = 0;
    this->stmt_ = NULL;
}

InformixStatement::~InformixStatement() {
    if (this->stmt_ != NULL) {
//        LOG_INFM_DEBUG("free InformixStatement: %s\n\r", "")
//        Drop();
        if (this->type_ == INF_Cursor) {
            delete (ITCursor*) this->stmt_;
        } else {
            delete (ITStatement*) this->stmt_;
        }
        this->stmt_ = NULL;
    }
}

ITBool InformixStatement::Prepare(const ITString& sql){
    bool is_select = is_select_sql(sql.Data());
//    LOG_INFM_DEBUG("is_select: %d\n\r", is_select)
    this->type_ = is_select? INF_Cursor: INF_Stmt;
//    LOG_INFM_DEBUG("type_: %d\n\r", this->type_)
    if (is_select) {
        ITCursor *cursor = new ITCursor(*conn_);
        this->row_count_ = 0;
        this->stmt_ = (void*) cursor;
        return cursor->Prepare(sql);
    } else {
        ITStatement *stmt = new ITStatement(*conn_);
        this->stmt_ = (void*) stmt;
        return stmt->Prepare(sql);
    }
    return true;
}

ITBool InformixStatement::Exec(){
    if(this->stmt_ == NULL)
        return false;

    if(this->type_ == INF_Cursor){
        this->row_count_ = 0;
        return ((ITCursor*)this->stmt_)->Open();
    }else{
        return ((ITStatement*)this->stmt_)->Exec();
    }
}

ITBool InformixStatement::Drop(){
    this->row_count_ = 0;
    if (this->stmt_ != NULL) {
        if(this->type_ == INF_Cursor){
//            LOG_INFM_DEBUG("drop cursor: %s\n\r", "")
//            LOG_INFM_DEBUG("cursor is null: %d\n\r", this->stmt_ == NULL)
            return ((ITCursor*)this->stmt_)->Drop();
        } else {
//            LOG_INFM_DEBUG("drop statement: %s\n\r", "")
            return ((ITStatement*)this->stmt_)->Drop();
        }
    }
    return true;
}

int InformixStatement::NumParams(){
    if(this->stmt_ == NULL)
        return -1;
    if(this->type_ == INF_Cursor){
        return ((ITCursor*)this->stmt_)->NumParams();
    }else{
        return ((ITStatement*)this->stmt_)->NumParams();
    }
}

ITValue* InformixStatement::Param(int index) {
    if (this->stmt_ == NULL)
        return NULL;
    
    if (this->type_ == INF_Cursor) {
        return ((ITCursor*)this->stmt_)->Param(index);
    } else {
        return ((ITStatement*)this->stmt_)->Param(index);
    }
}

ITRow* InformixStatement::NextRow(){
    if (this->stmt_ == NULL)
        return NULL;
//    LOG_INFM_DEBUG("type_: %d\n\r", this->type_)
    if (this->type_ == INF_Cursor) {
        ITRow *row;
//        LOG_INFM_DEBUG("QueryText: %s\n\r", (const char*)((ITCursor*)this->stmt_)->QueryText())
        if((row = ((ITCursor*)this->stmt_)->NextRow()) != NULL){
            this->row_count_ ++;
//            LOG_INFM_DEBUG("row_count_: %d\n\r", this->row_count_)
        }else {
            ((ITCursor*)this->stmt_)->Close();//close cursor when next row is null
        }
        return row;
    } else {
        return NULL;
    }
}

long InformixStatement::RowCount(){
    if (this->stmt_ == NULL)
        return -1;
    
    if (this->type_ == INF_Cursor) {
        return this->row_count_;
    } else {
        return ((ITStatement*)this->stmt_)->RowCount();
    }
}

const ITString& InformixStatement::Command(){
    if (this->stmt_ == NULL)
        return ITString::Null;

    if (this->type_ == INF_Cursor) {
        return ((ITCursor*)this->stmt_)->Command();
    } else {
        return ((ITStatement*)this->stmt_)->Command();
    }
}

ITBool InformixStatement::Error(){
    if (this->stmt_ == NULL)
        return false;
    return ((ITErrorManager*)this->stmt_)->Error();
}

const ITString& InformixStatement::ErrorText(){
    if (this->stmt_ == NULL)
        return ITString::Null;
    return ((ITErrorManager*)this->stmt_)->ErrorText();
}

ITBool InformixStatement::StartTransaction(){
    return this->conn_->SetTransaction(ITConnection::Begin);
}

ITBool InformixStatement::AbortTransaction(){
    return this->conn_->SetTransaction(ITConnection::Abort);
}

ITBool InformixStatement::CommitTransaction(){
    return this->conn_->SetTransaction(ITConnection::Commit);
}

inline bool InformixStatement::is_select_sql(const char* sql){
    const char *trim = sql;
    int pos = 0;
    while(*(trim = sql + pos) == ' '){
        pos++;
    } //trim

    char *tmp = new char[7];
    for(int i = 0; i < 6; i++){
        char c = *(sql + pos + i);
        if(c >= 'a' && c <= 'z'){
            c -= 32; //to upper char
        }else if(c < 'A' || c > 'Z'){
            return false;
        }
        tmp[i] = c;
    }
    tmp[6] = '\0';
    if(strcmp(tmp, "SELECT") == 0)
        return true;
    return false;
}
