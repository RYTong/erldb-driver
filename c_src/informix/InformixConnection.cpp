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

#include "InformixConnection.h"

using namespace rytong;


InformixConnection::InformixConnection() {
    set_db_type(INFORMIX_DB);
    conn_ = NULL;
}

InformixConnection::~InformixConnection() {
    disconnect();
}

void* InformixConnection::get_connection(){
    return (void*)conn_;
}

bool InformixConnection::disconnect() {
    if (conn_) {
        conn_->Close();
        delete conn_;
        conn_ = NULL;
    }
    return true;
}

bool InformixConnection::connect(const char* host, const char* user,
        const char* password, const char* db_name, unsigned int port){
    LOG_INFM_DEBUG("connect to informix db %s\n\r", "")
    ITDBInfo conn_info;
//    conn_info.SetSystem("ol_informix1170");
//    conn_info.SetDatabase("ewp_development");
//    conn_info.SetUser("root");
//    conn_info.SetPassword("rytong2010");
    conn_info.SetSystem(host);
    conn_info.SetDatabase(db_name);
    conn_info.SetUser(user);
    conn_info.SetPassword(password);

    conn_ = new ITConnection();

    if(!conn_->Open(conn_info) || conn_->Error()){
        const char* err_txt = conn_->ErrorText();
        LOG_INFM_DEBUG("connect error: %s\n\r", err_txt)
        return false;
    }

//    ITDBInfo info = conn_->GetDBInfo();
//    const char* system_info = info.GetSystem();
//    const char* db_info = info.GetDatabase();
//    const char* user_info = info.GetUser();
//    LOG_INFM_DEBUG("system_info: %s\n\r", system_info)
//    LOG_INFM_DEBUG("db_info: %s\n\r", db_info)
//    LOG_INFM_DEBUG("user_info: %s\n\r", user_info)

    return true;
}

bool InformixConnection::set_auto_commit(bool flag){
    //The state is either Begin or Auto
    if(flag && conn_->GetTransactionState() == ITConnection::Begin){
        //set transaction state to Auto
        //cannot set when connection is in a transaction
        return false;
    }

    if(!flag && conn_->GetTransactionState() == ITConnection::Auto){
        //set transaction state to Begin
        conn_->SetTransaction(ITConnection::Begin);
    }

    return true;
}

bool InformixConnection::is_auto_committed() const{
    return conn_->GetTransactionState() == ITConnection::Auto;
}


