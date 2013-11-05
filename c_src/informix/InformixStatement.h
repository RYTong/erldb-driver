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

#ifndef _RYT_INFORMIX_STMT_H
#define _RYT_INFORMIX_STMT_H

#include "it.h"

#include "../util/SysLogger.h"
#include "InformixUtils.h"

using namespace std;

namespace rytong {
enum Stmt_Type{
    INF_Cursor = 0,
    INF_Stmt   = 1
};

class InformixStatement {
public:

    InformixStatement(ITConnection*);

    ~InformixStatement();

    ITBool Prepare(const ITString&);

    ITBool Exec();

    ITBool Drop();

    int NumParams();

    ITValue* Param(int);

    ITBool Error();

    const ITString& ErrorText();

    const ITString& Command();

    long RowCount();

    ITRow* NextRow();

    ITBool StartTransaction();

    ITBool AbortTransaction();
    
    ITBool CommitTransaction();

private:
    Stmt_Type type_;
    void *stmt_;
    ITConnection *conn_;
    long row_count_;

    inline bool is_select_sql(const char*);

};
} //end of namespace rytong

#endif  // end of _RYT_INFORMIX_STMT_H
