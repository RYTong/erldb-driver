%% Copyright (c) 2009-2010 Beijing RYTong Information Technologies, Ltd.
%% All rights reserved.
%%
%% The contents of this file are subject to the Erlang Database Driver
%% Public License Version 1.0, (the "License"); you may not use this 
%% file except in compliance with the License. You should have received
%% a copy of the Erlang Database Driver Public License along with this
%% software. If not, it can be retrieved via the world wide web at
%% http://www.rytong.com/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%%% -------------------------------------------------------------------
%%% File    : db_driver.hrl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @end
%%% -------------------------------------------------------------------

-define(APPNAME,                    db_driver).
-define(DATABASE_LIB,               "database_drv").
-define(DATABASE_LIB_DIR,           "priv").
-define(DB_CONFIG_FILE,             "db.conf").
-define(DB_CONFIG_PATH,             "config").

-define(CONNECT_NAME,               db_name).

%% Supported database driver type.
-define(MYSQL_DB,                   0).
-define(ORACLE_DB,                  1).
-define(SYBASE_DB,                  2).
-define(DB2_DB,                     3).

%% Command mapping.
-define(DRV_EXECUTE,                1).
-define(DRV_INSERT,                 2).
-define(DRV_UPDATE,                 3).
-define(DRV_DELETE,                 4).
-define(DRV_SELECT,                 5).
-define(DRV_TRANSACTION_EXECUTE,    6).
-define(DRV_TRANSACTION_INSERT,     7).
-define(DRV_TRANSACTION_UPDATE,     8).
-define(DRV_TRANSACTION_DELETE,     9).
-define(DRV_TRANSACTION_SELECT,     10).
-define(DRV_TRANSACTION_BEGIN,      11).
-define(DRV_TRANSACTION_COMMIT,     12).
-define(DRV_TRANSACTION_ROLLBACK,   13).
-define(DRV_PREPARE,                14).
-define(DRV_PREPARE_EXECUTE,        15).
-define(DRV_PREPARE_CANCEL,         16).
-define(DRV_CONNECT,                17).
-define(DRV_DISCONNECT,             18).

%% Default database config.
-define(DEFAULT_DRIVER,             ?MYSQL_DB).
-define(DEFAULT_PORT,               3306).
-define(DEFAULT_MAX_THREAD_LENGTH,  1000).
-define(DEFAULT_MAX_QUEUE,          1000).

-define(DEFAULT_HOST,               "localhost").
-define(DEFAULT_DATABASE,           "test").
-define(DEFAULT_USER,               "root").
-define(DEFAULT_PASSWORD,           "").
-define(DEFAULT_POOLSIZE,           8).
