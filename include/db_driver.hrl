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
-define(TRANSCATION_CONNECTION,     transcation_connection).

-define(DB_TIMEOUT,                 8000).
-define(DB_LOCK_TIMEOUT,            5000).

-define(DB_APP_NAME,                db).

%% Supported database driver type.
-define(MYSQL_DB,                   mysql).
-define(ORACLE_DB,                  oracle).
-define(SYBASE_DB,                  sybase).
-define(DB2_DB,                     db2).
-define(INFORMIX_DB,                informix).

-define(DRIVER_NAME_POS,            1).
-define(DRIVER_CODE_POS,            2).
-define(DRIVER_PAIR, [
    {?MYSQL_DB,     0},
    {?ORACLE_DB,    1},
    {?SYBASE_DB,    2},
    {?DB2_DB,       3},
    {?INFORMIX_DB,  4}
]).

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

-define(DRV_CONNECT_DB,             19).
-define(DRV_DISCONNECT_DB,          20).
-define(DRV_INIT_DB,                21).
-define(DRV_EXECUTE_DB,             22).
-define(DRV_INSERT_DB,              23).
-define(DRV_UPDATE_DB,              24).
-define(DRV_DELETE_DB,              25).
-define(DRV_SELECT_DB,              26).
-define(DRV_TRANS_BEGIN_DB,         27).
-define(DRV_TRANS_COMMIT_DB,        28).
-define(DRV_TRANS_ROLLBACK_DB,      29).
-define(DRV_PREPARE_DB,             30).
-define(DRV_PREPARE_EXECUTE_DB,     31).
-define(DRV_PREPARE_CANCEL_DB,      32).
-define(DRV_STMT_MAP_INIT_DB,       33).
-define(DRV_STMT_MAP_DESTORY_DB,    34).

%% Mapping enum SqlKeyword in DBOperation.h.
-define(KEYWORD_MAPPING, [
    {'and',        0},
    {'or',         1},
    {'not',        2},
    {'like',       3},
    {'as',         4},
    {'=',          5},
    {'>',          6},
    {'>=',         7},
    {'<',          8},
    {'<=',         9},
    {'join',       10},
    {'left join',  11},
    {'right join', 12},
    {'!=',         13},
    {'order',      14},
    {'limit',      15},
    {'.',          16},
    {'group',      17},
    {'having',     18},
    {'between',    19},
    {'+',          20},
    {'-',          21},
    {'*',          22},
    {'/',          23},
    {'function',   24},
    {'inner join', 25},
    {'in',         26},
    {'is_null',    27},
    {'is_not_null',28},
    {datetime,     29},
    {date,         30},
    {time,         31},
    {timestamp,    32},
    {interval_ym,  33},
    {interval_ds,  34},
    {bfile,        35}
]).

%% Default database config.
-define(DEFAULT_DRIVER,             ?MYSQL_DB).
-define(DEFAULT_HOST,               "localhost").
-define(DEFAULT_PORT,               3306).
-define(DEFAULT_USER,               "test").
-define(DEFAULT_PASSWORD,           "").
-define(DEFAULT_DATABASE,           "test").
-define(DEFAULT_POOLSIZE,           3).
-define(DEFAULT_TABLE_INFO,         true).
-define(DEFAULT_DEFAULT_POOL,       false).
-define(DEFAULT_ENCODING,           utf8).

-record(conn_args, {
    driver       = ?DEFAULT_DRIVER,
    host         = ?DEFAULT_HOST,
    port         = ?DEFAULT_PORT,
    user         = ?DEFAULT_USER,
    password     = ?DEFAULT_PASSWORD,
    database     = ?DEFAULT_DATABASE,
    poolsize     = ?DEFAULT_POOLSIZE,
    table_info   = ?DEFAULT_TABLE_INFO,
    default_pool = ?DEFAULT_DEFAULT_POOL
}).

-record(pool, {
    id,
    size,
    conn_args = #conn_args{},
    table_schemas,
    error_handler,
    available = queue:new(),
    locked    = gb_trees:empty(),
    waiting   = queue:new()
}).

-record(db_connection, {
    id,
    pool_id,
    driver,
    alive = true
}).
