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
%%% ----------------------------------------------------------------------------
%%% File    : db_api.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc The driver's APIs.
%%%
%%% @end
%%% ----------------------------------------------------------------------------
-module(db_api).
-author("deng.lifen (deng.lifen@rytong.com)").

%% =============================================================================
%% Include files
%% =============================================================================
-include("db_driver.hrl").

%% =============================================================================
%% External exports
%% =============================================================================
-export([
    start/0,
    stop/0,

    init_conn_arg_record/1,
    add_pool/2,
    remove_pool/1,
    increment_pool_size/2,
    decrement_pool_size/2,

    get_pool_info/2,
    get_driver/1,

    get_table_schemas/1,
    refresh_table_schemas/0,
    refresh_table_schemas/1,
    get_table_schema/1,
    get_table_schema/2,

    execute_sql/1,
    execute_sql/2,
    execute_param/2,
    execute_param/3,

    insert/2,
    insert/3,
    update/3,
    update/4,
    delete/2,
    delete/3,
    select/2,
    select/3,
    select_with_fields/2,
    select_with_fields/3,
    select_with_fields_f/2,
    select_with_fields_f/3,

    transaction/1,
    transaction/2,

    prepare/2,
    prepare_execute/2,
    prepare_execute/3
]).

%% =============================================================================
%% External functions
%% =============================================================================

%% @doc Call db_app:start/0.
%% @see db_app:start/0
start() ->
    db_app:start().

%% @doc Call db_app:stop/0.
%% @see db_app:stop/0
stop() ->
    db_app:stop().

%% @doc Call db_app:init_conn_arg_record/1.
%% @see db_app:init_conn_arg_record/1
init_conn_arg_record(ConnArgList) ->
    db_app:init_conn_arg_record(ConnArgList).

%% @doc Call db_app:add_pool/2.
%% @see db_app:add_pool/2
add_pool(PoolId, ConnArg) ->
    db_app:add_pool(PoolId, ConnArg).

%% @doc Call db_app:remove_pool/1.
%% @see db_app:remove_pool/1
remove_pool(PoolId) ->
    db_app:remove_pool(PoolId).

%% @doc Call db_app:increment_pool_size/2.
%% @see db_app:increment_pool_size/2
increment_pool_size(PoolId, Num) ->
    db_app:increment_pool_size(PoolId, Num).

%% @doc Call db_app:decrement_pool_size/2.
%% @see db_app:decrement_pool_size/2
decrement_pool_size(PoolId, Num) ->
    db_app:decrement_pool_size(PoolId, Num).

%% @doc Call db_app:get_table_schema/1.
%% @see db_app:get_table_schema/1
get_table_schema(Table) ->
    db_app:get_table_schema(Table).

%% @doc Call db_app:get_table_schema/2.
%% @see db_app:get_table_schema/2
get_table_schema(PoolId, Table) ->
    db_app:get_table_schema(PoolId, Table).

%% @doc Call db_app:refresh_table_schemas/0.
%% @see db_app:refresh_table_schemas/0
refresh_table_schemas() ->
    db_app:refresh_table_schemas().

%% @doc Call db_app:refresh_table_schemas/1.
%% @see db_app:refresh_table_schemas/1
refresh_table_schemas(PoolId) ->
    db_app:refresh_table_schemas(PoolId).

%% @doc Call db_app:get_pool_info/2.
%% @see db_app:get_pool_info/2
get_pool_info(PoolId, Info) ->
    db_app:get_pool_info(PoolId, Info).

%% @doc Call db_app:get_driver/1.
%% @see db_app:get_driver/1
get_driver(PoolId) ->
    db_app:get_driver(PoolId).

%% @doc Call db_app:get_table_schemas/1.
%% @see db_app:get_table_schemas/1
get_table_schemas(PoolId) ->
    db_app:get_table_schemas(PoolId).

%% @doc Call db_app:execute_sql/1.
%% @see db_app:execute_sql/1
execute_sql(String) ->
    db_app:execute_sql(String).

%% @doc Call db_app:execute_sql/2.
%% @see db_app:execute_sql/2
execute_sql(String, Opts) ->
    db_app:execute_sql(String, Opts).

%% @doc Call db_app:execute_param/2.
%% @see db_app:execute_param/2
execute_param(Sql, ParamList) ->
    db_app:execute_param(Sql, ParamList).

%% @doc Call db_app:execute_param/3.
%% @see db_app:execute_param/3
execute_param(Sql, ParamList, Opts) ->
    db_app:execute_param(Sql, ParamList, Opts).

%% @doc Call db_app:insert/2.
%% @see db_app:insert/2
insert(TableName, ParamList) ->
    db_app:insert(TableName, ParamList).

%% @doc Call db_app:insert/3.
%% @see db_app:insert/3
insert(TableName, ParamList, Opts) ->
    db_app:insert(TableName, ParamList, Opts).

%% @doc Call db_app:update/3.
%% @see db_app:update/3
update(TableName, ParamList, Where) ->
    db_app:update(TableName, ParamList, Where).

%% @doc Call db_app:update/4.
%% @see db_app:update/4
update(TableName, ParamList, Where, Opts) ->
    db_app:update(TableName, ParamList, Where, Opts).

%% @doc Call db_app:delete/2.
%% @see db_app:delete/2
delete(TableName, Where) ->
    db_app:delete(TableName, Where).

%% @doc Call db_app:delete/3.
%% @see db_app:delete/3
delete(TableName, Where, Opts) when is_list(Opts) ->
    db_app:delete(TableName, Where, Opts).

%% @doc Call db_app:select/2.
%% @see db_app:select/2
select(TableList, Where) ->
    db_app:select(TableList, Where).

%% @doc Call db_app:select/3.
%% @see db_app:select/3
select(TableList, Where, Opts) when is_list(Opts) ->
    db_app:select(TableList, Where, Opts).

%% @doc Call db_app:select_with_fields_f/2.
%% @see db_app:select_with_fields_f/2
select_with_fields_f(Table, Where) ->
    db_app:select_with_fields_f(Table, Where).

%% @doc Call db_app:select_with_fields_f/3.
%% @see db_app:select_with_fields_f/3
select_with_fields_f(Table, Where, Opts) when is_list(Opts) ->
    db_app:select_with_fields_f(Table, Where, Opts).

%% @doc Call db_app:select_with_fields/2.
%% @see db_app:select_with_fields/2
select_with_fields(Table, Where) ->
    db_app:select_with_fields(Table, Where).

%% @doc Call db_app:select_with_fields/3.
%% @see db_app:select_with_fields/3
select_with_fields(Table, Where, Opts) when is_list(Opts) ->
    db_app:select_with_fields(Table, Where, Opts).

%% @doc Call db_app:transaction/1.
%% @see db_app:transaction/1
transaction(Fun) ->
    db_app:transaction(Fun).

%% @doc Call db_app:transaction/2.
%% @see db_app:transaction/2
transaction(Fun, Opts) ->
    db_app:transaction(Fun, Opts).

%% @doc Call db_app:prepare/2.
%% @see db_app:prepare/2
prepare(PrepareName, Sql) ->
    db_app:prepare(PrepareName, Sql).


%% @doc Call db_app:prepare_execute/2.
%% @see db_app:prepare_execute/2
prepare_execute(PrepareName, Value) ->
    db_app:prepare_execute(PrepareName, Value).

%% @doc Call db_app:prepare_execute/3.
%% @see db_app:prepare_execute/3
prepare_execute(PrepareName, Value, Opts) ->
    db_app:prepare_execute(PrepareName, Value, Opts).


