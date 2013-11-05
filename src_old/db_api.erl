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
%%% File    : db_api.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc The driver's APIs.
%%%
%%% @end
%%% -------------------------------------------------------------------
-module(db_api).
-author("deng.lifen (deng.lifen@rytong.com)").

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("db_driver.hrl").

%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------
-export([start/0,
         stop/1,
         connect/1,
         disconnect/1,
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
         select_join/5,
         select_join/6,
         trans_execute_sql/2,
         trans_execute_sql/3,
         trans_execute_param/3,
         trans_execute_param/4,
         trans_insert/3,
         trans_insert/4,
         trans_update/4,
         trans_update/5,
         trans_delete/3,
         trans_delete/4,
         trans_select/3,
         trans_select/4,
         trans_select_join/6,
         trans_select_join/7,
         transaction/2,
         transaction/3,
         prepare/2,
         prepare/3,
         prepare_execute/2,
         prepare_execute/3,
         unprepare/1,
         unprepare/2
        ]).

%% @type tables() = atom() | list()
%% @type fields() = atom() | list()
%% @type where_expr() = list() | tuple()
%% @type extras() = list() | tuple()
%% @type mybool() = bool() | 1 | 0
%% @type join_atom() = 'join' | 'left join' | 'right join' | 'inner join'
%% @type option() = {connect_id, string()}
%%                | {fields, fields()}
%%                | {extras, extras()}
%%                | {distinct, mybool()}
%% @type options() = [option()]

%% ====================================================================
%% External functions
%% ====================================================================
%% @doc Call db_driver:start/1.
%% @see db_driver:start/1
%% @spec start() -> pid()
start() ->
    db_driver:start().

%% @doc Call db_driver:stop/1.
%% @see db_driver:stop/1
%% @spec stop(Pid::pid()) -> ok
stop(Pid) ->
    db_driver:stop(Pid).

%% @doc Starts a connection and, if successful, add it to the
%%   connection pool in the driver.
%% @spec connect(ArgTuple::list()) -> {ok, Res} | {error, Err}
connect(ArgTuple) ->
    db_driver:connect(ArgTuple).

%% @doc Stops a connection and, if successful, remove it from the
%%   connection pool in the driver.
%% @spec disconnect(DbName::atom()) -> {ok, Res} | {error, Err}
disconnect(DbName) ->
    call_driver(disconnect, [], DbName).

%% @doc Call execute_sql/2.
%% @see execute_sql/2
%% @spec execute_sql(String::string()) -> {ok, Res} | {error, Err}
execute_sql(String) when is_list(String) ->
    execute_sql(String, []).

%% @doc Send a query to the driver and wait for the result.
%% @spec execute_sql(String::string(), Opts::options()) ->
%%    {ok, Res} | {error, Err}
execute_sql(String, Opts) when is_list(String) andalso is_list(Opts) ->
    call_driver(execute_sql, [String], Opts).

%% @doc Same as execute_param(Sql, ParamList, []).
%% @see execute_param/3
%% @spec execute_param(Sql::string(), ParamList::list()) -> {ok, Res} | {error, Err}
execute_param(Sql, ParamList) ->
    execute_param(Sql, ParamList, []).

%% @doc Send a query whith parameters to the driver and wait for the result.
%% @spec execute_param(Sql::string(), ParamList::list(), Opts::options()) ->
%%    {ok, Res} | {error, Err}
execute_param(Sql, [], Opts) ->
    execute_sql(Sql, Opts);
execute_param(Sql, ParamList, Opts) ->
    call_driver(execute_param, [Sql, ParamList], Opts).

%% Here the ParamList is a tupleList of format [{Key,Value}].

%% @doc Call insert/3.
%% @see insert/3
%% @spec insert(TableName::atom(), ParamList::list()) ->
%%    {ok, Res} | {error, Err}
insert(TableName, ParamList) ->
    insert(TableName, ParamList, []).

%% @doc Insert a record into database.
%%
%% @spec insert(TableName::atom(), ParamList::list(), Opts::options()) ->
%%    {ok, Res} | {error, Err}
insert(TableName, ParamList, Opts) when is_list(Opts) ->
    call_driver(insert, [TableName, ParamList], Opts).

%% @doc Call update/4.
%% @see update/4
%% @spec update(TableName::atom(), ParamList::list(), Where::where_expr()) ->
%%      {ok, Res} | {error, Err}
update(TableName, ParamList, Where) ->
    update(TableName, ParamList, Where, []).

%% @doc Update one or more records from the database,
%%  and return the number of rows updated.
%% @spec update(TableName::atom(), ParamList::list(), Where::where_expr(),
%%      Opts::options()) -> {ok, Res} | {error, Err}
update(TableName, ParamList, Where, Opts) when is_list(Opts) ->
    call_driver(update, [TableName, ParamList, Where], Opts).

%% @doc Call delete/3.
%% @see delete/3
%% @spec delete(TableName::atom(), Where::where_expr()) ->
%%    {ok, Res} | {error, Err}
delete(TableName, Where) ->
    delete(TableName, Where, []).

%% @doc Delete one or more records from the database,
%%  and return the number of rows deleted.
%% @spec delete(TableName::atom(), Where::where_expr(), Opts::options()) ->
%%    {ok, Res} | {error, Err}
delete(TableName, Where, Opts) when is_list(Opts) ->
    call_driver(delete, [TableName, Where], Opts).

%% @doc Call select/2.
%% @see select/2
%% @spec select_join(Table1::atom(), Join::join_atom(), Table2::atom(),
%%  On::where_expr(), Where::where_expr()) ->
%%    {ok, Res} | {error, Err}
select_join(Table1, Join, Table2, On, Where) ->
    select({Table1, Join, Table2, On}, Where).

%% @doc Call select/3.
%% @see select/3
%% @spec select_join(Table1::atom(), Join::join_atom(), Table2::atom(),
%%  On::where_expr(), Where::where_expr(), Opts::options()) ->
%%    {ok, Res} | {error, Err}
select_join(Table1, Join, Table2, On, Where, Opts) when is_list(Opts) ->
    select({Table1, Join, Table2, On}, Where, Opts).

%% @doc Call select/3.
%% @see select/3
%% @spec select(TableList::tables(), Where::where_expr()) ->
%%    {ok, Res} | {error, Err}
select(TableList, Where) ->
    select(TableList, Where, []).

%% @doc Find the records for the Where and Extras expressions.
%% If no records match the conditions, the function returns {ok, []}.
%% @spec select(TableList::tables(), Where::where_expr(), Opts::options()) ->
%%    {ok, Res} | {error, Err}
select(TableList, Where, Opts) when is_list(Opts) ->
    call_driver(select, [TableList, Where, Opts], Opts).

%% @doc Call trans_execute_sql/3.
%% @see trans_execute_sql/3
%% @spec trans_execute_sql(String::list(), Conn::binary()) ->
%%    {ok, Res} | {error, Err}
trans_execute_sql(String, Conn) when is_list(String) ->
    trans_execute_sql(String, Conn, []).

%% @doc Send a query to the driver in a transaction and wait for the result.
%% @spec trans_execute_sql(String::list(), Conn::binary(), Opts::options()) ->
%%      {ok, Res} | {error, Err}
trans_execute_sql(String, Conn, Opts) when is_list(String)
    andalso is_list(Opts) ->
    call_driver(trans_execute_sql, [String, Conn], Opts).

%% @doc Same as trans_execute_param(Sql, ParamList, Conn, []).
%% @see trans_execute_param/4
%% @spec trans_execute_param(Sql::string(), ParamList::list(), Conn::binary()) ->
%%    {ok, Res} | {error, Err}
trans_execute_param(Sql, ParamList, Conn) ->
    trans_execute_param(Sql, ParamList, Conn, []).

%% @doc Send a query with parameters to the driver in a transaction and wait for the result.
%% @spec trans_execute_param(Sql::list(), ParamList::list(), Conn::binary(), Opts::options()) ->
%%      {ok, Res} | {error, Err}
trans_execute_param(Sql, [], Conn, Opts) ->
    trans_execute_sql(Sql, Conn, Opts);
trans_execute_param(Sql, ParamList, Conn, Opts) ->
    call_driver(trans_execute_param, [Sql, ParamList, Conn], Opts).

%% @doc Call trans_insert/4.
%% @see trans_insert/4
%% @spec trans_insert(TableName::atom(), ParamList::list(), Conn::binary()) ->
%%    {ok, Res} | {error, Err}
trans_insert(TableName, ParamList, Conn) ->
    trans_insert(TableName, ParamList, Conn, []).

%% @doc Insert one record into the database in a transaction.
%% @spec trans_insert(TableName::atom(), ParamList::list(), Conn::binary(),
%%      Opts::options()) -> {ok, Res} | {error, Err}
trans_insert(TableName, ParamList, Conn, Opts) when is_list(Opts) ->
    call_driver(trans_insert, [TableName, ParamList, Conn], Opts).

%% @doc Call trans_update/5.
%% @see trans_update/5
%% @spec trans_update(TableName::atom(), ParamList::list(), Where::where_expr(),
%%      Conn::binary()) ->
%%    {ok, Res} | {error, Err}
trans_update(TableName, ParamList, Where, Conn) ->
    trans_update(TableName, ParamList, Where, Conn, []).

%% @doc Update one or more records from the database in a transaction,
%%  and return the number of rows updated.
%% @spec trans_update(TableName::atom(), ParamList::list(), Where::where_expr(),
%%      Conn::binary(), Opts::options()) -> {ok, Res} | {error, Err}
trans_update(TableName, ParamList, Where, Conn, Opts) when is_list(Opts) ->
    call_driver(trans_update, [TableName, ParamList, Where, Conn], Opts).

%% @doc Call trans_delete/4.
%% @see trans_delete/4
%% @spec trans_delete(TableName::atom(), Where::where_expr(), Conn::binary()) ->
%%    {ok, Res} | {error, Err}
trans_delete(TableName, Where, Conn) ->
    trans_delete(TableName, Where, Conn, []).

%% @doc Delete one or more records from the database in a transaction,
%%  and return the number of rows deleted.
%% @spec trans_delete(TableName::atom(), Where::where_expr(), Conn::binary(),
%%      Opts::options()) -> {ok, Res} | {error, Err}
trans_delete(TableName, Where, Conn, Opts) when is_list(Opts) ->
    call_driver(trans_delete, [TableName, Where, Conn], Opts).

%% @doc Call trans_select_join/7.
%% @see trans_select_join/7
%% @spec trans_select_join(Table1::atom(), Join::join_atom(), Table2::atom(),
%%      On::where_expr(), Where::where_expr(), Conn::binary()) ->
%%      {ok, Res} | {error, Err}
trans_select_join(Table1, Join, Table2, On, Where, Conn) ->
    trans_select_join(Table1, Join, Table2, On, Where, Conn, []).

%% @doc Call trans_select/4.
%% @see trans_select/4
%% @spec trans_select_join(Table1::atom(), Join::join_atom(), Table2::atom(),
%%      On::where_expr(), Where::where_expr(), Conn::binary(), Opts::options())
%%      -> {ok, Res} | {error, Err}
trans_select_join(Table1, Join, Table2, On, Where, Conn, Opts)
    when is_list(Opts) ->
    trans_select({Table1, Join, Table2, On}, Where, Conn, Opts).

%% @doc Call trans_select/4.
%% @see trans_select/4
%% @spec trans_select(TableList::tables(), Where::where_expr(), Conn::binary())
%%      -> {ok, Res} | {error, Err}
trans_select(TableList, Where, Conn) ->
    trans_select(TableList, Where, Conn, []).

%% @doc Find the records for the Where and Extras expressions in a transaction.
%% If no records match the conditions, the function returns {ok, []}.
%% @spec trans_select(TableList::tables(), Where::where_expr(), Conn::binary(),
%%      Opts::options()) -> {ok, Res} | {error, Err}
trans_select(TableList, Where, Conn, Opts) when is_list(Opts) ->
    call_driver(trans_select, [TableList, Where, Conn, Opts], Opts).

%% @doc Call transaction/3.
%% @see transaction/3
%% @spec transaction(Fun::function(), Arg::list()) ->
%%    {ok, commit} | {error, Err}
transaction(Fun, Arg) ->
    transaction(Fun, Arg, []).

%% @doc Execute a transaction.
%% @spec transaction(Fun::function(), Arg::list(), Opts::options()) ->
%%    {ok, Res} | {error, Err}
transaction(Fun, Arg, Opts) when is_list(Opts) ->
    call_driver(transaction, [Fun, Arg], Opts).

%% @doc Call prepare/3.
%% @see prepare/3
%% @spec prepare(PrepareName::string(), Sql::string()) ->
%%    {ok, PrepareName} | {error, Err}
prepare(PrepareName, Sql) ->
    prepare(PrepareName, Sql, []).

%% @doc Register a prepared statement with the dispatcher. If driver is oracle,
%%  then transform prepare sql, replace "?" to ":N(N = 1,2,3...)".
%% @spec prepare(PrepareName::string(), Sql::string(), Opts::options()) ->
%%    {ok, PrepareName} | {error, Err}
prepare(PrepareName, Sql, Opts) when is_list(Opts) ->
    DbName = proplists:get_value(db_name, Opts, default),
    TranSql =
        case db_server:get_drv_type(DbName) of
            ?ORACLE_DB ->
                transform_oracle_stmt_sql(Sql);
            _ ->
                Sql
        end,
    call_driver(prepare, [PrepareName, TranSql], Opts).

%% @doc Replace "?" to ":N(N = 1,2,3...)".
transform_oracle_stmt_sql(Str) ->
    case re:run(Str, "\\?", [global]) of
        nomatch ->
            Str;
        {match, Match} ->
            N = length(Match),
            stmt_replace(Str, N)
    end.

stmt_replace(Str, N) ->
    stmt_replace(Str, N, 1).

stmt_replace(Str, 0, _I) ->
    Str;
stmt_replace(Str, N, I) ->
    NewStr = re:replace(Str, "\\?", ":" ++ integer_to_list(I), [{return,list}]),
    stmt_replace(NewStr, N - 1, I + 1).

%% @doc Call prepare_execute/3.
%% @see prepare_execute/3
%% @spec prepare_execute(PrepareName::string(), Value::list()) ->
%%    {ok, Res} | {error, Err}
prepare_execute(PrepareName, Value) when is_list(Value) ->
    prepare_execute(PrepareName, Value, []).

%% @doc Execute a prepared statement it has prepared.
%%  The function is not safe in multithreading.
%%  Please execute the same prepareName int one process.
%% @spec prepare_execute(PrepareName::string(), Value::list(), Opts::options())
%%      -> {ok, Res} | {error, Err}
prepare_execute(PrepareName, Value, Opts) when is_list(Value) ->
    call_driver(prepare_execute, [PrepareName, Value], Opts).

%% @doc Call unprepare/2.
%% @see unprepare/2
%%
%% @spec unprepare(PrepareName::string()) -> ok
unprepare(PrepareName) ->
    unprepare(PrepareName, []).

%% @doc Unregister a statement that has previously been register with
%%   the dispatcher. All calls to prepare_execute() with the given statement
%%   will fail once the statement is unprepared. If the statement hasn't
%%   been prepared, nothing happens.
%% @spec unprepare(PrepareName::string(), Opts::options()) -> ok
unprepare(PrepareName, Opts) when is_list(Opts) ->
    call_driver(unprepare, [PrepareName], Opts).

call_driver(Interface, Args, Opts) when is_list(Opts) ->
    DbName = proplists:get_value(db_name, Opts, default),
    call_driver(Interface, Args, DbName);
call_driver(Interface, Args, DbName) when is_atom(DbName) ->
    try db_server:get_drv(DbName) of
        Drv ->
            erlang:apply(db_driver, Interface, [Drv | Args])
    catch
        error_name_of_db_drv ->
            {error, error_name_of_db_drv}
    end.
