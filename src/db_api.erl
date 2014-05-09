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
-author('deng.lifen@rytong.com').

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

%%------------------------------------------------------------------------------
%% @doc Start the database application.
%% @end
%%------------------------------------------------------------------------------
-spec start() -> ok | no_return().
start() ->
    application:start(?DB_APP_NAME).

%%------------------------------------------------------------------------------
%% @doc Stop the database application.
%% @end
%%------------------------------------------------------------------------------
-spec stop() -> ok | no_return().
stop() ->
    application:stop(?DB_APP_NAME).

%%------------------------------------------------------------------------------
%% @doc Synchronous call to the connection manager to add a pool.
%%
%% Creates a pool record, opens n=Size connections and calls
%% db_conn_server:add_pool/1 to make the pool known to the pool management,
%% it is translated into a blocking gen-server call.
%% @end
%%------------------------------------------------------------------------------
-spec add_pool(PoolId::atom(), ConnArg::record() | db_conn:conn_args()) -> ok | no_return().
add_pool(PoolId, ConnArgList) when is_list(ConnArgList) ->
    add_pool(PoolId, {init_conn_arg_record(ConnArgList), proplists:get_value(error_handler, ConnArgList)});
add_pool(PoolId, {ConnArg, ErrorHandler}) when is_record(ConnArg, conn_args) ->
    catch_throw(db_conn, add_pool, [PoolId, {ConnArg, ErrorHandler}]);
add_pool(PoolId, ConnArg) when is_record(ConnArg, conn_args) ->
    catch_throw(db_conn, add_pool, [PoolId, {ConnArg, undefined}]).

%%------------------------------------------------------------------------------
%% @doc The connection parameters conversion, converted into a record db_conn:conn_args().
%% @end
%%------------------------------------------------------------------------------
-spec init_conn_arg_record(ConnArgList::db_conn:conn_args()) -> record().
init_conn_arg_record(ConnArgList) when is_list(ConnArgList)  ->
    #conn_args{
        driver = proplists:get_value(driver, ConnArgList, ?DEFAULT_DRIVER),
        host = proplists:get_value(host, ConnArgList, ?DEFAULT_HOST),
        port = proplists:get_value(port, ConnArgList, ?DEFAULT_PORT),
        user = proplists:get_value(user, ConnArgList, ?DEFAULT_USER),
        password = proplists:get_value(password, ConnArgList, ?DEFAULT_PASSWORD),
        database = proplists:get_value(database, ConnArgList, ?DEFAULT_DATABASE),
        poolsize = proplists:get_value(poolsize, ConnArgList, ?DEFAULT_POOLSIZE),
        table_info = proplists:get_value(table_info, ConnArgList, ?DEFAULT_TABLE_INFO),
        default_pool = proplists:get_value(default_pool, ConnArgList, ?DEFAULT_DEFAULT_POOL)
    }.

%%------------------------------------------------------------------------------
%% @doc Synchronous call to the connection manager to remove a pool.
%% @end
%%------------------------------------------------------------------------------
-spec remove_pool(PoolId::atom()) -> ok | no_return().
remove_pool(PoolId) ->
    catch_throw(db_conn, remove_pool, [PoolId]).

%%------------------------------------------------------------------------------
%% @doc Synchronous call to the connection manager to enlarge a pool.
%%
%% This opens n=Num new connections and adds them to the pool of id PoolId.
%% @end
%%------------------------------------------------------------------------------
-spec increment_pool_size(PoolId::atom(), Num::integer()) -> ok | no_return().
increment_pool_size(PoolId, Num) ->
    catch_throw(db_conn, increment_pool_size, [PoolId, Num]).

%%------------------------------------------------------------------------------
%% @doc Synchronous call to the connection manager to shrink a pool.
%%
%% This reduces the connections by up to n=Num, but it only drops and closes available
%% connections that are not in use at the moment that this function is called. Connections
%% that are waiting for a server response are never dropped. In heavy duty, this function
%% may thus do nothing.
%%
%% If Num is higher than the amount of connections or the amount of available connections,
%% exactly all available connections are dropped and closed.
%% @end
%%------------------------------------------------------------------------------
-spec decrement_pool_size(PoolId::atom(), Num::integer()) -> ok | no_return().
decrement_pool_size(PoolId, Num) ->
    catch_throw(db_conn, decrement_pool_size, [PoolId, Num]).

%% @doc Call refresh_table_schemas/1.
%% @see refresh_table_schemas/1
-spec refresh_table_schemas() -> ok | no_return().
refresh_table_schemas() ->
    case db_conn_server:get_default_pool() of
        undefined ->
            {error, no_default_pool};
        PoolId ->
            refresh_table_schemas(PoolId)
    end.

%%------------------------------------------------------------------------------
%% @doc Get all table schemas from database, and stored in db_conn_server.
%% @end
%%------------------------------------------------------------------------------
-spec refresh_table_schemas(PoolId::atom()) -> ok | no_return().
refresh_table_schemas(PoolId) ->
    TableInfos = get_table_schemas(PoolId),
    db_conn_server:set_table_schema(PoolId, TableInfos).

%% @doc Call get_table_schema/2.
%% @see get_table_schema/2
-spec get_table_schema(Table::list() | atom()) -> list() | no_return().
get_table_schema(Table) ->
    case db_conn_server:get_default_pool() of
        undefined ->
            throw({error, no_default_pool});
        PoolId ->
            get_table_schema(PoolId, Table)
    end.

%%------------------------------------------------------------------------------
%% @doc Get table schemas from databases.
%%
%% Stored the schemas in db_conn_server when the server without them.
%% @end
%%------------------------------------------------------------------------------
-spec get_table_schema(PoolId::atom(), Table::list() | atom()) ->
   list() | no_return().
get_table_schema(PoolId, Table) when is_atom(Table) ->
    get_table_schema(PoolId, atom_to_list(Table));
get_table_schema(PoolId, Table) when is_list(Table)  ->
    TableInfos =
        case db_conn_server:get_pool_info(PoolId, table_schemas) of
            undefined ->
                TableInfos1 = get_table_schemas(PoolId),
                db_conn_server:set_table_schema(PoolId, TableInfos1),
                TableInfos1;
            TableInfos2 ->
                TableInfos2
        end,
    case proplists:get_value(Table, TableInfos) of
        undefined ->
            throw({error, no_table_info});
        Value ->
            Value
    end.

%%------------------------------------------------------------------------------
%% @doc Get pool information.
%%
%% Info: conn_args | table_schemas | size
%% @end
%%------------------------------------------------------------------------------
-spec get_pool_info(PoolId::atom(), Info::atom()) -> term().
get_pool_info(PoolId, Info) ->
    db_conn_server:get_pool_info(PoolId, Info).

%%------------------------------------------------------------------------------
%% @doc Get driver name from connection arguments.
%% @end
%%------------------------------------------------------------------------------
-spec get_driver(PoolId::atom()) -> atom().
get_driver(PoolId) ->
    ConnArgs = get_pool_info(PoolId, conn_args),
    ConnArgs#conn_args.driver.

%%------------------------------------------------------------------------------
%% @doc Get column_name of all tables.
%% @end
%%------------------------------------------------------------------------------
-spec get_table_schemas(PoolId::atom()) -> list() | no_return().
get_table_schemas(PoolId) ->
    ConnArgs = get_pool_info(PoolId, conn_args),
    DbType = ConnArgs#conn_args.driver,
    DbName = ConnArgs#conn_args.database,
    db_util:get_table_schemas(PoolId, DbType, DbName, []).

%% @doc Call execute_sql/2.
%% @see execute_sql/2
-spec execute_sql(Sql::string()) -> db_conn:result().
execute_sql(Sql) ->
    execute_sql(Sql, []).

%%------------------------------------------------------------------------------
%% @doc Send a query to the driver and wait for the result.
%% @end
%%------------------------------------------------------------------------------
-spec execute_sql(Sql::string(), Opts::db_conn:options()) -> db_conn:result().
execute_sql(Sql, Opts) ->
    monitor_work(Opts, {db_conn, execute_sql, [Sql]}).

%% @doc Call execute_param/3.
%% @see execute_param/3
-spec execute_param(Sql::string(), ParamList::db_conn:param_list()) -> db_conn:result().
execute_param(Sql, ParamList) ->
    execute_param(Sql, ParamList, []).

%%------------------------------------------------------------------------------
%% @doc Send a query with parameters to the driver and wait for the result.
%% @end
%%------------------------------------------------------------------------------
-spec execute_param(Sql::string(), ParamList::db_conn:param_list(), Opts::db_conn:options()) -> db_conn:result().
execute_param(Sql, ParamList, Opts) ->
    monitor_work(Opts, {db_conn, execute_param, [Sql, ParamList]}).

%% @doc Call insert/3.
%% @see insert/3
-spec insert(Table::atom(), ParamList::db_conn:param_list()) -> db_conn:result().
insert(Table, ParamList) ->
    insert(Table, ParamList, []).

%% @doc Insert a record into database.
%% @end
-spec insert(Table::atom(), ParamList::db_conn:param_list(), Opts::db_conn:options()) -> db_conn:result().
insert(Table, ParamList, Opts) ->
    monitor_work(Opts, {db_conn, insert, [Table, ParamList]}).

%% @doc Call update/4.
%% @see update/4
-spec update(Table::atom(), ParamList::db_conn:param_list(), Where::db_conn:where_expr()) -> db_conn:result().
update(Table, ParamList, Where) ->
    update(Table, ParamList, Where, []).

%%------------------------------------------------------------------------------
%% @doc Update one or more records from the database,
%% and return the number of rows updated.
%% @end
%%------------------------------------------------------------------------------
-spec update(Table::atom(), ParamList::db_conn:param_list(), Where::db_conn:where_expr(),
     Opts::db_conn:options()) -> db_conn:result().
update(Table, ParamList, Where, Opts) ->
    monitor_work(Opts, {db_conn, update, [Table, ParamList, Where]}).

%% @doc Call delete/3.
%% @see delete/3
-spec delete(Table::atom(), Where::db_conn:where_expr()) -> db_conn:result().
delete(Table, Where) ->
    delete(Table, Where, []).

%%------------------------------------------------------------------------------
%% @doc Delete one or more records from the database,
%% and return the number of rows deleted.
%% @end
%%------------------------------------------------------------------------------
-spec delete(Table::atom(), Where::db_conn:where_expr(), Opts::db_conn:options()) -> db_conn:result().
delete(Table, Where, Opts) ->
    monitor_work(Opts, {db_conn, delete, [Table, Where]}).

%% @doc Call select/3.
%% @see select/3
-spec select(TableList::db_conn:tables(), Where::db_conn:where_expr()) -> db_conn:result().
select(TableList, Where) ->
    select(TableList, Where, []).

%%------------------------------------------------------------------------------
%% @doc Find the records for the Where and Extras expressions.
%%
%% If no records match the conditions, the function returns {ok, []}.
%% @end
%%------------------------------------------------------------------------------
-spec select(TableList::db_conn:tables(), Where::db_conn:where_expr(), Opts::db_conn:options()) -> db_conn:result().
select(TableList, Where, Opts) ->
    monitor_work(Opts, {db_conn, select, [TableList, Where, Opts]}).

%% @doc Call select_with_fields_f/3.
%% @see select_with_fields_f/3
select_with_fields_f(Table, Where) ->
    select_with_fields_f(Table, Where, []).

%%------------------------------------------------------------------------------
%% @doc Find the record for the Where and Extras expressions.
%%
%% If no records match the conditions, the function returns undefined,
%% or return one record.
%% @end
%%------------------------------------------------------------------------------
-spec select_with_fields_f(Table::atom(), Where::db_conn:where_expr(), Opts::db_conn:options()) -> term().
select_with_fields_f(Table, Where, Opts) ->
    case select_with_fields(Table, Where, Opts) of
        [] ->
            undefined;
        [Record|_] ->
            Record
    end.

%% @doc Call select_with_fields/3.
%% @see select_with_fields/3
select_with_fields(Table, Where) ->
    select_with_fields(Table, Where, []).

%%------------------------------------------------------------------------------
%% @doc Find the record for the Where and Extras expressions.
%%
%% Returns the records can be used to select_with_fields with field name.
%% Only full field query a table.
%% @end
%%------------------------------------------------------------------------------
-spec select_with_fields(Table::atom(), Where::db_conn:where_expr(), Opts::db_conn:options()) -> term().
select_with_fields(Table, Where, Opts) ->
    {ok, Result} = select(Table, Where, Opts),
    Attrs = db_api:get_table_schema(Table),
    [lists:zip(Attrs, X) || X <- Result].

%% @doc Call transaction/2.
%% @see transaction/2
-spec transaction(Fun::function()) -> db_conn:result().
transaction(Fun) ->
    transaction(Fun, []).

%%------------------------------------------------------------------------------
%% @doc Execute a transaction.
%% @end
%%------------------------------------------------------------------------------
-spec transaction(Fun::function(), Opts::db_conn:options()) -> db_conn:result().
transaction(Fun, Opts) ->
    case get_pool_id(Opts) of
        {error, Error} ->
            {error, Error};
        PoolId ->
            db_conn:transaction(PoolId, Fun)
    end.

%% @doc Register a prepared statement with the server db_stmt.
%% @end
-spec prepare(PrepareName::string(), Sql::string()) -> db_conn:result().
prepare(StmtName, Statement) when is_atom(StmtName) andalso is_list(Statement) ->
    db_stmt:add(StmtName, Statement).

%% @doc Call prepare_execute/3.
%% @see prepare_execute/3
-spec prepare_execute(PrepareName::string(), Value::list()) -> db_conn:result().
prepare_execute(Name, Values) ->
    prepare_execute(Name, Values, []).

%%------------------------------------------------------------------------------
%% @doc Execute a prepared statement it has prepared.
%% @end
%%------------------------------------------------------------------------------
-spec prepare_execute(PrepareName::string(), Value::list(), Opts::db_conn:options()) ->
    db_conn:result().
prepare_execute(Name, Values, Opts) ->
    monitor_work(Opts, {db_conn, prepare_execute, [Name, Values]}).

%%==============================================================================
%% Local Functions
%%==============================================================================
default_timeout() ->
    ?DB_TIMEOUT.

monitor_work(Opts, {M, F, A}) ->
    case get_pool_id(Opts) of
        {error, Error} ->
            {error, Error};
        PoolId ->
            Connection = db_conn_server:wait_for_connection(PoolId),
            Timeout = get_timeout(Opts),
            do_monitor_work(Connection, Timeout, {M, F, A})
    end.

do_monitor_work({trans, Connection}, Timeout, {M,F,A}) ->
    case do_monitor_work(Connection, Timeout, {M,F,A}) of
        {error, Error} ->
            throw({error, Error});
        Result ->
            Result
    end;
do_monitor_work(Connection, Timeout, {M,F,A}) when is_record(Connection, db_connection) ->
    %% spawn a new process to do work, then monitor that process until
    %% it either dies, returns data or times out.
    Parent = self(),
    Pid = spawn(
            fun() ->
                    receive start ->
                            Parent ! {self(), apply(M, F, [Connection|A])}
                    end
            end),
    Mref = erlang:monitor(process, Pid),
    Pid ! start,
    receive
        {'DOWN', Mref, process, Pid, {_, closed}} ->
            %% io:format("monitor_work: ~p DOWN/closed -> renew~n", [Pid]),
            case db_conn:reset_connection(Connection, keep) of
                NewConnection when is_record(NewConnection, db_connection) ->
                    %% re-loop, with new connection.
                    [_OldConn | RestArgs] = A,
                    NewA = [NewConnection | RestArgs],
                    do_monitor_work(NewConnection, Timeout, {M, F, NewA});
                {error, FailedReset} ->
                    {error, {connection_down, {and_conn_reset_failed, FailedReset}}}
            end;
        {'DOWN', Mref, process, Pid, Reason} ->
            %% if the process dies, reset the connection
            %% and re-throw the error on the current pid.
            %% catch if re-open fails and also signal it.
            %% io:format("monitor_work: ~p DOWN ~p -> exit~n", [Pid, Reason]),
            case db_conn:reset_connection(Connection, pass) of
                {error,FailedReset} ->
                    {error, {Reason, {and_conn_reset_failed, FailedReset}}};
                _ ->
                    {error, Reason}
            end;
        {Pid, Result} ->
            %% if the process returns data, unlock the
            %% connection and collect the normal 'DOWN'
            %% message send from the child process
            %% io:format("monitor_work: ~p got result -> demonitor ~p, unlock connection ~p, return result~n", [Pid, Mref, Connection#db_connection.id]),
            erlang:demonitor(Mref, [flush]),
            case erlang:get(?TRANSCATION_CONNECTION) of
                undefined ->
                    db_conn_server:pass_connection(Connection);
                _ ->
                    go_on
            end,

            case (db_conn_server:get_pool(Connection#db_connection.pool_id))#pool.error_handler of
                undefined ->
                    Result;
                {ModError, FunError} ->
                    erlang:apply(ModError, FunError, [F, Result, A])
            end
    %% after Timeout ->
    %%         %% if we timeout waiting for the process to return,
    %%         %% then reset the connection and throw a timeout error
    %%         %% io:format("monitor_work: ~p TIMEOUT -> demonitor, reset connection, exit~n", [Pid]),
    %%         io:format("demonitor = ~p~n", [Mref]),
    %%         erlang:demonitor(Mref, [flush]),
    %%         exit(Pid, kill),
    %%         io:format("exit = ~p~n", [Pid]),
    %%         case db_conn:reset_connection(Connection, pass) of
    %%             {error, FailedReset} ->
    %%                 {error, {db_timeout, Timeout, {and_conn_reset_failed, FailedReset}}};
    %%             _ ->
    %%                 {error, {db_timeout, Timeout}}
    %%         end
    end.

get_pool_id(Opts) ->
    case proplists:get_value(pool, Opts) of
        undefined ->
            case db_conn_server:get_default_pool() of
                undefined ->
                    {error, no_default_pool};
                PoolId ->
                    PoolId
            end;
        Pool ->
            Pool
    end.

get_timeout(Opts) ->
    case proplists:get_value(timeout, Opts) of
        undefined ->
            default_timeout();
        Timeout ->
            Timeout
    end.

catch_throw(Mod, Fun, Args) ->
    try
        erlang:apply(Mod, Fun, Args)
    catch
        throw:{error, Error} ->
            {error, Error};
        throw:Err ->
            {error, Err};
        _:E ->
            {error, E}
    end.
