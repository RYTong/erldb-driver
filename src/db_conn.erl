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
%%% File    : db_conn.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc The driver's APIs.
%%%
%%% @end
%%% -------------------------------------------------------------------

-module(db_conn).

-export([
    start/0,
    stop/1,
    init/1,
    add_pool/2,
    remove_pool/1,
    increment_pool_size/2,
    decrement_pool_size/2,
    reset_connection/2,
    close_connection/1,
    connect/1,
    disconnect/1,
    execute_sql/2,
    execute_param/3,
    insert/3,
    update/4,
    delete/3,
    select/4,
    transaction/2,
    prepare/2,
    prepare_execute/3
]).

-export_type([
    driver_name/0,
    conn_args/0,
    tables/0,
    fields/0,
    where_expr/0,
    extras/0,
    mybool/0,
    param_list/0,
    options/0,
    throw/0,
    result/0
]).

-include("db_driver.hrl").

-type driver_name() :: mysql | oracle | db2 | sybase | informix.
-type conn_arg()
    :: {default_pool, boolean()}
    | {driver, driver_name()}
    | {host, string()}
    | {port, integer()}
    | {user, string()}
    | {password, string()}
    | {database, string()}
    | {poolsize, integer()}
    | {table_info, boolean()}
    | {default_pool, boolean()}.
-type conn_args() :: [conn_arg()].
-type tables() :: atom() | [atom()].
-type fields() :: atom() | [atom()].
-type where_expr() :: tuple() | [tuple()].
-type extras() :: tuple() | [tuple()].
-type mybool() :: boolean() | 1 | 0.
-type param_list() :: [{Key::atom(), Value::term()}].
-type option()
    :: {pool, atom()}
    | {fields, fields()}
    | {extras, extras()}
    | {distinct, mybool()}.
-type options() :: [option()].
-type throw() :: {error, Error::term()}.
-type result() :: {ok, Res::term()} | {error, Error::term()}.

%% ====================================================================
%% External functions
%% ====================================================================

%%------------------------------------------------------------------------------
%% @doc Spawn a process to load database driver library.
%% @end
%%------------------------------------------------------------------------------
-spec start() -> pid().
start() ->
    Path = db_util:check_lib_path(),
    Pid = proc_lib:spawn_link(db_util, load, [Path, self()]),
    receive
        failed ->
            {error, load_driver_failed};
        done ->
            Pid
        after 3000 ->
            {error, timeout}
    end.

%%------------------------------------------------------------------------------
%% @doc Unload database driver library and stop the process.
%% @end
%%------------------------------------------------------------------------------
-spec stop(Pid::pid()) -> ok.
stop(Pid) ->
    Pid ! stop.

%%------------------------------------------------------------------------------
%% @doc Starts a connection and, if successful, add it to the
%% connection pool in the driver.
%% @end
%%------------------------------------------------------------------------------
-spec connect(ConnArgs::conn_args()) -> result().
connect(ConnArgs) ->
    NewConnArgs = ConnArgs#conn_args{
        driver = db_util:drv_code(ConnArgs#conn_args.driver)},
    db_util:command(?DRV_CONNECT_DB, NewConnArgs).

%% @doc Stops a connection and, if successful, remove it from the
%%   connection pool in the driver.
-spec disconnect(Connection::record() | record()) -> result().
disconnect(Connection) ->
    Conn = get_conn(Connection),
    db_util:command(?DRV_DISCONNECT_DB, Conn).

%%------------------------------------------------------------------------------
%% @doc Initializes the length of the thread pool.
%% @end
%%------------------------------------------------------------------------------
-spec init(ThreadLen::integer()) -> ok.
init(ThreadLen) ->
    db_util:command(?DRV_INIT_DB, ThreadLen).

%%------------------------------------------------------------------------------
%% @doc Send a query to the driver and wait for the result.
%% @end
%%------------------------------------------------------------------------------
-spec execute_sql(Connection::record(), Sql::string()) -> result().
execute_sql(Connection, Sql) when is_list(Sql) ->
    Conn = get_conn(Connection),
    db_util:command(?DRV_EXECUTE_DB, {Conn, Sql}).

%%------------------------------------------------------------------------------
%% @doc Send a query with parameters to the driver and wait for the result.
%% @end
%%------------------------------------------------------------------------------
-spec execute_param(Connection::record(), Statement::string(), ParamList::param_list()) ->
    result().
execute_param(Connection, Statement, []) ->
    execute_sql(Connection, Statement);
execute_param(Connection, Statement, ParamList) ->
    Conn = get_conn(Connection),
    {ok, StmtData} = db_util:command(?DRV_PREPARE_DB, {Conn, Statement}),
    Res = db_util:command(?DRV_PREPARE_EXECUTE_DB, {Conn, {StmtData, ParamList}}),
    {ok, _} = db_util:command(?DRV_PREPARE_CANCEL_DB, {Conn, StmtData}),
    Res.

%%------------------------------------------------------------------------------
%% @doc Insert a record into database.
%% @end
%%------------------------------------------------------------------------------
-spec insert(Connection::record(), TableName::atom(), ParamList::param_list()) ->
     result().
insert(Connection, TableName, ParamList) ->
    Conn = get_conn(Connection),
    db_util:command(?DRV_INSERT_DB, {Conn, {TableName, db_util:make_pararmlist(ParamList)}}).

%%------------------------------------------------------------------------------
%% @doc Update one or more records from the database,
%% and return the number of rows updated.
%% @end
%%------------------------------------------------------------------------------
-spec update(Connection::record(), TableName::atom(), ParamList::param_list(),
     Where::where_expr()) -> result().
update(Connection, TableName, ParamList, Where) ->
    Conn = get_conn(Connection),
    db_util:command(?DRV_UPDATE_DB, {Conn, {TableName, db_util:make_pararmlist(ParamList),
        db_util:make_expr(Where)}}).

%%------------------------------------------------------------------------------
%% @doc Delete one or more records from the database,
%% and return the number of rows deleted.
%% @end
%%------------------------------------------------------------------------------
-spec delete(Connection::record(), TableName::atom(), Where::where_expr()) ->
     result().
delete(Connection, TableName, Where) ->
    Conn = get_conn(Connection),
    db_util:command(?DRV_DELETE_DB, {Conn, {TableName, db_util:make_expr(Where)}}).

%%------------------------------------------------------------------------------
%% @doc Find the records for the Where and Extras expressions.
%%
%% If no records match the conditions, the function returns {ok, []}.
%% @end
%%------------------------------------------------------------------------------
-spec select(Connection::record(), TableList::tables(), Where::where_expr(),
     Opts::options()) -> result().
select(Connection, TableList, Where, Opts) when is_list(Opts) ->
    Conn = get_conn(Connection),
    FieldList = proplists:get_value(fields, Opts),
    Extras = proplists:get_value(extras, Opts),
    Distinct = proplists:get_value(distinct, Opts),
    Arg = {db_util:is_true(Distinct),
           db_util:make_expr(FieldList),
           db_util:make_expr(TableList),
           db_util:make_expr(Where),
           db_util:make_expr(Extras)},
    db_util:command(?DRV_SELECT_DB, {Conn, Arg}).

%%------------------------------------------------------------------------------
%% @doc Execute a transaction.
%% @end
%%------------------------------------------------------------------------------
-spec transaction(PoolId::atom(), Fun::function()) -> result().
transaction(PoolId, Fun) ->
    Connection = db_conn_server:wait_for_connection(PoolId),
    erlang:put(?TRANSCATION_CONNECTION, Connection),
    Conn = get_conn(Connection),
    Res = (catch do_transaction(Conn, Fun)),
    db_conn_server:pass_connection(Connection),
    erlang:erase(?TRANSCATION_CONNECTION),
    Res.

do_transaction(Conn, Fun) ->
    case trans_begin(Conn) of
        {error, _} = Err ->
            io:format("Err = ~p~n", [Err]),
            {aborted, Err};
        {ok, _} ->
            case catch Fun() of
                error = Err -> trans_rollback(Conn, Err);
                {error, _} = Err -> trans_rollback(Conn, Err);
                {'EXIT', _} = Err -> trans_rollback(Conn, Err);
                Res ->
                    case trans_commit(Conn) of
                        {error, _} = Err ->
                            trans_rollback(Conn, {commit_error, Err});
                        _ ->
                            case Res of
                                {atomic, _} -> Res;
                                _ -> {atomic, Res}
                            end
                    end
            end
    end.

trans_begin(Conn) ->
    db_util:command(?DRV_TRANS_BEGIN_DB, {Conn, 0}).

trans_commit(Conn) ->
    db_util:command(?DRV_TRANS_COMMIT_DB, {Conn, 1}).

%% @doc Rollback a transaction.
-spec trans_rollback(Conn::binary(), Err::string()) ->
   result().
trans_rollback(Conn, Err) ->
    Res = db_util:command(?DRV_TRANS_ROLLBACK_DB, {Conn, 2}),
    {aborted, {Err, {rollback_result, Res}}}.

%% @doc Register a prepared statement.
%% @end
-spec prepare(Connection::record(), Statement::string()) -> result().
prepare(Connection, Statement) ->
    Conn = get_conn(Connection),
    db_util:command(?DRV_PREPARE_DB, {Conn, Statement}).

%%------------------------------------------------------------------------------
%% @doc Execute a prepared statement it has prepared.
%% @end
%%------------------------------------------------------------------------------
-spec prepare_execute(Connection::record(), StmtName::string(), Args::list()) ->
    result().
prepare_execute(Connection, StmtName, Args) when is_atom(StmtName), is_list(Args) ->
    Conn = get_conn(Connection),
    StmtData = prepare_statement(Connection, StmtName),
    db_util:command(?DRV_PREPARE_EXECUTE_DB, {Conn, {StmtData, Args}}).

%%------------------------------------------------------------------------------
%% @doc Deallocate all prepared statements on a connection.
%% @end
%%------------------------------------------------------------------------------
unprepare(Connection) when is_record(Connection, db_connection) ->
    Conn = get_conn(Connection),
    [db_util:command(?DRV_PREPARE_CANCEL_DB, {Conn, StmtData}) || StmtData <- db_stmt:remove(Conn)].

%%------------------------------------------------------------------------------
%% @doc Synchronous call to the connection manager to add a pool.
%%
%% Creates a pool record, opens n=Size connections and calls
%% db_conn_server:add_pool/1 to make the pool known to the pool management,
%% it is translated into a blocking gen-server call.
%% @end
%%------------------------------------------------------------------------------
-spec add_pool(PoolId::atom(), ConnArg::record()) -> ok | no_return().
add_pool(PoolId, {ConnArgs, ErrorHandler}) when is_record(ConnArgs, conn_args) ->
    Pool = #pool{
        id = PoolId,
        conn_args = ConnArgs,
        size = ConnArgs#conn_args.poolsize,
        error_handler = ErrorHandler
    },
    Pool1 = open_connections(Pool),
    %% io:format("Pool1 = ~p~n", [Pool1]),

    db_conn_server:add_pool(Pool1),

    case ConnArgs#conn_args.table_info of
        true ->
            set_table_schema(PoolId);
        _ ->
            ok
    end,

    case ConnArgs#conn_args.default_pool of
        true ->
            db_conn_server:set_default_pool(PoolId);
        _ ->
            ok
    end.

set_table_schema(PoolId) ->
    TableInfos = db_api:get_table_schemas(PoolId),
    db_conn_server:set_table_schema(PoolId, TableInfos).

%%------------------------------------------------------------------------------
%% @doc Synchronous call to the connection manager to remove a pool.
%% @end
%%------------------------------------------------------------------------------
-spec remove_pool(PoolId::atom()) -> ok | no_return().
remove_pool(PoolId) ->
    Pool = db_conn_server:remove_pool(PoolId),
    [close_connection(Conn) || Conn <- lists:append(queue:to_list(Pool#pool.available), gb_trees:values(Pool#pool.locked))],
    ok.

%%------------------------------------------------------------------------------
%% @doc Synchronous call to the connection manager to enlarge a pool.
%%
%% This opens n=Num new connections and adds them to the pool of id PoolId.
%% @end
%%------------------------------------------------------------------------------
-spec increment_pool_size(PoolId::atom(), Num::integer()) -> ok | no_return().
increment_pool_size(PoolId, Num) when is_integer(Num) ->
    Conns = open_n_connections(PoolId, Num),
    db_conn_server:add_connections(PoolId, Conns).

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
decrement_pool_size(PoolId, Num) when is_integer(Num) ->
    Conns = db_conn_server:remove_connections(PoolId, Num),
    [close_connection(Conn) || Conn <- Conns],
    ok.

%%------------------------------------------------------------------------------
%% @doc Create n connections.
%% @end
%%------------------------------------------------------------------------------
open_n_connections(PoolId, N) ->
    %% io:format("open ~p connections for pool ~p~n", [N, PoolId]),
    Pool = db_conn_server:get_pool(PoolId),
    lists:foldl(
        fun(_ ,Connections) ->
            %% Catch {'EXIT',_} errors so newly opened connections are not orphaned.
            case catch open_connection(Pool) of
                #db_connection{} = Connection ->
                    [Connection | Connections];
                _ ->
                    Connections
            end
        end, [], lists:seq(1, N)).

%%------------------------------------------------------------------------------
%% @doc According to the connection pool parameters, create multiple connections.
%% @end
%%------------------------------------------------------------------------------
open_connections(Pool) ->
    %% io:format("open connections loop: .. "),
    case (queue:len(Pool#pool.available) + gb_trees:size(Pool#pool.locked)) < Pool#pool.size of
        true ->
            %% io:format(" continues~n"),
            Conn = open_connection(Pool),
            %% io:format("opened connection: ~p~n", [Conn]),
            open_connections(Pool#pool{available = queue:in(Conn, Pool#pool.available)});
        false ->
            %% io:format(" done~n"),
            Pool
    end.

%%------------------------------------------------------------------------------
%% @doc Create a connection.
%% @end
%%------------------------------------------------------------------------------
open_connection(#pool{id = PoolId, conn_args = ConnArgs}) ->
    case connect(ConnArgs) of
        {ok, Conn} ->
            Connection = #db_connection{
                id = Conn,
                pool_id = PoolId,
                driver = ConnArgs#conn_args.driver
            },
            Connection;
        {error, Reason} ->
             io:format("~p open connection: ... ERROR ~p~n", [self(), Reason]),
             io:format("~p open connection: ... exit with failed_to_connect_to_database~n", [self()]),
            throw({error, {failed_to_connect_to_database, Reason}});
        What ->
             %% io:format("~p open connection: ... UNKNOWN ERROR ~p~n", [self(), What]),
            throw({error, {unknow_failed_to_connect_to_database, What}})
    end.

%%------------------------------------------------------------------------------
%% @doc Reset the connection.
%%
%% if a process dies or times out while doing work, the connection reset
%% in the conn_server state. Also a new connection needs to be opened to
%% replace the old one. If that fails, we queue the old as available for
%% the next try by the next caller process coming along. So the pool can't
%% run dry, even though it can freeze.
%% @end
%%------------------------------------------------------------------------------
-spec reset_connection(Conn::record(), StayLocked::pass | keep) ->
    record() | {error, Error::term()}.
reset_connection(Conn, StayLocked) ->
    spawn(fun() -> close_connection(Conn) end),
    Pool = db_conn_server:get_pool(Conn#db_connection.pool_id),
    %% io:format("... open new connection to renew~n"),
    case catch open_connection(Pool) of
        NewConn when is_record(NewConn, db_connection) ->
            %% io:format("... got it, replace old (~p)~n", [StayLocked]),
            case StayLocked of
                pass -> db_conn_server:replace_connection_as_available(Conn, NewConn);
                keep -> db_conn_server:replace_connection_as_locked(Conn, NewConn)
            end,
            %% io:format("... done, return new connection~n"),
            NewConn;
        Error ->
            DeadConn = Conn#db_connection{alive=false},
            db_conn_server:replace_connection_as_available(Conn, DeadConn),
            %% io:format("... failed to re-open. Shelving dead connection as available.~n"),
            {error, {cannot_reopen_in_reset, Error}}
    end.

%%------------------------------------------------------------------------------
%% @doc Deallocate all prepared statements and close the connection.
%% @end
%%------------------------------------------------------------------------------
close_connection(Connection) ->
    %% Deallocate prepared statements.
    unprepare(Connection),
    disconnect(Connection).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
get_conn(Connection) when is_record(Connection, db_connection) ->
    Connection#db_connection.id;
get_conn(Conn) ->
    Conn.

prepare_statement(Connection, StmtName) ->
    Conn = get_conn(Connection),
    case db_stmt:fetch(StmtName) of
        undefined ->
            throw({error, statement_has_not_been_prepared});
        Statement ->
            case db_stmt:stmtdata(Conn, StmtName) of
                undefined ->
                    {ok, StmtData} = prepare(Connection, Statement),
                    db_stmt:prepare(Conn, StmtName, StmtData),
                    StmtData;
                StmtData1 ->
                    StmtData1
            end
    end.

