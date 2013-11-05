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
%%% File    : db_driver.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc The driver's APIs.
%%% @end
%%% -------------------------------------------------------------------

-module(db_driver).
-author("deng.lifen (deng.lifen@rytong.com)").
-compile(export_all).
%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("db_driver.hrl").

-define(DEFAULT_MAX_THREAD_LENGTH,  1000).
-define(DEFAULT_MAX_QUEUE,          1000).

%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------
-export([start/0,
         stop/1]).

-export([connect/1,
         disconnect/1,
         execute_sql/2,
         execute_param/3,
         insert/3,
         update/4,
         delete/3,
         select/4,
         trans_execute_sql/3,
         trans_execute_param/4,
         trans_insert/4,
         trans_update/5,
         trans_delete/4,
         trans_select/5,
         transaction/3,
         prepare/3,
         prepare_execute/3,
         unprepare/2]).

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
%% @doc Spawn a process to load db driver lib.
%% @spec start() -> pid()
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

%% @doc Unload db driver lib and stop the process.
%% @spec stop(Pid::pid()) -> ok
stop(Pid) ->
    Pid ! stop.

%% @doc Starts a connection and, if successful, add it to the
%%   connection pool in the driver.
%% @spec connect(ArgTuple::list()) -> {ok, Res} | {error, Err}
connect(ArgTuple) ->
    Args = get_connect_tuple(ArgTuple),
    db_util:command(?DRV_CONNECT, Args).

%% @doc Stops a connection and, if successful, remove it from the
%%   connection pool in the driver.
%% @spec disconnect(ConnPool::binary()) ->
%%    {ok, Res} | {error, Err}
disconnect(ConnPool) ->
    db_util:command(?DRV_DISCONNECT, ConnPool).

%% @doc Send a query to the driver and wait for the result.
%% @spec execute_sql(ConnPool::binary(), String::string()) ->
%%    {ok, Res} | {error, Err}
execute_sql(ConnPool, String) when is_list(String)
    andalso is_binary(ConnPool) ->
    db_util:command(?DRV_EXECUTE, {ConnPool, String}).

%% @doc Send a query with parameters to the driver and wait for the result.
%% @spec execute_param(ConnPool::binary(), Sql::string(), ParamList::list()) ->
%%    {ok, Res} | {error, Err}
execute_param(ConnPool, Sql, ParamList) ->
    db_util:command(?DRV_EXECUTE, {ConnPool, {Sql, ParamList}}).

%% Here the ParamList is a tupleList of format [{Key,Value}].
%%
%% @doc Insert a record into database.
%% @spec insert(ConnPool::binary(), TableName::atom(), ParamList::list()) ->
%%      {ok, Res} | {error, Err}
insert(ConnPool, TableName, ParamList) ->
    db_util:command(?DRV_INSERT, {ConnPool, {TableName, db_util:make_pararmlist(ParamList)}}).


%% @doc Update one or more records from the database,
%%  and return the number of rows updated.
%% @spec update(ConnPool::binary(), TableName::atom(), ParamList::list(),
%%      Where::where_expr()) -> {ok, Res} | {error, Err}
update(ConnPool, TableName, ParamList, Where) ->
    db_util:command(?DRV_UPDATE, {ConnPool, {TableName, db_util:make_pararmlist(ParamList),
        db_util:make_expr(Where)}}).

%% @doc Delete one or more records from the database,
%%  and return the number of rows deleted.
%% @spec delete(ConnPool::binary(), TableName::atom(), Where::where_expr()) ->
%%      {ok, Res} | {error, Err}
delete(ConnPool, TableName, Where) ->
    db_util:command(?DRV_DELETE, {ConnPool, {TableName, db_util:make_expr(Where)}}).

%% @doc Find the records for the Where and Extras expressions.
%%  If no records match the conditions, the function returns {ok, []}.
%% @spec select(ConnPool::binary(), TableList::tables(), Where::where_expr(),
%%      Opts::options()) -> {ok, Res} | {error, Err}
select(ConnPool, TableList, Where, Opts) when is_list(Opts) ->
    FieldList = proplists:get_value(fields, Opts),
    Extras = proplists:get_value(extras, Opts),
    Distinct = proplists:get_value(distinct, Opts),
    Arg = {db_util:is_true(Distinct),
           db_util:make_expr(FieldList),
           db_util:make_expr(TableList),
           db_util:make_expr(Where),
           db_util:make_expr(Extras)},
    db_util:command(?DRV_SELECT, {ConnPool, Arg}).

%% @doc Send a query to the driver in a transaction and wait for the result.
%% @spec trans_execute_sql(ConnPool::binary(), String::list(), Conn::binary())
%%      -> {ok, Res} | {error, Err}
trans_execute_sql(ConnPool, String, Conn) when is_list(String) ->
    db_util:command_t(?DRV_TRANSACTION_EXECUTE, {ConnPool, {Conn, String}}).

%% @doc Send a query with parameters to the driver in a transaction and wait for the result.
%% @spec trans_execute_param(ConnPool::binary(), Sql::list(), ParamList::list(), Conn::binary())
%%      -> {ok, Res} | {error, Err}
trans_execute_param(ConnPool, Sql, ParamList, Conn) ->
    db_util:command_t(?DRV_TRANSACTION_EXECUTE, {ConnPool, {Conn, {Sql, ParamList}}}).

%% @doc Insert one record into the database in a transaction.
%% @spec trans_insert(ConnPool::binary(), TableName::atom(), ParamList::list(),
%%      Conn::binary()) -> {ok, Res} | {error, Err}
trans_insert(ConnPool, TableName, ParamList, Conn) ->
    db_util:command_t(?DRV_TRANSACTION_INSERT, {ConnPool, {Conn,
        {TableName, db_util:make_pararmlist(ParamList)}}}).

%% @doc Update one or more records from the database in a transaction,
%%  and return the number of rows updated.
%% @spec trans_update(ConnPool::binary(), TableName::atom(), ParamList::list(),
%%      Where::where_expr(), Conn::binary()) -> {ok, Res} | {error, Err}
trans_update(ConnPool, TableName, ParamList, Where, Conn) ->
    db_util:command_t(?DRV_TRANSACTION_UPDATE, {ConnPool, {Conn, {TableName,
        db_util:make_pararmlist(ParamList), db_util:make_expr(Where)}}}).

%% @doc Delete one or more records from the database in a transaction,
%%  and return the number of rows deleted.
%% @spec trans_delete(ConnPool::binary(), TableName::atom(),
%%      Where::where_expr(), Conn::binary()) ->
%%      {ok, Res} | {error, Err}
trans_delete(ConnPool, TableName, Where, Conn) ->
    db_util:command_t(?DRV_TRANSACTION_DELETE, {ConnPool, {Conn, {TableName,
        db_util:make_expr(Where)}}}).


%% @doc Find the records for the Where and Extras expressions in a transaction.
%%  If no records match the conditions, the function returns {ok, []}.
%% @spec trans_select(ConnPool::binary(), TableList::tables(),
%%      Where::where_expr(), Conn::binary(), Opts::options()) ->
%%      {ok, Res} | {error, Err}
trans_select(ConnPool, TableList, Where, Conn, Opts) when is_list(Opts) ->
    FieldList = proplists:get_value(fields, Opts),
    Extras = proplists:get_value(extras, Opts),
    Distinct = proplists:get_value(distinct, Opts),
    Arg = {db_util:is_true(Distinct),
           db_util:make_expr(FieldList),
           db_util:make_expr(TableList),
           db_util:make_expr(Where),
           db_util:make_expr(Extras)},
    db_util:command(?DRV_TRANSACTION_SELECT, {ConnPool, {Conn, Arg}}).

%% @doc Execute a transaction.
%% @spec transaction(ConnPool::binary(), Fun::function(), Arg::list()) ->
%%      {ok, Res} | {error, Err}
transaction(ConnPool, Fun, Arg) ->
    case db_util:command(?DRV_TRANSACTION_BEGIN, {ConnPool, "0"}) of
        {error, _} = Err ->
            io:format("Err = ~p~n", [Err]),
            {aborted, Err};
        {ok, ConnId} ->
            case catch Fun(ConnPool, ConnId, Arg) of
                error = Err -> rollback(ConnId, Err, ConnPool);
                {error, _} = Err -> rollback(ConnId, Err, ConnPool);
                {'EXIT', _} = Err -> rollback(ConnId, Err, ConnPool);
                Res ->
                    case db_util:command(?DRV_TRANSACTION_COMMIT, {ConnPool, ConnId}) of
                        {error, _} = Err ->
                            rollback(ConnId, {commit_error, Err}, ConnPool);
                        _ ->
                            case Res of
                                {atomic, _} -> Res;
                                _ -> {atomic, Res}
                            end
                    end
            end
    end.

%% @doc Rollback a transaction.
%% @spec rollback(ConnId::binary(), Err::string(), ConnPool::binary()) ->
%%    {ok, Res} | {error, Err}
rollback(ConnId, Err, ConnPool) ->
    Res = db_util:command(?DRV_TRANSACTION_ROLLBACK, {ConnPool, ConnId}),
    {aborted, {Err, {rollback_result, Res}}}.



%% @doc Register a prepared statement with the dispatcher.
%% @spec prepare(ConnPool::binary(), PrepareName::string(), Sql::string()) ->
%%      {ok, PrepareName} | {error, Err}
prepare(ConnPool, PrepareName, Sql) ->
    db_util:command(?DRV_PREPARE, {ConnPool, {PrepareName, Sql}}).

%% @doc Execute a prepared statement it has prepared.
%%  The function is not safe in multithreading.
%%  Please execute the same prepareName int one process.
%% @spec prepare_execute(ConnPool::binary(), PrepareName::string(),
%%      Value::list()) -> {ok, Res} | {error, Err}
prepare_execute(ConnPool, PrepareName, Value) when is_list(Value) ->
    db_util:command(?DRV_PREPARE_EXECUTE, {ConnPool, {PrepareName, Value}}).


%% @doc Unregister a statement that has previously been register with
%%   the dispatcher. All calls to prepare_execute() with the given statement
%%   will fail once the statement is unprepared. If the statement hasn't
%%   been prepared, nothing happens.
%% @spec unprepare(ConnPool::binary(), PrepareName::string()) -> ok
unprepare(ConnPool, PrepareName) ->
    db_util:command(?DRV_PREPARE_CANCEL, {ConnPool, PrepareName}).

%%--------------------------------------------------------------------
%% Local Functions
%%--------------------------------------------------------------------
get_connect_tuple(Arg) when is_list(Arg) ->
    ArgNames = [driver, host, user, password, database, port, poolsize,
                threadlength, trans_poolsize, maxqueue],
    list_to_tuple(lists:map(
                    fun(driver) ->
                            Driver = proplists:get_value(driver, Arg, ?DEFAULT_DRIVER),
                            drv_code(Driver);
                       (port) ->
                            proplists:get_value(port, Arg, ?DEFAULT_PORT);
                       (trans_poolsize) ->
                            proplists:get_value(trans_poolsize, Arg,
                                                ?DEFAULT_POOLSIZE);
                       (maxqueue) ->
                            proplists:get_value(maxqueue, Arg, ?DEFAULT_MAX_QUEUE);
                       (poolsize) ->
                           proplists:get_value(poolsize, Arg, ?DEFAULT_POOLSIZE);
                       (threadlength) ->
                            case proplists:get_value(threadlength, Arg) of
                                undefined ->
                                    ?DEFAULT_POOLSIZE;
                                ThreadLength ->
                                    ThreadLength
                            end;
                       (Key) ->
                            case proplists:get_value(Key, Arg) of
                                undefined ->
                                    throw(lists:concat([Key, " can not be empty!"]));
                                Value ->
                                    Value
                            end
                    end, ArgNames)).
