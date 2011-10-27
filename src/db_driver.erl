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

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("db_driver.hrl").

%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------
-export([start/0,
         start/1,
         stop/1,
         drv_number/1,
         load/1]).

-export([connect/1,
         disconnect/1,
         execute_sql/2,
         insert/3,
         update/4,
         delete/3,
         select/4,
         trans_execute_sql/3,
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
%% @doc Call start/1, use DATABASE_LIB_DIR as PrivPath.
%% @see start/1
%% @spec start() -> pid()
start() ->
    start(?DATABASE_LIB_DIR).

%% @doc Spawn a process to load db driver lib.
%% @spec start(DrvPath::string()) -> pid()
start(DrvPath) ->
    Path = check_lib_path(DrvPath),
    Pid = proc_lib:spawn_link(?MODULE, load, [Path]),
    timer:sleep(2000),
    Pid.

%% @doc Unload db driver lib and stop the process.
%% @spec stop(Pid::pid()) -> ok
stop(Pid) ->
    Pid ! stop.

%% @doc Start a connection and, if successful, add it to the
%%   connection pool in the driver.
%% @spec connect(ArgTuple::list()) -> {ok, Res} | {error, Err}
connect(ArgTuple) ->
    Args = get_connect_tuple(ArgTuple),
    command(?DRV_CONNECT, Args).

%% @doc Stop a connection and, if successful, remove it from the
%%   connection pool in the driver.
%% @spec disconnect(ConnPool::binary()) ->
%%    {ok, Res} | {error, Err}
disconnect(ConnPool) ->
    command(?DRV_DISCONNECT, ConnPool).

%% @doc Send a query to the driver and wait for the result.
%% @spec execute_sql(ConnPool::binary(), String::string()) ->
%%    {ok, Res} | {error, Err}
execute_sql(ConnPool, String) when is_list(String) 
    andalso is_binary(ConnPool) ->
    command(?DRV_EXECUTE, {ConnPool, String}).

%% Here the ParamList is a tupleList of format [{Key,Value}].
%%
%% @doc Insert a record into database.
%% @spec insert(ConnPool::binary(), TableName::atom(), ParamList::list()) -> 
%%      {ok, Res} | {error, Err}
insert(ConnPool, TableName, ParamList) ->
    command(?DRV_INSERT, {ConnPool, {TableName, make_pararmlist(ParamList)}}).


%% @doc Update one or more records from the database,
%%  and return the number of rows updated.
%% @spec update(ConnPool::binary(), TableName::atom(), ParamList::list(), 
%%      Where::where_expr()) -> {ok, Res} | {error, Err} 
update(ConnPool, TableName, ParamList, Where) ->
    command(?DRV_UPDATE, {ConnPool, {TableName, make_pararmlist(ParamList),
        make_expr(Where)}}).

%% @doc Delete one or more records from the database,
%%  and return the number of rows deleted.
%% @spec delete(ConnPool::binary(), TableName::atom(), Where::where_expr()) -> 
%%      {ok, Res} | {error, Err}
delete(ConnPool, TableName, Where) ->
    command(?DRV_DELETE, {ConnPool, {TableName, make_expr(Where)}}). 

%% @doc Find the records for the Where and Extras expressions.
%%  If no records match the conditions, the function returns {ok, []}.
%% @spec select(ConnPool::binary(), TableList::tables(), Where::where_expr(), 
%%      Opts::options()) -> {ok, Res} | {error, Err}
select(ConnPool, TableList, Where, Opts) when is_list(Opts) ->
    FieldList = proplists:get_value(fields, Opts),
    Extras = proplists:get_value(extras, Opts),
    Distinct = proplists:get_value(distinct, Opts),
    Arg = {is_true(Distinct), 
           make_expr(FieldList), 
           make_expr(TableList), 
           make_expr(Where), 
           make_expr(Extras)},
    command(?DRV_SELECT, {ConnPool, Arg}).

%% @doc Send a query to the driver in a transaction and wait for the result.
%% @spec trans_execute_sql(ConnPool::binary(), String::list(), Conn::binary()) 
%%      -> {ok, Res} | {error, Err}
trans_execute_sql(ConnPool, String, Conn) when is_list(String) ->
    command_t(?DRV_TRANSACTION_EXECUTE, {ConnPool, {Conn, String}}).

%% @doc Insert one record into the database in a transaction.
%% @spec trans_insert(ConnPool::binary(), TableName::atom(), ParamList::list(), 
%%      Conn::binary()) -> {ok, Res} | {error, Err}
trans_insert(ConnPool, TableName, ParamList, Conn) ->
    command_t(?DRV_TRANSACTION_INSERT, {ConnPool, {Conn, 
        {TableName, ParamList}}}).

%% @doc Update one or more records from the database in a transaction,
%%  and return the number of rows updated.
%% @spec trans_update(ConnPool::binary(), TableName::atom(), ParamList::list(), 
%%      Where::where_expr(), Conn::binary()) -> {ok, Res} | {error, Err}
trans_update(ConnPool, TableName, ParamList, Where, Conn) ->
    command_t(?DRV_TRANSACTION_UPDATE, {ConnPool, {Conn, {TableName,
        ParamList, make_expr(Where)}}}).

%% @doc Delete one or more records from the database in a transaction,
%%  and return the number of rows deleted.
%% @spec trans_delete(ConnPool::binary(), TableName::atom(), 
%%      Where::where_expr(), Conn::binary()) ->
%%      {ok, Res} | {error, Err}
trans_delete(ConnPool, TableName, Where, Conn) ->
    command_t(?DRV_TRANSACTION_DELETE, {ConnPool, {Conn, {TableName,
        make_expr(Where)}}}).


%% @doc Find the records for the Where and Extras expressions in a transaction.
%%  If no records match the conditions, the function returns {ok, []}.
%% @spec trans_select(ConnPool::binary(), TableList::tables(), 
%%      Where::where_expr(), Conn::binary(), Opts::options()) -> 
%%      {ok, Res} | {error, Err}
trans_select(ConnPool, TableList, Where, Conn, Opts) when is_list(Opts) ->
    FieldList = proplists:get_value(fields, Opts),
    Extras = proplists:get_value(extras, Opts),
    Distinct = proplists:get_value(distinct, Opts),
    Arg = {is_true(Distinct), 
           make_expr(FieldList), 
           make_expr(TableList), 
           make_expr(Where), 
           make_expr(Extras)},
    command(?DRV_TRANSACTION_SELECT, {ConnPool, {Conn, Arg}}).  

%% @doc Execute a transaction.
%% @spec transaction(ConnPool::binary(), Fun::function(), Arg::list()) ->
%%      {ok, Res} | {error, Err}
transaction(ConnPool, Fun, Arg) ->
    case command(?DRV_TRANSACTION_BEGIN, {ConnPool, "0"}) of
        {error, _} = Err ->
            io:format("Err = ~p~n", [Err]),
            {aborted, Err};
        {ok, ConnId} ->
            case catch Fun(ConnPool, ConnId, Arg) of
                error = Err -> rollback(ConnId, Err, ConnPool);
                {error, _} = Err -> rollback(ConnId, Err, ConnPool);
                {'EXIT', _} = Err -> rollback(ConnId, Err, ConnPool);
                Res ->
                    case command(?DRV_TRANSACTION_COMMIT, {ConnPool, ConnId}) of
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
    Res = command(?DRV_TRANSACTION_ROLLBACK, {ConnPool, ConnId}),
    {aborted, {Err, {rollback_result, Res}}}.



%% @doc Register a prepared statement with the dispatcher.
%% @spec prepare(ConnPool::binary(), PrepareName::string(), Sql::string()) ->
%%      {ok, PrepareName} | {error, Err}
prepare(ConnPool, PrepareName, Sql) ->
    command(?DRV_PREPARE, {ConnPool, {PrepareName, Sql}}).

%% @doc Execute a prepared statement it has prepared.
%%  The function is not safe in multithreading.
%%  Please execute the same prepareName int one process.
%% @spec prepare_execute(ConnPool::binary(), PrepareName::string(), 
%%      Value::list()) -> {ok, Res} | {error, Err}
prepare_execute(ConnPool, PrepareName, Value) when is_list(Value) ->
    command(?DRV_PREPARE_EXECUTE, {ConnPool, {PrepareName, Value}}).


%% @doc Unregister a statement that has previously been register with
%%   the dispatcher. All calls to prepare_execute() with the given statement
%%   will fail once the statement is unprepared. If the statement hasn't
%%   been prepared, nothing happens.
%% @spec unprepare(ConnPool::binary(), PrepareName::string()) -> ok
unprepare(ConnPool, PrepareName) ->
    command(?DRV_PREPARE_CANCEL, {ConnPool, PrepareName}).



%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------
%% @doc Load db driver lib.
%% @spec load(DrvPath::string()) -> none()
load(DrvPath) ->
    Start = now(),
    case erl_ddll:load_driver(DrvPath, ?DATABASE_LIB) of
        {error, Reason} when Reason /= already_loaded ->
            error_logger:format("failed reason to load database driver: ~p~n",
                [erl_ddll:format_error(Reason)]);
        _ ->
            error_logger:format("Loading time of thread pool : ~10.2f ms~n",
                [timer:now_diff(now(), Start) / 1000]),
            Port = open_port({spawn, ?DATABASE_LIB}, [binary]),
            loop(Port)
    end.

loop(Port) ->
    io:format("in loop now",[]),
    receive
        Msg ->
            io:format("the msg:~p~n",[Msg]),
            port_close(Port),
            unload()
    end.

check_lib_path(Path) ->
    case filelib:is_file(lists:concat([Path, "/", ?DATABASE_LIB, ".so"])) of
        true ->
            Path;
        false ->
            case code:priv_dir(?APPNAME) of
                {error, bad_name} ->
                    throw(lists:concat(["Can not find ", ?DATABASE_LIB]));
                PrivPath ->
                    PrivPath
            end
    end.

unload() ->
    erl_ddll:unload_driver(?DATABASE_LIB).

command_t(Cmd, Data) ->
    case command(Cmd, Data) of
        {ok, Res} ->
            {ok, Res};
        {error, Err} ->
            throw({error, Err})
    end.

command(Cmd, Data) ->
    case new_port() of
        fail ->
            {error, "failed to open port"};
        Port ->
            port_control(Port, Cmd, term_to_binary(Data)),
            Res = wait_for_res(Port),
            port_close(Port),
            Res
    end.

new_port() ->
    try
        open_port({spawn_driver, ?DATABASE_LIB},[binary])
    catch
        Type:einval ->
            error_logger:info_msg("error open port. ~p: einval~n", [Type]),
            new_port();
        Type1:Err ->
            error_logger:info_msg("error open port. ~p: ~p~n", [Type1, Err]),
            fail
    end.

wait_for_res(Port) ->
    receive
        {Port, {data, Data}} ->
            binary_to_term(Data)
    end.

get_connect_tuple(Arg) when is_list(Arg) ->
    ArgNames = [driver, host, user, password, database, port, poolsize,
        threadlength, maxthreadlength, maxqueue],
    list_to_tuple(lists:map(
        fun(driver) ->
            Driver = proplists:get_value(driver, Arg, ?DEFAULT_DRIVER),
            drv_number(Driver);
        (port) ->
            proplists:get_value(port, Arg, ?DEFAULT_PORT);
        (maxthreadlength) ->
            proplists:get_value(maxthreadlength, Arg,
                ?DEFAULT_MAX_THREAD_LENGTH);
        (maxqueue) ->
            proplists:get_value(maxqueue, Arg, ?DEFAULT_MAX_QUEUE);
        (threadlength) ->
            case proplists:get_value(threadlength, Arg) of
                undefined ->
                    proplists:get_value(poolsize, Arg);
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

%% fixme:should add DB2 and Sybase
%% @doc Transform driver name to driver code.
%% @spec drv_number(Drv::atom()) -> integer() | Drv
drv_number(mysql) ->
    ?MYSQL_DB;
drv_number(oracle) ->
    ?ORACLE_DB;
drv_number(sybase) ->
    ?SYBASE_DB;
drv_number(Drv) when is_integer(Drv) ->
    Drv.

is_true(true) ->
    1;
is_true(D) when is_integer(D), D > 0 ->
    1;
is_true(_)->
    0.

make_pararmlist(ParamList) ->
    make_pararmlist(ParamList, []).
make_pararmlist([], Acc) ->
    Acc;
make_pararmlist([{_Column, []}|ParamList], Acc) ->
    make_pararmlist(ParamList, Acc);
make_pararmlist([{_Column, undefined}|ParamList], Acc) ->
    make_pararmlist(ParamList, Acc);
make_pararmlist([{Column, Value}|ParamList], Acc) ->
    make_pararmlist(ParamList, [{Column, Value}|Acc]).

make_expr(undefined) ->
    0;
make_expr([]) ->
    0;
make_expr(0) ->
    0;
make_expr(A) when is_atom(A) ->
    [A];
make_expr(A) when is_tuple(A) ->
    [expr(A)];
make_expr(A) when is_list(A) ->
    [expr(X) || X <- A].

expr({'and', Expr}) ->
    {get_keyword_value('and'), make_expr(Expr)};
expr({Expr1, 'or', Expr2}) ->
    {get_keyword_value('or'), expr(Expr1), expr(Expr2)};
expr({Expr1, 'between', {Expr2, Expr3}}) ->
    {get_keyword_value('between'), expr(Expr1), expr(Expr2), expr(Expr3)};

expr({'not', Expr}) ->
    {get_keyword_value('not'), expr(Expr)};
expr({Expr1, 'like', Expr2}) ->
    {get_keyword_value('like'), expr(Expr1), expr(Expr2)};

expr({Expr1, 'as', Expr2}) ->
    {get_keyword_value('as'), expr(Expr1), expr(Expr2)};
expr({Expr1, '.', Expr2}) ->
    {get_keyword_value('.'), Expr1, Expr2};

expr({Expr1, '=', Expr2}) ->
    {get_keyword_value('='), expr(Expr1), expr(Expr2)};
expr({Expr1, '>', Expr2}) ->
    {get_keyword_value('>'), expr(Expr1), expr(Expr2)};
expr({Expr1, '>=', Expr2}) ->
    {get_keyword_value('>='), expr(Expr1), expr(Expr2)};
expr({Expr1, '<', Expr2}) ->
    {get_keyword_value('<'), expr(Expr1), expr(Expr2)};
expr({Expr1, '<=', Expr2}) ->
    {get_keyword_value('<='), expr(Expr1), expr(Expr2)};
expr({Expr1, '!=', Expr2}) ->
    {get_keyword_value('!='), expr(Expr1), expr(Expr2)};
expr({Expr1, '+', Expr2}) ->
    {get_keyword_value('+'), expr(Expr1), expr(Expr2)};
expr({Expr1, '-', Expr2}) ->
    {get_keyword_value('-'), expr(Expr1), expr(Expr2)};
expr({Expr1, '*', Expr2}) ->
    {get_keyword_value('*'), expr(Expr1), expr(Expr2)};
expr({Expr1, '/', Expr2}) ->
    {get_keyword_value('/'), expr(Expr1), expr(Expr2)};

expr({'order', {Expr, 'asc'}}) ->
    {get_keyword_value('order'), [{expr(Expr), 0}]};
expr({'order', {Expr, 'desc'}}) ->
    {get_keyword_value('order'), [{expr(Expr), 1}]};
expr({'order', Expr}) when is_tuple(Expr) ->
    {get_keyword_value('order'), [{expr(Expr), 0}]};
expr({'order', Expr}) when is_atom(Expr) ->
    {get_keyword_value('order'), [{Expr, 0}]};
expr({'order', Expr}) when is_list(Expr) ->
    {get_keyword_value('order'), make_expr(Expr)};

expr({'limit', M, N}) ->
    {get_keyword_value('limit'), M, N};
expr({'limit', N}) ->
    {get_keyword_value('limit'), 0, N};

expr({'group', Expr}) when is_atom(Expr) ->
    {get_keyword_value('group'), [Expr]};
expr({'group', Expr}) when is_tuple(Expr) ->
    {get_keyword_value('group'), [expr(Expr)]};
expr({'group', Expr}) when is_list(Expr) ->
    {get_keyword_value('group'), make_expr(Expr)};

expr({'having', Expr}) ->
    {get_keyword_value('having'), expr(Expr)};

expr({Table1, 'join', Table2, On}) ->
    {get_keyword_value('join'), expr(Table1), expr(Table2), expr(On)};
expr({Table1, 'left join', Table2, On}) ->
    {get_keyword_value('left join'), expr(Table1), expr(Table2), expr(On)};
expr({Table1, 'right join', Table2, On}) ->
    {get_keyword_value('right join'), expr(Table1), expr(Table2), expr(On)};
expr({Table1, 'inner join', Table2, On}) ->
    {get_keyword_value('inner join'), expr(Table1), expr(Table2), expr(On)};

expr({Fun, Arg}) when is_atom(Fun) andalso is_list(Arg) ->
    {get_keyword_value('function'), Fun, make_expr(Arg)};

expr({Expr, 'in', List}) when is_list(List) ->
    {get_keyword_value('in'), expr(Expr), List};

expr(Expr) ->
    Expr.

get_keyword_value(Key) when is_atom(Key) ->
    case proplists:get_value(Key, keyword_mapping()) of
        undefined ->
            throw(lists:concat(["Not support keyword named: ", Key]));
        Value ->
            Value
    end.

%% Mapping enum SqlKeyword in DBOperation.h.
keyword_mapping() ->
    [{'and',        0},
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
     {'in',         26}].
