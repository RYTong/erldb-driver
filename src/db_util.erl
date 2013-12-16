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
%%% File    : db_util.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc The public library.
%%%
%%% @end
%%% ----------------------------------------------------------------------------
-module(db_util).
-author('deng.lifen@rytong.com').

%%------------------------------------------------------------------------------
%% Exported Functions
%%------------------------------------------------------------------------------
-export([
    load/2,
    check_lib_path/0,
    command/2,
    command_t/2,
    get_table_schemas/4,
    drv_code/1,
    drv_name/1,
    is_true/1,
    make_pararmlist/1,
    make_expr/1
]).

%%------------------------------------------------------------------------------
%% Include files
%%------------------------------------------------------------------------------
-include("db_driver.hrl").

-define(DATABASE_LIB,               "database_drv").
-define(DATABASE_LIB_DIR,           "priv").
-define(DB_API_MOD,                 db_app).

%%------------------------------------------------------------------------------
%% API Functions
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% @doc Load database driver library, and then loop waiting to the message of
%% the process to stop.
%% @end
%%------------------------------------------------------------------------------
-spec load(DrvPath::string(), Parent::pid()) -> none().
load(DrvPath, Parent) ->
    Start = now(),
    case erl_ddll:load_driver(DrvPath, ?DATABASE_LIB) of
        {error, Reason} when Reason /= already_loaded ->
            error_logger:format("failed reason to load database driver: ~p~n",
                [erl_ddll:format_error(Reason)]),
            Parent ! failed;
        _ ->
            error_logger:format("Loading time of thread pool : ~10.2f ms~n",
                [timer:now_diff(now(), Start) / 1000]),
            Port = open_port({spawn, ?DATABASE_LIB}, [binary]),
            Parent ! done,
            loop(Port)
    end.

%%------------------------------------------------------------------------------
%% @doc Check the library path is exist.
%% @end
%%------------------------------------------------------------------------------
-spec check_lib_path() -> string() | no_return().
check_lib_path() ->
    {_, _, ModPath} = code:get_object_code(?MODULE),
    Path = filename:join(filename:dirname(filename:dirname(ModPath)), ?DATABASE_LIB_DIR),
    case filelib:is_file(lists:concat([Path, "/", ?DATABASE_LIB, ".so"])) of
        true ->
            Path;
        false ->
            case code:priv_dir(?DB_APP_NAME) of
                {error, bad_name} ->
                    throw(lists:concat(["Can not find ", ?DATABASE_LIB]));
                PrivPath ->
                    PrivPath
            end
    end.

%%------------------------------------------------------------------------------
%% @doc Send command to database library.
%% If responce is {error, Error}, throw it.
%% @end
%%------------------------------------------------------------------------------
-spec command_t(Cmd::integer(), Data::term()) -> ReturnType::term() | no_return().
command_t(Cmd, Data) ->
    case command(Cmd, Data) of
        {ok, Res} ->
            {ok, Res};
        {error, Err} ->
            throw({error, Err})
    end.

%%------------------------------------------------------------------------------
%% @doc Send command to database library.
%% @end
%%------------------------------------------------------------------------------
-spec command(Cmd::integer(), Data::term()) -> ReturnType::term().
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

%%------------------------------------------------------------------------------
%% @doc Get column names of all tables.
%% @end
%%------------------------------------------------------------------------------
-spec get_table_schemas(PoolId::atom(), DbType::atom(), DbName::atom(), TableList::list()) ->
    list().
get_table_schemas(PoolId, DbType, DbName, []) ->
    get_table_schemas(PoolId, DbType, DbName, undefined);
get_table_schemas(PoolId, DbType, DbName, undefined) ->
    %% error_logger:info_msg("get_tables :~p ~n", [{PoolId, DbName, DbType}]),
    case get_tables(PoolId, DbName, DbType) of
        {ok, []} ->
            [];
        {ok, TableList1} ->
            %% error_logger:info_msg("TableList :~p ~n", [TableList1]),
            get_table_schemas(PoolId, DbType, DbName, TableList1);
        Error ->
            error_logger:info_msg("Error :~p ~n", [Error]),
            throw(Error)
    end;
get_table_schemas(PoolId, DbType, DbName, TableList) ->
    lists:foldl(fun([TableName], Res) when is_list(TableName) ->
        %% error_logger:info_msg("get_fields :~p ~n", [{Name, DbName, TableName, DbType}]),
        case get_fields(PoolId, DbName, TableName, DbType) of
            {ok, FieldList} ->
                %% error_logger:info_msg("FieldList :~p ~n", [FieldList]),
                [{TableName, [list_to_atom(hd(X))||X<-FieldList]}| Res];
            Error ->
                error_logger:info_msg("Error :~p ~n", [Error]),
                throw(Error)
        end
    end, [], TableList).

%%------------------------------------------------------------------------------
%% @doc Transform driver name to driver code.
%% @end
%%------------------------------------------------------------------------------
-spec drv_code(Driver::atom()) -> integer().
drv_code(Driver) ->
    drv_convert(Driver, ?DRIVER_CODE_POS).

%%------------------------------------------------------------------------------
%% @doc Transform driver code to driver name.
%% @end
%%------------------------------------------------------------------------------
-spec drv_name(Driver::integer()) -> atom().
drv_name(Driver) ->
    drv_convert(Driver, ?DRIVER_NAME_POS).

%%------------------------------------------------------------------------------
%% @doc Determine whether is true.
%%
%% If Parameter is true, return 1.
%%
%% If Parameter is integer and Parameter > 0, return 1.
%%
%% Others return 0.
%% @end
%%------------------------------------------------------------------------------
-spec is_true(Parameter::term()) -> 1 | 0.
is_true(true) ->
    1;
is_true(Parameter) when is_integer(Parameter), Parameter > 0 ->
    1;
is_true(_)->
    0.

%%------------------------------------------------------------------------------
%% @doc The where statement format conversion for database library.
%% @end
%%------------------------------------------------------------------------------
-spec make_pararmlist(ParamList::term()) -> term().
make_pararmlist(ParamList) ->
    make_pararmlist(ParamList, []).

make_pararmlist([], Acc) ->
    Acc;
%% make_pararmlist([{Column, []}|ParamList], Acc) ->
%%     make_pararmlist([{Column, undefined}|ParamList], Acc);
make_pararmlist([{Column, undefined}|ParamList], Acc) ->
    make_pararmlist(ParamList, [{Column, 'NULL'}|Acc]);
make_pararmlist([{Column, Value}|ParamList], Acc) when is_tuple(Value) ->
    make_pararmlist(ParamList, [{Column, expr(Value)}|Acc]);
make_pararmlist([{Column, Value}|ParamList], Acc) ->
    make_pararmlist(ParamList, [{Column, Value}|Acc]).

%%------------------------------------------------------------------------------
%% @doc The expression format conversion for database library.
%% @end
%%------------------------------------------------------------------------------
-spec make_expr(Expr::term()) -> term().
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

%%------------------------------------------------------------------------------
%% Local Functions
%%------------------------------------------------------------------------------
loop(Port) ->
    io:format("in loop now",[]),
    receive
        Msg ->
            io:format("the msg:~p~n",[Msg]),
            port_close(Port),
            unload()
    end.

unload() ->
    erl_ddll:unload_driver(?DATABASE_LIB).

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

get_driver_pos(Driver) when is_atom(Driver) ->
    ?DRIVER_NAME_POS;
get_driver_pos(Driver) when is_integer(Driver) ->
    ?DRIVER_CODE_POS.

drv_convert(Driver, TypePos) ->
    case lists:keyfind(Driver, get_driver_pos(Driver), ?DRIVER_PAIR) of
        false ->
            undefined;
        Pair ->
            element(TypePos, Pair)
    end.

get_tables(PoolId, DbName, ?MYSQL_DB) ->
    ?DB_API_MOD:select({information_schema,'.',columns}, {table_schema,'=',DbName},
        [{fields,table_name}, {distinct,true}, {pool, PoolId}]);
get_tables(PoolId, _DbName, ?ORACLE_DB) ->
    ?DB_API_MOD:select(user_tables, [], [{fields, table_name},
        {distinct, true}, {pool, PoolId}]);
get_tables(PoolId, _DbName, ?SYBASE_DB) ->
    ?DB_API_MOD:select(sysobjects, {type, '=', "U"}, [{fields, name},
        {distinct, true}, {pool, PoolId}]);
get_tables(PoolId, _DbName, ?DB2_DB) ->
    {ok, [[CurrentSchema]]} = ?DB_API_MOD:execute_sql("VALUES CURRENT SCHEMA", [{pool, PoolId}]),
    {ok, Rows} = ?DB_API_MOD:execute_sql("SELECT DISTINCT TABSCHEMA, TABNAME FROM SYSCAT.TABLES", [{pool, PoolId}]),
    {ok, lists:foldl(
            fun ([TabSchema, TabName], Acc) ->
                 case string:strip(TabSchema) of
                     CurrentSchema ->
                         [[TabName], [CurrentSchema ++ "." ++ TabName] | Acc];
                     _             ->
                         [[string:strip(TabSchema) ++ "." ++ TabName] | Acc]
                 end
            end, [], Rows)};
get_tables(PoolId, _DbName, ?INFORMIX_DB) ->
    ?DB_API_MOD:execute_sql("select tabname from systables where tabid > 99 and tabtype = 'T'",
        [{pool, PoolId}]).
%select tabid from systables where tabid > 99 and tabtype = 'T'

get_fields(PoolId, DbName, TableName, ?MYSQL_DB) ->
    ?DB_API_MOD:select({information_schema,'.',columns},
        {'and', [{table_schema,'=',DbName}, {table_name, '=',TableName}]},
        [{fields, column_name}, {pool, PoolId}]);
get_fields(PoolId, _DbName, TableName, ?ORACLE_DB) ->
    ?DB_API_MOD:execute_sql("select column_name from user_tab_columns where table_name='"
        ++ TableName ++ "' order by column_id", [{pool, PoolId}]);
get_fields(PoolId, _DbName, TableName, ?SYBASE_DB) ->
    ?DB_API_MOD:execute_sql("select name from syscolumns where id=object_id('"
        ++ TableName ++ "')", [{pool, PoolId}]);
get_fields(PoolId, _DbName, TableName, ?DB2_DB) ->
    {ok, [[CS]]} = ?DB_API_MOD:execute_sql("VALUES CURRENT SCHEMA", [{pool, PoolId}]),
    {TabSchema, TabName} =
        case string:tokens(TableName, ".") of
            [TS, TN] -> {TS, TN};
            [TN]     -> {CS, TN}
        end,
    ?DB_API_MOD:execute_sql("SELECT COLNAME from SYSCAT.COLUMNS where TABSCHEMA='"  ++ TabSchema
        ++ "' AND TABNAME='" ++ TabName ++ "' ORDER BY COLNO", [{pool, PoolId}]);
get_fields(PoolId, _DbName, TableName, ?INFORMIX_DB) ->
    {ok, [[TableId]]} = ?DB_API_MOD:execute_sql("select tabid from systables where tabname = '"
        ++ TableName ++ "'", [{pool, PoolId}]),
    ?DB_API_MOD:execute_sql("select colname from syscolumns where tabid = "
        ++ integer_to_list(TableId) ++ " order by colno", [{pool, PoolId}]).

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

expr({Expr1, '=', undefined}) ->
    {get_keyword_value('is_null'), expr(Expr1)};
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
expr({Expr1, '!=', undefined}) ->
    {get_keyword_value('is_not_null'), expr(Expr1)};
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

expr({datetime, Expr}) ->
    {get_keyword_value(datetime), Expr};
expr({date, Expr}) ->
    {get_keyword_value(date), Expr};
expr({time, Expr}) ->
    {get_keyword_value(time), Expr};
expr({timestamp, Expr}) ->
    {get_keyword_value(timestamp), Expr};
expr({interval_ym, Expr}) ->
    {get_keyword_value(interval_ym), Expr};
expr({interval_ds, Expr}) ->
    {get_keyword_value(interval_ds), Expr};
expr({bfile, Expr}) ->
    {get_keyword_value(bfile), Expr};

expr(Expr) ->
    Expr.

get_keyword_value(Key) when is_atom(Key) ->
    case proplists:get_value(Key, ?KEYWORD_MAPPING) of
        undefined ->
            throw(lists:concat(["Not support keyword named: ", Key]));
        Value ->
            Value
    end.

%%------------------------------------------------------------------------------
%% Test Functions
%%------------------------------------------------------------------------------
