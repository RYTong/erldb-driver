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
%%% File    : db_server.erl
%%% @author cao.xu <cao.xu@rytong.com>
%%% @doc This server offers interfaces to fetch field names 
%%%      in database and fresh them, and manage the connection pool.
%%% @end
%%% -------------------------------------------------------------------
-module(db_server).
-author("cao.xu (cao.xu@rytong.com)").
-behaviour(gen_server).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
-include("db_driver.hrl").
-define(DBINFO, database_info).

%% --------------------------------------------------------------------
%% External exports
%% --------------------------------------------------------------------
-export([start/0,
         start_link/1,
         stop/0,
         init_default/1,
         init_default/2,
         make_default/1,
         connect/2,
         disconnect/1,
         get_db_config/1,
         get_db_config/2,
         get_drv/1,
         get_drv_type/1,
         get_keys/1,
         get_keys/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% ====================================================================
%% External functions
%% ====================================================================

%% @doc Call start_link/1, use DATABASE_LIB_DIR as PrivPath.
%% @see start_link/1
%% @spec start() -> pid()
start() ->
    start_link(?DATABASE_LIB_DIR).

%% @doc Start db server and load db driver lib.
%% @spec start_link(PrivPath::string()) -> pid()
start_link(PrivPath) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, PrivPath, []).

%% @doc Stop db server. Disconnect all conections and unload db driver lib.
%% @spec stop() -> ok
stop() ->
    gen_server:cast(?MODULE, stop).

%% @doc Read config and init default connection pool.
%%  Call init_default/2, use "config" as ConfigPath.
%% @see init_default/2
%% @spec init_default(ConfigName::atom()) -> ok | throw()
init_default(ConfigName) ->
    ConfigPath = get_config_path(),
    init_default(ConfigName, ConfigPath).

%% @doc Get config path. Find config/db.conf in current, if not exists, find
%%  it in install path.
get_config_path() ->
    {ok, CurrentDir} = file:get_cwd(),
    ConfigName = filename:join([CurrentDir, ?DB_CONFIG_PATH, ?DB_CONFIG_FILE]),
    case filelib:is_file(ConfigName) of
        true ->
            filename:dirname(ConfigName);
        false ->
            case code:lib_dir(?APPNAME, list_to_atom(?DB_CONFIG_PATH)) of
                {error, _Err} ->
                    throw(lists:concat(["Can not find ", ?DB_CONFIG_FILE]));
                Path ->
                    Path
            end
    end.

%% @doc Read config and init default connection pool.
%% @spec init_default(ConfigName::atom(), ConfigName::string()) -> ok | throw()
init_default(ConfigName, ConfigPath)  ->
    ConnArg = get_db_config(ConfigName, ConfigPath),
    make_default(ConfigName),
    case connect(ConfigName, ConnArg) of
        {error, Msg} ->
            throw(Msg);
        _ ->
            ok
    end.

%% @doc Make to default connection pool.
%% @spec make_default(Name::atom()) -> ok
make_default(Name) ->   
    ets:insert(?DBINFO, {default, Name}).

%% @doc Connect to database with connect parameters in Args.
%%  and register the database driver instance with name Name
%%  in ets ?DBINFO.
%% @spec connect(Name::atom(), ArgTuple::list()) -> {ok, Name} | {error, Err}
connect(Name, Args) ->
    case db_api:connect(Args) of
        {ok, Instance} ->
            %% error_logger:format("Args = ~p~n", [Args]), 
            %% error_logger:format("Instance = ~p~n", [Instance]),        
            DbType = db_driver:drv_number(proplists:get_value(driver, Args)),
            DbName = proplists:get_value(database, Args),
            ets:insert(?DBINFO, {Name, DbType, DbName, Instance}),
            %% error_logger:format("store = ~p~n", [{Name, DbType, DbName}]),
            store_tab_info(Name, DbType, DbName),
            %% error_logger:format("store_tab_info~n", []),
            {ok, Name};
        Msg ->
            error_logger:info_msg("~p:~p failed to connect database with "
                "params: ~p and the result :~p~n", [?MODULE, ?LINE, Args, Msg]),
            {error, Msg}
    end.

%% @doc Disconnect database with connect name.
%%  and unregister the database driver instance with name Name
%%  in ets ?DBINFO.
%% @spec disconnect(DbName::atom()) -> {ok, Res} | {error, Err}
disconnect(Name) ->
    case db_api:disconnect(Name) of
        {ok, Res} ->
            ets:delete(?DBINFO, Name),
            purge_tab_info(Name),
            {ok, Res};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Get db config content. Config path is config/db.conf in current dir
%%  or install dir.
%% @spec get_db_config(ConfigName::atom()) ->
%%      term() | throw()
get_db_config(ConfigName) ->
    get_db_config(ConfigName, get_config_path()).

%% @doc Get db config content.
%% @spec get_db_config(ConfigName::atom(), ConfigPath::string()) ->
%%      term() | throw()
get_db_config(ConfigName, ConfigPath) ->
    DBConfigFile = filename:join(ConfigPath, ?DB_CONFIG_FILE),
    %% error_logger:info_msg("the config file :~p~n", [DBConfigFile]),
    case file:consult(DBConfigFile) of
        {error, Err} ->
            throw(lists:concat(["error read config file: ", Err]));
        {ok, Config} when is_list(Config) ->
            case proplists:get_value(ConfigName, Config) of
                undefined ->
                    throw("default config error");
                Default ->
                    Default
            end;
        {ok, _} ->
            throw("config error")
    end.

%% @doc Get the field list of table.
%%  Call get_keys/2, use default drv name as Name.
%% @spec get_keys(Table::atom()) -> list()
get_keys(Table) when is_atom(Table) ->
    get_keys(default_drv_name(), Table).

%% @doc Get the field list of table.
%% @spec get_keys(Name::atom(), Table::atom()) -> list()
get_keys(Name, Table) when is_atom(Table) ->
    case ets:lookup(?DBINFO, {Name, atom_to_list(Table)}) of
        [{_, Value}] ->
            Value;
        _ ->
            throw(no_table_info)
    end.

%% @doc Get default connection pool name.
%% @spec default_drv_name() -> atom() | undefined
default_drv_name() ->
    case ets:lookup(?DBINFO, default) of
        [{default, Name}] ->
            Name;
        _ ->
            undefined
    end.

%% @doc Get connection pool id with name.
%% @spec get_drv(Name::atom()) -> binary() | throw()
get_drv(undefined) ->
    throw(error_name_of_db_drv);
get_drv(default) ->
    get_drv(default_drv_name());
get_drv(Name) ->
    case ets:lookup(?DBINFO, Name) of
        [{Name, _, _, Drv}] ->
            Drv;
        _ ->
            throw(error_name_of_db_drv)
    end.

%% @doc Get connection database type with name.
%% @spec get_drv_type(Name::atom()) -> integer() | undefined
get_drv_type(default) ->
    get_drv_type(default_drv_name());
get_drv_type(undefined) ->
    undefined;
get_drv_type(Name) ->
    case ets:lookup(?DBINFO, Name) of
        [{Name, Type, _, _}] ->
            Type;
        _ ->
            undefined
    end.


%% %% fixme: this function should be updated
%% refresh() ->
%%     gen_server:call(?MODULE, refresh).

%% ====================================================================
%% Server functions
%% ====================================================================

%% @doc Initiates the server, and load db driver lib.
%% @spec init(DrvPath::string()) -> {ok, [pid()]}
init(DrvPath) ->
    %% Note we must set trap_exit = true if we want terminate/2 to be called
    %% when the application is stopped
    process_flag(trap_exit, true),
    ets:new(?DBINFO, [public, named_table]),
    %% error_logger:info_msg("the database drv path: ~p~n", [DrvPath]),
    Pid = db_api:start(DrvPath),
    {ok, [Pid]}.

%% @doc Handling call messages.
%% @spec handle_call(Msg::atom(), From, State) ->
%%          {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   |
%%          {stop, Reason, State}
handle_call(state, _From, State) ->   
    {reply, State, State}.

%% @doc Handling cast messages.
%% @spec handle_cast(Msg::atom(), State) ->
%%          {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}
handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc Handling all non call/cast messages.
%% @spec handle_info(Info, State) ->
%%          {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc Disconnect all connections, unload db driver lib and shutdown
%%  the server.
%% @spec terminate(Reason, [Pid::pid()]) -> ok
terminate(_Reason, [Pid]) ->
    case ets:member(?DBINFO, default) of
        true ->
            %% error_logger:info_msg("disconnect1~n", []),
            db_api:disconnect(default);
            %% error_logger:info_msg("disconnect2~n", []);
        _ ->
            no_default_db_drv
    end,
    ets:delete(?DBINFO),
    %% error_logger:info_msg("delete~n", []),
    db_api:stop(Pid),
    %% error_logger:info_msg("stop~n", []),
    ok.

%% @doc Convert process state when code is changed.
%% @spec code_change(OldVsn, State, Extra) -> {ok, State}
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------

store_tab_info(Name, DbType, DbName) ->
    TableInfo = get_table_info(Name, DbType, DbName, []),
    [ets:insert(?DBINFO, {{Name, TableName},  FiledList})
        ||{TableName, FiledList} <- TableInfo],
    ok.
    
%% @doc get column_name of all tables.
get_table_info(Name, DbType, DbName, []) ->
    get_table_info(Name, DbType, DbName, undefined);
get_table_info(Name, DbType, DbName, undefined) ->
    %% error_logger:info_msg("get_tables :~p ~n", [{Name, DbName, DbType}]),
    case get_tables(Name, DbName, DbType) of
        {ok, []} ->
            [];
        {ok, TableList1} ->
            %% error_logger:info_msg("TableList :~p ~n", [TableList1]),
            get_table_info(Name, DbType, DbName, TableList1);
        Error ->
            error_logger:info_msg("Error :~p ~n", [Error])
    end;
get_table_info(Name, DbType, DbName, TableList) ->
    lists:foldl(fun([TableName], Res) when is_list(TableName) ->
        %% error_logger:info_msg("get_fields :~p ~n", [{Name, DbName, TableName, DbType}]),
        case get_fields(Name, DbName, TableName, DbType) of
            {ok, []} ->
                Res;
            {ok, FieldList} ->
                %% error_logger:info_msg("FieldList :~p ~n", [FieldList]),
                [{TableName, [list_to_atom(hd(X))||X<-FieldList]}| Res];
            Error ->
                error_logger:info_msg("Error :~p ~n", [Error]),
                Res
        end 
    end, [], TableList).

get_tables(Name, DbName, ?MYSQL_DB) ->
    db_api:select({information_schema,'.',columns}, {table_schema,'=',DbName},
        [{fields,table_name}, {distinct,true}, {db_name, Name}]);
get_tables(Name, _DbName, ?ORACLE_DB) ->
    db_api:select(user_tables, [], [{fields, table_name},
        {distinct, true}, {db_name, Name}]);
get_tables(Name, _DbName, ?SYBASE_DB) ->
    db_api:select(sysobjects, {type, '=', "U"}, [{fields, name},
        {distinct, true}, {db_name, Name}]).

get_fields(Name, DbName, TableName, ?MYSQL_DB) ->
    db_api:select({information_schema,'.',columns},
        {'and', [{table_schema,'=',DbName}, {table_name, '=',TableName}]},
        [{fields, column_name}, {db_name, Name}]);
get_fields(Name, _DbName, TableName, ?ORACLE_DB) ->
    db_api:select(user_tab_columns, {table_name, '=',TableName},
        [{fields, column_name}, {db_name, Name}]);
get_fields(Name, _DbName, TableName, ?SYBASE_DB) ->
    db_api:execute_sql("select name from syscolumns where id=object_id('" 
        ++ TableName ++ "')", [{db_name, Name}]).

purge_tab_info(Name) ->
    ets:match_delete(?DBINFO, {{Name, '_'}, '_'}).