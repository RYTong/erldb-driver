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
%%% File    : db_conn_server.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc The driver's APIs.
%%%
%%% @end
%%% -------------------------------------------------------------------

-module(db_conn_server).
-behaviour(gen_server).

-export([
    pools/0,
    get_pool/1,
    get_pool_info/2,
    get_conn_args/1,

    set_table_schema/2,

    set_default_pool/1,
    get_default_pool/0,

    add_pool/1,
    remove_pool/1,

    add_connections/2,
    remove_connections/2,

    wait_for_connection/1,
    pass_connection/1,

    replace_connection_as_available/2,
    replace_connection_as_locked/2
]).

%% gen_server callbacks
-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-include("db_driver.hrl").

-record(state, {pools, driver_pid, default_pool_id}).

%%====================================================================
%% API
%%====================================================================
%%------------------------------------------------------------------------------
%% @doc Get all pools.
%% @end
%%------------------------------------------------------------------------------
-spec pools() -> [record()].
pools() ->
    do_gen_call(pools).

%%------------------------------------------------------------------------------
%% @doc Set table_schemas in the connection pool.
%% @end
%%------------------------------------------------------------------------------
-spec set_table_schema(PoolId::atom(), TableSchemas::[{atom(), list()}]) -> ok.
set_table_schema(PoolId, TableSchemas) ->
    do_gen_call({set_table_schema, PoolId, TableSchemas}).

%%------------------------------------------------------------------------------
%% @doc Get the connection parameters in the connection pool.
%% @end
%%------------------------------------------------------------------------------
-spec get_conn_args(PoolId::atom()) -> record().
get_conn_args(PoolId) ->
    get_pool_info(PoolId, conn_args).

%%------------------------------------------------------------------------------
%% @doc Get information about the connection pool.
%% @end
%%------------------------------------------------------------------------------
-spec get_pool_info(PoolId::atom(), Info::conn_args | table_schemas | size) ->
    term().
get_pool_info(PoolId, conn_args) ->
    (get_pool(PoolId))#pool.conn_args;
get_pool_info(PoolId, table_schemas) ->
    (get_pool(PoolId))#pool.table_schemas;
get_pool_info(PoolId, size) ->
    (get_pool(PoolId))#pool.size.

%%------------------------------------------------------------------------------
%% @doc Get the connection pool.
%% @end
%%------------------------------------------------------------------------------
-spec get_pool(PoolId::atom()) -> record().
get_pool(PoolId) ->
    case find_pool(PoolId, pools()) of
        {Pool, _OtherPools} ->
            Pool;
        undefined ->
            throw({error, pool_not_found})
    end.

%%------------------------------------------------------------------------------
%% @doc Get the default connection pool.
%% @end
%%------------------------------------------------------------------------------
-spec get_default_pool() -> record() | undefined.
get_default_pool() ->
    do_gen_call(get_default_pool).

%%------------------------------------------------------------------------------
%% @doc Set the default connection pool.
%%
%% If the default pool is exist, overwrite it.
%% @end
%%------------------------------------------------------------------------------
-spec set_default_pool(PoolId::atom()) -> ok.
set_default_pool(PoolId) ->
    do_gen_call({set_default_pool, PoolId}).

%%------------------------------------------------------------------------------
%% @doc Add a connection pool.
%% @end
%%------------------------------------------------------------------------------
-spec add_pool(Pool::record()) -> ok.
add_pool(Pool) ->
    do_gen_call({add_pool, Pool}).

%%------------------------------------------------------------------------------
%% @doc Remove the connection pool and return it.
%% @end
%%------------------------------------------------------------------------------
-spec remove_pool(PoolId::atom()) -> record().
remove_pool(PoolId) ->
    do_gen_call({remove_pool, PoolId}).

%%------------------------------------------------------------------------------
%% @doc Add connections to the connection pool.
%% @end
%%------------------------------------------------------------------------------
-spec add_connections(PoolId::atom(), Conns::[record()]) -> ok.
add_connections(PoolId, Conns) when is_list(Conns) ->
    do_gen_call({add_connections, PoolId, Conns}).

%%------------------------------------------------------------------------------
%% @doc Remove N connections from the connection pool.
%%
%% If N > pool_size, then remove all connections.
%% @end
%%------------------------------------------------------------------------------
-spec remove_connections(PoolId::atom(), Num::integer()) -> ok.
remove_connections(PoolId, Num) when is_integer(Num) ->
    do_gen_call({remove_connections, PoolId, Num}).

%%------------------------------------------------------------------------------
%% @doc Wait for a available connection.
%%
%% If ?TRANSCATION_CONNECTION is exist, then return the transaction connection.
%%
%% If it isn't exist, try to lock a connection. If no connections are available
%% then wait to be notified of the next available connection.
%% @end
%%------------------------------------------------------------------------------
-spec wait_for_connection(PoolId::atom()) -> record().
wait_for_connection(PoolId)->
    case erlang:get(?TRANSCATION_CONNECTION) of
        undefined ->
            %% io:format("~p waits for connection to pool ~p~n", [self(), PoolId]),
            case do_gen_call({lock_connection_or_wait, PoolId}) of
                unavailable ->
                    %% io:format("~p is queued~n", [self()]),
                    receive
                        {connection, Connection} ->
                            %% io:format("~p gets a connection after waiting in queue~n", [self()]),
                            Connection
                    after ?DB_LOCK_TIMEOUT ->
                        %% io:format("~p gets no connection and times out -> EXIT~n~n", [self()]),
                        case do_gen_call({end_wait, PoolId}) of
                            ok ->
                                io:format("connection_lock_timeout = ~p~n", [connection_lock_timeout]),
                                throw({error, connection_lock_timeout});
                            not_waiting ->
                                %% If we aren't waiting, then someone must
                                %% have sent us a connection message at the
                                %% same time that we timed out.
                                receive_connection_not_waiting()
                        end
                    end;
                Connection ->
                    %% io:format("~p gets connection~n", [self()]),
                    Connection
            end;
        Conn ->
            {trans, Conn}
    end.

%%------------------------------------------------------------------------------
%% @doc Pass a locked connection.
%%
%% Check if any processes are waiting for a connection.
%%
%% If there is no program is in waiting, add the connection to the 'available'
%% queue and remove from 'locked' tree.
%%
%% If the waiting queue is not empty then remove the head of the queue and
%% send it the connection.
%%
%% Update the pool and queue in state once the head has been removed.
%% @end
%%------------------------------------------------------------------------------
-spec pass_connection(Connection::record()) -> ok | {error, Error::term()}.
pass_connection(Connection) ->
    do_gen_call({pass_connection, Connection}).

%%------------------------------------------------------------------------------
%% @doc Replace a close connection to a new one, and make it into available queue.
%%
%% if an error occurs while doing work over a connection then the connection
%% must be closed and a new one created in its place. The calling process is
%% responsible for creating the new connection, closing the old one and replacing
%% it in state.
%%
%% This function expects a new, available connection to be passed in to serve
%% as the replacement for the old one.
%% @end
%%------------------------------------------------------------------------------
-spec replace_connection_as_available(OldConn::record(), NewConn::record()) ->
    ok | {error, Error::term()}.
replace_connection_as_available(OldConn, NewConn) ->
    do_gen_call({replace_connection_as_available, OldConn, NewConn}).

%%------------------------------------------------------------------------------
%% @doc Replace a close connection to a new one, and make it into locked queue.
%%
%% Replace an existing, locked condition with the newly supplied one
%% and keep it in the locked list so that the caller can continue to use it
%% without having to lock another connection.
%% @end
%%------------------------------------------------------------------------------
-spec replace_connection_as_locked(OldConn::record(), NewConn::record()) ->
    ok | {error, Error::term()}.
replace_connection_as_locked(OldConn, NewConn) ->
    do_gen_call({replace_connection_as_locked, OldConn, NewConn}).

%% the stateful loop functions of the gen_server never
%% want to call exit/1 because it would crash the gen_server.
%% instead we want to return error tuples and then throw
%% the error once outside of the gen_server process
do_gen_call(Msg) ->
    try
        gen_server:call(?MODULE, Msg, infinity)
    catch
        throw:{error, Error} ->
            {error, Error};
        throw:ThrowError ->
            {error, ThrowError};
        _:OtherError ->
            throw(OtherError)
    end.

%%--------------------------------------------------------------------
%% @spec start_link(ThreadLen::integer()) -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server.
%%
%%      ThreadLen: Initializes the driver thread length.
%% @end
%%--------------------------------------------------------------------
start_link(ThreadLen) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, ThreadLen, []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @spec init(ThreadLen::integer()) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% @doc Initiates the server.
%%
%%      Starts the db driver and save driver pid in state.
%%
%%      ThreadLen: Initializes the driver thread length.
%% @end
%%--------------------------------------------------------------------
init(ThreadLen) ->
    error_logger:format("ThreadLen = ~p~n", [ThreadLen]),
    process_flag(trap_exit, true),
    Pid =
        case db_conn:start() of
            {error, Error} ->
                throw({error, Error});
            P ->
                P
        end,
    db_conn:init(ThreadLen),
    {ok, #state{pools=[], driver_pid=Pid}}.

%%--------------------------------------------------------------------
%% @spec
%% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% @doc Handling call messages
%% @end
%%--------------------------------------------------------------------
handle_call(pools, _From, State) ->
    {reply, State#state.pools, State};

handle_call({set_table_schema, PoolId, TableSchemas}, _From, State) ->
    case find_pool(PoolId, State#state.pools) of
        {Pool, OtherPools} ->
            State1 = State#state{
                pools = [Pool#pool{table_schemas = TableSchemas}|OtherPools]
            },
            {reply, ok, State1};
        undefined ->
            {reply, {error, pool_not_found}, State}
    end;

handle_call({set_default_pool, PoolId}, _From, State) ->
    {reply, ok, State#state{default_pool_id = PoolId}};

handle_call(get_default_pool, _From, State) ->
    {reply, State#state.default_pool_id, State};

handle_call({add_pool, Pool}, _From, State) ->
    case find_pool(Pool#pool.id, State#state.pools) of
        {_, _} ->
            {reply, {error, pool_already_exists}, State};
        undefined ->
            {reply, ok, State#state{pools = [Pool|State#state.pools]}}
    end;

handle_call({remove_pool, PoolId}, _From, State) ->
    case find_pool(PoolId, State#state.pools) of
        {Pool, OtherPools} ->
            {reply, Pool, State#state{pools=OtherPools}};
        undefined ->
            {reply, {error, pool_not_found}, State}
    end;

handle_call({add_connections, PoolId, Conns}, _From, State) ->
    case find_pool(PoolId, State#state.pools) of
        {Pool, OtherPools} ->
            OtherConns = Pool#pool.available,
            PoolSize = Pool#pool.size + length(Conns),
            State1 = State#state{
                pools = [Pool#pool{size = PoolSize,
                    available = queue:join(queue:from_list(Conns), OtherConns)}|OtherPools]
            },
            {reply, ok, State1};
        undefined ->
            {reply, {error, pool_not_found}, State}
    end;

handle_call({remove_connections, PoolId, Num}, _From, State) ->
    case find_pool(PoolId, State#state.pools) of
        {Pool, OtherPools} ->
            case Num > queue:len(Pool#pool.available) of
                true ->
                    PoolSize = Pool#pool.size - queue:len(Pool#pool.available),
                    State1 = State#state{pools = [Pool#pool{size = PoolSize, available = queue:new()}|OtherPools]},
                    {reply, queue:to_list(Pool#pool.available), State1};
                false ->
                    {Conns, OtherConns} = queue:split(Num, Pool#pool.available),
                    PoolSize = Pool#pool.size - Num,
                    State1 = State#state{pools = [Pool#pool{size = PoolSize,available = OtherConns}|OtherPools]},
                    {reply, queue:to_list(Conns), State1}
            end;
        undefined ->
            {reply, {error, pool_not_found}, State}
    end;

handle_call({lock_connection_or_wait, PoolId}, {From, _Mref}, State) ->
    case find_pool(PoolId, State#state.pools) of
        {Pool, OtherPools} ->
            case lock_next_connection(State, Pool, OtherPools) of
                {ok, NewConn, State1} ->
                    {reply, NewConn, State1};
                unavailable ->
                    %% place the calling pid at the end of the waiting queue of its pool
                    PoolNow = Pool#pool{ waiting = queue:in(From, Pool#pool.waiting) },
                    {reply, unavailable, State#state{pools=[PoolNow|OtherPools]}}
            end;
        undefined ->
            {reply, {error, pool_not_found}, State}
    end;

handle_call({end_wait, PoolId}, {From, _Mref}, State) ->
    case find_pool(PoolId, State#state.pools) of
        {Pool, OtherPools} ->
            %% Remove From from the wait queue
            QueueNow = queue:filter(fun(Pid) -> Pid =/= From end,
                Pool#pool.waiting),
            PoolNow = Pool#pool{waiting = QueueNow},
            %% See if the length changed to know if From was removed.
            OldLen = queue:len(Pool#pool.waiting),
            NewLen = queue:len(QueueNow),
            if
                OldLen =:= NewLen ->
                    Reply = not_waiting;
                true ->
                    Reply = ok
            end,
            {reply, Reply, State#state{pools=[PoolNow|OtherPools]}};
        undefined ->
            {reply, {error, pool_not_found}, State}
    end;

handle_call({pass_connection, Connection}, _From, State) ->
    {Result, State1} = pass_on_or_queue_as_available(State, Connection),
    {reply, Result, State1};

handle_call({replace_connection_as_available, OldConn, NewConn}, _From, State) ->
    %% if an error occurs while doing work over a connection then
    %% the connection must be closed and a new one created in its
    %% place. The calling process is responsible for creating the
    %% new connection, closing the old one and replacing it in state.
    %% This function expects a new, available connection to be
    %% passed in to serve as the replacement for the old one.
    case find_pool(OldConn#db_connection.pool_id, State#state.pools) of
        {Pool, OtherPools} ->
            Pool1 = Pool#pool{
                available = queue:in(NewConn, Pool#pool.available),
                locked = gb_trees:delete_any(OldConn#db_connection.id, Pool#pool.locked)
            },
            {reply, ok, State#state{pools=[Pool1|OtherPools]}};
        undefined ->
            {reply, {error, pool_not_found}, State}
    end;

handle_call({replace_connection_as_locked, OldConn, NewConn}, _From, State) ->
    %% replace an existing, locked condition with the newly supplied one
    %% and keep it in the locked list so that the caller can continue to use it
    %% without having to lock another connection.
    case find_pool(OldConn#db_connection.pool_id, State#state.pools) of
        {Pool, OtherPools} ->
            LockedStripped = gb_trees:delete_any(OldConn#db_connection.id, Pool#pool.locked),
            LockedAdded = gb_trees:enter(NewConn#db_connection.id, NewConn, LockedStripped),
            Pool1 = Pool#pool{locked = LockedAdded},
            {reply, ok, State#state{pools=[Pool1|OtherPools]}};
        undefined ->
            {reply, {error, pool_not_found}, State}
    end;

handle_call(_, _From, State) -> {reply, {error, invalid_call}, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @spec terminate(Reason, State) -> void()
%% @doc Close all connections and stop db driver.
%%
%% This function is called by a gen_server when it is about to terminate.
%% It should be the opposite of Module:init/1 and do any necessary
%% cleaning up.
%%
%% When it returns, the gen_server terminates with Reason. The return
%% value is ignored.
%% @end
%%--------------------------------------------------------------------
terminate(Reason, State) ->
    error_logger:format("terminate = ~p~n", [?MODULE]),
    error_logger:format("Reason = ~p~n", [Reason]),
    error_logger:format("State = ~p~n", [State]),
    Pools = State#state.pools,
    Pid = State#state.driver_pid,
    lists:map(
        fun(Pool) ->
            [db_conn:close_connection(Conn) ||
                Conn <- lists:append(queue:to_list(Pool#pool.available), gb_trees:values(Pool#pool.locked))]
        end, Pools),
    db_conn:stop(Pid).

%%--------------------------------------------------------------------
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
find_pool(PoolId, Pools) ->
    find_pool(PoolId, Pools, []).

find_pool(_, [], _) -> undefined;
find_pool(PoolId, [#pool{id = PoolId} = Pool|Tail], OtherPools) ->
    {Pool, lists:append(OtherPools, Tail)};
find_pool(PoolId, [Pool|Tail], OtherPools) ->
    find_pool(PoolId, Tail, [Pool|OtherPools]).

%% Check of connection in the pool.
lock_next_connection(State, Pool, OtherPools) ->
    case queue:out(Pool#pool.available) of
        {{value, Conn}, OtherConns} ->
            Locked = gb_trees:enter(Conn#db_connection.id, Conn, Pool#pool.locked),
            State1 = State#state{pools = [Pool#pool{available=OtherConns, locked=Locked}|OtherPools]},
            {ok, Conn, State1};
        {empty, _} ->
            unavailable
    end.

%% check if any processes are waiting for a connection.
%%
%% If there is no program is in waiting, add the connection to the 'available'
%% queue and remove from 'locked' tree.
%%
%% If the waiting queue is not empty then remove the head of
%% the queue and send it the connection.
%%
%% Update the pool and queue in state once the head has been removed.
%% This function does not wait, but may loop over the queue.
pass_on_or_queue_as_available(State, Connection) ->
    % get the pool that this connection belongs to
    case find_pool(Connection#db_connection.pool_id, State#state.pools) of
        {Pool, OtherPools} ->
            %% check if any processes are waiting for a connection
            Waiting = Pool#pool.waiting,
            case queue:is_empty(Waiting) of
                %% if no processes are waiting then unlock the connection
                true ->
                    %% find connection in locked tree
                    case gb_trees:lookup(Connection#db_connection.id, Pool#pool.locked) of
                        {value, Conn} ->
                            %% add the connection to the 'available' queue and remove from 'locked' tree
                            Pool1 = Pool#pool{
                                available = queue:in(Conn, Pool#pool.available),
                                locked = gb_trees:delete_any(Connection#db_connection.id, Pool#pool.locked)
                            },
                            {ok, State#state{pools = [Pool1|OtherPools]}};
                        none ->
                            {{error, connection_not_found}, State}
                    end;
                false ->
                    %% if the waiting queue is not empty then remove the head of
                    %% the queue and send it the connection.
                    %% Update the pool and queue in state once the head has been removed.
                    {{value, Pid}, OtherWaiting} = queue:out(Waiting),
                    PoolNow = Pool#pool{waiting = OtherWaiting},
                    StateNow = State#state{pools = [PoolNow|OtherPools]},
                    erlang:send(Pid, {connection, Connection}),
                    {ok, StateNow}
            end;
        undefined ->
            {{error, pool_not_found}, State}
    end.

%% This is called after we timed out, but discovered that we weren't waiting for a
%% connection.
receive_connection_not_waiting() ->
    receive
        {connection, Connection} ->
            %% io:format("~p gets a connection after timeout in queue~n", [self()]),
            Connection
    after
        %% This should never happen, as we should only be here if we had been sent a connection
        ?DB_LOCK_TIMEOUT ->
            %% io:format("~p gets no connection and times out again -> EXIT~n~n", [self()]),
            throw({error, connection_lock_second_timeout})
    end.
