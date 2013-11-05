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
%%% File    : db_stmt.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc Management prepared statement storage.
%%%
%%% @end
%%% -------------------------------------------------------------------
-module(db_stmt).
-behaviour(gen_server).

-export([
    start_link/0,
    fetch/1,
    add/2,
    remove/1,
    prepare/3,
    stmtdata/2
]).

%% gen_server callbacks.
-export([init/1, handle_call/3, handle_cast/2,
    handle_info/2, terminate/2, code_change/3]).

-include("db_driver.hrl").

-record(state, {
    statements = gb_trees:empty(),
    prepared = gb_trees:empty()
}).

%%====================================================================
%% API
%%====================================================================

%%--------------------------------------------------------------------
%% @spec fetch(StmtName::atom()) -> Statement::string() | undefined
%% @doc Lookup Statement.
%% @end
%%--------------------------------------------------------------------
fetch(StmtName) ->
    gen_server:call(?MODULE, {fetch, StmtName}, infinity).

%%--------------------------------------------------------------------
%% @spec add(StmtName::atom(), Statement::string()) ->
%%      ok | {error, statement_existed}
%% @doc Add Statement if not exist.
%% @end
%%--------------------------------------------------------------------
add(StmtName, Statement) ->
    gen_server:call(?MODULE, {add, StmtName, Statement}, infinity).

%%--------------------------------------------------------------------
%% @spec stmtdata(ConnId::binary(), StmtName::atom()) ->
%%      StmtData::binary() | undefined
%% @doc Lookup statement object pointer by connection object pointer.
%% @end
%%--------------------------------------------------------------------
stmtdata(ConnId, StmtName) ->
    gen_server:call(?MODULE, {stmtdata, ConnId, StmtName}, infinity).

%%--------------------------------------------------------------------
%% @spec prepare(ConnId::binary(), StmtName::atom(), StmtData::binary) ->
%%      StmtData::binary() | undefined
%% @doc Add statement object pointer to prepared.
%% @end
%%--------------------------------------------------------------------
prepare(ConnId, StmtName, StmtData) ->
    gen_server:call(?MODULE, {prepare, ConnId, StmtName, StmtData}, infinity).

%%--------------------------------------------------------------------
%% @spec remove(ConnId::binary()) -> [StmtData::binary()]
%% @doc Remove statement object pointer from prepared.
%% @end
%%--------------------------------------------------------------------
remove(ConnId) ->
    gen_server:call(?MODULE, {remove, ConnId}, infinity).

%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @spec init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% @doc Initiates the server
%% @end
%%--------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

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
handle_call({fetch, StmtName}, _From, State) ->
    {reply, lookup(StmtName, State#state.statements), State};

handle_call({add, StmtName, Statement}, _From, State) ->
    case lookup(StmtName, State#state.statements) of
        undefined ->
            State1 = State#state{
                statements = gb_trees:enter(StmtName, Statement, State#state.statements)
            },
            {reply, ok, State1};
        _Statement ->
            {reply, {error, statement_existed}, State}
    end;

handle_call({stmtdata, ConnId, StmtName}, _From, State) ->
    StmtData =
        case lookup(ConnId, State#state.prepared) of
            undefined -> undefined;
            StmtDatas -> lookup(StmtName, StmtDatas)
        end,
    {reply, StmtData, State};

handle_call({prepare, ConnId, StmtName, StmtData}, _From, State) ->
    StmtDatas =
        case lookup(ConnId, State#state.prepared) of
            undefined ->
                gb_trees:enter(StmtName, StmtData, gb_trees:empty());
            StmtDatas1 ->
                gb_trees:enter(StmtName, StmtData, StmtDatas1)
        end,
    Prepared = gb_trees:enter(ConnId, StmtDatas, State#state.prepared),
    {reply, ok, State#state{prepared=Prepared}};

handle_call({remove, ConnId}, _From, State) ->
    StmtDatas =
        case lookup(ConnId, State#state.prepared) of
            undefined -> [];
            StmtDatas1 -> gb_trees:values(StmtDatas1)
        end,
    Prepared = gb_trees:delete_any(ConnId, State#state.prepared),
    {reply, StmtDatas, State#state{prepared=Prepared}};

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
%% @doc Do nothing.
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
    ok.

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
lookup(Key, Tree) ->
    case gb_trees:lookup(Key, Tree) of
        {value, Val} -> Val;
        none -> undefined
    end.
