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
%%% File    : db_sup.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc The driver's supervisor. Starts db_stmt server and db_conn_server.
%%%
%%% @end
%%% ----------------------------------------------------------------------------
-module(db_sup).
-behaviour(supervisor).

%% Supervisor callbacks
-export([start_link/1, init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Arg), {I, {I, start_link, Arg}, permanent, 5000, Type, [I]}).

%% =============================================================================
%% API functions
%% =============================================================================

%%------------------------------------------------------------------------------
%% @spec(ThreadLen) -> Result
%%   ThreadLen = integer()
%%   Result = {ok, Pid::pid()} | ignore | {error, Error::term()}
%% @doc Creates a supervisor process as part of a supervision tree.
%%
%%      ThreadLen: Initializes the driver thread length.
%% @end
%%------------------------------------------------------------------------------
start_link(ThreadLen) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, ThreadLen).

%% =============================================================================
%% Supervisor callbacks
%% =============================================================================

%%------------------------------------------------------------------------------
%% @spec(ThreadLen) -> Result
%%   ThreadLen = integer()
%%   Result = {ok, {SupFlags, [ChildSpec]}} | ignore | {error, Reason}
%% @doc Start db_stmt server and db_conn_server.
%%
%%      Whenever a supervisor is started using
%%      supervisor:start_link/[2,3], this function is called by the new process
%%      to find out about restart strategy, maximum restart frequency and child
%%      specifications.
%%
%%      ThreadLen: Initializes the driver thread length.
%% @end
%%------------------------------------------------------------------------------
init(ThreadLen) ->
    error_logger:format("~p-~p: ThreadLen = ~p~n", [?MODULE, ?LINE, ThreadLen]),
    {ok, {{one_for_one, 10, 10}, [
        ?CHILD(db_stmt, worker, []),
        ?CHILD(db_conn_server, worker, [ThreadLen])
    ]}}.
