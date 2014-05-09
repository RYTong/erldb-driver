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
%%% File    : db_app.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc The driver's Application.
%%%
%%% @end
%%% -------------------------------------------------------------------
-module(db_app).
-author('deng.lifen@rytong.com').
-behaviour(application).

%%==============================================================================
%% Exported Functions
%%==============================================================================
%% Application callbacks
-export([start/2, stop/1]).

%%==============================================================================
%% Local Functions
%%==============================================================================
-include("db_driver.hrl").

-define(DEFAULT_THREAD_LEN, 5).

%%==============================================================================
%% Application callbacks
%%==============================================================================

%%------------------------------------------------------------------------------
%% @doc Application initialization callback function. Start the supervisor.
%% @end
%%------------------------------------------------------------------------------
start(_StartType, _StartArgs) ->
    db_sup:start_link(get_threadlen()).

%%------------------------------------------------------------------------------
%% @doc The callback function when the application stops.
%% @end
%%------------------------------------------------------------------------------
stop(_State) ->
    ok.

%%==============================================================================
%% Local Functions
%%==============================================================================
get_threadlen() ->
    case application:get_env(db, threadlen) of
        undefined -> ?DEFAULT_THREAD_LEN;
        {ok, ThreadLen} -> ThreadLen
    end.
