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
%%% File    : db_performance_test.erl
%%% @author cao.xu <cao.xu@rytong.com>
%%% @doc The db driver's performance tests.
%%% @end
%%% -------------------------------------------------------------------

-module(db_performance_test).
-compile(export_all).
-define(P_TEST_TABLE, performance_test).

start() ->
    test_util:start(?P_TEST_TABLE).

stop() ->
    test_util:stop(?P_TEST_TABLE).

test(N_Pro, N_Times) ->
    db_test(N_Pro,N_Times, infinity).

test(N_Pro, N_Times, LoopTime) ->
    db_test(N_Pro,N_Times, LoopTime).

db_test(_N_Pro, _N_Times, 0) ->
    over;
db_test(N_Pro, N_Times, LoopTime) ->
    %% io:format("work fun :~p~n",[WorkFun]),
    error_logger:logfile({open,"./db_test.log"}),
    MainId =  self(),
    P_Times =  N_Times div N_Pro,
    Collector = spawn(fun()->collect(MainId,N_Pro,0,0,0)end),
    Workers = [spawn(?MODULE, db_worker, [Collector, P_Times])
        || _I <- lists:seq(1, N_Pro)],
    Start = now(),
    [Worker!start||Worker<-Workers],
    receive
        {all_done, TotalSuc, TotalFail} ->
            Time = timer:now_diff(now(), Start) / 1000,
            error_logger:format("time consumed : ~p ms total successed :~p total failed :~p ~n",[Time,TotalSuc, TotalFail]),
            [exit(Worker,kill)||Worker<-Workers],
            case LoopTime of
                infinity->
                    db_test(N_Pro,N_Times, LoopTime);
                _ ->
                    db_test(N_Pro,N_Times, LoopTime-1)
            end;
        _ ->
            go_on
    end.

db_worker(Collector, Times) ->
    receive
        start ->
            do_job(Collector,Times, 0, 0)
    end.

do_job(Collector, 0, Suc, Fail) ->
    %% io:format("in job worker ~p done, suc num : ~p , fail num :~p ~n",[self(), Suc, Fail]),
    Collector!{done, self(), Suc, Fail};
do_job(Collector, Times, Suc, Fail) ->
    {NewSuc, NewFail} =
        case random_test() of
            {ok, _Msg} ->
%%                 io:format("the Msg ============: ~p~n",[Msg]),
                {Suc+1, Fail};
            Err ->
                error_logger:format("the error ============: ~p~n",[Err]),
                {Suc, Fail+1}
        end,
    do_job(Collector, Times - 1, NewSuc, NewFail).



collect(MainId, N_Pro, N_Finished, TotalSuc, TotalFail)
    when N_Pro == N_Finished ->
    MainId ! {all_done, TotalSuc, TotalFail};

collect(MainId, N_Pro, N_Finished, TotalSuc, TotalFail) ->
    receive
        {done, _Worker, Suc, Fail} ->
            %% io:format("worker ~p done, suc num : ~p , fail num :~p finished : ~p ~n",[Worker, Suc, Fail,N_Finished+1]),
            collect(MainId, N_Pro, N_Finished +1, TotalSuc+Suc, TotalFail+Fail)
    end.

random_test() ->
    case random:uniform(5) of
        1 ->
            r_insert();
        2 ->
            r_update();
        3 ->
            r_select();
        4 ->
            r_delete();
        5 ->
            r_select_all();
        _ ->
            {ok, skip}
    end.

r_insert() ->
    Datetime = calendar:local_time(),
    case db_api:insert(?P_TEST_TABLE, [
        {fbit, 1},
        {ftinyint, 1},
        {fsmallint, random:uniform(200)},
        {fmediunint, 1323},
        {fint, 38524},
        {fbigint, 2233434},
        {ffloat, 238954.345},
        {fdouble, 335623.276212},
        {fdecimal, 45656.12},
        {fdate, {date, {2010, 3, 24}}},
        {fdatetime, {datetime, Datetime}},
        {ftimestamp, {datetime, {{2010, 3, 1}, {2, 10, 30}}}},
        {ftime, {time, {11, 45, 22}}},
        {fyear, 2010},
        {fchar, 97},
        {fvarchar, "test varchar"},
        {fbinary, "fjdsgjnkdgbdf dfdfg\r\n isfdk"},
        {fvarbinary, "dkjsor klsjfsdfj"},
        {ftinytext, "dsfgd"},
        {ftext, ";ljkdf"},
        {fmediumtext, "dfuyejksf"},
        {flongtext, "indtvdf"},
        {ftinyblob, <<"hg">>},
        {fblob, <<34,56,0,54,75>>},
        {fmediumblob, <<97,98,99,100>>},
        {flongblob, <<"sdfweur3[590766%^#$^&(;lgf h">>}]) of
        {ok, _} ->
            {ok, insert};
        Err ->
            Err
    end.
  
r_update() ->
     {_Date, Time} = calendar:local_time(),
     case db_api:update(?P_TEST_TABLE, [{ftime, {time, Time}}],
         {fsmallint, '=', random:uniform(200)}) of
         {ok, _} ->
             {ok, update};
         Err ->
             Err
     end.

r_select() ->
    case db_api:select(?P_TEST_TABLE, [], [{extras, {limit, 1}}]) of
        {ok, _} ->
            {ok, select};
        Err ->
            Err
    end.
r_delete() ->
    case db_api:delete(?P_TEST_TABLE, {fsmallint, '=', random:uniform(200)}) of
        {ok, _} ->
            {ok, delete};
        Err ->
            Err
    end.

r_select_all() ->
     case db_api:select(?P_TEST_TABLE, []) of
        {ok, _} ->
            {ok, select_all};
        Err ->
            Err
    end.
