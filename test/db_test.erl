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
%%% File    : db_test.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc The db driver's test cases.
%%% @end
%%% -------------------------------------------------------------------

-module(db_test).
-compile(export_all).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------
% -include_lib("eunit/include/eunit.hrl").

-export([start/0,
         test/4,
         test_insert/3,
         test_delete/3,
         test_select/3,
         test_update/3,
         timer_test/0,
         timer_test_2/0,
         timer_test_1/0,
         spawn_insert/0,
         spawn_select/0,
         spawn_update/0,
         f/5]).

-export([db_test/3,
         db_worker/3,
         mysql_simple_select/0]).


-define(PoolId, erl_db).
-define(TimeOut, 20000).
-define(ODBC_CONN_STRING, "DSN=MySQL-test").

f(0, _Driver, _Action, _N_P, _N_T)->
    ok;
f(N, Driver, Action, N_P, N_T)->
    error_logger:format("N :~p~n",[N]),
    case Action of
        insert ->
            test_insert(Driver,N_P, N_T);
        update ->
            test_update(Driver,N_P, N_T);
        delete ->
            test_delete(Driver,N_P, N_T);
        select ->
            test_select(Driver,N_P, N_T)
    end,
    f(N-1, Driver, Action, N_P, N_T).

start() ->
    ets:new(?MODULE, [public, named_table]),
    
    ConnArg = db_server:get_db_config(test),
    start_db(ConnArg),
    
    % {ok, OdbcConn} = start_odbc(?ODBC_CONN_STRING, ?TimeOut),
    % ets:insert(?MODULE, {odbc_conn, OdbcConn}),
    
    Host = proplists:get_value(host, ConnArg),
    User = proplists:get_value(user, ConnArg),
    Password = proplists:get_value(password, ConnArg),
    Database = proplists:get_value(database, ConnArg),
    Poolsize = proplists:get_value(poolsize, ConnArg),
    start_mysql(?PoolId, Host, User, Password, Database, Poolsize).

start_db(ConnArg) ->
    Pid = db_driver:start(),
    {ok, ConnPool} = db_driver:connect(ConnArg),
    ets:insert(?MODULE, {db_conn, ConnPool}),
    ets:insert(?MODULE, {db_pid, Pid}).

get_db_conn() ->
    [{_, Conn}] = ets:lookup(?MODULE, db_conn),
    Conn.

start_db() ->
    db_server:start(),
    db_server:init_default(test).

start_odbc(ConnString, Timeout) ->
    odbc:start(),
    odbc:connect(ConnString, [{timeout, Timeout}]).
    
start_mysql(PoolId, Host, User, Password, Database, Poolsize) ->
    mysql:start_link(PoolId, Host, User, Password, Database),
    connect_mysql(PoolId, Host, User, Password, Database, Poolsize).    

connect_mysql(PoolId, Host, User, Password, Database, 1) ->
    mysql:connect(PoolId, Host, undefined, User, Password, Database, true);
connect_mysql(PoolId, Host, User, Password, Database, Poolsize) ->
    mysql:connect(PoolId, Host, undefined, User, Password, Database, true),
    connect_mysql(PoolId, Host, User, Password, Database, Poolsize - 1).

stop() ->
    % [{_, OdbcConn}] = ets:lookup(?MODULE, odbc_conn),
    
    ets:delete(?MODULE),
    
    db_server:stop().
    
    % odbc:disconnect(OdbcConn).

timer_test_1() ->
    timer:start(),
    error_logger:format("start-----------~n"),
    timer:apply_interval(5*1000, ?MODULE, spawn_insert, []).

timer_test() ->
    timer:start(),
    error_logger:format("start-----------~n"),
    timer:apply_interval(5*1000, ?MODULE, spawn_select, []).
timer_test_2() ->
    timer:start(),
    error_logger:format("start-----------~n"),
    timer:apply_interval(5*1000, ?MODULE, spawn_update, []).
spawn_select() ->
    spawn(?MODULE, test_select, [c,100,3000]).
spawn_insert() ->
    spawn(?MODULE, test_insert, [c,100,3000]).
spawn_update() ->
    spawn(?MODULE, test_update, [c,100,3000]).

test(Driver, Type, N_Pro, N_Times) ->
    Fun = case Driver of
              mysql when Type == select ->
                  fun mysql_simple_select/0;
              mysql when Type == insert ->
                  fun mysql_simple_insert/0;
              mysql when Type == update ->
                  fun mysql_simple_update/0;
              mysql when Type == delete ->
                  fun mysql_simple_delete/0;
              db when Type == select ->
                  fun db_simple_select/0;
              db when Type == insert ->
                  fun db_simple_insert/0;
              db when Type == update ->
                  fun db_simple_update/0;
              db when Type == delete ->
                  fun db_simple_delete/0;
              odbc when Type == select ->
                  fun odbc_simple_select/0;
              odbc when Type == insert ->
                  fun odbc_simple_insert/0;
              odbc when Type == update ->
                  fun odbc_simple_update/0;
              odbc when Type == delete ->
                  fun odbc_simple_delete/0
          end,
    db_test(N_Pro,N_Times, Fun).

test_select(Driver, N_Pro, N_Times) ->
    test(Driver, select, N_Pro, N_Times).

test_insert(Driver, N_Pro, N_Times) ->
    test(Driver, insert, N_Pro, N_Times).

test_update(Driver, N_Pro, N_Times) ->
    test(Driver, update, N_Pro, N_Times).

test_delete(Driver, N_Pro, N_Times) ->
    test(Driver, delete, N_Pro, N_Times).


db_test(N_Pro,N_Times, WorkFun) ->
    %    error_logger:format("work fun :~p~n",[WorkFun]),
    MainId =  self(),
    P_Times =  N_Times div N_Pro,
    Collector = spawn(fun()->collect(MainId,N_Pro,0,0,0)end),
    Workers = [spawn(?MODULE,db_worker,[Collector, P_Times, WorkFun])||_I<-lists:seq(1,N_Pro)],
    Start = now(),
    [Worker!start||Worker<-Workers],
    receive
        {all_done, TotalSuc, TotalFail} ->
            Time = timer:now_diff(now(), Start) / 1000,
            error_logger:format("time consumed : ~p ms total successed :~p total failed :~p ~n",[Time,TotalSuc, TotalFail]),
            [exit(Worker,kill)||Worker<-Workers],
            ok;
        _ ->
            go_on
    end.

db_worker(Collector, Times, Fun) ->
    receive
        start ->
            do_job(Collector,Times, Fun, 0, 0)
    end.

do_job(Collector, 0, _Fun, Suc, Fail) ->
    %    error_logger:format("in job worker ~p done, suc num : ~p , fail num :~p ~n",[self(), Suc, Fail]),
    Collector!{done, self(), Suc, Fail};
do_job(Collector, Times, Fun, Suc, Fail) ->
    {NewSuc, NewFail} =
        case Fun() of
            {ok,_} ->
                {Suc+1, Fail};
            {data,_} ->
                {Suc+1, Fail};
            {updated,_} ->
                {Suc+1, Fail};
            Err ->
                error_logger:format("the error : ~p~n",[Err]),
                {Suc, Fail+1}
        end,
    do_job(Collector, Times - 1, Fun, NewSuc, NewFail).



collect(MainId, N_Pro, N_Finished, TotalSuc, TotalFail) when N_Pro==N_Finished ->
    MainId ! {all_done, TotalSuc, TotalFail} ;

collect(MainId, N_Pro, N_Finished, TotalSuc, TotalFail) ->
    receive
        {done, _Worker, Suc, Fail} ->
            %            error_logger:format("worker ~p done, suc num : ~p , fail num :~p finished : ~p ~n",[Worker, Suc, Fail,N_Finished+1]),
            collect(MainId, N_Pro, N_Finished +1, TotalSuc+Suc, TotalFail+Fail)
    end.

odbc_query(Sql) ->
    case odbc:connect(?ODBC_CONN_STRING, [{timeout, ?TimeOut}]) of
        {ok, Conn} ->
            R = 
                case odbc:sql_query(Conn, Sql, ?TimeOut) of
                    {selected, _, Res} ->
                        {ok, Res};
                    {updated, Res} ->
                        {ok, Res};
                    Res ->
                        {error, Res}
                end,
            odbc:disconnect(Conn),
            R;
        Err ->
            {error, Err}
    end.

db_simple_select() ->
    db_driver:execute_sql(get_db_conn(), "select * from user where name = 'haoboy'").
mysql_simple_select() ->
    mysql:fetch(?PoolId,"select * from user where name = 'haoboy'", ?TimeOut).
odbc_simple_select() ->
    odbc_query("select * from user where name = 'haoboy'").

c1_simple_insert() ->
    random:seed(now()),
    Age = random:uniform(100),
    db_api:insert(user, [{name, "haoboy"}, {age, Age}, {gid, 1}]).
db_simple_insert() ->
    random:seed(now()),
    Age = integer_to_list(random:uniform(100)),
    db_driver:execute_sql(get_db_conn(), "insert into user(name,age,gid) values('haoboy'," ++ Age ++",1)").
mysql_simple_insert() ->
    random:seed(now()),
    Age = integer_to_list(random:uniform(100)),
    mysql:fetch(?PoolId,"insert into user(name,age,gid) values('haoboy',"++Age++",1)", ?TimeOut).
odbc_simple_insert() ->
    random:seed(now()),
    Age = integer_to_list(random:uniform(100)),
    odbc_query("insert into user(name,age,gid) values('haoboy',"++Age++",1)").

c1_simple_delete() ->
    random:seed(now()),
    Age = random:uniform(100),
    db_api:delete(user, {age, '=', Age}).
db_simple_delete() ->
    random:seed(now()),
    Age = integer_to_list(random:uniform(100)),
    db_api:execute_sql("delete from user where age = " ++ Age).
mysql_simple_delete() ->
    random:seed(now()),
    Age = integer_to_list(random:uniform(100)),
    mysql:fetch(?PoolId,"delete from user where age = " ++ Age, ?TimeOut).
odbc_simple_delete() ->
    random:seed(now()),
    Age = integer_to_list(random:uniform(100)),
    odbc_query("delete from user where age = " ++ Age).

c1_simple_update() ->
    random:seed(now()),
    Age = random:uniform(100),
    db_api:update(user, [{name, "caoxu"}], {age, '=', Age}).
db_simple_update() ->
    random:seed(now()),
    Age = integer_to_list(random:uniform(100)),
    db_api:execute_sql("update user set name='caoxu' where age = "++Age).
mysql_simple_update() ->
    random:seed(now()),
    Age = integer_to_list(random:uniform(100)),
    mysql:fetch(?PoolId,"update user set name='caoxu' where age = "++Age, ?TimeOut).
odbc_simple_update() ->
    random:seed(now()),
    Age = integer_to_list(random:uniform(100)),
    odbc_query("update user set name='caoxu' where age = "++Age).