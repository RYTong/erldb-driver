

# Module db_conn #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)


The driver's APIs.
__Authors:__ deng.lifen ([`deng.lifen@rytong.com`](mailto:deng.lifen@rytong.com)).
<a name="description"></a>

## Description ##

<a name="types"></a>

## Data Types ##




### <a name="type-conn_arg">conn_arg()</a> ###



<pre><code>
conn_arg() = {default_pool, boolean()} | {driver, <a href="#type-driver_name">driver_name()</a>} | {host, string()} | {port, integer()} | {user, string()} | {password, string()} | {database, string()} | {poolsize, integer()} | {table_info, boolean()} | {default_pool, boolean()}
</code></pre>





### <a name="type-conn_args">conn_args()</a> ###



<pre><code>
conn_args() = [<a href="#type-conn_arg">conn_arg()</a>]
</code></pre>





### <a name="type-driver_name">driver_name()</a> ###



<pre><code>
driver_name() = mysql | oracle | db2 | sybase | informix
</code></pre>





### <a name="type-extras">extras()</a> ###



<pre><code>
extras() = tuple() | [tuple()]
</code></pre>





### <a name="type-fields">fields()</a> ###



<pre><code>
fields() = atom() | [atom()]
</code></pre>





### <a name="type-mybool">mybool()</a> ###



<pre><code>
mybool() = boolean() | 1 | 0
</code></pre>





### <a name="type-option">option()</a> ###



<pre><code>
option() = {pool, atom()} | {fields, <a href="#type-fields">fields()</a>} | {extras, <a href="#type-extras">extras()</a>} | {distinct, <a href="#type-mybool">mybool()</a>}
</code></pre>





### <a name="type-options">options()</a> ###



<pre><code>
options() = [<a href="#type-option">option()</a>]
</code></pre>





### <a name="type-param_list">param_list()</a> ###



<pre><code>
param_list() = [{Key::atom(), Value::term()}]
</code></pre>





### <a name="type-result">result()</a> ###



<pre><code>
result() = {ok, Res::term()} | {error, Error::term()}
</code></pre>





### <a name="type-tables">tables()</a> ###



<pre><code>
tables() = atom() | [atom()]
</code></pre>





### <a name="type-throw">throw()</a> ###



<pre><code>
throw() = {error, Error::term()}
</code></pre>





### <a name="type-where_expr">where_expr()</a> ###



<pre><code>
where_expr() = tuple() | [tuple()]
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_pool-2">add_pool/2</a></td><td>Synchronous call to the connection manager to add a pool.</td></tr><tr><td valign="top"><a href="#close_connection-1">close_connection/1</a></td><td>Deallocate all prepared statements and close the connection.</td></tr><tr><td valign="top"><a href="#connect-1">connect/1</a></td><td>Starts a connection and, if successful, add it to the
connection pool in the driver.</td></tr><tr><td valign="top"><a href="#decrement_pool_size-2">decrement_pool_size/2</a></td><td>Synchronous call to the connection manager to shrink a pool.</td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td>Delete one or more records from the database,
and return the number of rows deleted.</td></tr><tr><td valign="top"><a href="#disconnect-1">disconnect/1</a></td><td>Stops a connection and, if successful, remove it from the
connection pool in the driver.</td></tr><tr><td valign="top"><a href="#execute_param-3">execute_param/3</a></td><td>Send a query with parameters to the driver and wait for the result.</td></tr><tr><td valign="top"><a href="#execute_sql-2">execute_sql/2</a></td><td>Send a query to the driver and wait for the result.</td></tr><tr><td valign="top"><a href="#increment_pool_size-2">increment_pool_size/2</a></td><td>Synchronous call to the connection manager to enlarge a pool.</td></tr><tr><td valign="top"><a href="#init-1">init/1</a></td><td>Initializes the length of the thread pool.</td></tr><tr><td valign="top"><a href="#insert-3">insert/3</a></td><td>Insert a record into database.</td></tr><tr><td valign="top"><a href="#prepare-2">prepare/2</a></td><td>Register a prepared statement.</td></tr><tr><td valign="top"><a href="#prepare_execute-3">prepare_execute/3</a></td><td>Execute a prepared statement it has prepared.</td></tr><tr><td valign="top"><a href="#remove_pool-1">remove_pool/1</a></td><td>Synchronous call to the connection manager to remove a pool.</td></tr><tr><td valign="top"><a href="#reset_connection-2">reset_connection/2</a></td><td>Reset the connection.</td></tr><tr><td valign="top"><a href="#select-4">select/4</a></td><td>Find the records for the Where and Extras expressions.</td></tr><tr><td valign="top"><a href="#start-0">start/0</a></td><td>Spawn a process to load database driver library.</td></tr><tr><td valign="top"><a href="#stop-1">stop/1</a></td><td>Unload database driver library and stop the process.</td></tr><tr><td valign="top"><a href="#transaction-2">transaction/2</a></td><td>Execute a transaction.</td></tr><tr><td valign="top"><a href="#update-4">update/4</a></td><td>Update one or more records from the database,
and return the number of rows updated.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="add_pool-2"></a>

### add_pool/2 ###


<pre><code>
add_pool(PoolId::atom(), ConnArg::<a href="#type-record">record()</a>) -&gt; ok | no_return()
</code></pre>

<br></br>



Synchronous call to the connection manager to add a pool.


Creates a pool record, opens n=Size connections and calls
db_conn_server:add_pool/1 to make the pool known to the pool management,
it is translated into a blocking gen-server call.
<a name="close_connection-1"></a>

### close_connection/1 ###

`close_connection(Connection) -> any()`

Deallocate all prepared statements and close the connection.
<a name="connect-1"></a>

### connect/1 ###


<pre><code>
connect(ConnArgs::<a href="#type-conn_args">conn_args()</a>) -&gt; <a href="#type-result">result()</a>
</code></pre>

<br></br>


Starts a connection and, if successful, add it to the
connection pool in the driver.
<a name="decrement_pool_size-2"></a>

### decrement_pool_size/2 ###


<pre><code>
decrement_pool_size(PoolId::atom(), Num::integer()) -&gt; ok | no_return()
</code></pre>

<br></br>



Synchronous call to the connection manager to shrink a pool.



This reduces the connections by up to n=Num, but it only drops and closes available
connections that are not in use at the moment that this function is called. Connections
that are waiting for a server response are never dropped. In heavy duty, this function
may thus do nothing.


If Num is higher than the amount of connections or the amount of available connections,
exactly all available connections are dropped and closed.
<a name="delete-3"></a>

### delete/3 ###


<pre><code>
delete(Connection::<a href="#type-record">record()</a>, TableName::atom(), Where::<a href="#type-where_expr">where_expr()</a>) -&gt; <a href="#type-result">result()</a>
</code></pre>

<br></br>


Delete one or more records from the database,
and return the number of rows deleted.
<a name="disconnect-1"></a>

### disconnect/1 ###


<pre><code>
disconnect(Connection::<a href="#type-record">record()</a> | <a href="#type-record">record()</a>) -&gt; <a href="#type-result">result()</a>
</code></pre>

<br></br>


Stops a connection and, if successful, remove it from the
connection pool in the driver.
<a name="execute_param-3"></a>

### execute_param/3 ###


<pre><code>
execute_param(Connection::<a href="#type-record">record()</a>, Statement::string(), ParamList::<a href="#type-param_list">param_list()</a>) -&gt; <a href="#type-result">result()</a>
</code></pre>

<br></br>


Send a query with parameters to the driver and wait for the result.
<a name="execute_sql-2"></a>

### execute_sql/2 ###


<pre><code>
execute_sql(Connection::<a href="#type-record">record()</a>, Sql::string()) -&gt; <a href="#type-result">result()</a>
</code></pre>

<br></br>


Send a query to the driver and wait for the result.
<a name="increment_pool_size-2"></a>

### increment_pool_size/2 ###


<pre><code>
increment_pool_size(PoolId::atom(), Num::integer()) -&gt; ok | no_return()
</code></pre>

<br></br>



Synchronous call to the connection manager to enlarge a pool.


This opens n=Num new connections and adds them to the pool of id PoolId.
<a name="init-1"></a>

### init/1 ###


<pre><code>
init(ThreadLen::integer()) -&gt; ok
</code></pre>

<br></br>


Initializes the length of the thread pool.
<a name="insert-3"></a>

### insert/3 ###


<pre><code>
insert(Connection::<a href="#type-record">record()</a>, TableName::atom(), ParamList::<a href="#type-param_list">param_list()</a>) -&gt; <a href="#type-result">result()</a>
</code></pre>

<br></br>


Insert a record into database.
<a name="prepare-2"></a>

### prepare/2 ###


<pre><code>
prepare(Connection::<a href="#type-record">record()</a>, Statement::string()) -&gt; <a href="#type-result">result()</a>
</code></pre>

<br></br>


Register a prepared statement.
<a name="prepare_execute-3"></a>

### prepare_execute/3 ###


<pre><code>
prepare_execute(Connection::<a href="#type-record">record()</a>, StmtName::string(), Args::list()) -&gt; <a href="#type-result">result()</a>
</code></pre>

<br></br>


Execute a prepared statement it has prepared.
<a name="remove_pool-1"></a>

### remove_pool/1 ###


<pre><code>
remove_pool(PoolId::atom()) -&gt; ok | no_return()
</code></pre>

<br></br>


Synchronous call to the connection manager to remove a pool.
<a name="reset_connection-2"></a>

### reset_connection/2 ###


<pre><code>
reset_connection(Conn::<a href="#type-record">record()</a>, StayLocked::pass | keep) -&gt; <a href="#type-record">record()</a> | {error, Error::term()}
</code></pre>

<br></br>



Reset the connection.


if a process dies or times out while doing work, the connection reset
in the conn_server state. Also a new connection needs to be opened to
replace the old one. If that fails, we queue the old as available for
the next try by the next caller process coming along. So the pool can't
run dry, even though it can freeze.
<a name="select-4"></a>

### select/4 ###


<pre><code>
select(Connection::<a href="#type-record">record()</a>, TableList::<a href="#type-tables">tables()</a>, Where::<a href="#type-where_expr">where_expr()</a>, Opts::<a href="#type-options">options()</a>) -&gt; <a href="#type-result">result()</a>
</code></pre>

<br></br>



Find the records for the Where and Extras expressions.


If no records match the conditions, the function returns {ok, []}.
<a name="start-0"></a>

### start/0 ###


<pre><code>
start() -&gt; pid()
</code></pre>

<br></br>


Spawn a process to load database driver library.
<a name="stop-1"></a>

### stop/1 ###


<pre><code>
stop(Pid::pid()) -&gt; ok
</code></pre>

<br></br>


Unload database driver library and stop the process.
<a name="transaction-2"></a>

### transaction/2 ###


<pre><code>
transaction(PoolId::atom(), Fun::function()) -&gt; <a href="#type-result">result()</a>
</code></pre>

<br></br>


Execute a transaction.
<a name="update-4"></a>

### update/4 ###


<pre><code>
update(Connection::<a href="#type-record">record()</a>, TableName::atom(), ParamList::<a href="#type-param_list">param_list()</a>, Where::<a href="#type-where_expr">where_expr()</a>) -&gt; <a href="#type-result">result()</a>
</code></pre>

<br></br>


Update one or more records from the database,
and return the number of rows updated.
