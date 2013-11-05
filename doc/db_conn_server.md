

# Module db_conn_server #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)


The driver's APIs.
__Behaviours:__ [`gen_server`](gen_server.md).

__Authors:__ deng.lifen ([`deng.lifen@rytong.com`](mailto:deng.lifen@rytong.com)).
<a name="description"></a>

## Description ##
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_connections-2">add_connections/2</a></td><td>Add connections to the connection pool.</td></tr><tr><td valign="top"><a href="#add_pool-1">add_pool/1</a></td><td>Add a connection pool.</td></tr><tr><td valign="top"><a href="#code_change-3">code_change/3</a></td><td>Convert process state when code is changed.</td></tr><tr><td valign="top"><a href="#get_conn_args-1">get_conn_args/1</a></td><td>Get the connection parameters in the connection pool.</td></tr><tr><td valign="top"><a href="#get_default_pool-0">get_default_pool/0</a></td><td>Get the default connection pool.</td></tr><tr><td valign="top"><a href="#get_pool-1">get_pool/1</a></td><td>Get the connection pool.</td></tr><tr><td valign="top"><a href="#get_pool_info-2">get_pool_info/2</a></td><td>Get information about the connection pool.</td></tr><tr><td valign="top"><a href="#handle_call-3">handle_call/3</a></td><td>Handling call messages.</td></tr><tr><td valign="top"><a href="#handle_cast-2">handle_cast/2</a></td><td>Handling cast messages.</td></tr><tr><td valign="top"><a href="#handle_info-2">handle_info/2</a></td><td>Handling all non call/cast messages.</td></tr><tr><td valign="top"><a href="#init-1">init/1</a></td><td>Initiates the server.</td></tr><tr><td valign="top"><a href="#pass_connection-1">pass_connection/1</a></td><td>Pass a locked connection.</td></tr><tr><td valign="top"><a href="#pools-0">pools/0</a></td><td>Get all pools.</td></tr><tr><td valign="top"><a href="#remove_connections-2">remove_connections/2</a></td><td>Remove N connections from the connection pool.</td></tr><tr><td valign="top"><a href="#remove_pool-1">remove_pool/1</a></td><td>Remove the connection pool and return it.</td></tr><tr><td valign="top"><a href="#replace_connection_as_available-2">replace_connection_as_available/2</a></td><td>Replace a close connection to a new one, and make it into available queue.</td></tr><tr><td valign="top"><a href="#replace_connection_as_locked-2">replace_connection_as_locked/2</a></td><td>Replace a close connection to a new one, and make it into locked queue.</td></tr><tr><td valign="top"><a href="#set_default_pool-1">set_default_pool/1</a></td><td>Set the default connection pool.</td></tr><tr><td valign="top"><a href="#set_table_schema-2">set_table_schema/2</a></td><td>Set table_schemas in the connection pool.</td></tr><tr><td valign="top"><a href="#start_link-1">start_link/1</a></td><td>Starts the server.</td></tr><tr><td valign="top"><a href="#terminate-2">terminate/2</a></td><td>Close all connections and stop db driver.</td></tr><tr><td valign="top"><a href="#wait_for_connection-1">wait_for_connection/1</a></td><td>Wait for a available connection.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="add_connections-2"></a>

### add_connections/2 ###


<pre><code>
add_connections(PoolId::atom(), Conns::[<a href="#type-record">record()</a>]) -&gt; ok
</code></pre>

<br></br>


Add connections to the connection pool.
<a name="add_pool-1"></a>

### add_pool/1 ###


<pre><code>
add_pool(Pool::<a href="#type-record">record()</a>) -&gt; ok
</code></pre>

<br></br>


Add a connection pool.
<a name="code_change-3"></a>

### code_change/3 ###


<pre><code>
code_change(OldVsn, State, Extra) -&gt; {ok, NewState}
</code></pre>

<br></br>


Convert process state when code is changed
<a name="get_conn_args-1"></a>

### get_conn_args/1 ###


<pre><code>
get_conn_args(PoolId::atom()) -&gt; <a href="#type-record">record()</a>
</code></pre>

<br></br>


Get the connection parameters in the connection pool.
<a name="get_default_pool-0"></a>

### get_default_pool/0 ###


<pre><code>
get_default_pool() -&gt; <a href="#type-record">record()</a> | undefined
</code></pre>

<br></br>


Get the default connection pool.
<a name="get_pool-1"></a>

### get_pool/1 ###


<pre><code>
get_pool(PoolId::atom()) -&gt; <a href="#type-record">record()</a>
</code></pre>

<br></br>


Get the connection pool.
<a name="get_pool_info-2"></a>

### get_pool_info/2 ###


<pre><code>
get_pool_info(PoolId::atom(), Info::conn_args | table_schemas | size) -&gt; term()
</code></pre>

<br></br>


Get information about the connection pool.
<a name="handle_call-3"></a>

### handle_call/3 ###


<pre><code>
handle_call(X1::Request, From, State) -&gt; {reply, Reply, State} | {reply, Reply, State, Timeout} | {noreply, State} | {noreply, State, Timeout} | {stop, Reason, Reply, State} | {stop, Reason, State}
</code></pre>

<br></br>


Handling call messages
<a name="handle_cast-2"></a>

### handle_cast/2 ###


<pre><code>
handle_cast(Msg, State) -&gt; {noreply, State} | {noreply, State, Timeout} | {stop, Reason, State}
</code></pre>

<br></br>


Handling cast messages
<a name="handle_info-2"></a>

### handle_info/2 ###


<pre><code>
handle_info(Info, State) -&gt; {noreply, State} | {noreply, State, Timeout} | {stop, Reason, State}
</code></pre>

<br></br>


Handling all non call/cast messages
<a name="init-1"></a>

### init/1 ###


<pre><code>
init(ThreadLen::integer()) -&gt; {ok, State} | {ok, State, Timeout} | ignore | {stop, Reason}
</code></pre>

<br></br>



Initiates the server.



Starts the db driver and save driver pid in state.


ThreadLen: Initializes the driver thread length.
<a name="pass_connection-1"></a>

### pass_connection/1 ###


<pre><code>
pass_connection(Connection::<a href="#type-record">record()</a>) -&gt; ok | {error, Error::term()}
</code></pre>

<br></br>



Pass a locked connection.



Check if any processes are waiting for a connection.



If there is no program is in waiting, add the connection to the 'available'
queue and remove from 'locked' tree.



If the waiting queue is not empty then remove the head of the queue and
send it the connection.


Update the pool and queue in state once the head has been removed.
<a name="pools-0"></a>

### pools/0 ###


<pre><code>
pools() -&gt; [<a href="#type-record">record()</a>]
</code></pre>

<br></br>


Get all pools.
<a name="remove_connections-2"></a>

### remove_connections/2 ###


<pre><code>
remove_connections(PoolId::atom(), Num::integer()) -&gt; ok
</code></pre>

<br></br>



Remove N connections from the connection pool.


If N > pool_size, then remove all connections.
<a name="remove_pool-1"></a>

### remove_pool/1 ###


<pre><code>
remove_pool(PoolId::atom()) -&gt; <a href="#type-record">record()</a>
</code></pre>

<br></br>


Remove the connection pool and return it.
<a name="replace_connection_as_available-2"></a>

### replace_connection_as_available/2 ###


<pre><code>
replace_connection_as_available(OldConn::<a href="#type-record">record()</a>, NewConn::<a href="#type-record">record()</a>) -&gt; ok | {error, Error::term()}
</code></pre>

<br></br>



Replace a close connection to a new one, and make it into available queue.



if an error occurs while doing work over a connection then the connection
must be closed and a new one created in its place. The calling process is
responsible for creating the new connection, closing the old one and replacing
it in state.


This function expects a new, available connection to be passed in to serve
as the replacement for the old one.
<a name="replace_connection_as_locked-2"></a>

### replace_connection_as_locked/2 ###


<pre><code>
replace_connection_as_locked(OldConn::<a href="#type-record">record()</a>, NewConn::<a href="#type-record">record()</a>) -&gt; ok | {error, Error::term()}
</code></pre>

<br></br>



Replace a close connection to a new one, and make it into locked queue.


Replace an existing, locked condition with the newly supplied one
and keep it in the locked list so that the caller can continue to use it
without having to lock another connection.
<a name="set_default_pool-1"></a>

### set_default_pool/1 ###


<pre><code>
set_default_pool(PoolId::atom()) -&gt; ok
</code></pre>

<br></br>



Set the default connection pool.


If the default pool is exist, overwrite it.
<a name="set_table_schema-2"></a>

### set_table_schema/2 ###


<pre><code>
set_table_schema(PoolId::atom(), TableSchemas::[{atom(), list()}]) -&gt; ok
</code></pre>

<br></br>


Set table_schemas in the connection pool.
<a name="start_link-1"></a>

### start_link/1 ###


<pre><code>
start_link(ThreadLen::integer()) -&gt; {ok, Pid} | ignore | {error, Error}
</code></pre>

<br></br>



Starts the server.


ThreadLen: Initializes the driver thread length.
<a name="terminate-2"></a>

### terminate/2 ###


<pre><code>
terminate(Reason, State) -&gt; <a href="#type-void">void()</a>
</code></pre>

<br></br>



Close all connections and stop db driver.



This function is called by a gen_server when it is about to terminate.
It should be the opposite of Module:init/1 and do any necessary
cleaning up.


When it returns, the gen_server terminates with Reason. The return
value is ignored.
<a name="wait_for_connection-1"></a>

### wait_for_connection/1 ###


<pre><code>
wait_for_connection(PoolId::atom()) -&gt; <a href="#type-record">record()</a>
</code></pre>

<br></br>



Wait for a available connection.



If ?TRANSCATION_CONNECTION is exist, then return the transaction connection.


If it isn't exist, try to lock a connection. If no connections are available
then wait to be notified of the next available connection.
