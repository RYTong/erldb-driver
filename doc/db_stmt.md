

# Module db_stmt #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)


Management prepared statement storage.
__Behaviours:__ [`gen_server`](gen_server.md).

__Authors:__ deng.lifen ([`deng.lifen@rytong.com`](mailto:deng.lifen@rytong.com)).
<a name="description"></a>

## Description ##
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add-2">add/2</a></td><td>Add Statement if not exist.</td></tr><tr><td valign="top"><a href="#code_change-3">code_change/3</a></td><td>Convert process state when code is changed.</td></tr><tr><td valign="top"><a href="#fetch-1">fetch/1</a></td><td>Lookup Statement.</td></tr><tr><td valign="top"><a href="#handle_call-3">handle_call/3</a></td><td>Handling call messages.</td></tr><tr><td valign="top"><a href="#handle_cast-2">handle_cast/2</a></td><td>Handling cast messages.</td></tr><tr><td valign="top"><a href="#handle_info-2">handle_info/2</a></td><td>Handling all non call/cast messages.</td></tr><tr><td valign="top"><a href="#init-1">init/1</a></td><td>Initiates the server.</td></tr><tr><td valign="top"><a href="#prepare-3">prepare/3</a></td><td>Add statement object pointer to prepared.</td></tr><tr><td valign="top"><a href="#remove-1">remove/1</a></td><td>Remove statement object pointer from prepared.</td></tr><tr><td valign="top"><a href="#start_link-0">start_link/0</a></td><td>Starts the server.</td></tr><tr><td valign="top"><a href="#stmtdata-2">stmtdata/2</a></td><td>Lookup statement object pointer by connection object pointer.</td></tr><tr><td valign="top"><a href="#terminate-2">terminate/2</a></td><td>Do nothing.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="add-2"></a>

### add/2 ###


<pre><code>
add(StmtName::atom(), Statement::string()) -&gt; ok | {error, statement_existed}
</code></pre>

<br></br>


Add Statement if not exist.
<a name="code_change-3"></a>

### code_change/3 ###


<pre><code>
code_change(OldVsn, State, Extra) -&gt; {ok, NewState}
</code></pre>

<br></br>


Convert process state when code is changed
<a name="fetch-1"></a>

### fetch/1 ###


<pre><code>
fetch(StmtName::atom()) -&gt; Statement::string() | undefined
</code></pre>

<br></br>


Lookup Statement.
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
init(X1::Args) -&gt; {ok, State} | {ok, State, Timeout} | ignore | {stop, Reason}
</code></pre>

<br></br>


Initiates the server
<a name="prepare-3"></a>

### prepare/3 ###


<pre><code>
prepare(ConnId::binary(), StmtName::atom(), StmtData::binary) -&gt; StmtData::binary() | undefined
</code></pre>

<br></br>


Add statement object pointer to prepared.
<a name="remove-1"></a>

### remove/1 ###


<pre><code>
remove(ConnId::binary()) -&gt; [StmtData::binary()]
</code></pre>

<br></br>


Remove statement object pointer from prepared.
<a name="start_link-0"></a>

### start_link/0 ###


<pre><code>
start_link() -&gt; {ok, Pid} | ignore | {error, Error}
</code></pre>

<br></br>


Starts the server
<a name="stmtdata-2"></a>

### stmtdata/2 ###


<pre><code>
stmtdata(ConnId::binary(), StmtName::atom()) -&gt; StmtData::binary() | undefined
</code></pre>

<br></br>


Lookup statement object pointer by connection object pointer.
<a name="terminate-2"></a>

### terminate/2 ###


<pre><code>
terminate(Reason, State) -&gt; <a href="#type-void">void()</a>
</code></pre>

<br></br>



Do nothing.



This function is called by a gen_server when it is about to terminate.
It should be the opposite of Module:init/1 and do any necessary
cleaning up.


When it returns, the gen_server terminates with Reason. The return
value is ignored.
