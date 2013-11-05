

# Module db_sup #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)


The driver's supervisor.
__Behaviours:__ [`supervisor`](supervisor.md).

__Authors:__ deng.lifen ([`deng.lifen@rytong.com`](mailto:deng.lifen@rytong.com)).
<a name="description"></a>

## Description ##
Starts db_stmt server and db_conn_server.
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#init-1">init/1</a></td><td>Start db_stmt server and db_conn_server.</td></tr><tr><td valign="top"><a href="#start_link-1">start_link/1</a></td><td>Creates a supervisor process as part of a supervision tree.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="init-1"></a>

### init/1 ###

`init(ThreadLen) -> any()`


Start db_stmt server and db_conn_server.



Whenever a supervisor is started using
supervisor:start_link/[2,3], this function is called by the new process
to find out about restart strategy, maximum restart frequency and child
specifications.


ThreadLen: Initializes the driver thread length.
<a name="start_link-1"></a>

### start_link/1 ###

`start_link(ThreadLen) -> any()`


Creates a supervisor process as part of a supervision tree.


ThreadLen: Initializes the driver thread length.
