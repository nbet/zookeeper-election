

#ifdef __cplusplus
extern "C"{
#endif

typedef enum {ZK_OK = 0,ZK_NOK} ZK_RET;

//print callback 
typedef void (*log_func_type)(const char *);

//notify master/slave callback
typedef int (*notify_master_type)(int, const char *);

//notify config data callback
typedef int (*notify_config_data_type)(const char *, char *);


/*
description:set log level
parameter:
	flag:
		0:necessary log
		1:all log
return:
	ZK_OK: 0
	ZK_NOK: 1
*/
ZK_RET sscs_zk_log_set(int flag);

/*
description:add auth function on monitor znode
parameter:
	flag:
		0:not use auth function
		1:use auth function
	usrpasswd:user and passwd. "sumscope:sumscope"
*/
ZK_RET sscs_zk_auth_set(int flag, const char *usrpasswd);

/*
description:create session with zookeeper cluster
parameter:
	hosts:the format is ip1:portnum[,ip2:portnum,...]
	znode_product:
		the znode of monitor;
		eg: znode_product = "/Product"
	print_log_func:print function
*/
ZK_RET sscs_zk_session_init(const char *hosts, const char *znode_product, log_func_type print_log_func);

/*
description:create session with zookeeper cluster
parameter:
	hostnames:"zoo1,zoo2,zoo3"
	znode_product:
		the znode of monitor;
		eg: znode_product = "/Product"
	print_log_func:print function
*/
ZK_RET sscs_zk_session_init_by_hostname(const char *hostnames, const char *znode_product, log_func_type print_log_func);

/*
description:
	create ephemeral znode and set monitor on parent path
parameter:
	znode_module:
		the znode of monitor;
		eg: znode_module = "/Product/Monitor"
	notify_func:notify master/slave status

*/
ZK_RET sscs_zk_master_create(const char *znode_module, notify_master_type notify_func);

/*
description:get master or slave flag
parameter:
	znode_module:
		the znode of monitor;
		eg: znode_module = "/Product/Monitor"
status:master or slave
*/
ZK_RET sscs_zk_master_status_get(const char *znode_module, int *status);

/*
description:get config data from znode and notify back to service 
parameter:
	znode:
		the znode storing config data;
		eg: znode = "/zk1"
	buffer:store config data
	buffer_len:the max length of buffer
	userpasswd:user and passwd.
	notify_func:notify service the config data
*/
ZK_RET sscs_zk_fetch_znode_data(const char *znode,char *buffer,int buffer_len,const char *usrpasswd,notify_config_data_type notify_func);

/*
description:set acl on path
parameter:
	path:"/Product/module"
	usrpasswd:user and passwd.
*/
ZK_RET sscs_zk_acl_set(const char *path,const char *usrpasswd);

/*
description:add auth
parameter:
	usrpasswd:user and passwd.
*/
ZK_RET sscs_zk_auth_add(const char *usrpasswd);

/*
description:create znode
parameter:
	znode_m:a node
*/
ZK_RET sscs_zk_znode_create(const char *znode_m);


ZK_RET sscs_zk_set_log_stream(FILE* logStream);


#ifdef __cplusplus
}
#endif

