
#include <zookeeper.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <ss_config_service.h>
#include <openssl/sha.h>
#include <openssl/crypto.h> 
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h> 

#ifdef WIN32
	#include <winSock2.h>
#else
	#include <netdb.h>
    #include <arpa/inet.h> 
	#include <netinet/in.h> 
#endif

#ifdef WIN32
	#define SLEEP(n)	Sleep((n)*1000)
#else 
	#define SLEEP(n)  	sleep(n)
#endif

#define zk_data_size 4096
#define zk_buffer_size 1024
#define zk_host_size 512
#define zk_usrpasswd_size 100
#define zk_path_length 100


//the default role is slave
enum WORK_MODE{MODE_SLAVE,MODE_MASTER,MODE_UNKNOWN} g_zk_mode = MODE_UNKNOWN;

//session handle
static zhandle_t *zkhandle; 

//session id
static clientid_t zk_myid;  

//"172.16.85.35:2181,172.16.85.34:2181,172.16.85.33:2181"
char g_zk_hosts[zk_buffer_size]={0};

//store children data
char g_zk_localhost[zk_buffer_size]={0};

//store config data from znode
char g_zk_data_buffer[zk_data_size]={0};

char g_zk_znode_product[zk_path_length]={0}; 
char g_zk_znode_module[zk_path_length]={0}; 

int g_zk_debug_flag = 0;
int g_zk_acl_flag = 0;

log_func_type g_zk_log_func = NULL;

notify_master_type g_zk_notify_master = NULL;

notify_config_data_type  g_zk_notify_data = NULL;

const char g_zk_scheme[10]="digest";
char g_zk_usrpasswd_all[zk_usrpasswd_size]={0};
char g_zk_usrpasswd_read[zk_usrpasswd_size]="sumscope:read";
char g_zk_usrpasswd_cs[zk_usrpasswd_size]={0};

#define ZK_DEBUG_PRINT(format, ...) \
		{ \
               char tmp[zk_buffer_size]={0}; \
               snprintf(tmp,zk_buffer_size, format, ##__VA_ARGS__);\
               g_zk_log_func(tmp);\
		}

void zk_getlocalhost(char *ip_pid,int len)  
{  
	char hostname[zk_host_size] = {0};  

	ZK_DEBUG_PRINT("[SSCS]zk_getlocalhost()");
		
    gethostname(hostname,sizeof(hostname));  
	snprintf(ip_pid,len,"%s:%d",hostname,getpid()); 
}  

void sscs_zk_choose_master(zhandle_t *zkhandle,const char *path)  
{  	
    int ret,i=0; 
	int zk_log_str_len = zk_buffer_size;
	int ip_pid_len = zk_host_size;  
	char master_path[zk_buffer_size] ={0};  
	char ip_pid[zk_host_size] = {0};  
	char tmp[zk_host_size]={0}; 
	struct String_vector procs; 

	ZK_DEBUG_PRINT("[SSCS]sscs_zk_choose_master()");	

	ret = zoo_get_children(zkhandle,path,1,&procs);  
    if(ret != ZOK || procs.count == 0){  
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_choose_master():Failed to get CHILDREN. PATH:%s RET:%d",path,ret);
		return;
    }

	zk_getlocalhost(g_zk_localhost,sizeof(g_zk_localhost));  

	//find the smallest znode 
	strncpy(tmp,procs.data[0],strlen(procs.data[0]));  
	for(i = 1; i < procs.count; ++i){  
		if(strncmp(tmp,procs.data[i],strlen(procs.data[i]))>0){  
			strncpy(tmp,procs.data[i],strlen(procs.data[i]));  
		}  
	}  
	snprintf(master_path,zk_buffer_size,"%s/%s",path,tmp);  

	ZK_DEBUG_PRINT("[SSCS]sscs_zk_choose_master():Master PATH %s",master_path);

	ret = zoo_get(zkhandle,master_path,0,ip_pid,&ip_pid_len,NULL);  
	if(ret != ZOK){  
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_choose_master():Failed to get DATA. PATH:%s RET:%d",master_path,ret);
		return;
	}else if(strncmp(ip_pid,g_zk_localhost,sizeof(g_zk_localhost))==0){  
		g_zk_mode = MODE_MASTER;  
	}else {
		g_zk_mode = MODE_SLAVE;  
	}

	ZK_DEBUG_PRINT("[SSCS]sscs_zk_choose_master():Master DATA %s, Local DATA %s",ip_pid,g_zk_localhost);

	if(g_zk_mode == MODE_MASTER){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_choose_master(): I am MASTER");
	}
	else if(g_zk_mode == MODE_SLAVE){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_choose_master(): I am SLAVE");
	}else {
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_choose_master(): Unknown");
	}
	
	return;
}  

void sscs_zk_list_nodes(zhandle_t *zkhandle,const char *path)  
{  
    int ret,i=0; 
	int master_len = zk_host_size; 
	int ip_pid_len = zk_host_size;  
	char zk_path[zk_buffer_size] ={0};  
	char master_path[zk_buffer_size] ={0};  
	char master_data[zk_host_size] = {0};  
	char ip_pid[zk_host_size] = {0};  
	struct String_vector procs; 

	ZK_DEBUG_PRINT("[SSCS]sscs_zk_list_nodes()");

	ret = zoo_get_children(zkhandle,path,0,&procs);  
    if(ret != ZOK || procs.count == 0){  
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_list_nodes():Failed to get CHILDREN. PATH:%s RET:%d",path,ret);
		return;
    }

	//find the smallest znode
	strncpy(zk_path,procs.data[0],strlen(procs.data[0]));  
	for(i = 1; i < procs.count; ++i){  
		if(strncmp(zk_path,procs.data[i],strlen(procs.data[i]))>0){  
			strncpy(zk_path,procs.data[i],strlen(procs.data[i]));  
		}
	}
	snprintf(master_path,zk_buffer_size,"%s/%s",path,zk_path); 

	ret = zoo_get(zkhandle,master_path,0,master_data,&master_len,NULL);  
	if(ret != ZOK){  
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_list_nodes():Failed to get DATA. PATH:%s RET:%d",master_path,ret);
		return;
	}

	for(i = 0; i < procs.count; ++i){  
		snprintf(zk_path,zk_buffer_size,"%s/%s",path,procs.data[i]); 
		
		ret = zoo_get(zkhandle,zk_path,0,ip_pid,&ip_pid_len,NULL);  
		if(ret != ZOK){  
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_list_nodes():Failed to get DATA. PATH:%s RET:%d",zk_path,ret);
		}else if(strncmp(ip_pid,master_data,master_len)==0){  
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_list_nodes():%s(Master)",ip_pid);
		}else{  
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_list_nodes():%s(Slave)",ip_pid);
		}		
		memset(ip_pid, 0, ip_pid_len);	
		ip_pid_len = sizeof(ip_pid);
	}  
	
	return;
}

ZK_RET sscs_zk_session_reestablish()
{
	ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_reestablish()");
	if(sscs_zk_session_init(g_zk_hosts, g_zk_znode_product, g_zk_log_func) == ZK_NOK){
		return ZK_NOK;
	}
	if(sscs_zk_master_create(g_zk_znode_module, g_zk_notify_master) == ZK_NOK){
		return ZK_NOK;
	}

	return ZK_OK;
}

void zk_watcher_g(zhandle_t* zh, int type, int state, const char* path, void* watcherCtx)    
{ 
	int len;
	int ret;
	int master_flag;

	ZK_DEBUG_PRINT("[SSCS]zk_watcher_g()");
	ZK_DEBUG_PRINT("[SSCS]watcher event[zkhandle: %d type: %d state: %d path: %s watcherCtx: %s]", zh,type,state,path,watcherCtx);

	if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            const clientid_t *id = zoo_client_id(zh);
            if (zk_myid.client_id == 0 || zk_myid.client_id != id->client_id) {
				ZK_DEBUG_PRINT("[SSCS]zk_watcher_g():New id is %d, old id is %d",id->client_id,zk_myid.client_id);
                zk_myid = *id;
            }
        } else if (state == ZOO_AUTH_FAILED_STATE) {
			ZK_DEBUG_PRINT("[SSCS]zk_watcher_g():Authentication failure. Shutting down...");
            zookeeper_close(zh);
			zkhandle = NULL;
			zk_myid.client_id = 0;
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
			ZK_DEBUG_PRINT("[SSCS]zk_watcher_g():Session expired. Shutting down...");
            zookeeper_close(zh);
			zkhandle = NULL;
			zk_myid.client_id = 0;

			if(sscs_zk_session_reestablish() == ZK_NOK)	{
				g_zk_mode = MODE_UNKNOWN;
			}
			if (g_zk_notify_master != NULL) {
				ZK_DEBUG_PRINT("[SSCS]zk_watcher_g():invoke g_zk_notify_master, it's %s",(g_zk_mode == MODE_MASTER)?"Master":((g_zk_mode == MODE_SLAVE)?"Slave":"Unknown"));
				master_flag = (g_zk_mode == MODE_MASTER)?1:((g_zk_mode == MODE_SLAVE)?0:2);	
				g_zk_notify_master(master_flag, path);
			}

        }
    }
	
    if(type == ZOO_CHILD_EVENT &&  
        state == ZOO_CONNECTED_STATE ){  
        sscs_zk_choose_master(zh,path);  
		sscs_zk_list_nodes(zh,path);  	
		      
		if (g_zk_notify_master != NULL) {			
			ZK_DEBUG_PRINT("[SSCS]zk_watcher_g():invoke g_zk_notify_master, it's %s", (g_zk_mode == MODE_MASTER)?"Master":((g_zk_mode == MODE_SLAVE)?"Slave":"Unknown"));
			master_flag = (g_zk_mode == MODE_MASTER)?1:((g_zk_mode == MODE_SLAVE)?0:2);	
			g_zk_notify_master(master_flag, path);
		}
	} 

	if((type == ZOO_CHANGED_EVENT ||
		type == ZOO_CREATED_EVENT) &&  
		state == ZOO_CONNECTED_STATE ){
		len = zk_data_size;
		memset(g_zk_data_buffer,0,len);
		ret = sscs_zk_fetch_znode_data(path,g_zk_data_buffer,len,g_zk_usrpasswd_cs,g_zk_notify_data);
		if(ret == ZK_NOK){

			ZK_DEBUG_PRINT("[SSCS]zk_watcher_g():Fetch DATA failed. PATH:%s RET:%d",path,ret);
		}
	}

	if(type == ZOO_DELETED_EVENT &&  
       state == ZOO_CONNECTED_STATE ){

		ret = zoo_exists(zh,path,1,NULL);
		if(ret == ZNONODE){

			ZK_DEBUG_PRINT("[SSCS]zk_watcher_g():Znode deleted, Set WATCH on %s ", path);
		}else{

			ZK_DEBUG_PRINT("[SSCS]zk_watcher_g():zoo_exists Unknown error. PATH:%s RET:%d ", path, ret);
		}
	}

}


ZK_RET sscs_zk_session_init(const char *hosts, const char *znode_product,log_func_type print_log_func)
{
	int i,ret;
	char path_buffer[zk_buffer_size]={0};   
	zkhandle = NULL;
	
	if (print_log_func == NULL){
		return ZK_NOK;
	}
	g_zk_log_func = print_log_func;

	ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init()");
	for(i=0;i< strlen(hosts);i++)
	{
		if (isdigit(hosts[i])|(hosts[i]=='.')|(hosts[i]==':')|(hosts[i]==','))
			continue;
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init():Hosts error %s",hosts);
		return ZK_NOK;
	}

	if (!(znode_product[0]=='/'))
	{
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init():znode_product error %s",znode_product);
		return ZK_NOK;
	}

	if(strlen(hosts)>zk_buffer_size)
	{
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init():Hosts length %d too long",strlen(hosts));
        return ZK_NOK;
	}
	//restore znode_module for reestablish
	strncpy(g_zk_hosts,hosts,strlen(hosts));
	strncpy(g_zk_znode_product,znode_product,strlen(znode_product));
	ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init():g_zk_znode_product:%s", g_zk_znode_product);

	//it is asynchronous function
	//in zookeeper，it will try sometimes to recreate 
    zkhandle = zookeeper_init((const char *)hosts, zk_watcher_g, 30000, 0, 0, 0);
    if (!zkhandle) {
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init():zookeeper_init failed.");
        return ZK_NOK;
    }

	SLEEP(1);

	//check session is OK
	if (zk_myid.client_id == 0){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init():Session not Established.");
		return ZK_NOK;
	}

	ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init():Create session successfully");

    ret = zoo_exists(zkhandle,znode_product,0,NULL);
	if(ret == ZNONODE){
		ret = zoo_create(zkhandle,znode_product,"1.0",3,    
                          &ZOO_OPEN_ACL_UNSAFE,0,    
                          path_buffer,sizeof(path_buffer));    
        if(ret != ZOK){  
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init():Failed to create. PATH:%s RET:%d",znode_product,ret);
			return ZK_NOK;
        }	

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init():Create successfully. PATH:%s",znode_product);
	}else if(ret == ZOK){  

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init():Exists. PATH:%s",znode_product);
	}else{

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init():unknown error. RET:%d",ret);
		return ZK_NOK;
	}

	return ZK_OK;
}


ZK_RET sscs_zk_hostname_to_ip(const char *hostnames, char *hosts)
{
	int ret,len=0;
	const char catstr1[10]=":2181";
	const char catstr2[10]=",";
	struct hostent *host;
	struct in_addr addr;
	char *pos = NULL;
	char hnames[512]={0};
	char hnames_swp[512]={0};
	char hname[100]={0};
#ifdef WIN32
	WSADATA wsaData;
	ret = WSAStartup(0x101,&wsaData);
	if(ret!= 0)
	{
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_hostname_to_ip():WSAStartup ERROR:%d RET:%d",WSAGetLastError(),ret);
		return ZK_NOK;
	}
#endif
	strncpy(hnames,hostnames,strlen(hostnames));
	pos = strchr(hnames,',');
	if(pos==NULL){
		if((host = gethostbyname(hnames)) == NULL)
		{
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_hostname_to_ip():Get host Failed. Hostname:%s",hnames);
			return ZK_NOK;
		}
		addr.s_addr = *(unsigned long *)host->h_addr;
		strncat(hosts,inet_ntoa(addr),strlen(inet_ntoa(addr)));
		strncat(hosts,catstr1,sizeof(catstr1));
	}else{
		do
		{
			len = pos-hnames;
			strncpy(hname,hnames,len);
			if((host = gethostbyname(hname)) == NULL)
			{
				ZK_DEBUG_PRINT("[SSCS]sscs_zk_hostname_to_ip():Get host Failed. Hostname:%s",hname);
				return ZK_NOK;
			}
			addr.s_addr = *(unsigned long *)host->h_addr;
			strncat(hosts,inet_ntoa(addr),strlen(inet_ntoa(addr)));
			strncat(hosts,catstr1,sizeof(catstr1));
			strncat(hosts,catstr2,sizeof(catstr2));
			
			memset(hnames_swp,0,sizeof(hnames_swp));
			strncpy(hnames_swp,hnames,strlen(hnames));
			memset(hnames,0,sizeof(hnames));
			memset(hname,0,sizeof(hname));
			strncpy(hnames,hnames_swp+len+1,strlen(hnames_swp)-(len+1));
		}while((pos = strchr(hnames,','))!=NULL);

		if((host = gethostbyname(hnames)) == NULL)
		{
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_hostname_to_ip():Get host Failed. Hostname:%s",hnames);
			return ZK_NOK;
		}
		addr.s_addr = *(unsigned long *)host->h_addr;
		strncat(hosts,inet_ntoa(addr),strlen(inet_ntoa(addr)));
		strncat(hosts,catstr1,sizeof(catstr1));
	}

#ifdef WIN32
	WSACleanup();
#endif
	ZK_DEBUG_PRINT("[SSCS]sscs_zk_hostname_to_ip():HOSTS:%s",hosts);
	return ZK_OK;
}


ZK_RET sscs_zk_session_init_by_hostname(const char *hostnames, const char *znode_product,log_func_type print_log_func)
{
	int i,ret;
	char hosts[zk_host_size]={0};
	char path_buffer[zk_buffer_size]={0};  

	zkhandle = NULL;

	if (print_log_func == NULL){
		return ZK_NOK;
	}
	g_zk_log_func = print_log_func;
	
	if(g_zk_debug_flag==0)
		zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
	else
		zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
	
	ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init_by_hostname()");
	if (!(znode_product[0]=='/'))
	{
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init_by_hostname():znode_product error %s",znode_product);
		return ZK_NOK;
	}
	
	ret = sscs_zk_hostname_to_ip(hostnames, hosts);
	if(ret == ZK_NOK){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init_by_hostname():Transfer Hostname to Ip Failed.");
        return ZK_NOK;
	}
	
	//restore znode_module for reestablish
	strncpy(g_zk_hosts,hosts,strlen(hosts));
	strncpy(g_zk_znode_product,znode_product,strlen(znode_product));
	ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init_by_hostname():g_zk_znode_product:%s", g_zk_znode_product);

	//it is asynchronous function
	//in zookeeper，it will try sometimes to recreate 
    zkhandle = zookeeper_init((const char *)hosts, zk_watcher_g, 30000, 0, 0, 0);
    if (!zkhandle) {
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init_by_hostname():zookeeper_init failed.");
        return ZK_NOK;
    }

	SLEEP(1);

	//check session is OK
	if (zk_myid.client_id == 0){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init_by_hostname():Session not Established.");
		return ZK_NOK;
	}

	ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init_by_hostname():Create session successfully");

    ret = zoo_exists(zkhandle,znode_product,0,NULL);
	if(ret == ZNONODE){
		ret = zoo_create(zkhandle,znode_product,"1.0",3,    
                          &ZOO_OPEN_ACL_UNSAFE,0,    
                          path_buffer,sizeof(path_buffer));    
        if(ret != ZOK){  
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init_by_hostname():Failed to create. PATH:%s RET:%d",znode_product,ret);
			return ZK_NOK;
        }	

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init_by_hostname():Create successfully. PATH:%s",znode_product);
	}else if(ret == ZOK){  

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init_by_hostname():Exists. PATH:%s",znode_product);
	}else{

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_session_init_by_hostname():unknown error. RET:%d",ret);
		return ZK_NOK;
	}

	return ZK_OK;
}

char *sscs_zk_base64_encode(const char *input,int length,int with_new_line)  
{  
    BIO * bmem = NULL;  
    BIO * b64 = NULL;  
    BUF_MEM * bptr = NULL;  
	char *buff;

    b64 = BIO_new(BIO_f_base64());  
    if(!with_new_line) {  
        BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);  
    }  
    bmem = BIO_new(BIO_s_mem());  
    b64 = BIO_push(b64, bmem);  
    BIO_write(b64, input, length);  
    BIO_flush(b64);  
    BIO_get_mem_ptr(b64, &bptr);  
  
	buff = (char *)malloc(bptr->length + 1);
    memcpy(buff,bptr->data,bptr->length);  
    buff[bptr->length] = 0;  
  
    BIO_free_all(b64);  
  
    return buff;  
}

ZK_RET sscs_zk_generate_digest(const char *usrpasswd,char *zk_digest)
{
    SHA_CTX c;  
    unsigned char md[SHA_DIGEST_LENGTH];  
	char *out_buff;
	char *pos;
	int len;

    SHA1_Init(&c);  
    SHA1_Update(&c, usrpasswd, strlen(usrpasswd));  
    SHA1_Final(md, &c);  
    OPENSSL_cleanse(&c, sizeof(c));  

	out_buff = sscs_zk_base64_encode(md, sizeof(md), 0); 

	pos = strchr(usrpasswd,':');
	len = pos-usrpasswd+1;
	strncpy(zk_digest,usrpasswd,len);
	strncpy(zk_digest+len,out_buff,strlen(out_buff));
	free(out_buff);

	ZK_DEBUG_PRINT("[SSCS]sscs_zk_generate_digest():usrpasswd/%s digest/%s",usrpasswd,zk_digest);

	return ZK_OK;
}


ZK_RET sscs_zk_master_create(const char *znode_module, notify_master_type notify_func)
{
	int ret;
	int master_flag;
	char child_path[zk_buffer_size] = {0}; 
	char path_buffer[zk_buffer_size]={0};   

	ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create()");

	if (notify_func == NULL){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():notify_func is NULL");
		return ZK_NOK;
	}
	g_zk_notify_master = notify_func;
	strncpy(g_zk_znode_module,znode_module,strlen(znode_module));
	ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():g_zk_znode_module:%s", g_zk_znode_module);

    ret = zoo_exists(zkhandle,znode_module,0,NULL);   
    if(ret == ZNONODE){  
        ret = zoo_create(zkhandle,znode_module,"abc",3,    
                          &ZOO_OPEN_ACL_UNSAFE,0,  
                          path_buffer,sizeof(path_buffer));    
        if(ret != ZOK){  

			ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():Create failed. PATH:%s RET:%d",znode_module,ret);
			return ZK_NOK;
        }

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():Create successfully. PATH:%s ",znode_module);
    }else if(ret == ZOK){ 

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():Exist. PATH:%s",znode_module);
	}else{

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():unknown error. RET:%d",ret);
		return ZK_NOK;
	} 
	
	zk_getlocalhost(g_zk_localhost,sizeof(g_zk_localhost));  
	snprintf(child_path,zk_buffer_size,"%s/proc-",znode_module); 

	//note: path and length must be consistent
	ret = zoo_create(zkhandle,child_path,g_zk_localhost,strlen(g_zk_localhost),    
					  &ZOO_OPEN_ACL_UNSAFE,ZOO_SEQUENCE|ZOO_EPHEMERAL,     
					  path_buffer,sizeof(path_buffer));    	
	if(ret != ZOK){  
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():Failed to create. PATH:%s BUFFER:%s RET:%d",child_path,path_buffer,ret);
		return ZK_NOK;
	}

	ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():Create successfully. PATH:%s ",path_buffer);
	
	sscs_zk_choose_master(zkhandle,znode_module);  
	sscs_zk_list_nodes(zkhandle,znode_module);  

	if (g_zk_notify_master != NULL) {
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():invoke g_zk_notify_master, it's %s",(g_zk_mode == MODE_MASTER)?"Master":((g_zk_mode == MODE_SLAVE)?"Slave":"Unknown"));
		master_flag = (g_zk_mode == MODE_MASTER)?1:((g_zk_mode == MODE_SLAVE)?0:2);	
		g_zk_notify_master(master_flag, znode_module);
	}

	return ZK_OK;
}


ZK_RET sscs_zk_master_status_get(const char *znode_module, int *status)
{
    int ret,i=0;  
	int ip_pid_len = zk_host_size;  
	char tmp[zk_host_size] ={0};  
	char master_path[zk_buffer_size] ={0};  
	char ip_pid[zk_host_size] = {0};  
	struct String_vector procs;  
	
	ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_status_get()");

	ret = zoo_get_children(zkhandle,znode_module,0,&procs);
    if(ret != ZOK || procs.count == 0){  
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_status_get():Failed to get children. PATH:%s RET:%d",znode_module,ret);
		return ZK_NOK;
    }
	
	//find the smallest znode
	strncpy(tmp,procs.data[0],strlen(procs.data[0]));  
	for(i = 1; i < procs.count; ++i){  
		if(strncmp(tmp,procs.data[i],strlen(procs.data[i]))>0){  
			strncpy(tmp,procs.data[i],strlen(procs.data[i]));  
		}  
	}
	snprintf(master_path,zk_buffer_size,"%s/%s",znode_module,tmp); 
	
	ret = zoo_get(zkhandle,master_path,0,ip_pid,&ip_pid_len,NULL);  
	if(ret != ZOK){  
		if(g_zk_log_func != NULL){
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_status_get():Failed to get DATA. PATH:%s RET:%d",master_path,ret);
		}
		return ZK_NOK;
	}else if(strncmp(ip_pid,g_zk_localhost,sizeof(g_zk_localhost))==0){  
		if(g_zk_log_func != NULL){
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_status_get():%s(Master)",g_zk_localhost);
		}
		*status = MODE_MASTER;
	}else{  
		if(g_zk_log_func != NULL){
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_status_get():%s(Slave)",g_zk_localhost);
		}
		*status = MODE_SLAVE;
	}
     
	return ZK_OK;
}

ZK_RET sscs_zk_auth_add(const char *usrpasswd){

	char *pos;
	int ret;

	pos = strchr(usrpasswd,':');
	if(pos == NULL){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_auth_add():usrpasswd invalid. %s",usrpasswd)
		return ZK_NOK;
	}
	if(strlen(usrpasswd)>=zk_usrpasswd_size){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_auth_add():usrpasswd invalid  %s",usrpasswd)
		return ZK_NOK;
	}

	ret = zoo_add_auth(zkhandle,g_zk_scheme,usrpasswd,strlen(usrpasswd),NULL,NULL);
	if(ret != ZOK){  
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_auth_add():Add Auth Failed. %s RET:%d",usrpasswd,ret);
		return ZK_NOK;
	}
	
	return ZK_OK;
}


ZK_RET sscs_zk_acl_set(const char *path,const char *usrpasswd){

	char *pos;
	int ret;
	char zk_digest[zk_usrpasswd_size]={0};
	char zk_digest_read[zk_usrpasswd_size]={0};
	struct ACL_vector aclv;
	struct ACL acl[2];

	//do check
	pos = strchr(usrpasswd,':');
	if(pos == NULL){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_acl_set():usrpasswd invalid. %s",usrpasswd)
		return ZK_NOK;
	}
	if(strlen(usrpasswd)>=zk_usrpasswd_size){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_acl_set():usrpasswd invalid  %s",usrpasswd)
		return ZK_NOK;
	}
	strncpy(g_zk_usrpasswd_all,usrpasswd,strlen(usrpasswd));

	if(zkhandle == NULL){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_acl_set():Session has been lost")
		return ZK_NOK;
	}

	ret = zoo_exists(zkhandle,path,0,NULL);   
	if(ret == ZOK){ 

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_acl_set():Exist. PATH:%s",path);
	}else{
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_acl_set():PATH not existed. PATH:%S RET:%d",path,ret);
		return ZK_NOK;
	}
	
	if(g_zk_acl_flag){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_acl_set():ACL has been set. PATH:%s RET:%d",path,ret);
		return ZK_NOK;
	}

	ret = sscs_zk_generate_digest((const char *)g_zk_usrpasswd_all,zk_digest);
	if(ret != ZOK){  
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_acl_set():Failed to generate digest. %s",g_zk_usrpasswd_all);
		return ZK_NOK;
	}
	acl[0].id.id = zk_digest;
	acl[0].perms = ZOO_PERM_ALL;
	acl[0].id.scheme = g_zk_scheme;

	ret = sscs_zk_generate_digest((const char *)g_zk_usrpasswd_read,zk_digest_read);
	if(ret != ZOK){  
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_acl_set():Failed to generate digest. %s",g_zk_usrpasswd_read);
		return ZK_NOK;
	}
	acl[1].id.id = zk_digest_read;
	acl[1].perms = ZOO_PERM_READ;
	acl[1].id.scheme = g_zk_scheme;

	aclv.count=2;
	aclv.data = acl; 	
	ret = zoo_set_acl(zkhandle,path,0,&aclv);
	if(ret != ZOK){  
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_acl_set():Failed to set acl. PATH:%s RET:%d",path,ret);
		return ZK_NOK;
	}

	g_zk_acl_flag = 1;

	ZK_DEBUG_PRINT("[SSCS]sscs_zk_acl_set():Set Acl sucessfully");

	return ZK_OK;
}


ZK_RET sscs_zk_fetch_znode_data(const char *znode,char *buffer,int buffer_len,const char *usrpasswd,notify_config_data_type notify_func)
{
	int ret;
	char *pos;

	ZK_DEBUG_PRINT("[SSCS]sscs_zk_fetch_znode_data()");

	if (!(znode[0]=='/'))
	{
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_fetch_znode_data():%s invalid",znode);
		return ZK_NOK;
	}

	if (notify_func == NULL){
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_fetch_znode_data():notify_func NULL");
		return ZK_NOK;
	}
	g_zk_notify_data = notify_func;

	if(usrpasswd){
		pos = strchr(usrpasswd,':');
		if(pos == NULL){
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_fetch_znode_data():%s invalid",usrpasswd);
			return ZK_NOK;
		}
		if(strlen(usrpasswd)>=zk_usrpasswd_size){
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_fetch_znode_data():length:%d invalid",strlen(usrpasswd));
			return ZK_NOK;
		}
		strncpy(g_zk_usrpasswd_cs,usrpasswd,strlen(usrpasswd));

		ret = zoo_add_auth(zkhandle,g_zk_scheme,g_zk_usrpasswd_cs,strlen(g_zk_usrpasswd_cs),NULL,NULL);
		if(ret != ZOK){  
			ZK_DEBUG_PRINT("[SSCS]sscs_zk_fetch_znode_data():ADD auth Failed. userpasswd:%s",g_zk_usrpasswd_cs);
			return ZK_NOK;
		}
	}

	ret = zoo_get(zkhandle,znode,1,buffer,&buffer_len,NULL);
	if(ret == ZOK){  
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_fetch_znode_data():get DATA successfully. PATH:%s",znode);

	}else{
		ZK_DEBUG_PRINT("[SSCS]sscs_zk_fetch_znode_data():get DATA failed. PATH:%s RET:%d",znode,ret);
		return ZK_NOK;
	}

	if(g_zk_notify_data){
		g_zk_notify_data(znode,buffer);
	}
	
	return ZK_OK;
}

ZK_RET sscs_zk_znode_create(const char *znode_m)
{
	int ret;
	char path_buffer[zk_buffer_size] = {0}; 
    
    ret = zoo_exists(zkhandle,znode_m,0,NULL);   
    if(ret == ZNONODE){  
        ret = zoo_create(zkhandle,znode_m,"abc",3,    
                          &ZOO_OPEN_ACL_UNSAFE,0,  
                          path_buffer,sizeof(path_buffer));    
        if(ret != ZOK){  

			ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():Create failed. PATH:%s",znode_m);
			return ZK_NOK;
        }

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():Create successfully. PATH:%s ",znode_m);
    }else if(ret == ZOK){ 

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():Exist. PATH:%s",znode_m);
	}else{

		ZK_DEBUG_PRINT("[SSCS]sscs_zk_master_create():unknown error. RET:%d",ret);
		return ZK_NOK;
	} 
	
	return ZK_OK;
}


ZK_RET sscs_zk_log_set(int flag)
{
	if(flag==0)
		zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
	else
		zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
	return ZK_OK;
}

ZK_RET sscs_zk_set_log_stream(FILE* logStream)
{
	if(logStream!=NULL){
	    zoo_set_log_stream(logStream);
		return ZK_OK;
	}else{
		return ZK_NOK;
	}
}
