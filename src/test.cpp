#ifdef WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif
#include <stdio.h>
#include <ss_config_service.h>

//#define OPENSSL_DEBUG

const char g_hosts[512]= "172.16.85.35:2181,172.16.85.34:2181,172.16.85.33:2181";
//const char g_hosts[512]= "172.16.18.77:2181,172.16.17.239:2181,172.16.17.186:2181";
const char znode_product[512]= "/Product"; 
const char znode_module[512]= "/Product/module"; 
const char znode_p[512]= "/P";
const char znode_m[512]= "/P/m";
const char usrpasswd[100]="sumscope:sumscope";
const char hostnames[100]="zookeeper1.sumscope.com,zookeeper2.sumscope.com,zookeeper3.sumscope.com";
char config_data[4096]={0};

#ifdef WIN32
	#define SLEEP(n)	Sleep((n)*1000)
#else 
	#define SLEEP(n)  	sleep(n)
#endif


void print_log_func(const char *log_str)
{
	printf("%s\n", log_str);
}
int notify_master_func(int master_flag, const char *path)
{
	if (master_flag == 1)
		printf("I am MASTER\n");
	else if(master_flag == 0){
		printf("I am SLAVE\n");
	}else
		printf("Unknown\n");
	return 0;
}
int notify_data_func(const char * znode, char *buffer)
{
	printf("znode:%s\n",znode);
	printf("%s\n",buffer);
	return 0;
}



void usage(void)
{
	printf("usage()\n");
	printf("	i:slave/master by ip addr\n");
	printf("	h:slave/master by hostname\n");
	printf("	g:data center case\n");
}

int main(int argc, char **argv) {
	FILE *stream;
	int status = 0;
	int len = 4096;
	stream=fopen("zookeeper.log","w+");
	
	if(argc < 2)
	{
		usage();
		return 0;
	}

	switch(*(argv[1])){
		case 'i':
		{
			sscs_zk_log_set(true);
			sscs_zk_set_log_stream(stream);
			
			if(sscs_zk_session_init(g_hosts, znode_product, print_log_func) == ZK_NOK){
				return 1;
			}
#ifdef OPENSSL_DEBUG
			if(sscs_zk_auth_add(usrpasswd) == ZK_NOK){
				return 1;
			}
#endif
			if(sscs_zk_master_create(znode_module, notify_master_func) == ZK_NOK){
				return 1;
			}
			
#ifdef OPENSSL_DEBUG
			if(sscs_zk_acl_set(znode_module, usrpasswd) == ZK_NOK){
				;
			}
#endif
			if(sscs_zk_master_status_get(znode_module, &status) == ZK_NOK){
				;
			}
			break;
		}

		case 'h':
		{
			if(sscs_zk_session_init_by_hostname(hostnames, znode_product, print_log_func) == ZK_NOK){
				return 1;
			}
			if(sscs_zk_master_create(znode_module, notify_master_func) == ZK_NOK){
				return 1;
			}
			if(sscs_zk_master_status_get(znode_module, &status) == ZK_NOK){
				;
			}
			break;
		}

		case 'g':
		{
			if(sscs_zk_session_init(g_hosts, znode_p, print_log_func) == ZK_NOK){
				return 1;
			}
#ifdef OPENSSL_DEBUG
			if(sscs_zk_auth_add(usrpasswd) == ZK_NOK){
				return 1;
			}
#endif
			if(sscs_zk_znode_create(znode_m) == ZK_NOK){
				return 1;
			}
#ifdef OPENSSL_DEBUG
			if(sscs_zk_acl_set(znode_m, usrpasswd) == ZK_NOK){
				;
			}
#endif
			if(sscs_zk_fetch_znode_data(znode_m, config_data, len,usrpasswd,notify_data_func) == ZK_NOK){
				return 1;
			}
			break;
		}
		default:
			usage();
	}


	while(1) {

		SLEEP(5); //5 seconds	

	}	
	
}


