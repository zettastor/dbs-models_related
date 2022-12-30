#ifndef _CLIENTPOOL_H
#define _CLIENTPOOL_H

#include <list>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
 
#include "DataNodeService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;


class ClientPool {
	private:
	int cursize;
	int maxsize;
	list<DataNodeServiceClient *>ClientList;
	pthread_mutex_t lock;
	static ClientPool * clientpool;

	DataNodeServiceClient * CreateClient(char host[], int port);
	void InitClientPool(int initialsize, char host[], int port);
	void DeleteClient(DataNodeServiceClient * client);
	void DestoryClientPool();
	ClientPool(int maxsize, char host[], int port);

	public:
	~ClientPool();
	static ClientPool * GetInstance(char host[], int port);
	DataNodeServiceClient * GetClient(char host[], int port);
	void ReleaseClient(DataNodeServiceClient * client);
};
#endif
