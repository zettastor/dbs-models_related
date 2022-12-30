#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "ClientPool.h"

#include "DataNodeService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

ClientPool * ClientPool::clientpool = NULL;

ClientPool::ClientPool(int maxsize, char host[], int port)
{
	this->maxsize = maxsize;
	this->cursize = 0;
	pthread_mutex_init(&lock, NULL);
	this->InitClientPool(5, host, port);
}

ClientPool * ClientPool::GetInstance(char host[], int port) {
	if(clientpool == NULL) {
		clientpool = new ClientPool(10, host, port);
	}
	return clientpool;
}

DataNodeServiceClient * ClientPool::CreateClient(char host[], int port) {
	boost::shared_ptr<TSocket> socket(new TSocket(host, port));

	socket->setConnTimeout(2000);
	socket->setSendTimeout(2000);
	socket->setRecvTimeout(2000);

	boost::shared_ptr<TTransport> transport(new TFramedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	DataNodeServiceClient *client = new DataNodeServiceClient(protocol);

	transport->open();
	
	return client;
}

void ClientPool::InitClientPool(int initialsize, char host[], int port) {
	DataNodeServiceClient * client;
	pthread_mutex_lock(&lock);
	printf("initial clientpool...\n");
	for(int i = 0; i < initialsize; i++) {
		client = this->CreateClient(host, port);
		if(client) {
			ClientList.push_back(client);
			cursize++;
			printf("clientpool cursize = %d\n", cursize);
		} else {
			perror("create client error");
		}
	}
	pthread_mutex_unlock(&lock);
}

DataNodeServiceClient * ClientPool::GetClient(char host[], int port) {
	DataNodeServiceClient * client;
	pthread_mutex_lock(&lock);
	if(ClientList.size() > 0) {
		printf("get client...\n");
		client = ClientList.front();
		ClientList.pop_front();

		if(client == NULL) {
			--cursize;
			perror("create client error");
		}

		pthread_mutex_unlock(&lock);
		return client;
	} else {
		if(cursize < maxsize) {
			client = this->CreateClient(host, port);
			if(client) {
				cursize++;
				printf("clientpool cursize = %d\n", cursize);
				pthread_mutex_unlock(&lock);
				return client;
			} else {
				perror("create client error");
				pthread_mutex_unlock(&lock);
				return NULL;
			}
		} else {
			perror("clientpool cursize equals maxsize");
			pthread_mutex_unlock(&lock);
			return NULL;
		}
	}
}


void ClientPool::ReleaseClient(DataNodeServiceClient * client) {
	if(client) {
		pthread_mutex_lock(&lock);
		ClientList.push_back(client);
	  	pthread_mutex_unlock(&lock);
	}
}

ClientPool::~ClientPool() {
	this->DestoryClientPool();
}

void ClientPool::DestoryClientPool() {
	list<DataNodeServiceClient *>::iterator i;
	pthread_mutex_lock(&lock);
	for(i = ClientList.begin(); i != ClientList.end(); i++) {
		this->DeleteClient(* i);
	}
	cursize = 0;
	ClientList.clear();
	pthread_mutex_unlock(&lock);
}

void ClientPool::DeleteClient(DataNodeServiceClient * client) {
	delete client;
}
/*
int main(int argc, char** argv) {
	ClientPool * clientpool = NULL;
	clientpool = clientpool->GetInstance("localhost", 9090);
	
	for(int i = 0; i < 8; i++) {
		DataNodeServiceClient * client = clientpool->GetClient("localhost", 9090);
		client->ping();
		printf("ping...\n");
		clientpool->ReleaseClient(client);
	}
	sleep(5);
	return 0;
}
*/
/*
void * create(void * arg) {
	ClientPool * clientpool = (ClientPool *)arg;
        DataNodeServiceClient * client = clientpool->GetClient("localhost", 9090);
        client->ping();
        printf("ping...\n");
	clientpool->ReleaseClient(client);
        return NULL;
}
 
int main(int argc, char** argv) {
         ClientPool * clientpool = NULL;
         clientpool = clientpool->GetInstance("localhost", 9090);
         pthread_t nid;
         for(int i = 0; i < 8; i++) {
                pthread_create(&nid, NULL, create, (void *)clientpool);
         }
         sleep(5);
         return 0;
}
*/
