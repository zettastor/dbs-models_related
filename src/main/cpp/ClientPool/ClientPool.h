/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
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
