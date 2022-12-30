#include"ObjectPool.h"
#include <stdio.h> 
#include "iostream"
#include <unistd.h>
#include <sys/time.h>
#include <thrift/protocol/TBinaryProtocol.h>  
#include <thrift/transport/TSocket.h>  
#include <thrift/transport/TTransportUtils.h>
#include"DataNodeService.h"
#include"InformationCenter.h"
//#include"clientpool"
using namespace std;  
using namespace apache::thrift;  
using namespace apache::thrift::protocol;  
using namespace apache::thrift::transport;
template<typename T>  
const int ObjectPool<T>::nDefaultChunkSize; 
template<typename T> 
int  ObjectPool<T>::port = 9000; 
template<typename T>
char  ObjectPool<T>::address[16] = "127.0.0.1"; 
template <typename T>  
ObjectPool<T>::ObjectPool() throw(std::invalid_argument,  
    std::bad_alloc)  
{      ChunkSize = nDefaultChunkSize;
       pthread_mutex_init(&lock,NULL);
    if (ChunkSize <= 0) {  
        throw std::invalid_argument("chunk size must be positive"); 
    } else{
    allocateChunk();  
	}
}
template <typename T>  
void ObjectPool<T>::allocateChunk()  
{  
    T* newObjects = new T[ChunkSize];
    pthread_mutex_lock(&lock);  
    mAllObjects.push_back(newObjects);  
    for (int i = 0; i < ChunkSize; i++) {  
        mFreeList.push(&newObjects[i]);  
    } 
    pthread_mutex_unlock(&lock);   
}  
template<typename T>  
void ObjectPool<T>::arrayDeleteObject(T* obj)  
{  
    delete [] obj;  
}  
  
template <typename T>  
ObjectPool<T>::~ObjectPool()  
{   
    for_each(mAllObjects.begin(), mAllObjects.end(), arrayDeleteObject);  
}  
template <typename T>  
T& ObjectPool<T>::acquireObject() throw(std::invalid_argument,  
    std::bad_alloc)   
{  
    if (mFreeList.empty()) {  
        throw std::invalid_argument("chunk size must be positive");   
    } else{ 
    pthread_mutex_lock(&lock);
    T* obj = mFreeList.front();  
    mFreeList.pop();
    pthread_mutex_unlock(&lock);  
    return (*obj); 
     
	}  
}
  
template <typename T>  
void ObjectPool<T>::releaseObject(T& obj)  
{  
    mFreeList.push(&obj);  
} /* 
class UserRequest  
{  
public:  
  UserRequest() {}  
  ~UserRequest() {}      
protected:    
}; */ 
class DataNodeClient : public DataNodeServiceClient{
public:
	boost::shared_ptr<TProtocol> setProtocol(){
		          // boost::shared_ptr<TSocket> socket(new TSocket("localhost", 9090));  
            int i = ObjectPool<DataNodeClient>::port;
            boost::shared_ptr<TSocket> socket(new TSocket(ObjectPool<DataNodeClient>::address, i));

			  socket->setConnTimeout(2000);
         		  socket->setSendTimeout(2000);
                          socket->setRecvTimeout(2000);

                           boost::shared_ptr<TTransport> transport(new TFramedTransport(socket));
			   boost::shared_ptr<TProtocol>  protocol(new TBinaryProtocol(transport));
				   transport->open();
				   return protocol;
	}
      DataNodeClient():DataNodeServiceClient(setProtocol()){

     }
	~DataNodeClient(){}
};
class InformationClient : public InformationCenterClient{
public:
        boost::shared_ptr<TProtocol> setProtocol(){
                          // boost::shared_ptr<TSocket> socket(new TSocket("localhost", 9090));
      
                           int i = ObjectPool<InformationClient>::port;
                           boost::shared_ptr<TSocket> socket(new TSocket(ObjectPool<InformationClient>::address, i));
                           socket->setConnTimeout(2000);
                           socket->setSendTimeout(2000);
                           socket->setRecvTimeout(2000);
   
                           boost::shared_ptr<TTransport> transport(new TFramedTransport(socket));
                           boost::shared_ptr<TProtocol>  protocol(new TBinaryProtocol(transport));
                                   transport->open();
                                   return protocol;
        }
      InformationClient():InformationCenterClient(setProtocol()){

     }
        ~InformationClient(){}
};
	
/*UserRequest& obtainUserRequest(ObjectPool<UserRequest>& pool)  
{  
  UserRequest& request = pool.acquireObject();  
  return (request);  
}  
  
void processUserRequest(ObjectPool<UserRequest>& pool, UserRequest& req)  
{   
  pool.releaseObject(req);  
}  
  
int main(int argc, char** argv)  
{  
  ObjectPool<UserRequest> requestPool;  
  while (true ) {  
    UserRequest& req = obtainUserRequest(requestPool);
	printf("a object created!\n");
    processUserRequest(requestPool, req);  
  }  
  
  return (0);  
}  
*/
DataNodeClient& obtainUserRequest(ObjectPool<DataNodeClient>& pool){
	DataNodeClient& request = pool.acquireObject();  
        return (request);
}
InformationClient& obtainUserRequest(ObjectPool<InformationClient>& pool){
        InformationClient& request = pool.acquireObject();
        return (request);
}
void processUserRequest(ObjectPool<DataNodeClient>& pool, DataNodeClient& req)  
{   
        pool.releaseObject(req);  
}  
void processUserRequest(ObjectPool<InformationClient>& pool,InformationClient & req) 
{
        pool.releaseObject(req);  
}   

//int main(int argc, char** argv) { 
//     int i;
//     ObjectPool<InformationClient>::port = 8020;
//     strcpy(ObjectPool<InformationClient>::address,"10.0.1.203");
//     ObjectPool<InformationClient> ClientPool;
       // ObjectPool<DataNodeClient> ClientPool;
//     for(i=0; i<10; i++){       
//       	InformationClient& client = obtainUserRequest(ClientPool);
       //DataNodeClient& client = obtainUserRequest(ClientPool); 
//           try {  
				  //     transport->open();
                                 //   obtainUserRequest(ClientPool).ping(); 
//				      client.ping();
//				      printf("ping()\n"); 
//				      client.shutdown();
//                                     processUserRequest(ClientPool,client);			   				//		transport->close(); 
//        	} 
//		catch (TException &tx) { 
//			     printf("ERROR:"); 
//	        }  
//     }
	// client = obtainUserRequest(ClientPool);
       //DataNodeClient& client = obtainUserRequest(ClientPool); 
              //  try {  
				  //     transport->open();
                                 //   obtainUserRequest(ClientPool).ping(); 
		//		      client.ping(); 
		//		      printf("ping()\n"); 
		//		      client.shutdown();		
                  //                    processUserRequest(ClientPool,client);	   				//		transport->close(); 
	//	} 
	//	catch (TException &tx) { 
	//		     printf("ERROR:"); 
	  //      }  
         
// return 0;
	
// }
