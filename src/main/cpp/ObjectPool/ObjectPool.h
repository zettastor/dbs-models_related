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
#include <queue>
#include <vector>  
#include <stdexcept>  
#include <memory>  
#include <pthread.h>
  
using std::queue;  
using std::vector;
template <typename T>  
class ObjectPool  
{  
 public:
        static char address[16];
        static int port; 
        ObjectPool()  
        throw(std::invalid_argument, std::bad_alloc); 
        ~ObjectPool(); 
        T& acquireObject()
		throw(std::invalid_argument, std::bad_alloc);;
        void releaseObject(T& obj);  
  
 protected:
            pthread_mutex_t lock; 
	    queue<T*> mFreeList;
	    vector<T*> mAllObjects;   
        int ChunkSize;  
        static const int nDefaultChunkSize = 100; 
        void allocateChunk();  
        static void arrayDeleteObject(T* obj);  
  
private:                 
        ObjectPool(const ObjectPool<T>& src);  
        ObjectPool<T>& operator=(const ObjectPool<T>& rhs);  
};  
