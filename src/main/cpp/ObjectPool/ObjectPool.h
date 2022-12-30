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
