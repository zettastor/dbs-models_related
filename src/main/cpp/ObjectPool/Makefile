THRIFT_DIR := /usr/local/include/thrift
LIB_DIR := /usr/local/lib
DIR := ../../../../thrift-generated-src-cpp
GEN_DIR := ../../../../target

GEN_SRC := $(DIR)/dn_constants.cpp $(DIR)/dn_types.cpp $(DIR)/shared_constants.cpp $(DIR)/shared_types.cpp $(DIR)/icshared_constants.cpp $(DIR)/icshared_types.cpp $(DIR)/infocenter_constants.cpp $(DIR)/infocenter_types.cpp $(DIR)/InformationCenter.cpp $(DIR)/DataNodeService.cpp

GEN_OBJ := $(patsubst %.cpp, %.o, $(GEN_SRC))
	      
INC := -I$(THRIFT_DIR)

.PHONY: all clean

all: $(GEN_DIR)/libObjectPool.so

%.o: %.cpp
	$(CXX) -Wall -fPIC $(INC) -I$(DIR) -c $< -o $@

$(GEN_DIR)/ObjectPool.o: ObjectPool.cpp
	$(CXX) -Wall -fPIC $(INC) -I$(DIR) -c $< -o $@

$(GEN_DIR)/libObjectPool.so: $(GEN_DIR)/ObjectPool.o $(GEN_OBJ)
	$(CXX) -shared -L$(LIB_DIR) $^ -o $@ -lthrift

clean:
	$(RM) $(GEN_DIR)/*.o $(GEN_DIR)/libObjectPool.so $(DIR)/*.o

