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

package py.netty.datanode;

/**
 * client:every method implement the same, just implement a same method with different
 * parameter.<br> . how to build a full header and send to server.<br> 1, how to get method
 * identifier?<br> 2, set the identifier to header?<br>
 *
 * <p>server:how to call corresponding method,<br> 1, get the service all methods.<br> 2, map
 * identifier to method, then when receving a request, we can call corresponding method.<br>
 *
 * <p>how to address parameter parse problem, which mainly for server? 1, server can get method,
 * can
 * get parameter type for this method.
 *
 */
public abstract class AbstractAsyncService implements AsyncDataNode.AsyncIface {
}
