/*
 * Copyright (c) 2022-2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
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
