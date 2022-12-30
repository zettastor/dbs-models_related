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

package py.datanode.client;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.EndPoint;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.BroadcastResponse;
import py.thrift.datanode.service.DataNodeService.AsyncClient;
import py.thrift.datanode.service.DataNodeService.AsyncClient.broadcast_call;

public abstract class AbstractBroadcastCallback implements
    AsyncMethodCallback<AsyncClient.broadcast_call> {
  private static final Logger logger = LoggerFactory.getLogger(AbstractBroadcastCallback.class);
  protected final ResponseCollector<EndPoint, BroadcastResponse> responseCollector;
  protected final BroadcastRequest request;
  protected final EndPoint endPoint;
  protected final int totalSize;

  public AbstractBroadcastCallback(ResponseCollector<EndPoint, BroadcastResponse> responseCollector,
      BroadcastRequest request, EndPoint endPoint, int totalSize) {
    this.responseCollector = responseCollector;
    this.request = request;
    this.endPoint = endPoint;
    this.totalSize = totalSize;
  }

  @Override
  public final void onComplete(broadcast_call arg0) {
    try {
      BroadcastResponse response = arg0.getResult();
      processComplete(response);
      responseCollector.addGoodResponse(endPoint, response);
    } catch (Throwable t) {
      logger.debug("onComplete. The request is {} The response has an exception: {}", request, t);
      responseCollector.addServerSideThrowable(endPoint, t);
    } finally {
      checkDone();
    }
  }

  @Override
  public final void onError(Exception e) {
    logger.info("onError. request is {} Fail to broadcast a request to the endpoint:{} ", request,
        endPoint, e);
    processError(e);
    responseCollector.addServerSideThrowable(endPoint, e);
    checkDone();
  }

  public void processComplete(BroadcastResponse response) {
  }

  public void processError(Exception e) {
  }

  public abstract void checkDone();
}
