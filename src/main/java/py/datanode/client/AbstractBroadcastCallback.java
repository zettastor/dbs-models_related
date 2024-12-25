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
