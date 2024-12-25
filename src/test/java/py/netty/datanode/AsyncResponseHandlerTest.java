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

import io.netty.buffer.ByteBufAllocator;
import java.util.function.Function;
import py.netty.client.AsyncResponseHandler;
import py.netty.client.MessageTimeManager;
import py.netty.core.MethodCallback;
import py.netty.core.Protocol;
import py.netty.message.MethodTypeInterface;

public class AsyncResponseHandlerTest extends AsyncResponseHandler {
  public AsyncResponseHandlerTest(Protocol protocol,
      Function<Integer, MethodTypeInterface> getMethodTypeInterface,
      MessageTimeManager messageTimeManager) {
    super(protocol, getMethodTypeInterface, messageTimeManager);
  }

  @Override
  public MethodCallback getCallback(long requestId) {
    return new MethodCallbackTest();
  }

  private class MethodCallbackTest<T> implements MethodCallback<T> {
    @Override
    public void complete(T object) {
      PyReadResponse pyReadResponse = (PyReadResponse) object;
      pyReadResponse.release();
    }

    @Override
    public void fail(Exception e) {
    }

    @Override
    public ByteBufAllocator getAllocator() {
      return null;
    }
  }
}
