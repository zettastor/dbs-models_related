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
