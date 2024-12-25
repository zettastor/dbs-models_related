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

package py.common.rpc;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jboss.netty.handler.timeout.TimeoutException;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import py.client.thrift.PyRequest;
import py.common.rpc.share.NiftyTimer;
import py.test.TestBase;

public class PyRequestTest extends TestBase {
  @Before
  public void init() throws Exception {
    super.init();
  }

  @Test
  public void testPyRequest() throws Exception {
    PyRequest request = new PyRequest(null, 500, 100);

    Thread.sleep(250);
    Assert.assertTrue(request.getRestTimeMs() > 0);

    Thread.sleep(300);
    Exception exception = null;
    try {
      request.getRestTimeMs();
    } catch (Exception e) {
      exception = e;
    }
    Assert.assertTrue(exception instanceof TimeoutException);
  }

  @Test
  public void testTimer() throws Exception {
    NiftyTimer timer = new NiftyTimer("fffff");

    long sendTimeoutMs = (long) 2000;

    final CountDownLatch latch = new CountDownLatch(1);
    TimerTask timerTask = new TimerTask() {
      @Override
      public void run(Timeout timeout) throws Exception {
        latch.countDown();
      }
    };

    Timeout sendTimeout = null;
    try {
      sendTimeout = timer.newTimeout(timerTask, sendTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (IllegalStateException e) {
      Assert.assertTrue("Unable to schedule send timeout", false);
    }

    Assert.assertTrue(!sendTimeout.isExpired());
    latch.await();
    Assert.assertTrue(sendTimeout.isExpired());
    timer.close();
  }
}
