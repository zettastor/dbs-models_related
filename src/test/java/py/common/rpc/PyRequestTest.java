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
