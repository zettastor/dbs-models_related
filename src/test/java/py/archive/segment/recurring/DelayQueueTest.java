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

package py.archive.segment.recurring;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.informationcenter.Utils;
import py.test.TestBase;

/**
 * test some methods in the DelayQueue.
 */
public class DelayQueueTest extends TestBase {
  private static final Logger logger = LoggerFactory.getLogger(DelayQueueTest.class);
  private AtomicInteger runTimes = new AtomicInteger();
  private AtomicInteger failedTimes = new AtomicInteger();

  @Test
  public void testMillSecondToString() throws Exception {
    int threadCount = 1000;

    final CountDownLatch mainLatch = new CountDownLatch(1);
    final CountDownLatch threadLatch = new CountDownLatch(threadCount);
    for (int i = 0; i < threadCount; i++) {
      Thread thread = new Thread() {
        public void run() {
          try {
            mainLatch.await();
            while (true) {
              Long inQueueTime = new Random().nextLong();
              String str = Utils.millsecondToString(inQueueTime);
              runTimes.addAndGet(1);
              if (runTimes.get() > 100000) {
                break;
              }
              logger.info("result: " + str);
              Thread.sleep(1);
            }
          } catch (Exception e) {
            logger.info("caught an exception run times " + runTimes, e);
            failedTimes.addAndGet(1);
          } finally {
            threadLatch.countDown();
          }
        }
      };
      thread.start();
    }
    final Long startTime = System.currentTimeMillis();
    mainLatch.countDown();
    threadLatch.await();
    assertEquals(0, failedTimes.get());
    Long endTime = System.currentTimeMillis();
    logger.info("Cost time: " + (endTime - startTime));
  }

  @Test
  public void testDrainTo() {
    DelayQueue<Delayed> queue = new DelayQueue<>();

    queue.add(new Delayed() {
      @Override
      public int compareTo(Delayed delayed) {
        if (delayed == null) {
          return 1;
        }

        if (delayed == this) {
          return 0;
        }

        long d = (getDelay(TimeUnit.MILLISECONDS) - delayed.getDelay(TimeUnit.MILLISECONDS));
        return ((d == 0) ? 0 : ((d < 0) ? -1 : 1));
      }

      @Override
      public long getDelay(TimeUnit unit) {
        return 1;
      }
    });

    Collection<Delayed> c = new ArrayList<Delayed>();
    // drainTo() method doesn't block when there is no available elements
    queue.drainTo(c);
    assertEquals(0, c.size());
  }
}
