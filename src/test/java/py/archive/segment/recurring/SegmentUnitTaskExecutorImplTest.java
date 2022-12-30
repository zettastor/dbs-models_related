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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import py.archive.segment.SegId;
import py.test.TestBase;

public class SegmentUnitTaskExecutorImplTest extends TestBase {
  private static int generateTenTimes = 0;
  SegmentUnitProcessorFactory segmentUnitProcessorFactory;
  SegmentUnitTaskContextFactory contextFactory;
  SegmentUnitCallbackTest segmentUnitCallbackTest;
  private AtomicBoolean abandonFlag = new AtomicBoolean();
  private AtomicBoolean workThreadStopFlag = new AtomicBoolean();

  @Before
  public void init() throws Exception {
    super.init();
    Logger logger1 = Logger.getLogger("py.archive.segment.recurring.SegmentUnitTaskExecutorImpl");
    logger1.setLevel(Level.ERROR);
  }

  @Test
  public void contextEqualsTest() throws Exception {
    SegId segId1 = new SegId(1L, 1);
    SegId segId2 = new SegId(1L, 2);

    final SegmentUnitTaskContext context1_2 =
        new SegmentUnitTaskContext(new MyContextKey(segId1, 1));

    final SegmentUnitTaskContext context2 =
        new SegmentUnitTaskContext(new MyContextKey(segId2, 1));
    final SegmentUnitTaskContext context3 =
        new SegmentUnitTaskContext(new MyContextKey(segId1, 2));

    SegmentUnitTaskContext context11 =
        new SegmentUnitTaskContext(new MyContextKey(segId1, 1));
    assertEquals(context11, context1_2);
    assertNotSame(context11, context2);
    assertNotSame(context11, context3);
    assertNotSame(context2, context3);
  }

  @Test
  public void waitTest() throws InterruptedException {
    DelayQueue<Delayed> dq = new DelayQueue<>();
    SegId segId1 = new SegId(1L, 1);

    SegmentUnitTaskContext context1 = new SegmentUnitTaskContext(new MyContextKey(segId1, 1));
    SegmentUnitTaskContext context2 = new SegmentUnitTaskContext(new MyContextKey(segId1, 2));

    context1.updateDelay(1000);
    context2.updateDelay(2000);

    dq.add(context1);
    dq.add(context2);
    long now = System.currentTimeMillis();
    SegmentUnitTaskContext contextTaken = (SegmentUnitTaskContext) dq.take();
    assertSame(context1, contextTaken);
    assertSame(context2, (SegmentUnitTaskContext) dq.take());
    long delay = System.currentTimeMillis() - now;
    logger.warn("waitTest, test delay:{}", delay);
    assertTrue(delay >= 2000);
  }

  @Test
  public void normalCaseTest() throws Exception {
    segmentUnitProcessorFactory = new SegmentUnitProcessorFactory() {
      @Override
      public SegmentUnitProcessor generate(SegmentUnitTaskContext context) {
        return new SegmentUnitProcessorForTesting(context, 0, null, false);
      }
    };

    contextFactory = new SegmentUnitTaskContextFactoryImplForTest();

    SegmentUnitTaskExecutorImpl taskExecutor = new SegmentUnitTaskExecutorImpl(
        segmentUnitProcessorFactory,
        contextFactory, 10, 20, 100, "Testing");
    logger.debug("start the executor");
    taskExecutor.start();
    SegId segId1 = new SegId(1L, 1);
    SegId segId2 = new SegId(2L, 2);
    SegId segId3 = new SegId(3L, 3);
    taskExecutor.addSegmentUnit(segId1);
    taskExecutor.addSegmentUnit(segId2);
    taskExecutor.addSegmentUnit(segId3);

    Thread.sleep(2000);
    Thread.sleep(1000);
    assertEquals(12, taskExecutor.getTaskCount());

    // add a duplicated segment unit and nothing is going to happen

    taskExecutor.addSegmentUnit(segId1);
    Thread.sleep(3000);
    stopWorkThread();
    Thread.sleep(1000);
    assertEquals(12, taskExecutor.getTaskCount());

    // pause one segment unit
    startWorkThread();
    taskExecutor.pause(segId1);
    taskExecutor.pause(segId2);
    taskExecutor.pause(segId3);
    Thread.sleep(1000);
    stopWorkThread();
    Thread.sleep(1000);
    assertEquals(12, taskExecutor.getTaskCount());

    // revive one segment unit
    startWorkThread();
    taskExecutor.revive(segId1);
    Thread.sleep(1000);
    stopWorkThread();
    Thread.sleep(1000);
    assertEquals(12, taskExecutor.getTaskCount());

    // remove the segment unit
    startWorkThread();
    segmentUnitCallbackTest = new SegmentUnitCallbackTest();
    taskExecutor.removeSegmentUnit(segId1, segmentUnitCallbackTest);
    taskExecutor.removeSegmentUnit(segId2, segmentUnitCallbackTest);
    taskExecutor.removeSegmentUnit(segId3, segmentUnitCallbackTest);
    stopProcess();
    Thread.sleep(3000);
    assertEquals(0, taskExecutor.getTaskCount());

    Thread.sleep(1000);
    taskExecutor.shutdown();

  }

  @Test
  public void delTaskTest() throws Exception {
    segmentUnitProcessorFactory = new SegmentUnitProcessorFactory() {
      @Override
      public SegmentUnitProcessor generate(SegmentUnitTaskContext context) {
        return new SegmentUnitProcessorForTesting(context, 0, null, false);
      }
    };

    contextFactory = new SegmentUnitTaskContextFactoryImplForTest();

    SegmentUnitTaskExecutorImpl taskExecutor = new SegmentUnitTaskExecutorImpl(
        segmentUnitProcessorFactory,
        contextFactory, 10, 20, 100, "Testing");

    logger.debug("start the executor");
    taskExecutor.start();
    SegId segId1 = new SegId(1L, 1);
    taskExecutor.addSegmentUnit(segId1);

    Thread.sleep(3000);
    assertEquals(4, taskExecutor.getTaskCount());
    segmentUnitCallbackTest = new SegmentUnitCallbackTest();
    taskExecutor.removeSegmentUnit(segId1, segmentUnitCallbackTest);
    stopProcess();
    Thread.sleep(3000);
    assertEquals(0, taskExecutor.getTaskCount());
  }

  @Test
  public void delayCaseTest() throws Exception {
    segmentUnitProcessorFactory = new SegmentUnitProcessorFactory() {
      @Override
      public SegmentUnitProcessor generate(SegmentUnitTaskContext context) {
        return new SegmentUnitProcessorForTesting(context, 1000, null, false);
      }
    };

    contextFactory = new SegmentUnitTaskContextFactoryImplForTest();

    SegmentUnitTaskExecutorImpl taskExecutor = new SegmentUnitTaskExecutorImpl(
        segmentUnitProcessorFactory,
        contextFactory, 10, 20, 100, "Testing");
    logger.debug("start the executor");
    taskExecutor.start();
    SegId segId1 = new SegId(1L, 1);
    taskExecutor.addSegmentUnit(segId1);
    Thread.sleep(3000);
    taskExecutor.shutdown();
    Thread.sleep(3000);
    assertEquals(4, taskExecutor.getTaskCount());
  }

  @Test
  public void throwExceptionTest() throws Exception {
    segmentUnitProcessorFactory = new SegmentUnitProcessorFactory() {
      @Override
      public SegmentUnitProcessor generate(SegmentUnitTaskContext context) {
        return new SegmentUnitProcessorForTesting(context, 0, new RuntimeException(), true);
      }
    };

    contextFactory = new SegmentUnitTaskContextFactoryImplForTest();

    SegmentUnitTaskExecutorImpl taskExecutor = new SegmentUnitTaskExecutorImpl(
        segmentUnitProcessorFactory,
        contextFactory, 10, 20, 100, "Testing");
    logger.debug("start the executor");
    taskExecutor.start();
    SegId segId1 = new SegId(1L, 1);
    taskExecutor.addSegmentUnit(segId1);
    Thread.sleep(3000);
    taskExecutor.shutdown();
    Thread.sleep(1000);
    assertEquals(4, taskExecutor.getTaskCount());
  }

  @Test
  public void testMapContainKeyMethod() {
    final SegId key = new SegId(1L, 1);
    final SegId key2 = new SegId(1L, 2);
    final ContextKey value = new MyContextKey(key, 1);
    final ContextKey value2 = new MyContextKey(key2, 2);
    final SegmentUnitTaskContext context1 = new SegmentUnitTaskContext(value);
    final SegmentUnitTaskContext context2 = new SegmentUnitTaskContext(value2);
    // Multimaps.synchronizedMultimap
    final Multimap<SegId, SegmentUnitTaskContext> allTasksRecord = Multimaps
        .synchronizedSetMultimap(HashMultimap
            .<SegId, SegmentUnitTaskContext>create());

    final CountDownLatch mainLatch = new CountDownLatch(2);
    final CountDownLatch threadLatch = new CountDownLatch(1);
    // two threads process same map
    class ProcessMapThread extends Thread {
      private boolean processWhich;

      public ProcessMapThread(boolean b) {
        processWhich = b;
      }

      public void run() {
        try {
          threadLatch.await();
        } catch (InterruptedException e) {
          logger.error("failed to await", e);
        }
        try {
          if (processWhich) {
            allTasksRecord.put(key, context1);
            allTasksRecord.remove(key, context1);
            Thread.sleep(100);
            allTasksRecord.put(key, context2);
          } else {
            allTasksRecord.put(key, context1);
            allTasksRecord.remove(key, context1);
            Thread.sleep(200);
            allTasksRecord.remove(key, context2);
          }

          logger.info("thread[" + processWhich + "] success to process synchronized map");
        } catch (Exception e) {
          logger.error("caugth an exception", e);
        } finally {
          mainLatch.countDown();
        }
      }
    }

    ProcessMapThread firstProcessThread = new ProcessMapThread(true);
    firstProcessThread.start();
    ProcessMapThread secondProcessThread = new ProcessMapThread(false);
    secondProcessThread.start();
    threadLatch.countDown();
    try {
      mainLatch.await();
    } catch (InterruptedException e) {
      logger.error("caugth an exception", e);
    }

    assertTrue(!allTasksRecord.containsKey(key));
    assertEquals(0, allTasksRecord.size());

    allTasksRecord.put(key, null);
    assertTrue(allTasksRecord.containsKey(key));
    assertEquals(1, allTasksRecord.size());

    allTasksRecord.put(key, context2);
    assertTrue(allTasksRecord.containsKey(key));
    assertEquals(2, allTasksRecord.size());

    allTasksRecord.remove(key, context2);
    assertTrue(allTasksRecord.containsKey(key));
    assertEquals(1, allTasksRecord.size());

    allTasksRecord.remove(key, null);
    assertTrue(!allTasksRecord.containsKey(key));
    assertEquals(0, allTasksRecord.size());
  }

  @Test
  public void testMapRemoveOneElement() {
    SegId key = new SegId(1L, 1);
    SegId key2 = new SegId(1L, 2);
    ContextKey value = new MyContextKey(key, 1);
    ContextKey value2 = new MyContextKey(key2, 2);
    SegmentUnitTaskContext context1 = new SegmentUnitTaskContext(value);
    SegmentUnitTaskContext context11 = new SegmentUnitTaskContext(context1);
    SegmentUnitTaskContext context2 = new SegmentUnitTaskContext(value2);

    long contextId1 = context1.getId();
    long contextId11 = context11.getId();
    assertTrue(contextId1 != contextId11);

    // Multimaps.synchronizedMultimap
    Multimap<SegId, SegmentUnitTaskContext> allTasksRecord = Multimaps
        .synchronizedSetMultimap(HashMultimap
            .<SegId, SegmentUnitTaskContext>create());

    assertTrue(context1.equals(context11));

    allTasksRecord.put(context1.getSegId(), context1);
    allTasksRecord.put(context11.getSegId(), context11);

    assertEquals(1, allTasksRecord.size());
    assertEquals(1, allTasksRecord.get(context1.getSegId()).size());
    assertEquals(1, allTasksRecord.get(context11.getSegId()).size());

    ArrayList<SegmentUnitTaskContext> taskList = new ArrayList<>();
    for (SegmentUnitTaskContext context : allTasksRecord.get(context1.getSegId())) {
      if (context.getId() == contextId1) {
        taskList.add(context2);
      }
      assertEquals(context.getId(), contextId1);
      // assertEquals(context.getId(), contextId11);
    }
    assertEquals(1, taskList.size());
    allTasksRecord.replaceValues(context11.getSegId(), taskList);
    assertEquals(1, allTasksRecord.size());
    allTasksRecord.removeAll(context11.getSegId());
    assertEquals(0, allTasksRecord.size());
    taskList.add(context1);
    allTasksRecord.putAll(context1.getSegId(), taskList);
    assertEquals(2, allTasksRecord.size());
  }

  @Test
  public void testReferenceInMap() {
    SegId key = new SegId(1L, 1);
    ContextKey value = new MyContextKey(key, 1);
    SegmentUnitTaskContext context1 = new SegmentUnitTaskContext(value);
    // Multimaps.synchronizedMultimap
    Multimap<SegId, SegmentUnitTaskContext> allTasksRecord = Multimaps
        .synchronizedSetMultimap(HashMultimap
            .<SegId, SegmentUnitTaskContext>create());
    assertEquals(false, context1.isAbandonedTask());
    for (SegmentUnitTaskContext context : allTasksRecord.get(context1.getSegId())) {
      assertEquals(false, context.isAbandonedTask());
    }
    allTasksRecord.put(context1.getSegId(), context1);
    context1.setAbandonedTask(true);
    assertEquals(true, context1.isAbandonedTask());
    for (SegmentUnitTaskContext context : allTasksRecord.get(context1.getSegId())) {
      assertEquals(true, context.isAbandonedTask());
    }
  }

  @Test
  public void testAbandonedTest() throws InterruptedException {
    segmentUnitProcessorFactory = new SegmentUnitProcessorFactory() {
      @Override
      public SegmentUnitProcessor generate(SegmentUnitTaskContext context) {
        return new SegmentUnitProcessorForTesting(context, 0, null, false);
      }
    };

    contextFactory = new SegmentUnitTaskContextFactoryImplForTestAbandonedTask();

    SegmentUnitTaskExecutorImpl taskExecutor = new SegmentUnitTaskExecutorImpl(
        segmentUnitProcessorFactory,
        contextFactory, 10, 20, 100, "Testing");
    logger.debug("start the executor");
    taskExecutor.start();
    SegId segId1 = new SegId(1L, 1);
    taskExecutor.addSegmentUnit(segId1);
    Thread.sleep(2000);
    stopWorkThread();
    Thread.sleep(2000);
    assertEquals(1, taskExecutor.getTaskCount());
  }

  @Test
  public void testAddAndRemoveInShortTime() throws InterruptedException {
    segmentUnitProcessorFactory = new SegmentUnitProcessorFactory() {
      @Override
      public SegmentUnitProcessor generate(SegmentUnitTaskContext context) {
        return new SegmentUnitProcessorForTesting(context, 0, null, false);
      }
    };

    contextFactory = new SegmentUnitTaskContextFactoryImplForTestAbandonedTask();

    SegmentUnitTaskExecutorImpl taskExecutor = new SegmentUnitTaskExecutorImpl(
        segmentUnitProcessorFactory,
        contextFactory, 10, 20, 100, "Testing");
    logger.debug("start the executor");
    taskExecutor.start();
    // add one segmentUnit first
    SegId segId1 = new SegId(1L, 1);
    taskExecutor.addSegmentUnit(segId1);
    stopWorkThread();
    Thread.sleep(3000);
    assertEquals(1, taskExecutor.getTaskCount());
    startWorkThread();
    segmentUnitCallbackTest = new SegmentUnitCallbackTest();
    taskExecutor.removeSegmentUnit(segId1, segmentUnitCallbackTest);
    Thread.sleep(3000);
    assertEquals(0, taskExecutor.getTaskCount());
    taskExecutor.addSegmentUnit(segId1);
    stopWorkThread();
    Thread.sleep(3000);
    assertEquals(1, taskExecutor.getTaskCount());
  }

  public void stopProcess() {
    abandonFlag.set(true);
  }

  public void startProcess() {
    abandonFlag.set(false);
  }

  public void startWorkThread() {
    workThreadStopFlag.set(false);
  }

  public void stopWorkThread() {
    workThreadStopFlag.set(true);
  }

  private static class MyContextKey extends ContextKey {
    private final int number;

    public MyContextKey(SegId segId, int number) {
      super(segId);
      this.number = number;
    }

    public SegId getSegId() {
      return segId;
    }

    public int getNumber() {
      return number;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + number;
      result = prime * result + ((segId == null) ? 0 : segId.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      MyContextKey other = (MyContextKey) obj;
      if (number != other.number) {
        return false;
      }
      if (segId == null) {
        if (other.segId != null) {
          return false;
        }
      } else if (!segId.equals(other.segId)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      return "MyContextKey [segId=" + segId + ", number=" + number + "]";
    }

  }

  private class SegmentUnitCallbackTest implements SegmentUnitTaskCallback {
    public SegmentUnitCallbackTest() {
    }

    @Override
    public void removing(SegmentUnitTaskContext tarGetContext) {
      // TODO Auto-generated method stub

    }

  }

  private class SegmentUnitProcessorForTesting extends SegmentUnitProcessor {
    private long delayToExecute;
    private Throwable executionException;
    private boolean throwUncaughtException;

    public SegmentUnitProcessorForTesting(SegmentUnitTaskContext context, long delayToExecute,
        Throwable t,
        boolean throwUncaughtException) {
      super(context);
      this.delayToExecute = delayToExecute;
      this.executionException = t;
      this.throwUncaughtException = throwUncaughtException;
    }

    @Override
    public SegmentUnitProcessResult process() {
      SegmentUnitProcessResult result = new SegmentUnitProcessResult(getContext());
      if (throwUncaughtException) {
        throw new RuntimeException();
      }

      if (executionException != null) {
        result.setExecutionSuccess(false);
        result.setExecutionException(executionException);
      } else {
        result.setExecutionSuccess(true);
      }

      result.setDelayToExecute(delayToExecute);
      if (abandonFlag.get()) {
        result.getContext().setAbandonedTask(true);
      }
      while (workThreadStopFlag.get()) {
        try {
          logger.info("stop process to wait continue notice " + getClass());
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          logger.error("caught an execption", e);
        }
      }
      return result;
    }
  }

  private class SegmentUnitTaskContextFactoryImplForTest implements SegmentUnitTaskContextFactory {
    /**
     * generate three tasks for one segId.
     */
    @Override
    public List<SegmentUnitTaskContext> generateProcessingContext(SegId segId) {
      List<SegmentUnitTaskContext> contexts = new ArrayList<SegmentUnitTaskContext>();

      for (int i = 0; i < 3; i++) {
        SegmentUnitTaskContext context = new SegmentUnitTaskContext(
            new MyContextKey(new SegId(segId.getVolumeId(), i + 10), i));
        contexts.add(context);
      }

      return contexts;
    }

    /**
     * after run one time, update three tasks to four.
     */
    @Override
    public List<SegmentUnitTaskContext> generateProcessingContext(SegmentUnitProcessResult result) {
      SegmentUnitTaskContext context = result.getContext();
      MyContextKey currentKey = (MyContextKey) context.getKey();

      boolean addAdditional = false;
      if (currentKey.getNumber() == 0) {
        addAdditional = true;
      }

      // increase its failure time
      if (!result.executionSuccess()) {
        context.incFailureTimes();
      }
      // add itself back
      context.setExternallyAdded(false);
      context.updateDelay(1000);
      List<SegmentUnitTaskContext> contexts = new ArrayList<>();
      contexts.add(context);
      // new a task which number=10
      if (addAdditional) {
        MyContextKey newKey = new MyContextKey(currentKey.getSegId(), 10);
        SegmentUnitTaskContext newContext = new SegmentUnitTaskContext(newKey);
        logger.info("generateNewTimes: " + generateTenTimes++ + ", newKey: " + newKey.getNumber()
            + ", oldKey: " + currentKey.getNumber());
        newContext.updateDelay(result.getDelayToExecute());
        newContext.setExternallyAdded(false);
        contexts.add(newContext);
      }
      return contexts;
    }
  }

  private class SegmentUnitTaskContextFactoryImplForTestAbandonedTask implements
      SegmentUnitTaskContextFactory {
    @Override
    public List<SegmentUnitTaskContext> generateProcessingContext(SegId segId) {
      List<SegmentUnitTaskContext> contexts = new ArrayList<SegmentUnitTaskContext>();
      SegmentUnitTaskContext context = new SegmentUnitTaskContext(new MyContextKey(segId, 10));
      contexts.add(context);

      return contexts;
    }

    @Override
    public List<SegmentUnitTaskContext> generateProcessingContext(SegmentUnitProcessResult result) {
      SegmentUnitTaskContext context = result.getContext();

      // increase its failure time
      if (!result.executionSuccess()) {
        context.incFailureTimes();
      }
      // add itself back
      context.updateDelay(0);
      context.setAbandonedTask(true);
      List<SegmentUnitTaskContext> contexts = new ArrayList<>();
      contexts.add(context);
      // add a new context copy from old context
      SegmentUnitTaskContext newContext = new SegmentUnitTaskContext(context);
      newContext.updateDelay(0);
      // set new context abandoned
      newContext.setAbandonedTask(false);
      // can not be hasBeenWorked
      newContext.setHasBeenWorked(false);
      contexts.add(newContext);
      return contexts;
    }
  }

}
