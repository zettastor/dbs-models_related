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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.NamedThreadFactory;

/**
 * To use this executor, we have to addSegment(segId) and call start() function.
 */
public class SegmentUnitTaskExecutorImpl implements SegmentUnitTaskExecutor {
  private static final Logger logger = LoggerFactory.getLogger(SegmentUnitTaskExecutorImpl.class);
  private static final long IDLE_THREAD_KEEP_ALIVE_TIME = 60; // seconds
  private static final long SLEEP_TIME_WHEN_EXECUTION_EXCEPTION_CAUGHT = 100; // MS
  private static final long DELAY_EXECUTION_OF_DISALLOW_SEGMENT_UNIT_MS = 500; // MS
  private final AtomicLong queueSeqId;
  private final String name;
  private final Multimap<SegId, SegmentUnitTaskContext> allTasksRecord;
  private final Map<SegId, SegmentUnitTaskCallback> removedSegIds;
  private final Map<SegId, ContextKey> disallowedTasks;
  private final SegmentUnitProcessorFactory processorFactory;
  private final SegmentUnitTaskContextFactory contextFactory;
  private final Thread taskPullerThread;
  private DelayQueue<SegmentUnitTaskContext> taskQueue;
  private Map<SegmentUnitTaskType, ThreadPoolExecutor> executorMap;
  private volatile boolean isInterrupted;
  /**
   * don't need to make it atomic to save system resources. Pausing the system a bit late doesn't
   * matter
   */
  private volatile boolean pause;

  /**
   * construct a task executor. It is paused after the construction and need to use restart() to
   * start it.
   */
  public SegmentUnitTaskExecutorImpl(SegmentUnitProcessorFactory segmentUnitProcessorFactory,
      SegmentUnitTaskContextFactory contextFactory, int corePoolSize, int maxPoolSize,
      int maxNumTasks,
      String name) {
    /*
     * Current use one puller thread to do the job, once one thread is not enough, should use thread
     *  safe multi-map,
     * and processContext should add synchronized
     */
    allTasksRecord = Multimaps
        .synchronizedSetMultimap(HashMultimap.<SegId, SegmentUnitTaskContext>create());
    disallowedTasks = new ConcurrentHashMap<>();
    removedSegIds = new ConcurrentHashMap<SegId, SegmentUnitTaskCallback>();
    this.processorFactory = segmentUnitProcessorFactory;
    this.contextFactory = contextFactory;
    taskQueue = new DelayQueue<SegmentUnitTaskContext>();
    executorMap = new ConcurrentHashMap<SegmentUnitTaskType, ThreadPoolExecutor>();
    this.name = name;

    final TaskPuller puller = new TaskPuller();
    taskPullerThread = new Thread(name + "-TaskPuller") {
      @Override
      public void run() {
        puller.run();
      }
    };
    taskPullerThread.start();

    this.queueSeqId = new AtomicLong(0);
    this.pause = true;
    this.isInterrupted = false;
    addThreadPoolExecutor(SegmentUnitTaskType.Default, corePoolSize, maxPoolSize);

  }

  public void stopTaskPullerThread() {
    this.isInterrupted = true;
    logger.warn("set isInterrupted true, {} task puller thread should exit", name);
  }

  /**
   * Here initialize the target type executor, then put it into map for reflectively use.
   */
  public void addThreadPoolExecutor(SegmentUnitTaskType threadPoolType, int corePoolSize,
      int maxPoolSize) {
    // if executorMap already has this given type, do nothing just return
    if (executorMap.containsKey(threadPoolType)) {
      return;
    }
    String executorName = name + "-" + threadPoolType.name();

    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutorWithCallBack(corePoolSize,
        maxPoolSize,
        IDLE_THREAD_KEEP_ALIVE_TIME, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new NamedThreadFactory(executorName));
    executorMap.put(threadPoolType, threadPoolExecutor);
  }

  @Override
  public void addSegmentUnit(SegId segId) throws RejectedExecutionException {
    addSegmentUnit(segId, false);
  }

  /**
   * It is possible that adding an existing segment unit can cause duplicated segment units due the
   * synchronization issue.
   *
   * <p>Check if it is ok for the system running on two same contexts
   */
  @Override
  public void addSegmentUnit(SegId segId, boolean pauseProcessing)
      throws RejectedExecutionException {
    List<SegmentUnitTaskContext> newContexts = contextFactory.generateProcessingContext(segId);
    if (newContexts == null || newContexts.size() == 0) {
      logger.debug("no contexts are generated for segId {}", segId);
      return;
    }

    // delete it from the removedSegIds to indicate that the segment is added back
    removedSegIds.remove(segId);

    // the specified segId might already exists. there are two cases:
    // The first is adding an existing the segment
    // the second is adding removed segment back but its corresponding contexts have not
    // be removed completely yet. In both cases, we only need to check if new created
    // contexts exists in the delay queue or being worked on.
    logger.debug("the size of newly generated context: {} ", newContexts.size());

    // In the case where multiple new contexts might be generated for a context, the size of new
    // contexts for
    // just added segment units might be less than the number of those in the queues.
    // Validate.isTrue(newContexts.size() >= tasksInDQ.size() + tasksWorking.size());
    if (removedSegIds.get(segId) != null) {
      logger.error("some one is deleting the segment unit: {}, pause: {}", segId, pauseProcessing);
      throw new RejectedExecutionException("the segment unit is being removed");
    } else {
      logger
          .warn("segId({}) added to delay system, pause: {}, contexts: {}", segId, pauseProcessing,
              newContexts);
    }

    if (pauseProcessing) {
      logger.debug("segId({}) added to delay system", segId);
      disallowedTasks.put(segId, new SegIdContextKey(segId));
    }

    // put the eligible context to the delayed queue
    for (SegmentUnitTaskContext context : newContexts) {
      context.setExternallyAdded(true);
      taskQueue.put(context);
      context.inQueue();
    }
  }

  @Override
  public void removeSegmentUnit(SegId segId, SegmentUnitTaskCallback callback) {
    logger.debug("try to remove {} contexts", segId);
    removedSegIds.put(segId, callback);
    disallowedTasks.remove(segId);
    // leave the context related to segId in the queue.
    // The queuePuller will pull them from the queue and remove them eventually
  }

  @Override
  public void pause(SegId segId) {
    // check if the segment unit exists
    ContextKey oldContextKey = disallowedTasks.put(segId, new SegIdContextKey(segId));
    logger.warn("pause the segment unit: {}, {}, disallowedTasks size {}", segId, oldContextKey,
        disallowedTasks.size());
  }

  @Override
  public void pause() {
    pause = true;
  }

  @Override
  public void revive(SegId segId) {
    logger.debug("reviveSegmentUnitProcessing the segId: {}", segId);
    // check if the segment unit exists
    ContextKey oldKey = disallowedTasks.remove(segId);
    logger.debug("remove the context key: {}", oldKey);
  }

  public boolean isAllContextsPaused(SegId segId) {
    Collection<SegmentUnitTaskContext> contexts = allTasksRecord.get(segId);
    for (SegmentUnitTaskContext context : contexts) {
      if (!context.isPaused()) {
        logger.info("context={} is not paused, all contexts={}", context, contexts);
        return false;
      }
    }
    return true;
  }

  @Override
  public void pauseSegmentUnitProcessing(ContextKey particularContext) {
    // check if the segment unit exists
    ContextKey oldKey = disallowedTasks
        .putIfAbsent(particularContext.getSegId(), particularContext);
    logger.warn("pause the segment unit: {}, old key={}", particularContext, oldKey);
  }

  /**
   * Retrieve the count of tasks that are waiting in the queue and being worked on.
   */
  @Override
  public int getTaskCount() {
    return allTasksRecord.size();
  }

  @Override
  public void shutdown() {
    stopTaskPullerThread();
    for (Entry<SegmentUnitTaskType, ThreadPoolExecutor> entry : executorMap.entrySet()) {
      entry.getValue().shutdown();
      try {
        if (!entry.getValue().awaitTermination(60, TimeUnit.SECONDS)) {
          logger.warn("some internal threads still are running");
        }
      } catch (InterruptedException e) {
        logger.error("caught an exception", e);
      }
    }
  }

  /**
   * multiply threads deal with after executor.
   */
  private void processDone(SegmentUnitProcessResult result) {
    List<SegmentUnitTaskContext> newContexts = contextFactory.generateProcessingContext(result);
    // just put new contexts to delay queue
    for (SegmentUnitTaskContext newContext : newContexts) {
      newContext.inQueue();
      taskQueue.put(newContext);
      logger.debug("Added a new context to the delay queue {}, execute by thread {} ", newContext,
          Thread.currentThread().getName());
    }
  }

  /**
   * This function is called when a task is pulled out of the delayed queue. In the above case,
   * there is no corresponding tasks in the queue.
   */
  private void processContextsPulledFromDelayQueue(List<SegmentUnitTaskContext> newContexts,
      Map<SegmentUnitTaskContext, HowToDealWithNewContext> howToProcess) {
    // TODO: if more than one puller thread exist, should consider synchronized
    for (SegmentUnitTaskContext newContext : newContexts) {
      logger.debug("context retrieved from delay queue {} ", newContext);
      boolean needProcess = false;
      try {
        SegId segId = newContext.getSegId();
        /*
         * determine the context from map is abandoned or not, if it's abandoned, remove it from map
         */
        Collection<SegmentUnitTaskContext> contextsInMap = allTasksRecord.get(segId);
        Iterator<SegmentUnitTaskContext> iterator = contextsInMap.iterator();
        while (iterator.hasNext()) {
          SegmentUnitTaskContext contextInMap = iterator.next();
          if (contextInMap.isAbandonedTask()) {
            iterator.remove();
          }
        }

        /*
         * determine this task has been abandon or not, one task submit to work thread, when work
         * thread finished its work, it will return current task and new normal tasks, also context
         *  factory will sign which task(s) is/are dead. so when a dead task coming, just discard
         * it, remove it from the record map.
         */
        if (newContext.isAbandonedTask()) {
          continue;
        }

        // if this context belongs to segId is not in removed SegId Set, should process it
        if (removedSegIds.containsKey(segId)) {
          // removedSegId contains segId, and if record map contains this context, remove this
          // context
          allTasksRecord.remove(segId, newContext);
          if (!allTasksRecord.containsKey(segId)) {
            removedSegIds.get(segId).removing(newContext);
          } else {
            logger.warn("there are still some other contexts in the multi map {}, {}", segId,
                allTasksRecord.get(segId));
          }
          logger.debug("remove segmentUnitTask {} from record map", newContext);
          continue;
        }

        if (newContext.isExternallyAdded()) {
          /*
           * this task is added by external action, like addSegmentUnit interface, usually the task
           *  will be
           * only one, according the record map is already contain segId of the task or not to
           * determine how
           * to process the task.
           */
          if (!allTasksRecord.containsKey(segId)) {
            needProcess = true;
            newContext.setExternallyAdded(false);
            logger
                .trace("Externally add task{} right now, record map doesn't have any task before.",
                    newContext);
            // put context into record map
            allTasksRecord.put(segId, newContext);
          } // else do nothing
        } else {
          /*
           * image that case: two context have the same value, if we don't sign them, they will show
           *  up in the delay queue at the same time, and they are duplicate. the first coming task
           *  will be worked at first, so we sign the first task has been worked in the record map,
           * then second task comes after, but we can look up in the record map, and find the first
           *  task has been worked, then discard the second task.
           */
          if (!newContext.isHasBeenWorked()) {
            if (!allTasksRecord.containsEntry(segId, newContext)) {
              // if this context is new one, and record map hasn't the same context yet, should
              // process it
              needProcess = true;
              logger.trace("A brand new task{}, gonna to submit to work thread.", newContext);
              // put context into record map
              allTasksRecord.put(segId, newContext);
            } // else is primary trans to start status, do nothing
          } else {
            // if this context has been worked, even rejected by work thread pool, it still has been
            // worked
            // Validate.isTrue(allTasksRecord.containsEntry(segId, newContext), "context is " +
            // newContext);
            logger.trace("Old task coming {}, pass it to work thread", newContext);
            needProcess = true;
          }
        }

        if (!needProcess) {
          continue;
        }

        logger.trace("segId {}. disallowedTasks size {}", segId, disallowedTasks.size());
        newContext.setHasBeenWorked(true);
        ContextKey disallowedKey = disallowedTasks.get(segId);
        if (howToProcess.containsKey(newContext)) {
          howToProcess.keySet().stream().forEach(key -> {
            if (key.hashCode() == newContext.hashCode()) {
              logger.error("exist {} new {}", key, newContext);
            }
          });
        }
        if (disallowedKey != null
            && (disallowedKey instanceof SegIdContextKey || disallowedKey
            .equals(newContext.getKey()))) {
          logger.trace("disallowed tasks: {}", disallowedKey);
          howToProcess.put(newContext, HowToDealWithNewContext.Delay);
          newContext.setPaused(true);
        } else {
          howToProcess.put(newContext, HowToDealWithNewContext.Execute);
          newContext.setPaused(false);
        }

      } catch (Exception e) {
        // if any exception happen in process, put it back into delay queue
        logger.error("some exception happen, put context {} back into delay queue", newContext, e);
        howToProcess.put(newContext, HowToDealWithNewContext.Execute);
      }
    } // for contexts loop
  }

  public String printAllTasks() {
    StringBuilder builder = new StringBuilder(allTasksRecord.toString());
    builder.append("\n ----------\n ");
    builder.append(allTasksRecord.toString());
    return builder.toString();
  }

  public SegmentUnitProcessorFactory getProcessorFactory() {
    return processorFactory;
  }

  public String printTasksInQueue() {
    Map<Long, SegmentUnitTaskContext> map = new TreeMap<>();
    StringBuilder builder = new StringBuilder();
    for (SegmentUnitTaskContext context : taskQueue) {
      map.put(context.getDelay(TimeUnit.MILLISECONDS), context);
    }

    for (Map.Entry<Long, SegmentUnitTaskContext> entry : map.entrySet()) {
      SegmentUnitTaskContext context = entry.getValue();
      builder.append("Id:");
      builder.append(context.getId());
      builder.append("|");
      builder.append(context.getKey());
      builder.append("|");
      builder.append("delay:");
      builder.append(entry.getKey() + "\n");
    }

    String prefix = "\n" + name + queueSeqId.incrementAndGet() + "\n=========================\n";
    return prefix + builder.toString() + "\n++++++++++++++++++++++++++++++\n";
  }

  @Override
  public void restart() {
    pause = false;
  }

  @Override
  public void start() {
    restart();
  }

  @Override
  public boolean isPaused(SegId segId) {
    if (disallowedTasks.get(segId) != null) {
      return true;
    } else {
      return false;
    }
  }

  private enum HowToDealWithNewContext {
    Discard, // throw the next context away
    Delay, // put it back to the delay queue
    Execute // execute the next context
  }

  private class ThreadPoolExecutorWithCallBack extends ThreadPoolExecutor {
    public ThreadPoolExecutorWithCallBack(int corePoolSize, int maximumPoolSize, long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      super.afterExecute(r, t);

      // the execution of SegmentUnitProcess must not throw any exception,
      // because SegmentUnitProcess should handle all exceptions and put them to the process result
      Validate.isTrue(t == null);

      if (r instanceof Future<?>) {
        // Try to get the result from the execution. If CancellationException, ExecutionException or
        // InterruptedException are caught,
        try {
          Object resultObject = ((Future<?>) r).get();
          Validate.isTrue(resultObject instanceof SegmentUnitProcessResult);
          // add here for temp verify context type and work thread pool type
          SegmentUnitProcessResult result = (SegmentUnitProcessResult) resultObject;

          processDone(result);
        } catch (CancellationException ce) {
          // some one externally cancel the task. This situation is most likely the shutdown case
          logger.warn("A SegmentUnitTask is cancelled", ce);
        } catch (ExecutionException ee) {
          logger.warn("No way we can get here", ee);
          Validate.isTrue(false);
        } catch (InterruptedException ie) {
          logger.warn("caught an interrupt exception", ie);
          Thread.currentThread().interrupt(); // ignore/reset
        } finally {
          logger.info("nothing need to do here");
        }
      } else {
        logger.error("No way {} is not an instance of a Future class", r);
        Validate.isTrue(false);
      }
    }
  }

  private class TaskPuller implements Runnable {
    @Override
    public void run() {
      List<SegmentUnitTaskContext> contextsRetrieved = new ArrayList<SegmentUnitTaskContext>();
      Map<SegmentUnitTaskContext, HowToDealWithNewContext> howTo = new HashMap<>();
      while (!isInterrupted) {
        try {
          if (pause) {
            try {
              Thread.sleep(DELAY_EXECUTION_OF_DISALLOW_SEGMENT_UNIT_MS);
            } catch (InterruptedException e) {
              isInterrupted = true;
            }
            continue;
          }

          // wait forever until an available task available to execute
          try {
            taskQueue.drainTo(contextsRetrieved);
          } catch (Exception e) {
            logger.error(
                "Caught an exception when draining contexts from the delay queue. "
                    + "Some contexts might miss because of this exception. Let's exit the system"
                    + " and restart it again",
                e);
            isInterrupted = true;
            continue;
          }

          if (contextsRetrieved.size() == 0) {
            // nothing is available, Sleep 10ms
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              isInterrupted = true;
            }
            continue;
          }

          // remove all contexts from map
          howTo.clear();
          // process contexts pulled from delay queue
          processContextsPulledFromDelayQueue(contextsRetrieved, howTo);
          if (howTo != null && howTo.size() != 0) {
            for (Map.Entry<SegmentUnitTaskContext, HowToDealWithNewContext> entry : howTo
                .entrySet()) {
              SegmentUnitTaskContext newContext = entry.getKey();
              newContext.leaveQueue();
              if (entry.getValue() == HowToDealWithNewContext.Execute) {
                submitContext(newContext);
                logger.debug("submitted a new context to the work thread pool {} ", newContext);
              } else if (entry.getValue() == HowToDealWithNewContext.Delay) {
                newContext.inQueue();
                newContext.updateDelay(DELAY_EXECUTION_OF_DISALLOW_SEGMENT_UNIT_MS);
                taskQueue.put(newContext);
                logger.debug("put back to the queue for delay {} ", newContext);
              } // else discard, do nothing
            }
          }

          contextsRetrieved.clear();
        } catch (Exception e) {
          logger.error("task puller caught an exception ", e);
        } finally {
          logger.info("nothing need to do here");
        }
      }
    }

    private void submitContext(SegmentUnitTaskContext context) {
      ThreadPoolExecutor executor = null;
      boolean needPutBack2DelayQueue = false;
      /*
       * get the target type executor from map by task context carries its type, if context doesn't
       * have any type, even not default, we use default work thread pool
       */
      try {
        SegmentUnitProcessor segmentUnitProcessor = processorFactory.generate(context);
        // maintain with metrics map
        if (context.getType() != null && executorMap.containsKey(context.getType())) {
          executor = executorMap.get(context.getType());
        } else {
          executor = executorMap.get(SegmentUnitTaskType.Default);
        }
        executor.execute(new FutureTask<SegmentUnitProcessResult>(segmentUnitProcessor));
      } catch (RejectedExecutionException re) {
        needPutBack2DelayQueue = true;
        logger
            .info("Because of RejectedExecutionException, Can't submit a task to work threads: {} ",
                context);
      } catch (Exception e) {
        logger.error("submit context {}, caught an exception", context, e);
        needPutBack2DelayQueue = true;
      } finally {
        /*
         * if caught any exception, should put context back into delay queue
         */
        if (needPutBack2DelayQueue) {
          try {
            // if thread pool reject this context, we update context delay time and put it back
            // into delay
            // queue
            // at this time, record map has this context, so we do nothing with map.
            context.incTimesOfBeingRejected();
            context.inQueue();
            if (context.getDelayAfterReject() != 0) {
              context.updateDelay(RandomUtils.nextInt(context.getDelayAfterReject()));
            } else {
              context.updateDelay(SLEEP_TIME_WHEN_EXECUTION_EXCEPTION_CAUGHT);
            }

            taskQueue.put(context);

          } catch (Exception e) {
            logger.error("caught an exception", e);
          }
        }
      }
    }
  }
}
