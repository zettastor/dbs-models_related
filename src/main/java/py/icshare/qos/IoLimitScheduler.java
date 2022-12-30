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

package py.icshare.qos;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.common.RequestIdBuilder;
import py.exception.EndPointNotFoundException;
import py.exception.GenericThriftClientFactoryException;
import py.exception.TooManyEndPointFoundException;
import py.icshare.DriverKey;
import py.infocenter.client.InformationCenterClientFactory;
import py.io.qos.IoLimitManager;
import py.io.qos.IoLimitation;
import py.io.qos.IoLimitationEntry;
import py.periodic.UnableToStartException;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverNotFoundExceptionThrift;
import py.thrift.share.GetIoLimitationRequestThrift;
import py.thrift.share.GetIoLimitationResponseThrift;
import py.thrift.share.IoLimitationThrift;
import py.thrift.share.ServiceIsNotAvailableThrift;

/**
 * Scheduling the IOPS controller.
 */
public class IoLimitScheduler {
  public static final int IO_WEIGHT = 10;
  public static final int WRITE_WEIGHT = 17;
  public static final int READ_WEIGHT = IO_WEIGHT;
  private static final Logger logger = LoggerFactory.getLogger(IoLimitScheduler.class);
  private DelayQueue<Delayed> taskQueue;

  private IoLimitManager limitManager;

  private IoLimitationEntry staticIoLimitationEntry; // store static limit entry

  private IoLimitationEntry currentLimitationEntry;

  private boolean staticLimit;

  private InformationCenterClientFactory informationCenterClientFactory;

  private DriverKey driverKey;

  private boolean ready;

  private Map<Long, IoLimitationEntry> ioLimitationEntryRecord; // store all dynamic limit entries

  public IoLimitScheduler(IoLimitManager ioLimitManager) {
    this.limitManager = ioLimitManager;
    this.ioLimitationEntryRecord = new ConcurrentHashMap<>();
    this.taskQueue = new DelayQueue<>();
  }

  public synchronized void addOrModifyIoLimitation(IoLimitation modifyIoLimitation) {
    logger.warn("add or edit request, io limitation:{}", modifyIoLimitation);

    for (IoLimitationEntry modifyEntry : modifyIoLimitation.getEntries()) {
      if (ioLimitationEntryRecord.containsKey(modifyEntry.getEntryId())) {
        IoLimitationEntry currentEntry = ioLimitationEntryRecord.get(modifyEntry.getEntryId());
        if (!currentEntry.equals(modifyEntry)) {
          deleteIoLimitationEntry(modifyEntry.getEntryId());
          addIoLimitationEntry(modifyIoLimitation.getLimitType(), modifyEntry);
          logger.warn("delete old io limitationEntry: {}, save new io limitationEntry: {}",
              currentEntry,
              modifyEntry);
        } else {
          logger.warn("already has io limitationEntry: {}", currentEntry);
        }
      } else {
        addIoLimitationEntry(modifyIoLimitation.getLimitType(), modifyEntry);
        if (modifyIoLimitation.getLimitType().equals(IoLimitation.LimitType.Dynamic)) {
          ioLimitationEntryRecord.put(modifyEntry.getEntryId(), modifyEntry);
        }
        logger.warn("add new io limitationEntry: {}", modifyIoLimitation);
      }
      logger.warn("after operation, current jobs {}", taskQueue);
    }
  }

  private void addIoLimitationEntry(IoLimitation.LimitType type, IoLimitationEntry entry) {
    ready = true;
    if (type.equals(IoLimitation.LimitType.Static)) {
      staticIoLimitationEntry = entry;
      setStaticLimit(true);
      return;
    }
    taskQueue.put(entry.setStatusAndUpdateDelay(IoLimitationEntry.LimitStatus.Waiting));
    logger.warn("new LimitEntry added: {}", entry.getEntryId());
  }

  @Deprecated
  public synchronized boolean deleteIoLimitation(long id) {
    return true;
  }

  public synchronized boolean deleteIoLimitationEntry(long id) {
    if (staticIoLimitationEntry != null && staticIoLimitationEntry.getEntryId() == id) {
      staticIoLimitationEntry = null;
      setStaticLimit(false);
      return true;
    }
    if (!ioLimitationEntryRecord.containsKey(id)) {
      logger.warn("can not find io limitationEntry: {} to delete", id);
      return false;
    }
    logger.warn("delete io limitationEntry: {} request", ioLimitationEntryRecord.get(id));
    IoLimitationEntry limitToDelete = null;
    for (Delayed task : taskQueue) {
      if (!(task instanceof IoLimitationEntry)) {
        return false;
      }
      IoLimitationEntry limit = (IoLimitationEntry) task;
      if (limit.getEntryId() == id) {
        limitToDelete = limit;
        break;
      }
    }
    if (limitToDelete == currentLimitationEntry && limitToDelete != null) {
      currentLimitationEntry = null;
      if (limitManager.getIoLimitationEntry() == limitToDelete) {
        limitManager.close();
      }
    }
    ioLimitationEntryRecord.remove(id);
    return taskQueue.remove(limitToDelete);
  }

  public void clearLimitationEntries() {
    List<Long> entryIds = new ArrayList<>();
    entryIds.addAll(ioLimitationEntryRecord.keySet());

    for (Long id : entryIds) {
      deleteIoLimitationEntry(id);
    }

    if (staticIoLimitationEntry != null) {
      deleteIoLimitationEntry(staticIoLimitationEntry.getEntryId());
    }
  }

  public void deleteStaticLimitationEntry() {
    if (staticIoLimitationEntry != null) {
      deleteIoLimitationEntry(staticIoLimitationEntry.getEntryId());
    }
  }

  public void startJob() {
    while (true) {
      try {
        if (!ready) {
          try {
            retrieveLimitationInfo();
          } catch (Exception e) {
            logger.error("cannot get limitation info right now", e);
            Thread.sleep(3000);
          }
        } else {
          /*
           * Get the available tasks, there are two situations
           *
           * If the pre-task's end time is the same as the post-task's start time, there will be two
           *  tasks available at the same time. In this situation, we will not close the limit
           * manager, but just update the limitation to the new value
           *
           * If not, there should be only one task either start task or end task. In this situation,
           *  we will close or open the limit manager
           *
           */
          LinkedList<Delayed> tasks = new LinkedList<>();
          tasks.addFirst(taskQueue.take());
          Delayed another = taskQueue.poll(1000, TimeUnit.MILLISECONDS);
          if (another != null) {
            tasks.addFirst(another);
          }
          while (!tasks.isEmpty()) {
            Delayed task = tasks.removeFirst();
            logger.debug("task got {}", task);
            if (!(task instanceof IoLimitationEntry)) {
              logger.warn("exit flag got, quit!");
              return;
            }
            IoLimitationEntry limit = (IoLimitationEntry) task;
            processIoLimitationEntry(limit, tasks);
          }
        }
      } catch (Exception e) {
        logger.error("error processing limitation ", e);
      }
    }
  }

  private synchronized void retrieveLimitationInfo()
      throws EndPointNotFoundException, TooManyEndPointFoundException,
      GenericThriftClientFactoryException, DriverNotFoundExceptionThrift,
      ServiceIsNotAvailableThrift, TException, UnableToStartException {
    Validate.notNull(informationCenterClientFactory);
    Long driverContainerId = driverKey.getDriverContainerId();
    logger.warn("can not pull io limitations from driver container: {}, try pull from infocenter",
        driverContainerId);
    GetIoLimitationRequestThrift requestThrift = new GetIoLimitationRequestThrift();
    requestThrift.setRequestId(RequestIdBuilder.get());
    requestThrift.setDriverContainerId(driverContainerId);
    InformationCenter.Iface infoCenterClient = informationCenterClientFactory.build().getClient();
    GetIoLimitationResponseThrift getIoLimitationResponseThrift = infoCenterClient
        .getIoLimitationsInOneDriverContainer(requestThrift);
    logger.warn("get io limitations from infocenter: {}", getIoLimitationResponseThrift);

    boolean staticLimit = false;
    if (getIoLimitationResponseThrift.getMapDriver2ItsIoLimitations() != null
        && !getIoLimitationResponseThrift
        .getMapDriver2ItsIoLimitations().isEmpty()) {
      for (Map.Entry<DriverKeyThrift, List<IoLimitationThrift>>
          entry : getIoLimitationResponseThrift.getMapDriver2ItsIoLimitations().entrySet()) {
        DriverKeyThrift driverKeyThrift = entry.getKey();
        DriverKey getDriverKey = RequestResponseHelper.buildDriverKeyFrom(driverKeyThrift);
        if (getDriverKey.equals(driverKey)) {
          if (entry.getValue() != null && !entry.getValue().isEmpty()) {
            for (IoLimitationThrift ioLimitationThrift : entry.getValue()) {
              IoLimitation ioLimitation = RequestResponseHelper
                  .buildIoLimitationFrom(ioLimitationThrift);
              // get IoLimitationEntry
              for (IoLimitationEntry limitEntry : ioLimitation.getEntries()) {
                addIoLimitationEntry(ioLimitation.getLimitType(), limitEntry);

              }

              if (ioLimitation.getLimitType() == IoLimitation.LimitType.Static) {
                staticLimit = true;
              }
            }
          }
        }
      } // for loop map
    }

    setStaticLimit(staticLimit);

    ready = true;
  }

  public void open() {
    Thread thread = new Thread() {
      @Override
      public void run() {
        startJob();
      }
    };
    thread.setName("io-limit-scheduler");
    thread.start();
  }

  public void setStaticLimit(boolean staticLimit) {
    logger.warn("change limit type !! static ? {}", staticLimit);
    if (staticLimit) {
      Validate.notNull(staticIoLimitationEntry);
    }
    this.staticLimit = staticLimit;
    try {
      if (staticLimit) {
        limitManager.updateLimitationsAndOpen(staticIoLimitationEntry);
      } else {
        if (currentLimitationEntry != null) {
          limitManager.updateLimitationsAndOpen(currentLimitationEntry);
        } else {
          limitManager.close();
        }
      }
    } catch (UnableToStartException e) {
      // TODO error handle, maybe retry some times;
      logger.error("cannot start limitation", e);
    }
  }

  private void processIoLimitationEntry(IoLimitationEntry limit, LinkedList<Delayed> remainingTasks)
      throws UnableToStartException {
    if (limit.available()) {
      logger.warn("available, working now {}", limit);
      currentLimitationEntry = limit;
      if (!staticLimit && !(limitManager.isOpen()
          && limitManager.getIoLimitationEntry() == currentLimitationEntry)) {
        limitManager.updateLimitationsAndOpen(limit);
      }
      taskQueue.put(limit.setStatusAndUpdateDelay(IoLimitationEntry.LimitStatus.Working));
    } else if (!remainingTasks
        .isEmpty()) { // if there are both starting task and closing task, the closing task
      // will be done after the starting task
      remainingTasks.addLast(limit);
    } else {
      logger.warn("expired, waiting now {}", limit);
      if (currentLimitationEntry == limit) {
        currentLimitationEntry = null;
      }
      if (limitManager.getIoLimitationEntry() == limit) {
        limitManager.close();
      }
      taskQueue.put(limit.setStatusAndUpdateDelay(IoLimitationEntry.LimitStatus.Waiting));
    }
  }

  public void tryGettingReadIos(int count) {
    if (limitManager != null) {
      tryGettingIos(count * READ_WEIGHT);
    }
  }

  public void tryGettingWriteIos(int count) {
    if (limitManager != null) {
      tryGettingIos(count * WRITE_WEIGHT);
    }
  }

  public void tryGettingAnIo() {
    if (limitManager != null) {
      limitManager.tryGettingAnIo();
    }
  }

  public void tryGettingIos(int ioCount) {
    if (limitManager != null) {
      for (int i = 0; i < ioCount; i++) {
        limitManager.tryGettingAnIo();
      }
    }
  }

  public void tryReadThroughput(long size) {
    if (limitManager != null && limitManager.isOpen()) {
      limitManager.tryThroughput(size * READ_WEIGHT);
    }
  }

  public void tryWriteThroughput(long size) {
    if (limitManager != null && limitManager.isOpen()) {
      limitManager.tryThroughput(size * WRITE_WEIGHT);
    }
  }

  public void tryThroughput(long size) {
    if (limitManager != null && limitManager.isOpen()) {
      limitManager.tryThroughput(size);
    }
  }

  public void slowDownExceptFor(long volumeId, int level) throws TException {
    limitManager.slowDownExceptFor(volumeId, level);
  }

  public void resetSlowLevel(long volumeId) throws TException {
    limitManager.resetSlowLevel(volumeId);
  }

  public InformationCenterClientFactory getInformationCenterClientFactory() {
    return informationCenterClientFactory;
  }

  public void setInformationCenterClientFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }

  public DriverKey getDriverKey() {
    return driverKey;
  }

  public void setDriverKey(DriverKey driverKey) {
    this.driverKey = driverKey;
  }

  public void close() {
    limitManager.close();
    taskQueue.add(new Delayed() {
      @Override
      public int compareTo(Delayed o) {
        return 1;
      }

      @Override
      public long getDelay(TimeUnit unit) {
        return -1;
      }
    });
  }

  public Map<Long, IoLimitationEntry> getIoLimitationEntryRecord() {
    return ioLimitationEntryRecord;
  }
}