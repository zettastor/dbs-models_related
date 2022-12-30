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

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import py.archive.segment.SegId;
import py.common.RequestIdBuilder;
import py.informationcenter.Utils;

public class SegmentUnitTaskContext implements Delayed {
  private final long id;
  private final ContextKey key;
  private long delay;
  private long timeSettingDelay;

  private Long inQueueTime;
  private Long outQueueTime;
  // beginningExecutionTime can be used to record the time when the ticket was first being
  // processing.
  private Long executionBeginTime;
  private Long executionEndTime;
  private int timesOfBeingWorked;
  private int timesOfBeingRejected;
  private SegmentUnitTaskType type;
  private AtomicBoolean externallyAdded;
  private AtomicBoolean hasBeenWorked;
  private AtomicBoolean abandonedTask;
  private int delayAfterReject;
  private volatile boolean paused;

  private int failureTimes;
  private AtomicReference<Thread> workerThreadRef;

  public SegmentUnitTaskContext(ContextKey k) {
    this(k, null);
  }

  public SegmentUnitTaskContext(SegmentUnitTaskContext otherContext) {
    this(otherContext.getKey(), otherContext);
  }

  private SegmentUnitTaskContext(ContextKey k, SegmentUnitTaskContext otherContext) {
    id = RequestIdBuilder.get();
    this.key = k;
    externallyAdded = new AtomicBoolean();
    hasBeenWorked = new AtomicBoolean();
    abandonedTask = new AtomicBoolean();
    if (otherContext != null) {
      delay = otherContext.delay;
      timeSettingDelay = otherContext.timeSettingDelay;
      timesOfBeingWorked = otherContext.timesOfBeingWorked;
      timesOfBeingRejected = otherContext.timesOfBeingRejected;
      failureTimes = otherContext.failureTimes;
      type = otherContext.type;
      hasBeenWorked.set(true);
      delayAfterReject = otherContext.delayAfterReject;
      paused = otherContext.paused;
    } else {
      delay = 0;
      timeSettingDelay = System.currentTimeMillis();
      timesOfBeingWorked = 0;
      timesOfBeingRejected = 0;
      failureTimes = 0;
      type = SegmentUnitTaskType.Default;
      hasBeenWorked.set(false);
      delayAfterReject = 0;
      paused = false;
    }
    externallyAdded.set(false);
    abandonedTask.set(false);
    inQueueTime = null;
    outQueueTime = null;
    executionBeginTime = null;
    executionEndTime = null;
    workerThreadRef = new AtomicReference<Thread>(null);

  }

  public SegId getSegId() {
    return key.getSegId();
  }

  public ContextKey getKey() {
    return key;
  }

  public long getId() {
    return id;
  }

  /**
   * Only update the delay to new value when the new one is larger than the current one.
   */
  public void updateDelay(long newDelay) {
    long now = System.currentTimeMillis();
    if (now + newDelay > getExpireTime()) {
      delay = newDelay;
      timeSettingDelay = now;
    }
  }

  private long getExpireTime() {
    return delay + timeSettingDelay;
  }

  /**
   * Update the delay no matter what.
   */
  public void updateDelayWithForce(long newDelay) {
    delay = newDelay;
    timeSettingDelay = System.currentTimeMillis();
  }

  /* get the delay */
  @Override
  public long getDelay(TimeUnit unit) {
    // internally the delay is in milliseconds
    return unit.convert(getExpireTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
  }

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

  public void executionBegin(Thread currentThread) {
    workerThreadRef.set(Thread.currentThread());
    executionBeginTime = System.currentTimeMillis();
    timesOfBeingRejected = 0;
  }

  public void executionEnd() {
    workerThreadRef.set(null);
    executionEndTime = System.currentTimeMillis();
    incTimesOfBeingWorked();
  }

  public void inQueue() {
    inQueueTime = System.currentTimeMillis();
    outQueueTime = null;
    executionBeginTime = null;
    executionEndTime = null;
  }

  public void leaveQueue() {
    outQueueTime = System.currentTimeMillis();
  }

  public void incFailureTimes() {
    failureTimes++;
  }

  public void incTimesOfBeingWorked() {
    timesOfBeingWorked++;
  }

  public void incTimesOfBeingRejected() {
    timesOfBeingRejected++;
  }

  public void resetFailureTime() {
    failureTimes = 0;
  }

  public Thread getWorkerThread() {
    return workerThreadRef.get();
  }

  public int getFailureTimes() {
    return failureTimes;
  }

  /**
   * for unit test only. getDelay() should be used instead, in order to make delay queue working.
   *
   */
  public long getInternalDelay() {
    return delay;
  }

  @Override
  public String toString() {
    return "SegmentUnitTaskContext [id=" + id + ", segId=" + getSegId() + ", key=" + key
        + ", inQueueTime="
        + (inQueueTime == null ? null : Utils.millsecondToString(inQueueTime)) + ", outQueueTime="
        + (outQueueTime == null ? null : Utils.millsecondToString(outQueueTime))
        + ", executionBeginTime="
        + (executionBeginTime == null ? null : Utils.millsecondToString(executionBeginTime))
        + ", executionEndTime=" + (executionEndTime == null ? null
        : Utils.millsecondToString(executionEndTime))
        + ", timesOfBeingWorked=" + timesOfBeingWorked + ", timesOfBeingRejected ="
        + timesOfBeingRejected
        + ", delayInMS=" + delay + ", failureTimes=" + failureTimes + ", worker=" + workerThreadRef
        .get()
        + ", className= " + getClass().getName() + ", type=" + type + ", hasBeenWorked="
        + hasBeenWorked
        + ", externallyAdded=" + externallyAdded + ", hasAbandoned=" + abandonedTask + ", paused="
        + paused
        + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
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
    SegmentUnitTaskContext other = (SegmentUnitTaskContext) obj;
    if (key == null) {
      if (other.key != null) {
        return false;
      }
    } else if (!key.equals(other.key)) {
      return false;
    }
    if (type == null) {
      if (other.type != null) {
        return false;
      }
    } else if (!type.equals(other.type)) {
      return false;
    }
    return true;
  }

  public SegmentUnitTaskType getType() {
    return type;
  }

  public void setType(SegmentUnitTaskType type) {
    this.type = type;
  }

  public boolean isExternallyAdded() {
    return externallyAdded.get();
  }

  public void setExternallyAdded(boolean externallyAdded) {
    this.externallyAdded.set(externallyAdded);
  }

  public boolean isHasBeenWorked() {
    return hasBeenWorked.get();
  }

  public void setHasBeenWorked(boolean hasBeenWorked) {
    this.hasBeenWorked.set(hasBeenWorked);
  }

  public boolean isAbandonedTask() {
    return abandonedTask.get();
  }

  public void setAbandonedTask(boolean abandonedTask) {
    this.abandonedTask.set(abandonedTask);
  }

  public int getDelayAfterReject() {
    return delayAfterReject;
  }

  public void setDelayAfterReject(int delayAfterReject) {
    this.delayAfterReject = delayAfterReject;
  }

  public boolean isPaused() {
    return paused;
  }

  public void setPaused(boolean paused) {
    this.paused = paused;
  }

}
