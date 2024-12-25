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

package py.rebalance;

import py.archive.segment.SegmentUnitMetadata;
import py.common.RequestIdBuilder;
import py.instance.InstanceId;

/**
 * A RebalanceTask must contain two things: <br>
 *
 * <p>One is the segment unit to remove. <br> The other is the data node for that segment unit to
 * migrate to. <br>
 *
 */
public class RebalanceTask {
  private final int taskExpireTimeSeconds;
  private final RebalanceTaskType taskType;
  private final SegmentUnitMetadata sourceSegmentUnit;
  private final InstanceId destInstanceId;
  private final long taskId;
  private long targetArchiveId;
  private long bornTime;

  public RebalanceTask(long taskId, SegmentUnitMetadata sourceSegmentUnit,
      InstanceId destInstanceId,
      RebalanceTaskType taskType) {
    this.taskId = taskId;
    this.sourceSegmentUnit = sourceSegmentUnit;
    this.destInstanceId = destInstanceId;
    this.taskType = taskType;
    this.bornTime = System.currentTimeMillis();
    this.taskExpireTimeSeconds = Integer.MAX_VALUE;
  }

  public RebalanceTask(SegmentUnitMetadata sourceSegmentUnit, InstanceId destInstanceId,
      int taskExpireTimeSeconds,
      RebalanceTaskType taskType) {
    this.sourceSegmentUnit = sourceSegmentUnit;
    this.destInstanceId = destInstanceId;
    this.taskType = taskType;
    this.taskId = RequestIdBuilder.get();
    this.bornTime = System.currentTimeMillis();
    this.taskExpireTimeSeconds = taskExpireTimeSeconds;
  }

  public RebalanceTaskType getTaskType() {
    return taskType;
  }

  public SegmentUnitMetadata getSourceSegmentUnit() {
    return sourceSegmentUnit;
  }

  public InstanceId getDestInstanceId() {
    return destInstanceId;
  }

  public InstanceId getInstanceToMigrateFrom() {
    return sourceSegmentUnit.getInstanceId();
  }

  public long getTaskId() {
    return taskId;
  }

  public boolean expired() {
    return System.currentTimeMillis() - bornTime > taskExpireTimeSeconds * 1000;
  }

  public long getTargetArchiveId() {
    return targetArchiveId;
  }

  public void setTargetArchiveId(long targetArchiveId) {
    this.targetArchiveId = targetArchiveId;
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
    if (obj instanceof RebalanceTask) {
      RebalanceTask other = (RebalanceTask) obj;
      SegmentUnitMetadata hisSegmentUnitToRemove = other.getSourceSegmentUnit();
      InstanceId hisInstanceToMigrateTo = other.getDestInstanceId();
      if (hisSegmentUnitToRemove == null && this.sourceSegmentUnit != null) {
        return false;
      }
      if (hisInstanceToMigrateTo == null && this.destInstanceId != null) {
        return false;
      }
      return (hisSegmentUnitToRemove.equals(this.sourceSegmentUnit) && hisInstanceToMigrateTo
          .equals(this.destInstanceId));
    }
    return false;
  }

  @Override
  public String toString() {
    return "RebalanceTask [(" + sourceSegmentUnit.getInstanceId() + " to " + destInstanceId
        + " type=" + taskType
        + "), id=" + taskId + ", sourceSegmentUnit=" + sourceSegmentUnit + ", bornTime=" + bornTime
        + ", expireInMS=" + (bornTime + taskExpireTimeSeconds * 1000 - System.currentTimeMillis())
        + "]";
  }

  public String toSimpleString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Rebalance task : type=").append(taskType).append(", from ")
        .append(sourceSegmentUnit.getInstanceId())
        .append("(").append(sourceSegmentUnit.getArchiveId()).append(")").append(" to ")
        .append(destInstanceId)
        .append("(").append(targetArchiveId).append(")").append(", segId")
        .append(sourceSegmentUnit.getSegId());
    return sb.toString();
  }

  public enum RebalanceTaskType {
    PrimaryRebalance, NormalRebalance, InsideRebalance
  }
}
