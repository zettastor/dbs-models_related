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

package py.icshare;

import java.util.Objects;

public class InstanceMaintenanceInformation {
  private long instanceId;
  private long startTime;
  private long duration;
  private long endTime;
  private String ip;

  public InstanceMaintenanceInformation() {
  }

  public InstanceMaintenanceInformation(long instanceId, long startTime, long duration, String ip) {
    this.instanceId = instanceId;
    this.startTime = startTime;
    this.duration = duration;
    endTime = startTime + duration;
    this.ip = ip;
  }

  public InstanceMaintenanceInformation(long instanceId, long startTime, long duration,
      long endTime, String ip) {
    this.instanceId = instanceId;
    this.startTime = startTime;
    this.duration = duration;
    this.endTime = endTime;
    this.ip = ip;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InstanceMaintenanceInformation that = (InstanceMaintenanceInformation) o;
    return instanceId == that.instanceId && startTime == that.startTime && duration == that.duration
        && endTime == that.endTime && ip == that.ip;
  }

  @Override
  public int hashCode() {
    return Objects.hash(instanceId, startTime, duration, endTime, ip);
  }

  @Override
  public String toString() {
    return "InstanceMaintenanceInformation{" + "instanceId=" + instanceId + ", startTime="
        + startTime
        + ", duration=" + duration + ", endTime=" + endTime + ", ip=" + ip + '}';
  }

  public long getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(long instanceId) {
    this.instanceId = instanceId;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getDuration() {
    return duration;
  }

  public void setDuration(long duration) {
    this.duration = duration;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }
}
