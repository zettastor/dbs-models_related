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

import com.google.common.base.Objects;
import java.util.Date;

public class StorageInformation {
  private long instanceId;
  private long totalCapacity;
  private long freeCapacity;
  private long availableCapacity;
  private Date createdTime;
  private int ssdCacheStatus; // 0 -- has no cache, 1-- has cache
  private long ssdCacheSize;
  private String tagKey;
  private String tagValue;
  private Long domainId;
  private long datanodeType;      //1:normal; 2:simple

  public StorageInformation() {
  }

  public StorageInformation(long instanceId, long totalCapacity, long freeCapacity,
      long availableCapacity, Date createdTime, Long domainId) {
    this.instanceId = instanceId;
    this.totalCapacity = totalCapacity;
    this.freeCapacity = freeCapacity;
    this.availableCapacity = availableCapacity;
    this.createdTime = createdTime;
    this.domainId = domainId;
  }

  public long getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(long instanceId) {
    this.instanceId = instanceId;
  }

  public long getTotalCapacity() {
    return totalCapacity;
  }

  public void setTotalCapacity(long totalCapacity) {
    this.totalCapacity = totalCapacity;
  }

  public long getFreeCapacity() {
    return freeCapacity;
  }

  public void setFreeCapacity(long freeCapacity) {
    this.freeCapacity = freeCapacity;
  }

  public long getAvailableCapacity() {
    return availableCapacity;
  }

  public void setAvailableCapacity(long availableCapacity) {
    this.availableCapacity = availableCapacity;
  }

  public Date getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(Date createdTime) {
    this.createdTime = createdTime;
  }

  public boolean hasSsdCache() {
    if (ssdCacheStatus == 0) {
      return false;
    } else {
      return true;
    }
  }

  public long getSsdCacheSize() {
    return ssdCacheSize;
  }

  public void setSsdCacheSize(long ssdCacheSize) {
    this.ssdCacheSize = ssdCacheSize;
  }

  public int getSsdCacheStatus() {
    return ssdCacheStatus;
  }

  public void setSsdCacheStatus(int ssdCacheStatus) {
    this.ssdCacheStatus = ssdCacheStatus;
  }

  public String getTagKey() {
    return tagKey;
  }

  public void setTagKey(String tagKey) {
    this.tagKey = tagKey;
  }

  public String getTagValue() {
    return tagValue;
  }

  public void setTagValue(String tagValue) {
    this.tagValue = tagValue;
  }

  public Long getDomainId() {
    return domainId;
  }

  public void setDomainId(Long domainId) {
    this.domainId = domainId;
  }

  public long getDatanodeType() {
    return datanodeType;
  }

  public void setDatanodeType(long datanodeType) {
    this.datanodeType = datanodeType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StorageInformation)) {
      return false;
    }
    StorageInformation that = (StorageInformation) o;
    return instanceId == that.instanceId
        && totalCapacity == that.totalCapacity
        && freeCapacity == that.freeCapacity
        && availableCapacity == that.availableCapacity
        && ssdCacheStatus == that.ssdCacheStatus
        && ssdCacheSize == that.ssdCacheSize
        && datanodeType == that.datanodeType
        && Objects.equal(createdTime, that.createdTime)
        && Objects.equal(tagKey, that.tagKey)
        && Objects.equal(tagValue, that.tagValue)
        && Objects.equal(domainId, that.domainId);
  }

  @Override
  public String toString() {
    return "StorageInformation{"
        + "instanceId=" + instanceId
        + ", totalCapacity=" + totalCapacity
        + ", freeCapacity=" + freeCapacity
        + ", availableCapacity=" + availableCapacity
        + ", createdTime=" + createdTime
        + ", ssdCacheStatus=" + ssdCacheStatus
        + ", ssdCacheSize=" + ssdCacheSize
        + ", tagKey='" + tagKey + '\''
        + ", tagValue='" + tagValue + '\''
        + ", domainId=" + domainId
        + ", datanodeType=" + datanodeType
        + '}';
  }

}
