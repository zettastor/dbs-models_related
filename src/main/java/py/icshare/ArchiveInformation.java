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
import py.archive.ArchiveStatus;
import py.archive.StorageType;

public class ArchiveInformation {
  private long archiveId;
  private long instanceId;
  private String serialNumber;
  private long archiveCapacity;
  private int archiveStatus;
  private int storageType;
  private String description;
  private Date createdTime;
  private Date updatedTime;
  private Long storagePoolId;
  private String slotNo;

  public ArchiveInformation() {
  }

  public ArchiveInformation(long archiveId, long instanceId, StorageType storageType,
      ArchiveStatus archiveStatus,
      long capacity, Long storagePoolId) {
    this.archiveId = archiveId;
    this.instanceId = instanceId;
    if (storageType != null) {
      this.storageType = storageType.getValue();
    }

    if (archiveStatus != null) {
      this.archiveStatus = archiveStatus.getValue();
    }

    this.archiveCapacity = capacity;
    this.storagePoolId = storagePoolId;
  }

  public long getArchiveId() {
    return archiveId;
  }

  public void setArchiveId(long archiveId) {
    this.archiveId = archiveId;
  }

  public long getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(long instanceId) {
    this.instanceId = instanceId;
  }

  public long getDiskCapacity() {
    return archiveCapacity;
  }

  public void setDiskCapacity(long diskCapacity) {
    this.archiveCapacity = diskCapacity;
  }

  public int getArchiveStatus() {
    return archiveStatus;
  }

  public void setArchiveStatus(int archiveStatus) {
    this.archiveStatus = archiveStatus;
  }

  public ArchiveStatus archiveStatus() {
    return ArchiveStatus.findByValue(archiveStatus);
  }

  public void archiveStatus(ArchiveStatus archiveStatus) {
    if (archiveStatus != null) {
      this.archiveStatus = archiveStatus.getValue();
    }
  }

  public StorageType storageType() {
    return StorageType.findByValue(storageType);
  }

  public int getStorageType() {
    return storageType;
  }

  public void setStorageType(int storageType) {
    this.storageType = storageType;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Date getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(Date createdTime) {
    this.createdTime = createdTime;
  }

  public Date getUpdatedTime() {
    return updatedTime;
  }

  public void setUpdatedTime(Date updatedTime) {
    this.updatedTime = updatedTime;
  }

  public String getSerialNumber() {
    return serialNumber;
  }

  public void setSerialNumber(String serialNumber) {
    this.serialNumber = serialNumber;
  }

  public long getArchiveCapacity() {
    return archiveCapacity;
  }

  public void setArchiveCapacity(long archiveCapacity) {
    this.archiveCapacity = archiveCapacity;
  }

  public Long getStoragePoolId() {
    return storagePoolId;
  }

  public void setStoragePoolId(Long storagePoolId) {
    this.storagePoolId = storagePoolId;
  }

  public String getSlotNo() {
    return slotNo;
  }

  public void setSlotNo(String slotNo) {
    this.slotNo = slotNo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ArchiveInformation)) {
      return false;
    }
    ArchiveInformation that = (ArchiveInformation) o;
    return archiveId == that.archiveId
        && instanceId == that.instanceId
        && archiveCapacity == that.archiveCapacity
        && archiveStatus == that.archiveStatus
        && storageType == that.storageType
        && Objects.equal(serialNumber, that.serialNumber)
        && Objects.equal(description, that.description)
        && Objects.equal(createdTime, that.createdTime)
        && Objects.equal(updatedTime, that.updatedTime)
        && Objects.equal(storagePoolId, that.storagePoolId)
        && Objects.equal(slotNo, that.slotNo);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(archiveId, instanceId, serialNumber, archiveCapacity,
        archiveStatus, storageType, description, createdTime, updatedTime, storagePoolId, slotNo);
  }

  @Override
  public String toString() {
    return "ArchiveInformation{"
        + "archiveId=" + archiveId
        + ", instanceId=" + instanceId
        + ", serialNumber='" + serialNumber + '\''
        + ", archiveCapacity=" + archiveCapacity
        + ", archiveStatus=" + archiveStatus
        + ", storageType=" + storageType
        + ", description='" + description + '\''
        + ", createdTime=" + createdTime
        + ", updatedTime=" + updatedTime
        + ", storagePoolId=" + storagePoolId
        + ", slotNo='" + slotNo + '\''
        + '}';
  }

}
