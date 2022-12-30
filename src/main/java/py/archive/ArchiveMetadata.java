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

package py.archive;

import org.slf4j.LoggerFactory;
import py.instance.InstanceId;

/**
 * The ArchiveMetaData looks like.:
 *
 * <p><pre>
 * ------------------------------------------------------------------------------------------------
 * ----
 * magic number (8 bytes) | len (4 bytes) of archive md | archive meta data base(8K) | checksum (8
 * bytes)| magic number (8 bytes) | len (4 bytes) of archive md | Special archive  meta data |
 * checksum (8 bytes)| ----------------------------------------------------------------------------
 * ------------------------
 * PS:The 8Bytes-Checksum is constructed by |magic number (4 bytes) | checksum (4 bytes) |  <br> and
 * the magic number is decided by Algorithm <br>
 * </pre>
 *
 * <p>All types of Archive meta data  have the  ArchiveMetadata Different types pf Archive have the
 * different Special Archive meta data
 */
public class ArchiveMetadata {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ArchiveMetadata.class);

  // a big random identifier for the archive
  private Long archiveId;

  // serial number is optional
  private String serialNumber;

  private String slotNo;

  // instance id
  private InstanceId instanceId;

  // Page Size
  private int pageSize;

  // the name of the device at the OS
  private String deviceName;

  // archive status
  private ArchiveStatus status;

  // archive type: SSD, SATA, SAS
  private StorageType storageType;

  private int version;

  private ArchiveType archiveType;

  private long createdTime;
  private long updatedTime;
  private String updatedBy;
  private String createdBy;

  // arbitrary description
  private String description;

  // space that can be used to create segment units
  private long logicalSpace;

  // device maybe partitioned, and one of the partitions is used as file system
  private String fileSystemPartitionName;

  public ArchiveMetadata() {
    this.archiveId = 0L;
    this.serialNumber = null;
    this.instanceId = null;
    this.pageSize = 0;
    this.version = 0;
    this.createdTime = 0;
    this.updatedTime = 0;
    this.updatedBy = null;
    this.createdBy = null;
    this.description = null;
    this.deviceName = null;
    this.setStatus(ArchiveStatus.OFFLINED);
    this.archiveType = ArchiveType.RAW_DISK;
    this.fileSystemPartitionName = null;
    this.slotNo = null;
  }

  public ArchiveMetadata(ArchiveMetadata archiveMetadata) {
    this.archiveId = archiveMetadata.getArchiveId();
    this.serialNumber = archiveMetadata.getSerialNumber();
    this.instanceId = archiveMetadata.getInstanceId();
    this.pageSize = archiveMetadata.getPageSize();
    this.version = archiveMetadata.getVersion();
    this.createdTime = archiveMetadata.getCreatedTime();
    this.updatedTime = archiveMetadata.getUpdatedTime();
    this.updatedBy = archiveMetadata.getUpdatedBy();
    this.createdBy = archiveMetadata.getUpdatedBy();
    this.description = archiveMetadata.getDescription();
    this.deviceName = archiveMetadata.getDeviceName();
    this.setStatus(archiveMetadata.getStatus());
    this.archiveType = archiveMetadata.archiveType;
    this.logicalSpace = archiveMetadata.logicalSpace;
    //Added by Vin Xu for StorageType null.
    this.storageType = archiveMetadata.storageType;
    this.fileSystemPartitionName = archiveMetadata.fileSystemPartitionName;
    this.slotNo = archiveMetadata.slotNo;
  }

  // Setter and Getter
  public Long getArchiveId() {
    return archiveId;
  }

  public void setArchiveId(Long archiveId) {
    this.archiveId = archiveId;
  }

  public String getSerialNumber() {
    return serialNumber;
  }

  public void setSerialNumber(String serialNumber) {
    this.serialNumber = serialNumber;
  }

  public InstanceId getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(InstanceId instanceId) {
    this.instanceId = instanceId;
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public long getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(long createdTime) {
    this.createdTime = createdTime;
  }

  public long getUpdatedTime() {
    return updatedTime;
  }

  public void setUpdatedTime(long updatedTime) {
    this.updatedTime = updatedTime;
  }

  public String getUpdatedBy() {
    return updatedBy;
  }

  public void setUpdatedBy(String updatedBy) {
    this.updatedBy = updatedBy;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getDeviceName() {
    return deviceName;
  }

  public void setDeviceName(String deviceName) {
    this.deviceName = deviceName;
  }

  public long getLogicalSpace() {
    return logicalSpace;
  }

  public void setLogicalSpace(long logicalSpace) {
    this.logicalSpace = logicalSpace;
  }

  public String getFileSystemPartitionName() {
    return fileSystemPartitionName;
  }

  public void setFileSystemPartitionName(String fileSystemPartitionName) {
    this.fileSystemPartitionName = fileSystemPartitionName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (archiveId ^ (archiveId >>> 32));
    result = prime * result + ((serialNumber == null) ? 0 : serialNumber.hashCode());
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
    ArchiveMetadata other = (ArchiveMetadata) obj;
    if (archiveId.longValue() != other.archiveId.longValue()) {
      return false;
    }
    if (serialNumber == null) {
      if (other.serialNumber != null) {
        return false;
      }
    } else if (!serialNumber.equals(other.serialNumber)) {
      return false;
    }
    return true;
  }

  public boolean copy(ArchiveMetadata archiveMetadataBase) {
    boolean hasCopy = false;
    if (archiveMetadataBase.archiveId != 0) {
      this.archiveId = archiveMetadataBase.archiveId;
      hasCopy = true;

    }
    if (archiveMetadataBase.serialNumber != null) {
      this.serialNumber = archiveMetadataBase.serialNumber;
      hasCopy = true;
    }

    if (archiveMetadataBase.instanceId != null) {
      this.instanceId = archiveMetadataBase.instanceId;
      hasCopy = true;
    }
    if (archiveMetadataBase.pageSize != 0) {
      this.pageSize = archiveMetadataBase.pageSize;
      hasCopy = true;
    }
    if (archiveMetadataBase.version != 0) {
      this.version = archiveMetadataBase.version;
      hasCopy = true;
    }

    if (archiveMetadataBase.createdTime != 0) {
      this.createdTime = archiveMetadataBase.createdTime;
      hasCopy = true;
    }
    if (archiveMetadataBase.updatedTime != 0) {
      this.updatedTime = archiveMetadataBase.updatedTime;
      hasCopy = true;
    }
    if (archiveMetadataBase.updatedBy != null) {
      this.updatedBy = archiveMetadataBase.updatedBy;
      hasCopy = true;
    }
    if (archiveMetadataBase.createdBy != null) {
      this.createdBy = archiveMetadataBase.createdBy;
      hasCopy = true;
    }
    if (archiveMetadataBase.description != null) {
      this.description = archiveMetadataBase.description;
      hasCopy = true;
    }

    if (archiveMetadataBase.status != null) {
      this.status = archiveMetadataBase.status;
      hasCopy = true;
    }

    if (archiveMetadataBase.deviceName != null) {
      this.deviceName = archiveMetadataBase.deviceName;
      hasCopy = true;
    }

    if (archiveMetadataBase.archiveType != null) {
      this.archiveType = archiveMetadataBase.archiveType;
      hasCopy = true;
    }

    if (archiveMetadataBase.logicalSpace != 0) {
      this.logicalSpace = archiveMetadataBase.logicalSpace;
      hasCopy = true;
    }

    if (archiveMetadataBase.fileSystemPartitionName != null) {
      this.fileSystemPartitionName = archiveMetadataBase.fileSystemPartitionName;
      hasCopy = true;
    }

    if (archiveMetadataBase.slotNo != null) {
      this.slotNo = archiveMetadataBase.slotNo;
      hasCopy = true;
    }

    return hasCopy;
  }

  public ArchiveStatus getStatus() {
    return status;
  }

  public void setStatus(ArchiveStatus status) {
    this.status = status;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  public void setStorageType(StorageType archiveType) {
    this.storageType = archiveType;
  }

  public ArchiveType getArchiveType() {
    return archiveType;
  }

  public void setArchiveType(ArchiveType archiveType) {
    this.archiveType = archiveType;
  }

  public String getSlotNo() {
    return slotNo;
  }

  public void setSlotNo(String slotNo) {
    this.slotNo = slotNo;
  }

  @Override
  public String toString() {
    return "ArchiveMetadata{"
        + "archiveId=" + archiveId
        + ", serialNumber='" + serialNumber + '\''
        + ", archiveType='" + archiveType + '\''
        + ", instanceId=" + instanceId
        + ", pageSize=" + pageSize
        + ", deviceName='" + deviceName + '\''
        + ", status=" + status
        + ", storageType=" + storageType
        + ", version=" + version
        + ", archiveType=" + archiveType
        + ", createdTime=" + createdTime
        + ", updatedTime=" + updatedTime
        + ", updatedBy='" + updatedBy + '\''
        + ", createdBy='" + createdBy + '\''
        + ", description='" + description + '\''
        + ", logicalSpace=" + logicalSpace
        + ", fileSystemPartitionName='" + fileSystemPartitionName + '\''
        + ", slotNo='" + slotNo + '\''
        + '}';
  }
}
