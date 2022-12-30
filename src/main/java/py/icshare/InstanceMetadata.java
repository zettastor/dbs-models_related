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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveMetadata;
import py.archive.RawArchiveMetadata;
import py.instance.Group;
import py.instance.InstanceDomain;
import py.instance.InstanceId;

public class InstanceMetadata {
  private static final Logger logger = LoggerFactory.getLogger(InstanceMetadata.class);

  private InstanceId instanceId;
  // available physical disk space. It is a fixed value, unless disks are broken
  private long capacity;

  // available logic disk spaces for allocating segment units. This value is fixed unless disks are
  // broken
  // The different between available space and capacity is that capcity includes disk spaces that
  // have to be used to
  // store meta data, and can't be used to allocate segment units.
  private long logicalCapacity;

  // free logic disk space for allocating segment units. This value varies accordingly when
  // are created and deleted
  private long freeSpace;

  private String endpoint;

  private List<RawArchiveMetadata> archives;
  private List<ArchiveMetadata> archiveMetadatas;

  private Long lastUpdated;

  private Group group;

  private InstanceDomain instanceDomain;

  private DatanodeStatus datanodeStatus;

  //datanode type:
  //normal datanode: can be used to create normal segment unit or arbiter segment unit(when no
  // enough simple datanode)
  //simple datanode: only can be used to create arbiter segment unit
  private DatanodeType datanodeType;

  public InstanceMetadata(InstanceId instanceId) {
    super();
    this.instanceId = instanceId;
    archives = new ArrayList<RawArchiveMetadata>();
    archiveMetadatas = new ArrayList<ArchiveMetadata>();
    this.instanceDomain = new InstanceDomain();
  }

  public RawArchiveMetadata getArchiveById(Long archiveId) {
    RawArchiveMetadata archive = null;
    Validate.notNull(archiveId);
    for (RawArchiveMetadata archiveTmp : archives) {
      if (archiveTmp.getArchiveId() == archiveId.longValue()) {
        archive = archiveTmp;
      }
    }
    return archive;
  }

  public boolean isFree() {
    return this.instanceDomain.isFree();
  }

  public void setFree() {
    this.instanceDomain.setFree();
  }

  /**
   * Not all freeSpace is available to use. This excludes the reserved spaces.
   *
   */
  public long getCurrentFreeSpace() {
    return this.freeSpace;
  }

  public InstanceId getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(InstanceId instanceId) {
    this.instanceId = instanceId;
  }

  public long getCapacity() {
    return capacity;
  }

  public void setCapacity(long capacity) {
    this.capacity = capacity;
  }

  public long getFreeSpace() {
    return freeSpace;
  }

  public void setFreeSpace(long freeSpace) {
    this.freeSpace = freeSpace;
  }

  public long getLogicalCapacity() {
    return logicalCapacity;
  }

  public void setLogicalCapacity(long logicalCapacity) {
    this.logicalCapacity = logicalCapacity;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public List<RawArchiveMetadata> getArchives() {
    return archives;
  }

  public void setArchives(List<RawArchiveMetadata> archives) {
    this.archives = archives;
  }

  public Long getLastUpdated() {
    return lastUpdated;
  }

  public void setLastUpdated(Long lastUpdated) {
    this.lastUpdated = lastUpdated;
  }

  public Group getGroup() {
    return group;
  }

  public void setGroup(Group group) {
    this.group = group;
  }

  public StorageInformation toStorageInformation() {
    logger.info(
        "toStorageInformation instanceId ={} capacity = {} freeSpace = {} logical "
            + "capacity = {} date:{}",
        instanceId.getId(), capacity, freeSpace, logicalCapacity, new Date());
    Long domainId = (instanceDomain == null ? null : instanceDomain.getDomainId());
    StorageInformation storageInformation = new StorageInformation(instanceId.getId(), capacity,
        freeSpace,
        logicalCapacity, null, domainId);
    storageInformation.setDatanodeType(datanodeType.getValue());
    return storageInformation;
  }

  public List<ArchiveInformation> toArchivesInformation() {
    List<ArchiveInformation> archives = new ArrayList<ArchiveInformation>();

    for (RawArchiveMetadata archive : this.archives) {
      ArchiveInformation archiveInformation = new ArchiveInformation(archive.getArchiveId(),
          instanceId.getId(),
          archive.getStorageType(), archive.getStatus(), archive.getLogicalSpace(),
          archive.getStoragePoolId());
      archiveInformation.setCreatedTime(new Date(archive.getCreatedTime()));
      archiveInformation.setUpdatedTime(new Date(archive.getUpdatedTime()));
      archiveInformation.setSerialNumber(archive.getSerialNumber());
      archiveInformation.setSlotNo(archive.getSlotNo());
      archives.add(archiveInformation);
    }

    return archives;
  }

  @Override
  public String toString() {
    return "InstanceMetadata{"
        + "instanceId=" + instanceId
        + ", capacity=" + capacity
        + ", logicalCapacity=" + logicalCapacity
        + ", freeSpace=" + freeSpace
        + ", endpoint='" + endpoint + '\''
        + ", archives=" + archives
        + ", archiveMetadatas=" + archiveMetadatas
        + ", lastUpdated=" + lastUpdated
        + ", group=" + group
        + ", instanceDomain=" + instanceDomain
        + ", datanodeStatus=" + datanodeStatus
        + ", datanodeType=" + datanodeType
        + '}';
  }

  public List<ArchiveMetadata> getArchiveMetadatas() {
    return archiveMetadatas;
  }

  public void setArchiveMetadatas(List<ArchiveMetadata> archiveMetadatas) {
    this.archiveMetadatas = archiveMetadatas;
  }

  public InstanceDomain getInstanceDomain() {
    return instanceDomain;
  }

  public void setInstanceDomain(InstanceDomain instanceDomain) {
    this.instanceDomain = instanceDomain;
  }

  public Long getDomainId() {
    return this.instanceDomain.getDomainId();
  }

  public void setDomainId(Long domainId) {
    this.instanceDomain.setDomainId(domainId);
  }

  public DatanodeStatus getDatanodeStatus() {
    return datanodeStatus;
  }

  public void setDatanodeStatus(DatanodeStatus datanodeStatus) {
    this.datanodeStatus = datanodeStatus;
  }

  public DatanodeType getDatanodeType() {
    return datanodeType;
  }

  public void setDatanodeType(DatanodeType datanodeType) {
    this.datanodeType = datanodeType;
  }

  public enum DatanodeStatus {
    OK(1),
    UNKNOWN(2),
    SEPARATED(3);

    private final int value;

    DatanodeStatus(int value) {
      this.value = value;
    }

    public static DatanodeStatus findByValue(int value) {
      switch (value) {
        case 1:
          return OK;
        case 2:
          return UNKNOWN;
        case 3:
          return SEPARATED;
        default:
          return null;
      }
    }

    public static DatanodeStatus findByName(String name) {
      if (name == null) {
        return null;
      }

      if (name.equals(OK.name())) {
        return OK;
      } else if (name.equals(UNKNOWN.name())) {
        return UNKNOWN;
      } else if (name.equals(SEPARATED.name())) {
        return SEPARATED;
      } else {
        return null;
      }
    }

    public int getValue() {
      return value;
    }
  }

  public enum DatanodeType {
    NORMAL(1),
    SIMPLE(2);

    private final int value;

    DatanodeType(int value) {
      this.value = value;
    }

    public static DatanodeType findByValue(int value) {
      switch (value) {
        case 1:
          return NORMAL;
        case 2:
          return SIMPLE;
        default:
          return null;
      }
    }

    public static DatanodeType findByName(String name) {
      if (name == null) {
        return null;
      }

      if (name.equals(NORMAL.name())) {
        return NORMAL;
      } else if (name.equals(SIMPLE.name())) {
        return SIMPLE;
      } else {
        return null;
      }
    }

    public int getValue() {
      return value;
    }
  }
}
