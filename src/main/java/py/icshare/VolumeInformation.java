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

import java.util.Date;

public class VolumeInformation {
  private long volumeId;
  private long rootVolumeId;
  private Long childVolumeId;
  private long volumeSize;
  private long extendingSize;
  private String name;
  private String volumeType;
  private String volumeStatus;
  private long accountId;
  private long segmentSize;
  private String tagKey;
  private String tagValue;
  private Long deadTime;
  private Long domainId;
  private Long storagePoolId;
  private String volumeLayout;

  private String freeSpaceRatio;
  private Date volumeCreatedTime;
  private Date lastExtendedTime;
  private String volumeSource;
  private String readWrite;
  private String inAction;

  private int pageWrappCount;
  private int segmentWrappCount;
  private boolean enableLaunchMultiDrivers;

  private long rebalanceVersion;
  private String eachTimeExtendVolumeSize;
  private boolean markDelete;

  private long clientLastConnectTime;
  private String volumeDescription;

  public VolumeInformation() {
  }

  public long getRebalanceVersion() {
    return rebalanceVersion;
  }

  public void setRebalanceVersion(long rebalanceVersion) {
    this.rebalanceVersion = rebalanceVersion;
  }

  public Long getDeadTime() {
    return deadTime;
  }

  public void setDeadTime(Long deadTime) {
    this.deadTime = deadTime;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public long getRootVolumeId() {
    return rootVolumeId;
  }

  public void setRootVolumeId(long rootVolumeId) {
    this.rootVolumeId = rootVolumeId;
  }

  public Long getChildVolumeId() {
    return childVolumeId;
  }

  public void setChildVolumeId(Long childVolumeId) {
    this.childVolumeId = childVolumeId;
  }

  public long getVolumeSize() {
    return volumeSize;
  }

  public void setVolumeSize(long volumeSize) {
    this.volumeSize = volumeSize;
  }

  public long getExtendingSize() {
    return extendingSize;
  }

  public void setExtendingSize(long extendingSize) {
    this.extendingSize = extendingSize;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getVolumeType() {
    return volumeType;
  }

  public void setVolumeType(String volumeType) {
    this.volumeType = volumeType;
  }

  public String getVolumeStatus() {
    return volumeStatus;
  }

  public void setVolumeStatus(String volumeStatus) {
    this.volumeStatus = volumeStatus;
  }

  public long getAccountId() {
    return accountId;
  }

  public void setAccountId(long accountId) {
    this.accountId = accountId;
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  public void setSegmentSize(long segmentSize) {
    this.segmentSize = segmentSize;
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

  public String getEachTimeExtendVolumeSize() {
    return eachTimeExtendVolumeSize;
  }

  public void setEachTimeExtendVolumeSize(String eachTimeExtendVolumeSize) {
    this.eachTimeExtendVolumeSize = eachTimeExtendVolumeSize;
  }

  public String getVolumeDescription() {
    return volumeDescription;
  }

  public void setVolumeDescription(String volumeDescription) {
    this.volumeDescription = volumeDescription;
  }

  public long getClientLastConnectTime() {
    return clientLastConnectTime;
  }

  public void setClientLastConnectTime(long clientLastConnectTime) {
    this.clientLastConnectTime = clientLastConnectTime;
  }

  public boolean equals(VolumeInformation volumeInformation) {
    if (this == volumeInformation) {
      return true;
    }
    if (volumeInformation == null) {
      return false;
    }
    if (getClass() != volumeInformation.getClass()) {
      return false;
    }

    if ((volumeId != volumeInformation.getVolumeId())
        || (rootVolumeId != volumeInformation.getRootVolumeId())
        || (volumeSize != volumeInformation.getVolumeSize())
        || (extendingSize != volumeInformation.getExtendingSize())
        || (accountId != volumeInformation.getAccountId())
        || (segmentSize != volumeInformation.getSegmentSize())
        || (enableLaunchMultiDrivers != volumeInformation.isEnableLaunchMultiDrivers())
        || (clientLastConnectTime != volumeInformation.getClientLastConnectTime())
        || (markDelete != volumeInformation.isMarkDelete())
    ) {
      return false;
    }

    // volumeStatus
    if (volumeStatus == null) {
      if (volumeInformation.getVolumeStatus() != null) {
        return false;
      }
    } else {
      if (!volumeStatus.equals(volumeInformation.getVolumeStatus())) {
        return false;
      }
    }

    // ChildVolumeId
    if (childVolumeId == null) {
      if (volumeInformation.getChildVolumeId() != null) {
        return false;
      }
    } else {
      if (!childVolumeId.equals(volumeInformation.getChildVolumeId())) {
        return false;
      }
    }
    // name
    if (name == null) {
      if (volumeInformation.getName() != null) {
        return false;
      }
    } else {
      if (!name.equals(volumeInformation.getName())) {
        return false;
      }
    }
    // VolumeType
    if (volumeType == null) {
      if (volumeInformation.getVolumeType() != null) {
        return false;
      }
    } else {
      if (!volumeType.equals(volumeInformation.getVolumeType())) {
        return false;
      }
    }
    // tagKey
    if (tagKey == null) {
      if (volumeInformation.getTagKey() != null) {
        return false;
      }
    } else {
      if (!tagKey.equals(volumeInformation.getTagKey())) {
        return false;
      }
    }
    // tagValue
    if (tagValue == null) {
      if (volumeInformation.getTagValue() != null) {
        return false;
      }
    } else {
      if (!tagValue.equals(volumeInformation.getTagValue())) {
        return false;
      }
    }
    // DeadTime
    if (deadTime == null) {
      if (volumeInformation.getDeadTime() != null) {
        return false;
      }
    } else {
      if (!deadTime.equals(volumeInformation.getDeadTime())) {
        return false;
      }
    }

    // domainId
    if (domainId == null) {
      if (volumeInformation.getDomainId() != null) {
        return false;
      }
    } else {
      if (!domainId.equals(volumeInformation.getDomainId())) {
        return false;
      }
    }

    // volumeCreatedTime
    if (volumeCreatedTime == null) {
      if (volumeInformation.getVolumeCreatedTime() != null) {
        return false;
      }
    } else {
      if (!volumeCreatedTime.equals(volumeInformation.getVolumeCreatedTime())) {
        return false;
      }
    }

    // lastExtendedTime
    if (lastExtendedTime == null) {
      if (volumeInformation.getLastExtendedTime() != null) {
        return false;
      }
    } else {
      if (!lastExtendedTime.equals(volumeInformation.getLastExtendedTime())) {
        return false;
      }
    }

    // volumeSource
    if (volumeSource == null) {
      if (volumeInformation.getVolumeSource() != null) {
        return false;
      }
    } else {
      if (!volumeSource.equals(volumeInformation.getVolumeSource())) {
        return false;
      }
    }

    // readWrite
    if (readWrite == null) {
      if (volumeInformation.getReadWrite() != null) {
        return false;
      }
    } else {
      if (!readWrite.equals(volumeInformation.getReadWrite())) {
        return false;
      }
    }

    // inAction
    if (inAction == null) {
      if (volumeInformation.getInAction() != null) {
        return false;
      }
    } else {
      if (!inAction.equals(volumeInformation.getInAction())) {
        return false;
      }
    }

    // eachTimeExtendVolumeSize
    if (eachTimeExtendVolumeSize == null) {
      if (volumeInformation.getEachTimeExtendVolumeSize() != null) {
        return false;
      }
    } else {
      if (!eachTimeExtendVolumeSize.equals(volumeInformation.getEachTimeExtendVolumeSize())) {
        return false;
      }
    }

    // volumeDescription
    if (volumeDescription == null) {
      if (volumeInformation.getVolumeDescription() != null) {
        return false;
      }
    } else {
      if (!volumeDescription.equals(volumeInformation.getVolumeDescription())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return "VolumeInformation{"
        + "volumeId=" + volumeId
        + ", rootVolumeId=" + rootVolumeId
        + ", childVolumeId=" + childVolumeId
        + ", volumeSize=" + volumeSize
        + ", extendingSize=" + extendingSize
        + ", name='" + name + '\''
        + ", volumeType='" + volumeType + '\''
        + ", volumeStatus='" + volumeStatus + '\''
        + ", accountId=" + accountId
        + ", segmentSize=" + segmentSize
        + ", tagKey='" + tagKey + '\''
        + ", tagValue='" + tagValue + '\''
        + ", deadTime=" + deadTime
        + ", domainId=" + domainId
        + ", storagePoolId=" + storagePoolId
        + ", volumeLayout='" + volumeLayout + '\''
        + ", freeSpaceRatio='" + freeSpaceRatio + '\''
        + ", volumeCreatedTime=" + volumeCreatedTime
        + ", lastExtendedTime=" + lastExtendedTime
        + ", volumeSource='" + volumeSource + '\''
        + ", readWrite='" + readWrite + '\''
        + ", inAction='" + inAction + '\''
        + ", pageWrappCount=" + pageWrappCount
        + ", segmentWrappCount=" + segmentWrappCount
        + ", enableLaunchMultiDrivers=" + enableLaunchMultiDrivers
        + ", rebalanceVersion=" + rebalanceVersion
        + ", eachTimeExtendVolumeSize='" + eachTimeExtendVolumeSize + '\''
        + ", markDelete=" + markDelete
        + ", volumeDescription=" + volumeDescription
        + ", clientLastConnectTime=" + clientLastConnectTime
        + '}';
  }

  public Long getDomainId() {
    return domainId;
  }

  public void setDomainId(Long domainId) {
    this.domainId = domainId;
  }

  public Long getStoragePoolId() {
    return storagePoolId;
  }

  public void setStoragePoolId(Long storagePoolId) {
    this.storagePoolId = storagePoolId;
  }

  public String getVolumeLayout() {
    return volumeLayout;
  }

  public void setVolumeLayout(String volumeLayout) {
    this.volumeLayout = volumeLayout;
  }

  public String getFreeSpaceRatio() {
    return freeSpaceRatio;
  }

  public void setFreeSpaceRatio(String freeSpaceRatio) {
    this.freeSpaceRatio = freeSpaceRatio;
  }

  public Date getVolumeCreatedTime() {
    return volumeCreatedTime;
  }

  public void setVolumeCreatedTime(Date volumeCreatedTime) {
    this.volumeCreatedTime = volumeCreatedTime;
  }

  public Date getLastExtendedTime() {
    return lastExtendedTime;
  }

  public void setLastExtendedTime(Date lastExtendedTime) {
    this.lastExtendedTime = lastExtendedTime;
  }

  public String getVolumeSource() {
    return volumeSource;
  }

  public void setVolumeSource(String volumeSource) {
    this.volumeSource = volumeSource;
  }

  public String getReadWrite() {
    return readWrite;
  }

  public void setReadWrite(String readWrite) {
    this.readWrite = readWrite;
  }

  public int getPageWrappCount() {
    return pageWrappCount;
  }

  public void setPageWrappCount(int pageWrappCount) {
    this.pageWrappCount = pageWrappCount;
  }

  public int getSegmentWrappCount() {
    return segmentWrappCount;
  }

  public void setSegmentWrappCount(int segmentWrappCount) {
    this.segmentWrappCount = segmentWrappCount;
  }

  public String getInAction() {
    return inAction;
  }

  public void setInAction(String inAction) {
    this.inAction = inAction;
  }

  public boolean isEnableLaunchMultiDrivers() {
    return enableLaunchMultiDrivers;
  }

  public void setEnableLaunchMultiDrivers(boolean enableLaunchMultiDrivers) {
    this.enableLaunchMultiDrivers = enableLaunchMultiDrivers;
  }

  public boolean isMarkDelete() {
    return markDelete;
  }

  public void setMarkDelete(boolean markDelete) {
    this.markDelete = markDelete;
  }
}
