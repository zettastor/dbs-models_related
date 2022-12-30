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

import py.common.RequestIdBuilder;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

public class VolumeCreationRequest {
  private long rootVolumeId;
  private long volumeId;
  private String name;
  private long volumeSize;
  private String volumeType;
  private long segmentSize;
  private long accountId;
  private long createdAt;
  private long domainId;
  private long storagePoolId;

  private String status;
  private RequestType requestType;
  private String requestTypeString;
  // for clone volume 's creation
  private long srcVolumeId;
  private boolean enableLaunchMultiDrivers;
  private int totalSegmentCount;
  private String volumeDescription;

  public VolumeCreationRequest() {
  }

  // Can be used to create a volume, i.e., generate a random volume id.

  public VolumeCreationRequest(long volumeId, long volumeSize, VolumeType volumeType,
      long accountId, boolean enableLaunchMultiDrivers) {
    // rootVolumeId is set to 0
    if (volumeId == 0) {
      this.volumeId = RequestIdBuilder.get();
    } else {
      this.volumeId = volumeId;
    }
    constructorFunc(this.volumeId, this.volumeId, volumeSize, volumeType, accountId,
        enableLaunchMultiDrivers);
  }

  // Can be used to extend a volume.

  public VolumeCreationRequest(long rootVolumeId, long volumeId, long volumeSize,
      VolumeType volumeType, long accountId,
      boolean enableLaunchMultiDrivers) {
    constructorFunc(rootVolumeId, volumeId, volumeSize, volumeType, accountId,
        enableLaunchMultiDrivers);
  }

  private void constructorFunc(long rootVolumeId, long volumeId, long volumeSize,
      VolumeType volumeType,
      long accountId,
      boolean enableLaunchMultiDrivers) {
    this.setRootVolumeId(rootVolumeId);
    this.volumeId = volumeId;
    this.volumeSize = volumeSize;
    this.volumeType = volumeType.name();
    this.accountId = accountId;
    this.createdAt = System.currentTimeMillis();
    this.status = VolumeStatus.ToBeCreated.name();
    this.enableLaunchMultiDrivers = enableLaunchMultiDrivers;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public long getVolumeSize() {
    return volumeSize;
  }

  public void setVolumeSize(long volumeSize) {
    this.volumeSize = volumeSize;
  }

  public long getRootVolumeId() {
    return rootVolumeId;
  }

  public void setRootVolumeId(long parentVolumeId) {
    this.rootVolumeId = parentVolumeId;
  }

  public String getVolumeType() {
    return volumeType;
  }

  public void setVolumeType(String volumeType) {
    this.volumeType = volumeType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
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

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public long getDomainId() {
    return domainId;
  }

  public void setDomainId(long domainId) {
    this.domainId = domainId;
  }

  public long getSrcVolumeId() {
    return srcVolumeId;
  }

  public void setSrcVolumeId(long srcVolumeId) {
    this.srcVolumeId = srcVolumeId;
  }

  public RequestType getRequestType() {
    return requestType;
  }

  public void setRequestType(VolumeCreationRequest.RequestType requestType) {
    this.requestType = requestType;
    this.requestTypeString = requestType.name();
  }

  public String getRequestTypeString() {
    return requestTypeString;
  }

  public void setRequestTypeString(String requestTypeString) {
    this.requestTypeString = requestTypeString;
    this.requestType = RequestType.valueOf(requestTypeString);
  }

  public long getStoragePoolId() {
    return storagePoolId;
  }

  public void setStoragePoolId(long storagePoolId) {
    this.storagePoolId = storagePoolId;
  }

  public boolean isEnableLaunchMultiDrivers() {
    return enableLaunchMultiDrivers;
  }

  public void setEnableLaunchMultiDrivers(boolean enableLaunchMultiDrivers) {
    this.enableLaunchMultiDrivers = enableLaunchMultiDrivers;
  }

  public int getTotalSegmentCount() {
    return totalSegmentCount;
  }

  public void setTotalSegmentCount(int totalSegmentCount) {
    this.totalSegmentCount = totalSegmentCount;
  }

  public String getVolumeDescription() {
    return volumeDescription;
  }

  public void setVolumeDescription(String volumeDescription) {
    this.volumeDescription = volumeDescription;
  }

  @Override
  public String toString() {
    return "VolumeCreationRequest{"
        + "rootVolumeId=" + rootVolumeId
        + ", volumeId=" + volumeId
        + ", name='" + name + '\''
        + ", volumeSize=" + volumeSize
        + ", volumeType='" + volumeType + '\''
        + ", segmentSize=" + segmentSize
        + ", accountId=" + accountId
        + ", createdAt=" + createdAt
        + ", domainId=" + domainId
        + ", storagePoolId=" + storagePoolId
        + ", status='" + status + '\''
        + ", requestType=" + requestType
        + ", requestTypeString='" + requestTypeString + '\''
        + ", srcVolumeId=" + srcVolumeId
        + ", enableLaunchMultiDrivers=" + enableLaunchMultiDrivers
        + ", totalSegmentCount=" + totalSegmentCount
        + ", volumeDescription=" + volumeDescription
        + '}';
  }

  /**
   * Mark the Create Request type, for common volume creation or clone volume creation.
   */
  public enum RequestType {
    CREATE_VOLUME, EXTEND_VOLUME
  }
}
