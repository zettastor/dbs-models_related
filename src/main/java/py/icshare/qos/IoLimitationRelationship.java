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

package py.icshare.qos;

import py.driver.DriverType;
import py.icshare.DriverKey;

public class IoLimitationRelationship {
  private long relationshipId;
  private long driverContainerId;
  private long volumeId;
  private int snapshotId;
  private DriverType driverType;
  private long ruleId;
  private IoLimitationStatusBindingDrivers status;

  public IoLimitationRelationship() {
  }

  public IoLimitationRelationship(IoLimitationRelationshipInformation relationshipInfo) {
    this.relationshipId = relationshipInfo.getRelationshipId();
    this.driverContainerId = relationshipInfo.getDriverContainerId();
    this.volumeId = relationshipInfo.getVolumeId();
    this.snapshotId = relationshipInfo.getSnapshotId();
    this.driverType = DriverType.findByName(relationshipInfo.getDriverType());
    this.ruleId = relationshipInfo.getRuleId();
    this.status = IoLimitationStatusBindingDrivers.valueOf(relationshipInfo.getStatus());
  }

  public long getRelationshipId() {
    return relationshipId;
  }

  public void setRelationshipId(long relationshipId) {
    this.relationshipId = relationshipId;
  }

  public long getDriverContainerId() {
    return driverContainerId;
  }

  public void setDriverContainerId(long driverContainerId) {
    this.driverContainerId = driverContainerId;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public int getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(int snapshotId) {
    this.snapshotId = snapshotId;
  }

  public DriverType getDriverType() {
    return driverType;
  }

  public void setDriverType(DriverType driverType) {
    this.driverType = driverType;
  }

  public long getRuleId() {
    return ruleId;
  }

  public void setRuleId(long ruleId) {
    this.ruleId = ruleId;
  }

  public IoLimitationStatusBindingDrivers getStatus() {
    return status;
  }

  public void setStatus(IoLimitationStatusBindingDrivers status) {
    this.status = status;
  }

  public IoLimitationRelationshipInformation toIoLimitationRelationshipInformation() {
    IoLimitationRelationshipInformation ioLimitationRelationshipInformation =
        new IoLimitationRelationshipInformation();
    ioLimitationRelationshipInformation.setRelationshipId(relationshipId);
    ioLimitationRelationshipInformation.setRuleId(ruleId);
    ioLimitationRelationshipInformation.setDriverContainerId(driverContainerId);
    ioLimitationRelationshipInformation.setVolumeId(volumeId);
    ioLimitationRelationshipInformation.setSnapshotId(snapshotId);
    ioLimitationRelationshipInformation.setDriverType(driverType.name());
    ioLimitationRelationshipInformation.setStatus(status.name());

    return ioLimitationRelationshipInformation;
  }

  @Override
  public String toString() {
    return "MigrationRuleRelationship [relationshipId=" + relationshipId + ", driverKeyInfo="
        + new DriverKey(driverContainerId, volumeId, snapshotId, driverType) + ", ruleId="
        + ruleId + ", status=" + status + "]";
  }
}
