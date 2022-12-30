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
