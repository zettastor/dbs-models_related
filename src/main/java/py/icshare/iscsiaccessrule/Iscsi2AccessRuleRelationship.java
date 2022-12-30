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

package py.icshare.iscsiaccessrule;

import py.driver.DriverType;
import py.icshare.AccessRuleStatusBindingVolume;
import py.icshare.DriverKey;

public class Iscsi2AccessRuleRelationship {
  private long relationshipId;
  private long driverContainerId;
  private long volumeId;
  private int snapshotId;
  private String driverType;
  private long ruleId;
  private AccessRuleStatusBindingVolume status;

  public Iscsi2AccessRuleRelationship() {
  }

  public Iscsi2AccessRuleRelationship(IscsiRuleRelationshipInformation relationshipInfo) {
    this.relationshipId = relationshipInfo.getRelationshipId();
    this.driverContainerId = relationshipInfo.getDriverContainerId();
    this.volumeId = relationshipInfo.getVolumeId();
    this.snapshotId = relationshipInfo.getSnapshotId();
    this.driverType = relationshipInfo.getDriverType();
    this.ruleId = relationshipInfo.getRuleId();
    this.status = AccessRuleStatusBindingVolume.valueOf(relationshipInfo.getStatus());
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

  public String getDriverType() {
    return driverType;
  }

  public void setDriverType(String driverType) {
    this.driverType = driverType;
  }

  public long getRuleId() {
    return ruleId;
  }

  public void setRuleId(long ruleId) {
    this.ruleId = ruleId;
  }

  public AccessRuleStatusBindingVolume getStatus() {
    return status;
  }

  public void setStatus(AccessRuleStatusBindingVolume status) {
    this.status = status;
  }

  public IscsiRuleRelationshipInformation toIscsiRuleRelationshipInformation() {
    IscsiRuleRelationshipInformation iscsiRuleRelationshipInformation = new
        IscsiRuleRelationshipInformation();
    iscsiRuleRelationshipInformation.setRelationshipId(relationshipId);
    iscsiRuleRelationshipInformation.setRuleId(ruleId);
    iscsiRuleRelationshipInformation.setDriverContainerId(driverContainerId);
    iscsiRuleRelationshipInformation.setVolumeId(volumeId);
    iscsiRuleRelationshipInformation.setSnapshotId(snapshotId);
    iscsiRuleRelationshipInformation.setDriverType(driverType);
    iscsiRuleRelationshipInformation.setStatus(status.name());
    return iscsiRuleRelationshipInformation;
  }

  @Override
  public String toString() {
    return "Iscsi2AccessRuleRelationship [relationshipId=" + relationshipId + ", driverKeyInfo="
        + new DriverKey(driverContainerId, volumeId, snapshotId, DriverType.valueOf(driverType))
        + ", ruleId="
        + ruleId + ", status=" + status + "]";
  }
}
