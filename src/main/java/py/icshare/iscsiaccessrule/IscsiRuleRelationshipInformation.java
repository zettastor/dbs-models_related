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

public class IscsiRuleRelationshipInformation {
  private long relationshipId;
  private long driverContainerId;
  private long volumeId;
  private int snapshotId;
  private String driverType;
  private long ruleId;
  private String status;

  public IscsiRuleRelationshipInformation() {
  }

  public IscsiRuleRelationshipInformation(long relationshipId, long driverContainerId,
      long volumeId,
      int snapshotId, String driverType, long ruleId) {
    this.relationshipId = relationshipId;
    this.driverContainerId = driverContainerId;
    this.volumeId = volumeId;
    this.snapshotId = snapshotId;
    this.driverType = driverType;
    this.ruleId = ruleId;
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

  public long getRelationshipId() {
    return relationshipId;
  }

  public void setRelationshipId(long relationshipId) {
    this.relationshipId = relationshipId;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public Iscsi2AccessRuleRelationship toIscsi2AccessRuleRelationship() {
    Iscsi2AccessRuleRelationship iscsi2Rule = new Iscsi2AccessRuleRelationship();
    iscsi2Rule.setRelationshipId(relationshipId);
    iscsi2Rule.setDriverContainerId(driverContainerId);
    iscsi2Rule.setVolumeId(volumeId);
    iscsi2Rule.setSnapshotId(snapshotId);
    iscsi2Rule.setDriverType(driverType);
    iscsi2Rule.setRuleId(ruleId);
    iscsi2Rule.setStatus(AccessRuleStatusBindingVolume.findByName(status));
    return iscsi2Rule;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (relationshipId ^ (relationshipId >>> 32));
    result = prime * result + (int) (driverContainerId ^ (driverContainerId >>> 32));
    result = prime * result + (int) (volumeId ^ (volumeId >>> 32));
    result = prime * result + (int) (snapshotId ^ (snapshotId >>> 32));
    result = prime * result + ((driverType == null) ? 0 : driverType.hashCode());
    result = prime * result + (int) (snapshotId ^ (snapshotId >>> 32));
    result = prime * result + ((status == null) ? 0 : status.hashCode());
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
    py.icshare.iscsiaccessrule.IscsiRuleRelationshipInformation other =
        (py.icshare.iscsiaccessrule.IscsiRuleRelationshipInformation) obj;
    if (relationshipId != other.relationshipId) {
      return false;
    }
    if (driverContainerId != other.driverContainerId) {
      return false;
    }
    if (volumeId != other.volumeId) {
      return false;
    }
    if (snapshotId != other.snapshotId) {
      return false;
    }
    if (driverType != other.driverType) {
      return false;
    }
    if (ruleId != other.ruleId) {
      return false;
    }
    if (status == null) {
      if (other.status != null) {
        return false;
      }
    } else if (!status.equals(other.status)) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return "IscsiRuleRelationshipInformation [relationshipId=" + relationshipId + ", driverKeyInfo="
        + new DriverKey(driverContainerId, volumeId, snapshotId, DriverType.valueOf(driverType))
        + ", ruleId=" + ruleId + ", status=" + status + "]";
  }

}
