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
