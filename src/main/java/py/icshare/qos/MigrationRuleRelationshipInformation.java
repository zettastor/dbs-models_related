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

public class MigrationRuleRelationshipInformation {
  private long relationshipId;
  private long storagePoolId;
  private long ruleId;
  private String status;

  public MigrationRuleRelationshipInformation() {
  }

  public MigrationRuleRelationshipInformation(long relationshipId, long storagePoolId,
      long ruleId) {
    this.relationshipId = relationshipId;
    this.storagePoolId = storagePoolId;
    this.ruleId = ruleId;
  }

  public long getStoragePoolId() {
    return storagePoolId;
  }

  public void setStoragePoolId(long storagePoolId) {
    this.storagePoolId = storagePoolId;
  }

  public long getRuleId() {
    return ruleId;
  }

  public void setRuleId(long ruleId) {
    this.ruleId = ruleId;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public long getRelationshipId() {
    return relationshipId;
  }

  public void setRelationshipId(long relationshipId) {
    this.relationshipId = relationshipId;
  }

  public MigrationRuleRelationship toMigrationRuleRelationship() {
    MigrationRuleRelationship migrationRuleRelationship = new MigrationRuleRelationship();
    migrationRuleRelationship.setStoragePoolId(storagePoolId);
    migrationRuleRelationship.setRelationshipId(relationshipId);
    migrationRuleRelationship.setRuleId(ruleId);
    migrationRuleRelationship.setStatus(MigrationRuleStatusBindingPools.findByName(status));
    return migrationRuleRelationship;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (storagePoolId ^ (storagePoolId >>> 32));
    result = prime * result + (int) (ruleId ^ (ruleId >>> 32));
    result = prime * result + ((status == null) ? 0 : status.hashCode());
    result = prime * result + (int) (relationshipId ^ (relationshipId >>> 32));
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
    MigrationRuleRelationshipInformation other = (MigrationRuleRelationshipInformation) obj;
    if (relationshipId != other.relationshipId) {
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
    if (storagePoolId != other.storagePoolId) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "MigrationRuleRelationshipInformation [relationshipId=" + relationshipId
        + ", storagePoolId=" + storagePoolId
        + ", ruleId=" + ruleId + ", status=" + status + "]";
  }

}
