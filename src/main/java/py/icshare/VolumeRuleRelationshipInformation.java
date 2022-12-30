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

public class VolumeRuleRelationshipInformation {
  private long relationshipId;
  private long volumeId;
  private long ruleId;
  private String status;

  public VolumeRuleRelationshipInformation() {
  }

  public VolumeRuleRelationshipInformation(long relationshipId, long volumeId, long ruleId) {
    this.relationshipId = relationshipId;
    this.volumeId = volumeId;
    this.ruleId = ruleId;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
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

  public Volume2AccessRuleRelationship toVolume2AccessRuleRelationship() {
    Volume2AccessRuleRelationship volume2Rule = new Volume2AccessRuleRelationship();
    volume2Rule.setVolumeId(volumeId);
    volume2Rule.setRelationshipId(relationshipId);
    volume2Rule.setRuleId(ruleId);
    volume2Rule.setStatus(AccessRuleStatusBindingVolume.findByName(status));

    return volume2Rule;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (relationshipId ^ (relationshipId >>> 32));
    result = prime * result + (int) (ruleId ^ (ruleId >>> 32));
    result = prime * result + ((status == null) ? 0 : status.hashCode());
    result = prime * result + (int) (volumeId ^ (volumeId >>> 32));
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
    VolumeRuleRelationshipInformation other = (VolumeRuleRelationshipInformation) obj;
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
    if (volumeId != other.volumeId) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "VolumeRuleRelationshipInformation [relationshipId=" + relationshipId + ", volumeId="
        + volumeId
        + ", ruleId=" + ruleId + ", status=" + status + "]";
  }

}
