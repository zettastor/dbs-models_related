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

/**
 * A class which represent relationship between access rules and volumes.
 */
public class Volume2AccessRuleRelationship {
  private long relationshipId;
  private long volumeId;
  private long ruleId;
  private AccessRuleStatusBindingVolume status;

  public Volume2AccessRuleRelationship() {
  }

  public Volume2AccessRuleRelationship(VolumeRuleRelationshipInformation relationshipInfo) {
    this.relationshipId = relationshipInfo.getRelationshipId();
    this.volumeId = relationshipInfo.getVolumeId();
    this.ruleId = relationshipInfo.getRuleId();
    this.status = AccessRuleStatusBindingVolume.valueOf(relationshipInfo.getStatus());
  }

  public long getRelationshipId() {
    return relationshipId;
  }

  public void setRelationshipId(long relationshipId) {
    this.relationshipId = relationshipId;
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

  public AccessRuleStatusBindingVolume getStatus() {
    return status;
  }

  public void setStatus(AccessRuleStatusBindingVolume status) {
    this.status = status;
  }

  public VolumeRuleRelationshipInformation toVolumeRuleRelationshipInformation() {
    VolumeRuleRelationshipInformation volumeRuleRelationshipInformation =
        new VolumeRuleRelationshipInformation();
    volumeRuleRelationshipInformation.setRelationshipId(relationshipId);
    volumeRuleRelationshipInformation.setRuleId(ruleId);
    volumeRuleRelationshipInformation.setVolumeId(volumeId);
    volumeRuleRelationshipInformation.setStatus(status.name());

    return volumeRuleRelationshipInformation;
  }

  @Override
  public String toString() {
    return "Volume2AccessRuleRelationship [relationshipId=" + relationshipId + ", volumeId="
        + volumeId
        + ", ruleId=" + ruleId + ", status=" + status + "]";
  }

}
