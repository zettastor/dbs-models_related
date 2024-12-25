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
