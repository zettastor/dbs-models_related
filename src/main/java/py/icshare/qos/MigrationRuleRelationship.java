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

public class MigrationRuleRelationship {
  private long relationshipId;
  private long storagePoolId;
  private long ruleId;
  private MigrationRuleStatusBindingPools status;

  public MigrationRuleRelationship() {
  }

  public MigrationRuleRelationship(MigrationRuleRelationshipInformation relationshipInfo) {
    this.relationshipId = relationshipInfo.getRelationshipId();
    this.storagePoolId = relationshipInfo.getStoragePoolId();
    this.ruleId = relationshipInfo.getRuleId();
    this.status = MigrationRuleStatusBindingPools.valueOf(relationshipInfo.getStatus());
  }

  public long getRelationshipId() {
    return relationshipId;
  }

  public void setRelationshipId(long relationshipId) {
    this.relationshipId = relationshipId;
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

  public MigrationRuleStatusBindingPools getStatus() {
    return status;
  }

  public void setStatus(MigrationRuleStatusBindingPools status) {
    this.status = status;
  }

  public MigrationRuleRelationshipInformation toMigrationSpeedRelationshipInformation() {
    MigrationRuleRelationshipInformation migrationRuleRelationshipInformation =
        new MigrationRuleRelationshipInformation();
    migrationRuleRelationshipInformation.setRelationshipId(relationshipId);
    migrationRuleRelationshipInformation.setRuleId(ruleId);
    migrationRuleRelationshipInformation.setStoragePoolId(storagePoolId);
    migrationRuleRelationshipInformation.setStatus(status.name());

    return migrationRuleRelationshipInformation;
  }

  @Override
  public String toString() {
    return "MigrationRuleRelationship [relationshipId=" + relationshipId + ", storagePoolId="
        + storagePoolId
        + ", ruleId=" + ruleId + ", status=" + status + "]";
  }
}
