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
