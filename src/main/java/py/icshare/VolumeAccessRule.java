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

import py.informationcenter.AccessPermissionType;
import py.informationcenter.AccessRuleStatus;

/**
 * A class collects features of volume access rule.
 *
 */
public class VolumeAccessRule {
  private long ruleId;

  private String incommingHostName;

  private AccessPermissionType permission;

  private AccessRuleStatus status;

  public VolumeAccessRule() {
  }

  public VolumeAccessRule(AccessRuleInformation accessRuleInformation) {
    this.ruleId = accessRuleInformation.getRuleId();
    this.incommingHostName = accessRuleInformation.getIpAddress();
    this.permission = AccessPermissionType.findByValue(accessRuleInformation.getPermission());
    this.status = AccessRuleStatus.valueOf(accessRuleInformation.getStatus());
  }

  public long getRuleId() {
    return ruleId;
  }

  public void setRuleId(long ruleId) {
    this.ruleId = ruleId;
  }

  public String getIncommingHostName() {
    return incommingHostName;
  }

  public void setIncommingHostName(String incommingHostName) {
    this.incommingHostName = incommingHostName;
  }

  public AccessPermissionType getPermission() {
    return permission;
  }

  public void setPermission(AccessPermissionType permission) {
    this.permission = permission;
  }

  public AccessRuleStatus getStatus() {
    return status;
  }

  public void setStatus(AccessRuleStatus status) {
    this.status = status;
  }

  public AccessRuleInformation toAccessRuleInformation() {
    AccessRuleInformation accessRuleInformation = new AccessRuleInformation();
    accessRuleInformation.setRuleId(ruleId);
    accessRuleInformation.setIpAddress(incommingHostName);
    accessRuleInformation.setPermission(permission.getValue());
    accessRuleInformation.setStatus(status.name());
    return accessRuleInformation;
  }

  @Override
  public String toString() {
    return "VolumeAccessRule [ruleId=" + ruleId + ", incommingHostName=" + incommingHostName
        + ", permission="
        + permission + ", status=" + status + "]";
  }
}
