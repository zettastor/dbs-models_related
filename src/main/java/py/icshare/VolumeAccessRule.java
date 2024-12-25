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
