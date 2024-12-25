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

import py.informationcenter.AccessPermissionType;
import py.informationcenter.AccessRuleStatus;

public class IscsiAccessRule {
  private long ruleId;

  private String ruleNotes;

  private String initiatorName;

  private String user;

  private String passed;

  private String outUser;

  private String outPassed;

  private AccessPermissionType permission;

  private AccessRuleStatus status;

  public IscsiAccessRule() {
  }

  public IscsiAccessRule(IscsiAccessRuleInformation iscsiAccessRuleInformation) {
    this.ruleId = iscsiAccessRuleInformation.getRuleId();
    this.ruleNotes = iscsiAccessRuleInformation.getRuleNotes();
    this.initiatorName = iscsiAccessRuleInformation.getInitiatorName();
    this.user = iscsiAccessRuleInformation.getUser();
    this.passed = iscsiAccessRuleInformation.getPassed();
    this.outUser = iscsiAccessRuleInformation.getOutUser();
    this.outPassed = iscsiAccessRuleInformation.getOutPassed();
    this.permission = AccessPermissionType.findByValue(iscsiAccessRuleInformation.getPermission());
    this.status = AccessRuleStatus.valueOf(iscsiAccessRuleInformation.getStatus());
  }

  public long getRuleId() {
    return ruleId;
  }

  public void setRuleId(long ruleId) {
    this.ruleId = ruleId;
  }

  public String getRuleNotes() {
    return ruleNotes;
  }

  public void setRuleNotes(String ruleNotes) {
    this.ruleNotes = ruleNotes;
  }

  public AccessRuleStatus getStatus() {
    return status;
  }

  public void setStatus(AccessRuleStatus status) {
    this.status = status;
  }

  public String getInitiatorName() {
    return initiatorName;
  }

  public void setInitiatorName(String initiatorName) {
    this.initiatorName = initiatorName;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassed() {
    return passed;
  }

  public void setPassed(String passed) {
    this.passed = passed;
  }

  public String getOutUser() {
    return outUser;
  }

  public void setOutUser(String outUser) {
    this.outUser = outUser;
  }

  public String getOutPassed() {
    return outPassed;
  }

  public void setOutPassed(String outPassed) {
    this.outPassed = outPassed;
  }

  public AccessPermissionType getPermission() {
    return permission;
  }

  public void setPermission(AccessPermissionType permission) {
    this.permission = permission;
  }

  public IscsiAccessRuleInformation toIscsiAccessRuleInformation() {
    IscsiAccessRuleInformation iscsiAccessRuleInformation = new IscsiAccessRuleInformation();
    iscsiAccessRuleInformation.setRuleId(ruleId);
    iscsiAccessRuleInformation.setRuleNotes(ruleNotes);
    iscsiAccessRuleInformation.setInitiatorName(initiatorName);
    iscsiAccessRuleInformation.setUser(user);
    iscsiAccessRuleInformation.setPassed(passed);
    iscsiAccessRuleInformation.setOutUser(outUser);
    iscsiAccessRuleInformation.setOutPassed(outPassed);
    iscsiAccessRuleInformation.setPermission(permission.getValue());
    iscsiAccessRuleInformation.setStatus(status.name());
    return iscsiAccessRuleInformation;
  }

  @Override
  public String toString() {
    return "IscsiAccessRule [ruleId=" + ruleId + ", ruleNotes=" + ruleNotes + ", initiatorName="
        + initiatorName + ", user=" + user
        + ", passed=" + passed + ", outUser=" + outUser
        + ", outPassed=" + outPassed + ", permission=" + permission + ", status=" + status + "]";
  }
}
