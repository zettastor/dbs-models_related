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

package py.icshare.iscsiaccessrule;

import py.informationcenter.AccessPermissionType;
import py.informationcenter.AccessRuleStatus;

/**
 * A class which is used to get iscsi access rule records from db or save them to it.
 */
public class IscsiAccessRuleInformation {
  private long ruleId;
  private String ruleNotes;
  private String initiatorName;
  private String user;
  private String passed;
  private String outUser;
  private String outPassed;
  private int permission;
  private String status;

  public IscsiAccessRuleInformation() {
  }

  public IscsiAccessRuleInformation(long ruleId, String ruleNotes, String initiatorName,
      String user, String passwd,
      String outUser, String outPasswd, int permission) {
    this.ruleId = ruleId;
    this.ruleNotes = ruleNotes;
    this.initiatorName = initiatorName;
    this.user = user;
    this.passed = passwd;
    this.outPassed = outPasswd;
    this.outUser = outUser;
    this.permission = permission;
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

  public int getPermission() {
    return permission;
  }

  public void setPermission(int permission) {
    this.permission = permission;
  }

  public AccessPermissionType permission() {
    return AccessPermissionType.findByValue(permission);
  }

  public void permission(AccessPermissionType permission) {
    if (permission != null) {
      this.permission = permission.getValue();
    }
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
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

  public IscsiAccessRule toIscsiAccessRule() {
    IscsiAccessRule accessRule = new IscsiAccessRule();
    accessRule.setRuleId(ruleId);
    accessRule.setRuleNotes(ruleNotes);
    accessRule.setInitiatorName(initiatorName);
    accessRule.setUser(user);
    accessRule.setPassed(passed);
    accessRule.setOutUser(outUser);
    accessRule.setOutPassed(outPassed);
    accessRule.setPermission(AccessPermissionType.findByValue(permission));
    accessRule.setStatus(AccessRuleStatus.findByName(status));
    return accessRule;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((ruleNotes == null) ? 0 : ruleNotes.hashCode());
    result = prime * result + ((initiatorName == null) ? 0 : initiatorName.hashCode());
    result = prime * result + ((user == null) ? 0 : user.hashCode());
    result = prime * result + ((passed == null) ? 0 : passed.hashCode());
    result = prime * result + ((outUser == null) ? 0 : outUser.hashCode());
    result = prime * result + ((outPassed == null) ? 0 : outPassed.hashCode());
    result = prime * result + permission;
    result = prime * result + (int) (ruleId ^ (ruleId >>> 32));
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
    py.icshare.iscsiaccessrule.IscsiAccessRuleInformation other =
        (py.icshare.iscsiaccessrule.IscsiAccessRuleInformation) obj;

    if (ruleNotes == null) {
      if (other.ruleNotes != null) {
        return false;
      }
    } else if (!ruleNotes.equals(other.ruleNotes)) {
      return false;
    }

    if (initiatorName == null) {
      if (other.initiatorName != null) {
        return false;
      }
    } else if (!initiatorName.equals(other.initiatorName)) {
      return false;
    }
    if (user == null) {
      if (other.user != null) {
        return false;
      }
    } else if (!user.equals(other.user)) {
      return false;
    }

    if (passed == null) {
      if (other.passed != null) {
        return false;
      }
    } else if (!passed.equals(other.passed)) {
      return false;
    }

    if (outUser == null) {
      if (other.outUser != null) {
        return false;
      }
    } else if (!outUser.equals(other.outUser)) {
      return false;
    }

    if (outPassed == null) {
      if (other.outPassed != null) {
        return false;
      }
    } else if (!outPassed.equals(other.outPassed)) {
      return false;
    }

    if (permission != other.permission) {
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
    return "IscsiAccessRuleInformation [ruleId=" + ruleId + ", ruleNotes=" + ruleNotes
        + ", initiatorName=" + initiatorName + ", user=" + user
        + ", passwd=" + passed + ", outUser=" + outUser
        + ", outPassed=" + outPassed + ", permission=" + permission + ", status=" + status + "]";
  }
}
