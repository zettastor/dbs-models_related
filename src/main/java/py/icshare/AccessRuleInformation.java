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
 * A class which is used to get access rule records from db or save them to it.
 */
public class AccessRuleInformation {
  private long ruleId;
  private String ipAddress;
  private int permission;
  /**
   * name of {@link AccessRuleStatus}.
   */
  private String status;

  public AccessRuleInformation() {
  }

  public AccessRuleInformation(long ruleId, String ipAddress, int permission) {
    this.ruleId = ruleId;
    this.ipAddress = ipAddress;
    this.permission = permission;
  }

  public long getRuleId() {
    return ruleId;
  }

  public void setRuleId(long ruleId) {
    this.ruleId = ruleId;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
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

  public VolumeAccessRule toVolumeAccessRule() {
    VolumeAccessRule accessRule = new VolumeAccessRule();
    accessRule.setRuleId(ruleId);
    accessRule.setIncommingHostName(ipAddress);
    accessRule.setPermission(AccessPermissionType.findByValue(permission));
    accessRule.setStatus(AccessRuleStatus.findByName(status));
    return accessRule;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((ipAddress == null) ? 0 : ipAddress.hashCode());
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
    AccessRuleInformation other = (AccessRuleInformation) obj;
    if (ipAddress == null) {
      if (other.ipAddress != null) {
        return false;
      }
    } else if (!ipAddress.equals(other.ipAddress)) {
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
    return "AccessRuleInformation [ruleId=" + ruleId + ", ipAddress=" + ipAddress + ", permission="
        + permission
        + ", status=" + status + "]";
  }
}
