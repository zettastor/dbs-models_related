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
