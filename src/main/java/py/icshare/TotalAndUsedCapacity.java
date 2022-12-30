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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

@JsonAutoDetect(fieldVisibility = Visibility.ANY)
public class TotalAndUsedCapacity {
  private Long totalCapacity;

  private Long usedCapacity;

  public TotalAndUsedCapacity() {
    super();
  }

  public TotalAndUsedCapacity(Long totalCapacity, Long usedCapacity) {
    this.totalCapacity = totalCapacity;
    this.usedCapacity = usedCapacity;
  }

  public TotalAndUsedCapacity(String totalCapacity, String usedCapacity) {
    this.totalCapacity = Long.valueOf(totalCapacity);
    this.usedCapacity = Long.valueOf(usedCapacity);
  }

  public Long getTotalCapacity() {
    return totalCapacity;
  }

  public void setTotalCapacity(Long totalCapacity) {
    this.totalCapacity = totalCapacity;
  }

  public Long getUsedCapacity() {
    return usedCapacity;
  }

  public void setUsedCapacity(Long usedCapacity) {
    this.usedCapacity = usedCapacity;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((totalCapacity == null) ? 0 : totalCapacity.hashCode());
    result = prime * result + ((usedCapacity == null) ? 0 : usedCapacity.hashCode());
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
    TotalAndUsedCapacity other = (TotalAndUsedCapacity) obj;
    if (totalCapacity == null) {
      if (other.totalCapacity != null) {
        return false;
      }
    } else if (!totalCapacity.equals(other.totalCapacity)) {
      return false;
    }
    if (usedCapacity == null) {
      if (other.usedCapacity != null) {
        return false;
      }
    } else if (!usedCapacity.equals(other.usedCapacity)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "TotalAndUsedCapacity [totalCapacity=" + totalCapacity + ", usedCapacity=" + usedCapacity
        + "]";
  }
}
