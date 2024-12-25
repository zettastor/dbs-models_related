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
