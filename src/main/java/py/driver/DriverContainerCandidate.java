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

package py.driver;

/**
 * control center gets driver container from info center to launch a volume in order to ensure a
 * successful launching, info center need to return more than one driver container to control center
 * a candidate represents a driver container.
 *
 */
public class DriverContainerCandidate {
  private String hostName;

  private int port;

  private long sequenceId;

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public long getSequenceId() {
    return sequenceId;
  }

  public void setSequenceId(long sequenceId) {
    this.sequenceId = sequenceId;
  }

  /**
   * almost only care hostname, before modify these funcs, should check.
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
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
    DriverContainerCandidate other = (DriverContainerCandidate) obj;
    if (hostName == null) {
      if (other.hostName != null) {
        return false;
      }
    } else if (!hostName.equals(other.hostName)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "DriverContainerCandidate [hostName=" + hostName + ", port=" + port + ", sequenceId="
        + sequenceId + "]";
  }
}
