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
