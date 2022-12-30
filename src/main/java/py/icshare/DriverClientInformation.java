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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverClientInformation {
  private static final Logger logger = LoggerFactory.getLogger(DriverClientInformation.class);

  private DriverClientKeyInformation driverClientKeyInformation;
  private String driverName;
  private String hostName;
  private boolean status;
  private String volumeName;
  private String volumeDescription;

  public DriverClientInformation() {
  }

  public DriverClientInformation(DriverClientKey driverClientKey, long time, String driverName,
      String hostName,
      boolean status, String volumeName, String volumeDescription) {
    this.driverClientKeyInformation = new DriverClientKeyInformation(driverClientKey, time);
    this.driverName = driverName;
    this.hostName = hostName;
    this.status = status;
    this.volumeName = volumeName;
    this.volumeDescription = volumeDescription;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public String getDriverName() {
    return driverName;
  }

  public void setDriverName(String driverName) {
    this.driverName = driverName;
  }

  public DriverClientKeyInformation getDriverClientKeyInformation() {
    return driverClientKeyInformation;
  }

  public void setDriverClientKeyInformation(DriverClientKeyInformation driverClientKeyInformation) {
    this.driverClientKeyInformation = driverClientKeyInformation;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public boolean isStatus() {
    return status;
  }

  public void setStatus(boolean status) {
    this.status = status;
  }

  public String getVolumeDescription() {
    return volumeDescription;
  }

  public void setVolumeDescription(String volumeDescription) {
    this.volumeDescription = volumeDescription;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DriverClientInformation that = (DriverClientInformation) o;

    if (status != that.status) {
      return false;
    }
    if (driverClientKeyInformation != null ? !driverClientKeyInformation
        .equals(that.driverClientKeyInformation) : that.driverClientKeyInformation != null) {
      return false;
    }
    if (!driverName.equals(that.driverName)) {
      return false;
    }
    if (!hostName.equals(that.hostName)) {
      return false;
    }
    if (!volumeName.equals(that.volumeName)) {
      return false;
    }
    return volumeDescription != null ? volumeDescription.equals(that.volumeDescription)
        : that.volumeDescription == null;
  }

  @Override
  public int hashCode() {
    int result = driverClientKeyInformation != null ? driverClientKeyInformation.hashCode() : 0;
    result = 31 * result + driverName.hashCode();
    result = 31 * result + hostName.hashCode();
    result = 31 * result + (status ? 1 : 0);
    result = 31 * result + volumeName.hashCode();
    result = 31 * result + (volumeDescription != null ? volumeDescription.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DriverClientInformation{"
        + "driverClientKeyInformation=" + driverClientKeyInformation
        + ", driverName='" + driverName + '\''
        + ", hostName='" + hostName + '\''
        + ", status=" + status
        + ", volumeName='" + volumeName + '\''
        + ", volumeDescription='" + volumeDescription + '\''
        + '}';
  }
}
