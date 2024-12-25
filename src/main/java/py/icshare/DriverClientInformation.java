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
