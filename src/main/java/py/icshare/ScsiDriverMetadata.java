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

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScsiDriverMetadata {
  private static final Logger logger = LoggerFactory.getLogger(ScsiDriverMetadata.class);

  private DriverKeyForScsi driverKeyForScsi;

  private String scsiDeviceStatus;

  private String scsiDevice;

  private long lastReportTime = 0;

  public ScsiDriverMetadata() {
  }

  public DriverKeyForScsi getDriverKeyForScsi() {
    return driverKeyForScsi;
  }

  public void setDriverKeyForScsi(DriverKeyForScsi driverKeyForScsi) {
    this.driverKeyForScsi = driverKeyForScsi;
  }

  public String getScsiDeviceStatus() {
    return scsiDeviceStatus;
  }

  public void setScsiDeviceStatus(String scsiDeviceStatus) {
    this.scsiDeviceStatus = scsiDeviceStatus;
  }

  public String getScsiDevice() {
    return scsiDevice;
  }

  public void setScsiDevice(String scsiDevice) {
    this.scsiDevice = scsiDevice;
  }

  public long getLastReportTime() {
    return lastReportTime;
  }

  public void setLastReportTime(long lastReportTime) {
    this.lastReportTime = lastReportTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ScsiDriverMetadata)) {
      return false;
    }
    ScsiDriverMetadata that = (ScsiDriverMetadata) o;
    return lastReportTime == that.lastReportTime
        && Objects.equal(driverKeyForScsi, that.driverKeyForScsi)
        && Objects.equal(scsiDeviceStatus, that.scsiDeviceStatus)
        && Objects.equal(scsiDevice, that.scsiDevice);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(driverKeyForScsi, scsiDeviceStatus, scsiDevice, lastReportTime);
  }

  @Override
  public String toString() {
    return "SCSIDriverMetadata{"
        + "driverKeyForSCSI=" + driverKeyForScsi
        + ", scsiDeviceStatus='" + scsiDeviceStatus + '\''
        + ", scsiDevice='" + scsiDevice + '\''
        + ", lastReportTime=" + lastReportTime
        + '}';
  }

}
