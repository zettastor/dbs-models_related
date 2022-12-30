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
