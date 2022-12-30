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

import java.io.Serializable;
import py.driver.DriverType;

public class DriverClientKeyInformation implements Serializable {
  private static final long serialVersionUID = 1L;

  private long driverContainerId;

  private long volumeId;

  private int snapshotId;

  private String driverType;

  private String clientInfo;

  //connect or dis time
  private long time;

  public DriverClientKeyInformation() {
  }

  public DriverClientKeyInformation(DriverClientKey driverClientKey, long time) {
    this.driverContainerId = driverClientKey.getDriverContainerId();
    this.volumeId = driverClientKey.getVolumeId();
    this.snapshotId = driverClientKey.getSnapshotId();
    this.driverType = driverClientKey.getDriverType().name();
    this.clientInfo = driverClientKey.getClientInfo();
    this.time = time;
  }

  public DriverClientKeyInformation(long driverContainerId, long volumeId, int snapshotId,
      String driverType, String clientInfo, long time) {
    this.driverContainerId = driverContainerId;
    this.volumeId = volumeId;
    this.snapshotId = snapshotId;
    this.driverType = driverType;
    this.clientInfo = clientInfo;
    this.time = time;
  }

  public long getDriverContainerId() {
    return driverContainerId;
  }

  public void setDriverContainerId(long driverContainerId) {
    this.driverContainerId = driverContainerId;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public int getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(int snapshotId) {
    this.snapshotId = snapshotId;
  }

  public String getDriverType() {
    return driverType;
  }

  public void setDriverType(String driverType) {
    this.driverType = driverType;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public String getClientInfo() {
    return clientInfo;
  }

  public void setClientInfo(String clientInfo) {
    this.clientInfo = clientInfo;
  }

  public DriverKey toDriverKey() {
    DriverKey driverKey = new DriverKey(driverContainerId, volumeId, snapshotId,
        DriverType.valueOf(driverType));
    return driverKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DriverClientKeyInformation that = (DriverClientKeyInformation) o;

    if (driverContainerId != that.driverContainerId) {
      return false;
    }
    if (volumeId != that.volumeId) {
      return false;
    }
    if (snapshotId != that.snapshotId) {
      return false;
    }
    if (time != that.time) {
      return false;
    }
    if (!driverType.equals(that.driverType)) {
      return false;
    }
    return clientInfo.equals(that.clientInfo);
  }

  @Override
  public int hashCode() {
    int result = (int) (driverContainerId ^ (driverContainerId >>> 32));
    result = 31 * result + (int) (volumeId ^ (volumeId >>> 32));
    result = 31 * result + snapshotId;
    result = 31 * result + driverType.hashCode();
    result = 31 * result + clientInfo.hashCode();
    result = 31 * result + (int) (time ^ (time >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "DriverClientKeyInformation{"
        + "driverContainerId=" + driverContainerId
        + ", volumeId=" + volumeId
        + ", snapshotId=" + snapshotId
        + ", driverType='" + driverType + '\''
        + ", clientInfo='" + clientInfo + '\''
        + ", time=" + time
        + '}';
  }
}