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

import py.driver.DriverMetadata;
import py.driver.DriverType;

public class DriverClientKey {
  private long driverContainerId;

  private long volumeId;

  private int snapshotId;

  private DriverType driverType;

  private String clientInfo;

  public DriverClientKey() {
  }

  public DriverClientKey(DriverMetadata driverMetadata, String clientInfo) {
    this.driverContainerId = driverMetadata.getDriverContainerId();
    this.volumeId = driverMetadata.getVolumeId();
    this.snapshotId = driverMetadata.getSnapshotId();
    this.driverType = driverMetadata.getDriverType();
    this.clientInfo = clientInfo;
  }

  public DriverClientKey(long driverContainerId, long volumeId, int snapshotId,
      DriverType driverType, String clientInfo) {
    this.driverContainerId = driverContainerId;
    this.volumeId = volumeId;
    this.snapshotId = snapshotId;
    this.driverType = driverType;
    this.clientInfo = clientInfo;
  }

  public DriverClientKey(DriverClientKeyInformation driverClientKeyInformation) {
    this.driverContainerId = driverClientKeyInformation.getDriverContainerId();
    this.volumeId = driverClientKeyInformation.getVolumeId();
    this.snapshotId = driverClientKeyInformation.getSnapshotId();
    this.driverType = DriverType.findByName(driverClientKeyInformation.getDriverType());
    this.clientInfo = driverClientKeyInformation.getClientInfo();
  }

  public String getClientInfo() {
    return clientInfo;
  }

  public void setClientInfo(String clientInfo) {
    this.clientInfo = clientInfo;
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

  public DriverType getDriverType() {
    return driverType;
  }

  public void setDriverType(DriverType driverType) {
    this.driverType = driverType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DriverClientKey that = (DriverClientKey) o;

    if (driverContainerId != that.driverContainerId) {
      return false;
    }
    if (volumeId != that.volumeId) {
      return false;
    }
    if (snapshotId != that.snapshotId) {
      return false;
    }
    if (driverType != that.driverType) {
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
    return result;
  }

  @Override
  public String toString() {
    return "DriverClientKey{"
        + "driverContainerId=" + driverContainerId
        + ", volumeId=" + volumeId
        + ", snapshotId=" + snapshotId
        + ", driverType=" + driverType
        + ", clientInfo='" + clientInfo + '\''
        + '}';
  }
}
