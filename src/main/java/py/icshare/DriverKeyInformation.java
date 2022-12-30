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

public class DriverKeyInformation implements Serializable {
  private static final long serialVersionUID = 1L;

  private long driverContainerId;

  private long volumeId;

  private int snapshotId;

  private String driverType;

  private DriverKeyInformation() {
  }

  public DriverKeyInformation(long driverContainerId, long volumeId, int snapshotId,
      String driverType) {
    this.driverContainerId = driverContainerId;
    this.volumeId = volumeId;
    this.snapshotId = snapshotId;
    this.driverType = driverType;
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

  public DriverKey toDriverKey() {
    DriverKey driverKey = new DriverKey(driverContainerId, volumeId, snapshotId,
        DriverType.valueOf(driverType));
    return driverKey;
  }

  /**
   * care about driverContainerId, volumeId, snapshotId, driverType.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DriverKeyInformation)) {
      return false;
    }

    DriverKeyInformation driverKey = (DriverKeyInformation) o;

    if (driverContainerId != driverKey.driverContainerId) {
      return false;
    }
    if (volumeId != driverKey.volumeId) {
      return false;
    }
    if (snapshotId != driverKey.snapshotId) {
      return false;
    }
    return driverType == driverKey.driverType;
  }

  @Override
  public int hashCode() {
    int result = (int) (driverContainerId ^ (driverContainerId >>> 32));
    result = 31 * result + (int) (volumeId ^ (volumeId >>> 32));
    result = 31 * result + snapshotId;
    result = 31 * result + (driverType != null ? driverType.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DriverKeyInformation{" + "driverContainerId=" + driverContainerId + ", volumeId="
        + volumeId
        + ", snapshotId=" + snapshotId + ", driverType=" + driverType + '}';
  }
}