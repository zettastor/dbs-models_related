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

import py.driver.DriverType;

public class DriverKey {
  private long driverContainerId;

  private long volumeId;

  private int snapshotId;

  private DriverType driverType;

  public DriverKey() {
  }

  public DriverKey(long driverContainerId, long volumeId, int snapshotId, DriverType driverType) {
    this.driverContainerId = driverContainerId;
    this.volumeId = volumeId;
    this.snapshotId = snapshotId;
    this.driverType = driverType;
  }

  public long getDriverContainerId() {
    return driverContainerId;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public int getSnapshotId() {
    return snapshotId;
  }

  public DriverType getDriverType() {
    return driverType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DriverKey)) {
      return false;
    }

    DriverKey driverKey = (DriverKey) o;

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

  public DriverKeyInformation toDriverKeyInformation() {
    DriverKeyInformation driverKeyInformation = new DriverKeyInformation(driverContainerId,
        volumeId, snapshotId,
        driverType.name());
    return driverKeyInformation;
  }

  @Override
  public String toString() {
    return "DriverKey{" + "driverContainerId=" + driverContainerId + ", volumeId=" + volumeId
        + ", snapshotId="
        + snapshotId + ", driverType=" + driverType + '}';
  }
}
