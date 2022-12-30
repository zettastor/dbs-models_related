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
import java.io.Serializable;

public class DriverKeyForScsi implements Serializable {
  private static final long serialVersionUID = 1L;
  private long drivercontainerId;

  private long volumeId;

  private int snapshotId;

  public DriverKeyForScsi() {
  }

  public DriverKeyForScsi(long drivercontainerId, long volumeId, int snapshotId) {
    this.drivercontainerId = drivercontainerId;
    this.volumeId = volumeId;
    this.snapshotId = snapshotId;
  }

  public long getDrivercontainerId() {
    return drivercontainerId;
  }

  public void setDrivercontainerId(long drivercontainerId) {
    this.drivercontainerId = drivercontainerId;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DriverKeyForScsi)) {
      return false;
    }
    DriverKeyForScsi that = (DriverKeyForScsi) o;
    return drivercontainerId == that.drivercontainerId
        && volumeId == that.volumeId
        && snapshotId == that.snapshotId;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(drivercontainerId, volumeId, snapshotId);
  }

  @Override
  public String toString() {
    return "DriverKeyForSCSI{"
        + "drivercontainerId=" + drivercontainerId
        + ", volumeId=" + volumeId
        + ", snapshotId=" + snapshotId
        + '}';
  }
}
