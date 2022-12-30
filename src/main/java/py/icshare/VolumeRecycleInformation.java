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

import java.util.Objects;

public class VolumeRecycleInformation {
  private long volumeId;
  private long timeInRecycle;

  public VolumeRecycleInformation() {
  }

  public VolumeRecycleInformation(long volumeId, long timeInRecycle) {
    this.volumeId = volumeId;
    this.timeInRecycle = timeInRecycle;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public long getTimeInRecycle() {
    return timeInRecycle;
  }

  public void setTimeInRecycle(long timeInRecycle) {
    this.timeInRecycle = timeInRecycle;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VolumeRecycleInformation that = (VolumeRecycleInformation) o;
    return volumeId == that.volumeId && timeInRecycle == that.timeInRecycle;
  }

  @Override
  public int hashCode() {
    return Objects.hash(volumeId, timeInRecycle);
  }

  @Override
  public String toString() {
    return "VolumeRecycleInformation{"
        + "volumeId=" + volumeId
        + ", timeInRecycle=" + timeInRecycle
        + '}';
  }
}