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

public class VolumeDeleteDelayInformation {
  private long volumeId;
  private long timeForDelay;
  private boolean stopDelay;

  public VolumeDeleteDelayInformation() {
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public long getTimeForDelay() {
    return timeForDelay;
  }

  public void setTimeForDelay(long timeForDelay) {
    this.timeForDelay = timeForDelay;
  }

  public boolean isStopDelay() {
    return stopDelay;
  }

  public void setStopDelay(boolean stopDelay) {
    this.stopDelay = stopDelay;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VolumeDeleteDelayInformation that = (VolumeDeleteDelayInformation) o;
    return volumeId == that.volumeId && timeForDelay == that.timeForDelay
        && stopDelay == that.stopDelay;
  }

  @Override
  public int hashCode() {
    return Objects.hash(volumeId, timeForDelay, stopDelay);
  }

  @Override
  public String toString() {
    return "VolumeRecycleInformation{"
        + "volumeId=" + volumeId
        + ", timeForDelay=" + timeForDelay
        + ", stopDelay=" + stopDelay
        + '}';
  }
}