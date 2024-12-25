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