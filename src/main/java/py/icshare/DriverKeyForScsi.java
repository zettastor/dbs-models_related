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
