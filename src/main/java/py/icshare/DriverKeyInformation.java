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