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
