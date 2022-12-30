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

package py.infocenter.client;

import java.util.ArrayList;
import java.util.List;
import py.driver.DriverMetadata;
import py.volume.VolumeMetadata;

public class VolumeMetadataAndDrivers {
  public VolumeMetadata volumeMetadata;
  public List<DriverMetadata> driverMetadatas;

  public VolumeMetadataAndDrivers() {
    driverMetadatas = new ArrayList<>();
  }

  public VolumeMetadata getVolumeMetadata() {
    return volumeMetadata;
  }

  public void setVolumeMetadata(VolumeMetadata volumeMetadata) {
    this.volumeMetadata = volumeMetadata;
  }

  public List<DriverMetadata> getDriverMetadatas() {
    return driverMetadatas;
  }

  public void setDriverMetadatas(List<DriverMetadata> driverMetadatas) {
    this.driverMetadatas = null;
    this.driverMetadatas = driverMetadatas;
  }

  @Override
  public String toString() {
    return "VolumeMetadataAndDrivers{"
        + "volumeMetadata=" + volumeMetadata
        + ", driverMetadatas=" + driverMetadatas
        + '}';
  }
}
