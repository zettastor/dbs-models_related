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

import java.sql.Blob;
import java.util.List;
import py.informationcenter.ScsiClient;

public interface ScsiClientStore {
  void clearDb();

  void saveOrUpdateScsiClientInfo(ScsiClient scsiClient);

  void deleteScsiClient(List<String> ipNames);

  int deleteScsiClientInfoByVolumeIdAndSnapshotId(String ipName, long volumeId, int snapshotId);

  ScsiClient getScsiClientInfoByVolumeIdAndSnapshotId(String ipName, long volumeId, int snapshotId);

  List<ScsiClient> listAllScsiClients();

  List<ScsiClient> getScsiClientByIp(String ipName);

  int deleteScsiClientByIp(String ipName);

  int updateScsiPydDriverContainerId(String ipName, long volumeId, int snapshotId,
      long driverContainerId);

  int updateScsiDriverDescription(String ipName, long volumeId, int snapshotId,
      String statusDescription);

  int updateScsiDriverStatus(String ipName, long volumeId, int snapshotId, String scsiDriverStatus);

  int updateScsiDriverStatusAndDescription(String ipName, long volumeId, int snapshotId,
      String scsiDriverStatus,
      String statusDescription, String descriptionType);

  public Blob createBlob(byte[] bytes);
}
