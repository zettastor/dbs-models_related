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

import java.io.Serializable;

public class UmountDriverRequest implements Serializable {
  private static final long serialVersionUID = 1L;
  public long requestId;
  public long accountId;
  public long volumeId;
  public int snapshotId;
  public String scsiClientIp;

  public UmountDriverRequest() {
  }

  public UmountDriverRequest(long requestId, long accountId, long volumeId, int snapshotId,
      String scsiClientIp) {
    this.requestId = requestId;
    this.accountId = accountId;
    this.volumeId = volumeId;
    this.snapshotId = snapshotId;
    this.scsiClientIp = scsiClientIp;
  }

  public static long getSerialVersionUid() {
    return serialVersionUID;
  }

  public long getRequestId() {
    return requestId;
  }

  public void setRequestId(long requestId) {
    this.requestId = requestId;
  }

  public long getAccountId() {
    return accountId;
  }

  public void setAccountId(long accountId) {
    this.accountId = accountId;
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

  public String getScsiClientIp() {
    return scsiClientIp;
  }

  public void setScsiClientIp(String scsiClientIp) {
    this.scsiClientIp = scsiClientIp;
  }

}
