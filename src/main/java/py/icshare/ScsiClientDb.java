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
import javax.persistence.Lob;
import org.hibernate.annotations.Type;

public class ScsiClientDb {
  private String ipName;

  @Lob
  @Type(type = "org.hibernate.type.BlobType")
  private Blob scsiClientInfos;

  public ScsiClientDb() {
  }

  public String getIpName() {
    return ipName;
  }

  public void setIpName(String ipName) {
    this.ipName = ipName;
  }

  public Blob getScsiClientInfos() {
    return scsiClientInfos;
  }

  public void setScsiClientInfos(Blob scsiClientInfos) {
    this.scsiClientInfos = scsiClientInfos;
  }

  @Override
  public String toString() {
    return "ScsiClientDB{"
        + "ipName='" + ipName + '\''
        + ", scsiClientInfos=" + scsiClientInfos
        + '}';
  }
}
