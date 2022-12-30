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
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class SegmentId implements Serializable {
  private static final long serialVersionUID = 1L;

  private long volumeId;
  private int index;
  private long instanceId;

  public SegmentId() {
  }

  public SegmentId(long volumeId, int index, long instanceId) {
    this.volumeId = volumeId;
    this.index = index;
    this.instanceId = instanceId;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof SegmentId)) {
      return false;
    }

    SegmentId tmp = (SegmentId) obj;
    if (this.index == tmp.index && this.volumeId == tmp.volumeId
        && this.instanceId == tmp.instanceId) {
      return true;
    }

    return false;
  }

  public int hashCode() {
    return new HashCodeBuilder(-528253723, -475504089).appendSuper(super.hashCode())
        .append(this.volumeId)
        .append(this.index).toHashCode();
  }

  @Override
  public String toString() {
    return "SegmentId [volumeId=" + volumeId + ", index=" + index + ", instanceId=" + instanceId
        + "]";
  }

  public long getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(long instanceId) {
    this.instanceId = instanceId;
  }

}
