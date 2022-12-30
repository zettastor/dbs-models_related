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

package py.archive.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import py.volume.VolumeId;

/**
 * The id of a segment, or the logic location of the segment.
 *
 * <p>It has two parts. The first part is the volumeId, and other part is the index of the seg in
 * the volume
 */
public class SegId implements Comparable<SegId> {
  public static final int BYTES = VolumeId.BYTES + Integer.BYTES;

  public static final SegId SYS_RESERVED_SEGID = new SegId(Long.MAX_VALUE, 0);
  private final VolumeId volumeId;
  private final int index;

  @JsonCreator
  public SegId(@JsonProperty("volumeId") VolumeId volumeId, @JsonProperty("index") int index) {
    this.volumeId = volumeId;
    this.index = index;
  }

  public SegId(long volumeId, int index) {
    this.volumeId = new VolumeId(volumeId);
    this.index = index;
  }

  public SegId(SegId src) {
    this.volumeId = new VolumeId(src.volumeId.getId());
    this.index = src.index;
  }

  public VolumeId getVolumeId() {
    return volumeId;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public int hashCode() {
    int result = ((volumeId == null) ? 0 : volumeId.hashCode());
    return (result << 12) + index;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SegId other = (SegId) obj;
    if (index != other.index) {
      return false;
    }
    if (volumeId == null) {
      return other.volumeId == null;
    } else {
      if (other.volumeId == null) {
        return false;
      } else {
        return volumeId.getId() == other.volumeId.getId();
      }
    }
  }

  @Override
  public int compareTo(SegId obj) {
    if (this == obj) {
      return 0;
    }
    if (obj == null) {
      return 1;
    }
    SegId other = (SegId) obj;
    if (volumeId == null) {
      if (other.volumeId != null) {
        return -1;
      }
    } else if (volumeId.equals(other.volumeId)) {
      return index - other.index;
    } else {
      return volumeId.compareTo(other.volumeId);
    }
    return 0;
  }

  @Override
  public String toString() {
    return "[volumeId=" + volumeId + ", index=" + index + "]";
  }
}
