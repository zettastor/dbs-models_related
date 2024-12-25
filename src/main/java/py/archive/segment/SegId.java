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
