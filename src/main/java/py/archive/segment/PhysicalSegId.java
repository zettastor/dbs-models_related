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

/**
 * Different from logical segment unit id {@link SegId}, this class defines physical segment id in
 * archive.
 */
public class PhysicalSegId {
  public static final int BYTES = Long.BYTES + Long.BYTES;

  private final long archiveId;
  private final long segUnitOffset;

  public PhysicalSegId(PhysicalSegId other) {
    this(other.archiveId, other.segUnitOffset);
  }

  public PhysicalSegId(long archiveId, long segUnitOffset) {
    super();
    this.archiveId = archiveId;
    this.segUnitOffset = segUnitOffset;
  }

  public long getArchiveId() {
    return archiveId;
  }

  public long getSegUnitOffset() {
    return segUnitOffset;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (archiveId ^ (archiveId >>> 32));
    result = prime * result + (int) (segUnitOffset ^ (segUnitOffset >>> 32));
    return result;
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
    PhysicalSegId other = (PhysicalSegId) obj;
    if (archiveId != other.archiveId) {
      return false;
    }
    if (segUnitOffset != other.segUnitOffset) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "PhysicalSegId [archiveId=" + archiveId + ", segUnitOffset=" + segUnitOffset + "]";
  }
}
