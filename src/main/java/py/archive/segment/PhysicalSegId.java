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
