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

package py.archive.page;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveOptions;
import py.archive.segment.SegId;
import py.storage.Storage;
import py.volume.VolumeId;

public class PageAddressImpl implements PageAddress {
  private static final Logger logger = LoggerFactory.getLogger(PageAddressImpl.class);
  private final long segUnitOffsetInArchive;
  private final long offsetInSegment;
  private final Storage storage;
  private SegId segId;

  public PageAddressImpl(PageAddress pageAddress) {
    this.segId = pageAddress.getSegId();
    this.segUnitOffsetInArchive = pageAddress.getSegUnitOffsetInArchive();
    this.offsetInSegment = pageAddress.getOffsetInSegment();
    this.storage = pageAddress.getStorage();
  }

  public PageAddressImpl(SegId segId, long segUnitOffsetInArchive, long pageOffset,
      Storage storage) {
    this.segId = segId;
    this.segUnitOffsetInArchive = segUnitOffsetInArchive;
    this.offsetInSegment = pageOffset;
    this.storage = storage;
  }

  public PageAddressImpl(VolumeId volumeId, int segIndex, long segUnitOffsetInArchive,
      long pageOffset,
      Storage storage) {
    this(new SegId(volumeId, segIndex), segUnitOffsetInArchive, pageOffset, storage);
  }

  public PageAddressImpl(long volumeId, int segIndex, long segUnitOffsetInArchive, long pageOffset,
      Storage storage) {
    this(new SegId(volumeId, segIndex), segUnitOffsetInArchive, pageOffset, storage);
  }

  public SegId getSegId() {
    return segId;
  }

  @Override
  public void setSegId(SegId segId) {
    this.segId = segId;
  }

  public Storage getStorage() {
    return storage;
  }

  @Override
  public String toString() {
    return "PageAddressImpl [segId=" + segId + ", segUnitOffsetInArchive=" + segUnitOffsetInArchive
        + ", offsetInSegment=" + offsetInSegment + ", storage=" + storage + ", hash=" + hashCode()
        + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PageAddressImpl that = (PageAddressImpl) o;

    if (segUnitOffsetInArchive != that.segUnitOffsetInArchive) {
      return false;
    }
    if (offsetInSegment != that.offsetInSegment) {
      return false;
    }
    return storage != null ? storage.equals(that.storage) : that.storage == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (segUnitOffsetInArchive ^ (segUnitOffsetInArchive >>> 32));
    result = 31 * result + (int) (offsetInSegment ^ (offsetInSegment >>> 32));
    result = 31 * result + (storage != null ? storage.hashCode() : 0);
    return result;
  }

  public long getSegUnitOffsetInArchive() {
    return segUnitOffsetInArchive;
  }

  public long getOffsetInSegment() {
    return offsetInSegment;
  }

  public long getPhysicalOffsetInArchive() {
    return segUnitOffsetInArchive + offsetInSegment;
  }

  /**
   * Get the logical address of the page in the segment unit.
   *
   * @param logicalPageSize the logic page size that is configured in the data node configuration
   *                        file
   */
  public long getLogicOffsetInSegment(int logicalPageSize) {
    long physicalPageSize = ArchiveOptions.PAGE_PHYSICAL_SIZE;
    Validate.isTrue(offsetInSegment % physicalPageSize == 0);
    long pageNo = offsetInSegment / (long) physicalPageSize;
    return pageNo * (long) logicalPageSize;
  }

  @Override
  public int compareTo(PageAddress o) {
    if (o == null) {
      return 1;
    }

    if (!(o instanceof PageAddressImpl)) {
      // The other page might be a a bogus address. I am larger.
      return 1;
    }

    if (!this.getStorage().equals(o.getStorage())) {
      logger
          .error("two page addresses {} and {} are in different storages. They are not comparable.",
              this, o);
      throw new RuntimeException();
    }

    long diff = this.getPhysicalOffsetInArchive() - o.getPhysicalOffsetInArchive();
    if (diff > 0) {
      return 1;
    } else if (diff < 0) {
      return -1;
    } else {
      return 0;
    }
  }

  @Override
  public boolean isAdjacentTo(PageAddress pageAddress, int physicalPageSize) {
    if (pageAddress == null) {
      return false;
    }
    if (!(pageAddress instanceof PageAddressImpl)) {
      return false;
    }

    // pageAddress is not null and it is a PageAddressImpl
    // Let's check if they are at the same storage
    if (!this.getStorage().equals(pageAddress.getStorage())) {
      return false;
    }

    long diff = Math
        .abs(this.getPhysicalOffsetInArchive() - pageAddress.getPhysicalOffsetInArchive());
    if (diff == (long) physicalPageSize) {
      return true;
    } else {
      return false;
    }
  }
}