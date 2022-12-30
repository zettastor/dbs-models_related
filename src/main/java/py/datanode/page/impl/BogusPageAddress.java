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

package py.datanode.page.impl;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.NotImplementedException;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.storage.Storage;

/**
 * every page needs an address, but for brand new pages, we don't have an address. This class make a
 * bogus unique address for a page.
 */
public class BogusPageAddress implements PageAddress {
  private static final AtomicInteger nextId = new AtomicInteger();
  public static final  BogusPageAddress BOGUSPAGEADDRESS = new BogusPageAddress();

  private final int id;
  private static final long BOGUS_VOLUME_ID = 0;
  private static final int BOGUS_SEGMENT_INDEX = -1;
  private static final long BOGUS_SEGMENT_OFFSET_IN_ARCHIVE = -1;
  private static final long BOGUS_PAGE_OFFSET_IN_SEGMENT = -1;
  private static final SegId BOGUS_SEGMENT_ID = new SegId(BOGUS_VOLUME_ID, BOGUS_SEGMENT_INDEX);

  public BogusPageAddress() {
    // make the bogus page address invalid address
    id = nextId.getAndIncrement();
  }

  public static boolean isAddressBogus(PageAddress address) {
    return isAddressBogus(address.getSegId().getVolumeId().getId(), address.getSegId().getIndex(),
        address.getSegUnitOffsetInArchive(), address.getOffsetInSegment())
        || address instanceof BogusPageAddress;
  }

  public static boolean isAddressBogus(long volumeId, int segmentIndex, long segmentOffsetInArchive,
      long pageOffsetInSegment) {
    return (volumeId == BOGUS_VOLUME_ID && segmentIndex == BOGUS_SEGMENT_INDEX
        && segmentOffsetInArchive == BOGUS_SEGMENT_OFFSET_IN_ARCHIVE
        && pageOffsetInSegment == BOGUS_SEGMENT_OFFSET_IN_ARCHIVE);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof BogusPageAddress) {
      BogusPageAddress bogusPageAddress = (BogusPageAddress) obj;
      return bogusPageAddress.id == id;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id;
  }

  @Override
  public String toString() {
    return "BogusPageAddress(id:" + id + ")";
  }

  @Override
  public SegId getSegId() {
    return BOGUS_SEGMENT_ID;
  }

  @Override
  public void setSegId(SegId segId) {
    throw new NotImplementedException("this is a bogus page address");
  }

  @Override
  public Storage getStorage() {
    return null;
  }

  @Override
  public long getSegUnitOffsetInArchive() {
    return BOGUS_SEGMENT_OFFSET_IN_ARCHIVE;
  }

  @Override
  public long getOffsetInSegment() {
    return BOGUS_PAGE_OFFSET_IN_SEGMENT;
  }

  @Override
  public long getPhysicalOffsetInArchive() {
    return -Math.abs(id);
  }

  @Override
  public long getLogicOffsetInSegment(int logicalPageSize) {
    throw new NotImplementedException("this is a bogus page address");
  }

  @Override
  public int compareTo(PageAddress o) {
    if (o == null) {
      return 1;
    } else if (!(o instanceof BogusPageAddress)) {
      // I am a bogus page address and other is not. Any other PageAddress implementations are
      // larger
      return -1;
    }

    // both of us are BogusPageAddress now
    return this.id - ((BogusPageAddress) o).id;
  }

  @Override
  public boolean isAdjacentTo(PageAddress pageAddress, int physicalPageSize) {
    // always return false
    return false;
  }

}
