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
 * Garbage page address is used for shadow. When some data was written to page, but
 * the page is never written before,
 * so there is no need to shadow the page, just record the shadow
 * page with GarbagePageAddress
 */
public class GarbagePageAddress implements PageAddress {
  private static final AtomicInteger nextId = new AtomicInteger();
  private static final long GARBAGE_VOLUME_ID = Long.MIN_VALUE;
  private static final int GARBAGE_SEGMENT_INDEX = Integer.MIN_VALUE;
  private static final long GARBAGE_SEGMENT_OFFSET_IN_ARCHIVE = 1L << 50;
  private static final long GRABAGE_PAGE_OFFSET_IN_SEGMENT = 0;
  private static final long GRABAGE_PAGE_OFFSET_IN_ARCHIVE = (GARBAGE_SEGMENT_OFFSET_IN_ARCHIVE
      + GRABAGE_PAGE_OFFSET_IN_SEGMENT);
  private static final SegId GRABAGE_SEGMENT_ID = new SegId(GARBAGE_VOLUME_ID,
      GARBAGE_SEGMENT_INDEX);
  private final int id;

  public GarbagePageAddress() {
    id = nextId.getAndIncrement();
  }

  public static boolean isGarbagePageAddress(PageAddress pageAddress) {
    return (pageAddress instanceof GarbagePageAddress);
  }

  public static boolean isGarbageOffset(long offset) {
    return offset == GRABAGE_PAGE_OFFSET_IN_ARCHIVE;
  }

  @Override
  public SegId getSegId() {
    return GRABAGE_SEGMENT_ID;
  }

  @Override
  public void setSegId(SegId segId) {
    throw new NotImplementedException("this is a garbage page address");
  }

  @Override
  public Storage getStorage() {
    return null;
  }

  @Override
  public long getSegUnitOffsetInArchive() {
    return GARBAGE_SEGMENT_OFFSET_IN_ARCHIVE;
  }

  @Override
  public long getOffsetInSegment() {
    return GRABAGE_PAGE_OFFSET_IN_SEGMENT;
  }

  @Override
  public long getPhysicalOffsetInArchive() {
    return GRABAGE_PAGE_OFFSET_IN_ARCHIVE;
  }

  @Override
  public long getLogicOffsetInSegment(int logicalPageSize) {
    throw new NotImplementedException("this is a garbage page address");
  }

  @Override
  public String toString() {
    return "GarbagePageAddress(id:" + id + ")";
  }

  @Override
  public int compareTo(PageAddress o) {
    throw new NotImplementedException("this is a garbage page address");
  }

  @Override
  public boolean isAdjacentTo(PageAddress pageAddress, int physicalPageSize) {
    return false;
  }

}
