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
