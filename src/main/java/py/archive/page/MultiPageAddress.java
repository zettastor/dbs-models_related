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

package py.archive.page;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import py.archive.ArchiveOptions;
import py.archive.segment.SegId;
import py.datanode.page.impl.BogusPageAddress;
import py.datanode.page.impl.GarbagePageAddress;
import py.storage.Storage;

public class MultiPageAddress implements Comparable<MultiPageAddress> {
  private static int pageSize = -1;

  private final PageAddress startPageAddress;
  private final int pageCount;

  public MultiPageAddress(PageAddress startPageAddress, int pageCount) {
    this.startPageAddress = startPageAddress;
    this.pageCount = pageCount;
  }

  private static boolean isAdjacent(PageAddress first, PageAddress second) {
    if (pageSize < 0) {
      throw new IllegalStateException("unknown page size, which should be initialized first");
    }
    return second.getPhysicalOffsetInArchive() - first.getPhysicalOffsetInArchive() == pageSize;
  }

  public PageAddress getStartPageAddress() {
    return startPageAddress;
  }

  public int getPageCount() {
    return pageCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MultiPageAddress that = (MultiPageAddress) o;
    return pageCount == that.pageCount
        && Objects.equals(startPageAddress, that.startPageAddress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startPageAddress, pageCount);
  }

  @Override
  public int compareTo(MultiPageAddress o) {
    int compareStartAddress = startPageAddress.compareTo(o.startPageAddress);
    if (compareStartAddress != 0) {
      return compareStartAddress;
    } else {
      return Integer.compare(pageCount, o.pageCount);
    }
  }

  public List<PageAddress> convertToPageAddresses() {
    List<PageAddress> pageAddresses = new ArrayList<>();
    if (startPageAddress instanceof GarbagePageAddress
        || startPageAddress instanceof BogusPageAddress) {
      for (int index = 0; index < pageCount; index++) {
        pageAddresses.add(startPageAddress);
      }

      return pageAddresses;
    }

    SegId segId = startPageAddress.getSegId();
    long segUnitPhysicalAddressInArchive = startPageAddress.getSegUnitOffsetInArchive();
    long dataPhysicalOffsetInSegUnit = startPageAddress.getOffsetInSegment();
    Storage storage = startPageAddress.getStorage();
    for (int index = 0; index < pageCount; index++) {
      PageAddress pageAddress = new PageAddressImpl(segId, segUnitPhysicalAddressInArchive,
          dataPhysicalOffsetInSegUnit + index * ArchiveOptions.PAGE_PHYSICAL_SIZE, storage);

      pageAddresses.add(pageAddress);
    }

    return pageAddresses;
  }

  @Override
  public String toString() {
    return "MultiPageAddress{"
        + "startPageAddress=" + startPageAddress
        + ", pageCount=" + pageCount
        + '}';
  }

  public static class Builder {
    private PageAddress startAddress;
    private PageAddress endAddress;
    private int pageCount;

    public Builder(PageAddress startAddress, int physicalPageSize) {
      if (MultiPageAddress.pageSize < 0) {
        MultiPageAddress.pageSize = physicalPageSize;
      }
      this.startAddress = startAddress;
      this.endAddress = startAddress;
      this.pageCount = 1;
    }

    public boolean append(PageAddress next) {
      if (!isAdjacent(endAddress, next)) {
        return false;
      } else {
        endAddress = next;
        pageCount++;
        return true;
      }
    }

    public MultiPageAddress build() {
      return new MultiPageAddress(startAddress, pageCount);
    }

  }
}
