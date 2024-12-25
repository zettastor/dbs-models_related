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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import py.archive.ArchiveOptions;
import py.archive.segment.SegId;
import py.storage.impl.DummyStorage;

public class PageAddressTest {
  @Test
  public void testToGetPageLogicalOffset() throws Exception {
    int pageSize = 128 * 1024;
    ArchiveOptions
        .initContants(pageSize, pageSize * 100, ArchiveOptions.DEFAULT_MAX_FLEXIBLE_COUNT);
    int physicalPageSize = pageSize + ArchiveOptions.PAGE_METADATA_LENGTH;

    long pagePhysicalOffset = (1024L * 1024L * 1024L * 1024 * 1024L + 193L * 1111L);
    pagePhysicalOffset -= pagePhysicalOffset % physicalPageSize;

    PageAddressImpl pageAddress = new PageAddressImpl(new SegId(0L, 0), 1000L, pagePhysicalOffset,
        null);
    assertEquals(pageAddress.getLogicOffsetInSegment(pageSize),
        (pagePhysicalOffset / (pageSize + ArchiveOptions.PAGE_METADATA_LENGTH)) * pageSize);
  }

  @Test
  public void testPageAdjacent() throws Exception {
    int pageSize = 128 * 1024;
    int physicalPageSize = pageSize + ArchiveOptions.PAGE_METADATA_LENGTH;

    long pagePhysicalOffset1 = (1024L * 1024L * 1024L * 1024 * 1024L + 193L * 1111L);
    pagePhysicalOffset1 -= pagePhysicalOffset1 % physicalPageSize;
    PageAddressImpl pageAddress1 = new PageAddressImpl(new SegId(0L, 0), 1000L, pagePhysicalOffset1,
        new DummyStorage("1", 1));

    // should not be null
    assertFalse(pageAddress1.isAdjacentTo(null, physicalPageSize));
    PageAddressImpl pageAddress2 = new PageAddressImpl(new SegId(0L, 0), 1000L, pagePhysicalOffset1,
        new DummyStorage("2", 1));

    // storage is different
    assertFalse(pageAddress1.isAdjacentTo(pageAddress2, physicalPageSize));

    long pagePhysicalOffset2 = pagePhysicalOffset1 + physicalPageSize;
    pageAddress2 = new PageAddressImpl(new SegId(0L, 0), 1000L, pagePhysicalOffset2,
        new DummyStorage("1", 1));
    assertTrue(pageAddress1.isAdjacentTo(pageAddress2, physicalPageSize));

    pagePhysicalOffset2 = pagePhysicalOffset1 - physicalPageSize;
    pageAddress2 = new PageAddressImpl(new SegId(0L, 0), 1000L, pagePhysicalOffset2,
        new DummyStorage("1", 1));
    assertTrue(pageAddress1.isAdjacentTo(pageAddress2, physicalPageSize));

    pagePhysicalOffset2 = pagePhysicalOffset1 + 2 * physicalPageSize;
    pageAddress2 = new PageAddressImpl(new SegId(0L, 0), 1000L, pagePhysicalOffset2,
        new DummyStorage("1", 1));
    assertFalse(pageAddress1.isAdjacentTo(pageAddress2, physicalPageSize));

    pagePhysicalOffset2 = pagePhysicalOffset1 - 2 * physicalPageSize;
    pageAddress2 = new PageAddressImpl(new SegId(0L, 0), 1000L, pagePhysicalOffset2,
        new DummyStorage("1", 1));
    assertFalse(pageAddress1.isAdjacentTo(pageAddress2, physicalPageSize));
  }
}
