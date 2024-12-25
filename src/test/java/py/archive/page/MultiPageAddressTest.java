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

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.mockito.Mock;
import py.archive.segment.SegId;
import py.storage.Storage;
import py.test.TestBase;

public class MultiPageAddressTest extends TestBase {
  private static final int MY_PAGE_SIZE = 4096;

  @Mock
  private SegId segId;

  @Mock
  private Storage storage;

  @Test
  public void test() {
    int pageCount = 10;

    List<PageAddress> addressList = new ArrayList<>();

    for (int i = 0; i < pageCount; i++) {
      addressList.add(new PageAddressImpl(segId, 0, i * MY_PAGE_SIZE, storage));
    }

    MultiPageAddress.Builder builder = new MultiPageAddress.Builder(addressList.get(0),
        MY_PAGE_SIZE);

    for (int i = 1; i < pageCount; i++) {
      assertTrue(builder.append(addressList.get(i)));
    }

    MultiPageAddress multiAddress = builder.build();

    assertEquals(pageCount, multiAddress.getPageCount());
    assertEquals(addressList.get(0), multiAddress.getStartPageAddress());

    PageAddress badPageAddress = new PageAddressImpl(segId, 0, pageCount * 2 * MY_PAGE_SIZE,
        storage);
    assertFalse(builder.append(badPageAddress));

    assertEquals(multiAddress, builder.build());

  }

  @Test
  public void testComparing() {
    PageAddress pageAddress1 = new PageAddressImpl(segId, 0, 0, storage);
    PageAddress pageAddress2 = new PageAddressImpl(segId, 0, MY_PAGE_SIZE, storage);

    MultiPageAddress multiAddress1 = new MultiPageAddress(pageAddress1, 1);
    MultiPageAddress multiAddress2 = new MultiPageAddress(pageAddress2, 1);
    assertTrue(multiAddress1.compareTo(multiAddress2) < 0);

    multiAddress1 = new MultiPageAddress(pageAddress1, 100);
    multiAddress2 = new MultiPageAddress(pageAddress2, 1);
    assertTrue(multiAddress1.compareTo(multiAddress2) < 0);

    multiAddress1 = new MultiPageAddress(pageAddress1, 1);
    multiAddress2 = new MultiPageAddress(pageAddress2, 100);
    assertTrue(multiAddress1.compareTo(multiAddress2) < 0);

    multiAddress1 = new MultiPageAddress(pageAddress1, 1);
    multiAddress2 = new MultiPageAddress(pageAddress1, 100);
    assertTrue(multiAddress1.compareTo(multiAddress2) < 0);

  }

}