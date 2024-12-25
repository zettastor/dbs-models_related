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

package py.archive;

import static org.junit.Assert.assertEquals;

import org.apache.commons.lang.Validate;
import org.junit.Test;
import py.archive.segment.SegmentUnitBitmap;
import py.test.TestBase;

public class ArchiveOptionsTest extends TestBase {
  @Test
  public void testMaxFlexible() throws Exception {
    int pageSzie = 8192;
    long segMentSize = (1024 * 1024 * 1024 * 16L);
    int flaxible = 4000;
    ArchiveOptions.initContants(pageSzie, segMentSize, flaxible);

    /*
     *  4000 * (2048 + 524800 + 64 * 1024)
     * */
    long bitmap = SegmentUnitBitmap.bitMapLength((int) (segMentSize / pageSzie));
    if (bitmap / 512 != 0) {
      bitmap = (bitmap / 512 + 1) * 512;
    }
    long unitLeng = 2048 + bitmap + 512;

    long expect = flaxible * unitLeng;
    Validate.isTrue(expect > 0);
    assertEquals(ArchiveOptions.ALL_FLEXIBLE_LENGTH, expect);

  }

  @Test
  public void testMaxFlexibleMore() throws Exception {
    int pageSzie = 8192;
    long segMentSize = (1024 * 1024 * 1024 * 32L);
    int flaxible = 8000;
    ArchiveOptions.initContants(pageSzie, segMentSize, flaxible);

    /*
     *  4000 * (2048 + 524800 + 64 * 1024)
     * */
    long bitmap = SegmentUnitBitmap.bitMapLength((int) (segMentSize / pageSzie));
    if (bitmap / 512 != 0) {
      bitmap = (bitmap / 512 + 1) * 512;
    }
    long unitLeng = 2048 + bitmap + 512;

    long expect = flaxible * unitLeng;
    Validate.isTrue(expect > 0);
    assertEquals(ArchiveOptions.ALL_FLEXIBLE_LENGTH, expect);

  }
}
