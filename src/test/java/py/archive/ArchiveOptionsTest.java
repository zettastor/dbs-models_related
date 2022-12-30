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
