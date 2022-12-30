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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static py.archive.segment.SegmentUnitBitmap.SegmentUnitBitMapType.Data;
import static py.archive.segment.SegmentUnitBitmap.SegmentUnitBitMapType.Migration;

import java.util.Random;
import org.junit.Before;
import org.junit.Test;
import py.archive.ArchiveOptions;
import py.common.bitmap.Bitmap;

/**
 * segment unit bitmap test.
 */
public class SegmentUnitBitmapTest {
  @Before
  public void before() {
    ArchiveOptions.initContants(8192, 500 * 8192, 500);
  }

  @Test
  public void cloneSet() throws Exception {
    SegmentUnitBitmap bitmap = new SegmentUnitBitmap(64);
    for (int i = 0; i < 64; i++) {
      assertFalse(bitmap.get(i, Migration));
      bitmap.set(i, Migration);
      assertTrue(bitmap.get(i, Migration));
    }
  }

  @Test
  public void dataSet() throws Exception {
    SegmentUnitBitmap bitmap = new SegmentUnitBitmap(64);
    for (int i = 0; i < 64; i++) {
      assertFalse(bitmap.get(i, Data));
      bitmap.set(i, Data);
      assertTrue(bitmap.get(i, Data));
    }
    for (int i = 0; i < 64; i++) {
      assertTrue(bitmap.get(i, Data));
      bitmap.clear(i, Data);
      assertFalse(bitmap.get(i, Data));
    }
    for (int i = 0; i < 64; i++) {
      bitmap.set(i, Data);
      assertTrue(bitmap.get(i, Data));
    }
    bitmap.clear(Data);
    for (int i = 0; i < 64; i++) {
      assertFalse(bitmap.get(i, Data));
    }
  }

  @Test
  public void nextClearDataBit() throws Exception {
    SegmentUnitBitmap bitmap = new SegmentUnitBitmap(64);

    bitmap.set(0, Data);
    bitmap.set(1, Data);
    bitmap.set(2, Data);
    bitmap.set(3, Data);

    bitmap.set(5, Data);
    bitmap.set(6, Data);

    bitmap.set(8, Data);

    bitmap.set(10, Data);
    bitmap.set(12, Data);
    bitmap.set(13, Data);

    assertEquals(4, bitmap.nextClearBit(0, Data));
    assertEquals(4, bitmap.nextClearBit(4, Data));
    assertEquals(7, bitmap.nextClearBit(5, Data));
    assertEquals(9, bitmap.nextClearBit(8, Data));
    assertEquals(14, bitmap.nextClearBit(12, Data));

    assertEquals(10, bitmap.cardinality(Data));
  }

  @Test
  public void bitMapLength() throws Exception {
    assertEquals(20, SegmentUnitBitmap.bitMapLength(64));
  }

  @Test
  public void toByteArray() throws Exception {
    SegmentUnitBitmap bitmap = new SegmentUnitBitmap(64);
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < 64; i++) {
      if (random.nextBoolean()) {
        bitmap.set(i, Migration);
      }
      if (random.nextBoolean()) {
        bitmap.set(i, Migration);
      }
    }
    byte[] array = bitmap.toByteArray();
    assertEquals(SegmentUnitBitmap.bitMapLength(64), array.length);

    SegmentUnitBitmap fromArray = SegmentUnitBitmap.valueOf(array);

    for (int i = 0; i < 64; i++) {
      assertEquals(bitmap.get(i, Data), fromArray.get(i, Data));
      assertEquals(bitmap.get(i, Migration), fromArray.get(i, Migration));
      assertEquals(bitmap.nextClearBit(i, Data), fromArray.nextClearBit(i, Data));
    }
  }

  @Test
  public void initForClone() throws Exception {
    SegmentUnitBitmap cloneBitmap = new SegmentUnitBitmap(64);
    Bitmap sourceBitmap = new Bitmap(64);
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < 64; i++) {
      if (random.nextBoolean()) {
        sourceBitmap.set(i);
      }
    }
    cloneBitmap.initForClone(sourceBitmap);
    for (int i = 0; i < 64; i++) {
      if (sourceBitmap.get(i)) {
        assertTrue(cloneBitmap.get(i, Data));
        assertFalse(cloneBitmap.get(i, Migration));
      } else {
        assertTrue(cloneBitmap.get(i, Migration));
        assertFalse(cloneBitmap.get(i, Data));
      }
    }
  }

}