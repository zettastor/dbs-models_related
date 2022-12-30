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

import java.nio.ByteBuffer;
import java.util.HashMap;
import org.apache.commons.lang3.Validate;
import py.archive.ArchiveOptions;
import py.common.bitmap.Bitmap;

/**
 * Segment unit bitmap.
 *
 * <p>Actually, there are 2 bit maps for each segment unit, one is data bitmap and the other is
 * clone bitmap
 *
 * <p><pre>
 * ----------------------------------------------------------
 * | PageCount | data bitmap | clone bitmap | padding space |
 * ----------------------------------------------------------
 * \           ^             ^              ^               /
 * \_________/ \___________/ \____________/ \_____________/
 *      |            |             |               |
 *      4         dynamic        dynamic   to align with 512
 * </pre>
 *
 * <p>To redefine the meaning of bitmap, We except to model it more and serve more business:
 *
 * <p>in fact, bitmap serves for log system. The two bitmaps here record whether the log system has
 * received external writing and whether it has written data to page.
 *
 * <p>More businesses will benefit from this define.
 *
 * <p>A instance, we no need load page from disk when log system has some log of the page, but it
 * has not apply to page system.
 *
 * <p>Another instance, a flexible volume page has create a some snapshot but has not writen any
 * data, and then plal got a log of the page, we no need allocate shadow page for that page no valid
 * data.
 */
public class SegmentUnitBitmap {
  private final HashMap<SegmentUnitBitMapType, Bitmap> mapTypeToBitMap = new HashMap<>();
  private int pageCount;
  private Bitmap dataBitMap;
  private Bitmap migrateBitMap;

  public SegmentUnitBitmap(int pageCount) {
    this.pageCount = pageCount;
    dataBitMap = new Bitmap(pageCount);
    migrateBitMap = new Bitmap(pageCount);
    mapTypeToBitMap.put(SegmentUnitBitMapType.Data, dataBitMap);
    mapTypeToBitMap.put(SegmentUnitBitMapType.Migration, migrateBitMap);
  }

  private SegmentUnitBitmap(int pageCount, Bitmap dataBitMap, Bitmap migrateBitMap) {
    this.pageCount = pageCount;
    this.dataBitMap = dataBitMap;
    this.migrateBitMap = migrateBitMap;
    mapTypeToBitMap.put(SegmentUnitBitMapType.Data, dataBitMap);
    mapTypeToBitMap.put(SegmentUnitBitMapType.Migration, migrateBitMap);
  }

  public static int bitMapLength(int pageCount) {
    return 4 + singleBitmapLength(pageCount) * 2;
  }

  private static int singleBitmapLength(int pageCount) {
    return pageCount / 8;
  }

  public static SegmentUnitBitmap valueOf(byte[] array) {
    ByteBuffer buffer = ByteBuffer.wrap(array);
    int pageCount = buffer.getInt();
    int length = singleBitmapLength(pageCount);
    if (pageCount > ArchiveOptions.PAGE_NUMBER_PER_SEGMENT) {
      Validate.isTrue(false, String.format(
          "some one modify the segmentSize or PageSize,the pagecount   %d in disk large that "
              + "PAGE_NUMBER_PER_SEGMENT %d",
          pageCount, ArchiveOptions.PAGE_NUMBER_PER_SEGMENT));
    }

    byte[] dataArray = new byte[length];
    byte[] cloneArray = new byte[length];

    buffer.get(dataArray);
    buffer.get(cloneArray);

    Bitmap dataBitMap = Bitmap.valueOf(dataArray);
    Bitmap cloneBitMap = Bitmap.valueOf(cloneArray);

    return new SegmentUnitBitmap(pageCount, dataBitMap, cloneBitMap);
  }

  public void set(int pageIndex, SegmentUnitBitMapType type) {
    mapTypeToBitMap.get(type).set(pageIndex);
  }

  public boolean get(int pageIndex, SegmentUnitBitMapType type) {
    return mapTypeToBitMap.get(type).get(pageIndex);
  }

  public void clear(int pageIndex, SegmentUnitBitMapType type) {
    mapTypeToBitMap.get(type).clear(pageIndex);
  }

  public void clear(SegmentUnitBitMapType type) {
    mapTypeToBitMap.get(type).clear();
  }

  public int nextClearBit(int fromIndex, SegmentUnitBitMapType type) {
    return mapTypeToBitMap.get(type).nextClearBit(fromIndex);
  }

  public int cardinality(SegmentUnitBitMapType type) {
    return mapTypeToBitMap.get(type).cardinality();
  }

  public int getNbits(SegmentUnitBitMapType type) {
    return mapTypeToBitMap.get(type).getNbits();
  }

  public boolean allSet(SegmentUnitBitMapType type) {
    return mapTypeToBitMap.get(type).allSet();
  }

  public byte[] toByteArray(SegmentUnitBitMapType type) {
    return mapTypeToBitMap.get(type).toByteArray();
  }

  public byte[] toByteArray() {
    // integer to byte array
    byte[] pageCountArray = new byte[]{(byte) (pageCount >> 24), (byte) (pageCount >> 16),
        (byte) (pageCount >> 8), (byte) pageCount
    };
    byte[] dataArray = dataBitMap.toByteArray();
    byte[] cloneArray = migrateBitMap.toByteArray();

    final byte[] joinedArray = new byte[pageCountArray.length + dataArray.length
        + cloneArray.length];
    System.arraycopy(pageCountArray, 0, joinedArray, 0, pageCountArray.length);
    System.arraycopy(dataArray, 0, joinedArray, pageCountArray.length, dataArray.length);
    System.arraycopy(cloneArray, 0, joinedArray, pageCountArray.length + dataArray.length,
        cloneArray.length);
    return joinedArray;
  }

  // TODO : this is not only for clone now

  public void initForClone(Bitmap sourceDataBitmap) {
    dataBitMap.or(sourceDataBitmap);
    /* for inverse, it must be clear */
    migrateBitMap.clear();
    migrateBitMap.or(sourceDataBitmap);
    migrateBitMap.inverse();
  }

  public int getPageCount() {
    return pageCount;
  }

  public Bitmap getBitMap(SegmentUnitBitMapType type) {
    if (type == SegmentUnitBitMapType.Migration) {
      return migrateBitMap;
    } else if (type == SegmentUnitBitMapType.Data) {
      return dataBitMap;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SegmentUnitBitmap)) {
      return false;
    }

    SegmentUnitBitmap that = (SegmentUnitBitmap) o;

    if (pageCount != that.pageCount) {
      return false;
    }
    if (dataBitMap != null ? !dataBitMap.equals(that.dataBitMap) : that.dataBitMap != null) {
      return false;
    }
    return migrateBitMap != null ? migrateBitMap.equals(that.migrateBitMap)
        : that.migrateBitMap == null;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("dataBitMap:").append(dataBitMap.toString()).append("migrateBitMap:")
        .append(migrateBitMap.toString());
    return sb.toString();
  }

  public enum SegmentUnitBitMapType {
    Data, Migration // migration bitmap is used for both lazy-clone and inner-migration
  }
}
