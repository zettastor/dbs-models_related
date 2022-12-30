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

import static py.archive.segment.SegmentUnitBitmap.SegmentUnitBitMapType.Data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.brick.BrickMetadata;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitBitmap;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitType;
import py.storage.Storage;

/**
 * This class is for physical unit in archive. Archive unit has two sub-type: <br> 1. Segment unit
 * {@link SegmentUnitMetadata}: for volume segment unit and store the data of segment unit <br> 2.
 * Shadow unit: for shadow and store the shadow page for snapshot
 */
public abstract class AbstractSegmentUnitMetadata {
  private static final Logger logger = LoggerFactory.getLogger(AbstractSegmentUnitMetadata.class);

  // Logic Id of the seg, made of two parts, volumeId + segIndex
  // For instance, the seg belongs to "V1bcd" volume and
  // its index in the volume is 5, the segId is V1bcd_5
  private final SegId segId;

  // physical offset in archive from which the real data will be stored
  protected long dataOffset;

  @JsonIgnore
  // physical address in archive
  protected BrickMetadata brickMetadata;

  // meta data offset in archive from which the meta data of archive will be stored
  @JsonIgnore
  protected long metadataOffsetInArchive;
  // storage which hold this archive
  @JsonIgnore
  protected Storage storage;
  /**
   * comments Bitmap for page's status.
   *
   * <p>For {@link SegmentUnitMetadata}: <br> 0, never been written ; <br> 1, this page has been
   * written.<br>
   *
   * <p>Notice that whenever we are using bit map in {@link SegmentUnitMetadata} , we should follow
   * a principle that the bit map is
   *
   * <p>ONLY USED TO IMPROVE PERFORMANCE
   *
   * <p>Which leads to a simple theory that nothing can go wrong if a bit was set to 1 but is
   * actually 0. On the other hand, a bit should never be 0 if it is actually 1.
   *
   * <p>For ShadowUnitMetadata: <br> 0, this page not allocated;<br> 1, this page allocated <br>
   */
  @JsonIgnore
  protected SegmentUnitBitmap bitmap;
  /**
   * mark the bitmap need to persist to disk.
   */
  @JsonIgnore
  protected boolean isBitmapNeedPersisted = false;
  /**
   * mark segment unit is a arbiter or a normal one.
   */
  protected SegmentUnitType segmentUnitType;
  // free page count in this segment unit
  @JsonIgnore
  private int freePageCount = 0;
  /*
   * total page count in archive unit: pageCount = segmentUnitSize/pageSize
   */
  @JsonIgnore
  private int pageCount;

  public AbstractSegmentUnitMetadata(SegId segId, long metadataOffset, long offset,
      Storage storage) {
    this.segId = segId;
    bitmap = new SegmentUnitBitmap(ArchiveOptions.PAGE_NUMBER_PER_SEGMENT);
    logger.debug("PAGE_NUMBER_PER_SEGMENT is {}, bytes length {}",
        ArchiveOptions.PAGE_NUMBER_PER_SEGMENT,
        bitmap.toByteArray().length);

    this.pageCount = ArchiveOptions.PAGE_NUMBER_PER_SEGMENT;
    this.freePageCount = ArchiveOptions.PAGE_NUMBER_PER_SEGMENT;
    this.metadataOffsetInArchive = metadataOffset;
    this.dataOffset = offset;
    this.storage = storage;
  }

  /**
   * This constructor just care about the offset. It' is used outside of the data node service.
   */
  public AbstractSegmentUnitMetadata(SegId segId, long offset) {
    this.segId = segId;
    bitmap = new SegmentUnitBitmap(ArchiveOptions.PAGE_NUMBER_PER_SEGMENT);
    this.dataOffset = offset;
    this.pageCount = ArchiveOptions.PAGE_NUMBER_PER_SEGMENT;
  }

  /*
   * return unit can be reusable. When the unit is deleted, the unit can be used
   */
  @JsonIgnore
  public abstract boolean isUnitReusable();

  public void setPage(int pageIndex) {
    Validate.isTrue(0 <= pageIndex && pageIndex <= pageCount - 1);
    boolean fail = false;
    synchronized (bitmap) {
      if (!bitmap.get(pageIndex, Data)) {
        bitmap.set(pageIndex, Data);
        this.freePageCount--;
      } else {
        fail = true;
      }
    }

    if (fail) {
      logger.warn("this page has been set {}", pageIndex);
    }
  }

  public void clearPage(int pageIndex) {
    Validate.isTrue(0 <= pageIndex && pageIndex <= pageCount - 1);
    boolean fail = false;
    synchronized (bitmap) {
      if (bitmap.get(pageIndex, Data)) {
        bitmap.clear(pageIndex, Data);
        this.freePageCount++;
      } else {
        fail = true;
      }
    }

    if (fail) {
      logger.trace("this page has been clear with pageIndex {}", pageIndex);
    }
  }

  @JsonIgnore
  public boolean isPageFree(int pageIndex) {
    return !bitmap.get(pageIndex, Data);
  }

  public long getLogicalDataOffset() {
    return this.dataOffset;
  }

  public void setLogicalDataOffset(long dataOffset) {
    this.dataOffset = dataOffset;
  }

  @JsonIgnore
  public long getPhysicalDataOffset() {
    if (brickMetadata == null) {
      return 0;
    }
    return this.brickMetadata.getDataOffset();
  }

  public long getMetadataOffsetInArchive() {
    return this.metadataOffsetInArchive;
  }

  public void setMetadataOffsetInArchive(long metadataOffsetInArchive) {
    this.metadataOffsetInArchive = metadataOffsetInArchive;
  }

  public int getFreePageCount() {
    return freePageCount;
  }

  public void setFreePageCount(int freePageCount) {
    this.freePageCount = freePageCount;
  }

  public int getPageCount() {
    return this.pageCount;
  }

  protected int getFirstFreePageIndex() {
    return bitmap.nextClearBit(0, Data);
  }

  public Storage getStorage() {
    return storage;
  }

  public void setStorage(Storage storage) {
    this.storage = storage;
  }

  public SegmentUnitBitmap getBitmap() {
    return bitmap;
  }

  // it is only used for initializing the disk.

  public void setBitMap(SegmentUnitBitmap bitmap) {
    this.bitmap = bitmap;
    Validate.isTrue(bitmap.toByteArray(Data).length * 8 == ArchiveOptions.PAGE_NUMBER_PER_SEGMENT);
    this.freePageCount = ArchiveOptions.PAGE_NUMBER_PER_SEGMENT - bitmap.cardinality(Data);
  }

  public boolean isBitmapNeedPersisted() {
    return isBitmapNeedPersisted;
  }

  public void setBitmapNeedPersisted(boolean isBitmapNeedPersisted) {
    this.isBitmapNeedPersisted = isBitmapNeedPersisted;
  }

  /**
   * Clear the bitmap.
   */
  public void clear() {
    synchronized (bitmap) {
      this.bitmap.clear(Data);
      this.freePageCount = ArchiveOptions.PAGE_NUMBER_PER_SEGMENT;
    }
  }

  @JsonIgnore
  public boolean isArbiter() {
    return segmentUnitType == SegmentUnitType.Arbiter;
  }

  public SegmentUnitType getSegmentUnitType() {
    return segmentUnitType;
  }

  public void setSegmentUnitType(SegmentUnitType segmentUnitType) {
    this.segmentUnitType = segmentUnitType;
  }

  public SegId getSegId() {
    return segId;
  }

  @Override
  public String toString() {
    return "AbstractSegmentUnitMetadata{" + "segId=" + segId + ", dataOffset=" + dataOffset
        + ", metadataOffsetInArchive=" + metadataOffsetInArchive + ", freePageCount="
        + freePageCount
        + ", storage=" + storage + ", pageCount=" + pageCount + ", isBitmapNeedPersisted="
        + isBitmapNeedPersisted + ", segmentUnitType=" + segmentUnitType + '}';
  }

  public BrickMetadata getBrickMetadata() {
    return brickMetadata;
  }

  public void setBrickMetadata(BrickMetadata brickMetadata) {
    if (null != brickMetadata) {
      brickMetadata.setSegId(segId);
      brickMetadata.setBelongToSegment(true);
    } else {
      this.brickMetadata.setBelongToSegment(false);
    }
    this.brickMetadata = brickMetadata;
  }
}
