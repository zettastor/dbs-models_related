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

package py.archive.brick;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.Validate;
import py.archive.ArchiveOptions;
import py.archive.page.MultiPageAddress;
import py.archive.segment.SegId;
import py.common.bitmap.Bitmap;
import py.exception.NotEnoughSpaceException;
import py.storage.Storage;

/**
 * This class is responsible of manager physical address in archive for segment unit. brick
 * allocated at three cases : 1. a normal segment unit has been created, allocate a whole brick as
 * it physical space 2. a flexible segment unit write a page, allocate a shadow page from brick who
 * has been allocate for shadow pages allocate. 3. write a page log after create snapshot, we need
 * allocate a shadow page from brick.
 */
public class BrickMetadata {
  /**
   * mark the bitmap need to persist to disk.
   */
  @JsonIgnore
  protected boolean isBitmapNeedPersisted = false;
  // brick belong which segment unit
  /// if brick allocate shadow pages for more than one segment, set it to shadow unit seg id
  private SegId segId;
  // physical offset in archive from which the real data will be stored
  private long dataOffset;
  // meta data offset in archive from which the meta data of archive will be stored
  private long metadataOffsetInArchive;
  // brickStatus of brick
  private BrickStatus status;
  /**
   * Bitmap for page's allocated brickStatus.
   *
   * <p>0, this page not allocated;<br> 1, this page allocated
   */
  @JsonIgnore
  private Bitmap allocateBitmap;
  // storage which hold this archive
  @JsonIgnore
  private Storage storage;
  /*
   * total page count in brick : pageCount = segmentUnitSize/pageSize
   */
  private int pageCount;
  /*
   * free page count in brick
   */
  private int freePageCount;
  /*
   *
   */
  private boolean dirty;
  @JsonIgnore
  private BrickSpaceManager brickSpaceManager;

  @JsonIgnore
  private AtomicBoolean markedBrickCannotAllocatePages = new AtomicBoolean(false);

  @JsonIgnore
  private AtomicBoolean isPreAllocated = new AtomicBoolean(false);

  @JsonIgnore
  private AtomicBoolean belongToSegment = new AtomicBoolean(false);

  public BrickMetadata() {
  }

  public BrickMetadata(SegId segId, long dataOffset, long metadataOffsetInArchive,
      BrickStatus status, Storage storage, int pageCount, int freePageCount) {
    this.segId = segId;
    this.dataOffset = dataOffset;
    this.metadataOffsetInArchive = metadataOffsetInArchive;
    this.status = status;
    this.storage = storage;
    this.pageCount = pageCount;
    this.freePageCount = freePageCount;
    this.allocateBitmap = new Bitmap(pageCount);
  }

  public static Bitmap bitmapValueOf(byte[] array) {
    ByteBuffer buffer = ByteBuffer.wrap(array);
    int pageCount = ArchiveOptions.PAGE_NUMBER_PER_SEGMENT;
    int length = singleBitmapLength(pageCount);

    byte[] allocatedArray = new byte[length];

    buffer.get(allocatedArray);

    Bitmap allocateBitmap = Bitmap.valueOf(allocatedArray);

    return allocateBitmap;
  }

  public static int bitmapLength(int pageCount) {
    return singleBitmapLength(pageCount);
  }

  private static int singleBitmapLength(int pageCount) {
    return pageCount / 8;
  }

  @JsonIgnore
  public boolean isShadowUnit() {
    return status == BrickStatus.shadowPageAllocated;
  }

  public long getMetadataOffsetInArchive() {
    return metadataOffsetInArchive;
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
    return pageCount;
  }

  public void setPageCount(int pageCount) {
    this.pageCount = pageCount;
  }

  @JsonIgnore
  public boolean isUnitReusable() {
    // TODO
    return status == BrickStatus.free && !this.isBelongToSegment();
  }

  @JsonIgnore
  public boolean isFreeStatus() {
    return status == BrickStatus.free;
  }

  @JsonIgnore
  public boolean isAllocated() {
    return status == BrickStatus.allocated;
  }

  public BrickStatus getStatus() {
    return status;
  }

  public void setStatus(BrickStatus status) {
    this.status = status;
  }

  public long getDataOffset() {
    return dataOffset;
  }

  public void setDataOffset(long dataOffset) {
    this.dataOffset = dataOffset;
  }

  public void markPageUsed(MultiPageAddress multiPageAddress) {
    setBitmapNeedPersisted(true);
  }

  public void markAllPageUsed() {
    brickSpaceManager.markAllPageUsed();
    setStatus(BrickStatus.allocated);
    setBitmapNeedPersisted(true);
  }

  public void clear() {
    status = BrickStatus.free;
  }

  public void clear(boolean dirty) {
    setDirty(dirty);
    clear();
  }

  public boolean isDirty() {
    return dirty;
  }

  public void setDirty(boolean dirty) {
    this.dirty = dirty;
  }

  public SegId getSegId() {
    return segId;
  }

  public void setSegId(SegId segId) {
    this.segId = segId;
  }

  public Bitmap getAllocateBitmap() {
    return allocateBitmap;
  }

  public void setAllocateBitmap(Bitmap bitmap) {
    this.allocateBitmap = bitmap;
    Validate.isTrue(brickSpaceManager == null);
    Validate.isTrue(bitmap.getNbits() == ArchiveOptions.PAGE_NUMBER_PER_SEGMENT);
    this.freePageCount = ArchiveOptions.PAGE_NUMBER_PER_SEGMENT - bitmap.cardinality();

    setBitmapNeedPersisted(true);
  }

  public byte[] bitmapByteArray() {
    return allocateBitmap.toByteArray();
  }

  public boolean isBitmapNeedPersisted() {
    return isBitmapNeedPersisted;
  }

  public void setBitmapNeedPersisted(boolean bitmapNeedPersisted) {
    isBitmapNeedPersisted = bitmapNeedPersisted;
  }

  public Storage getStorage() {
    return storage;
  }

  public void setStorage(Storage storage) {
    this.storage = storage;
  }

  /**
   * just for unit test.
   */
  @VisibleForTesting
  public BrickSpaceManager getBrickSpaceManager() {
    return brickSpaceManager;
  }

  public void setBrickSpaceManager(BrickSpaceManager brickSpaceManager) {
    this.brickSpaceManager = brickSpaceManager;
  }

  public boolean getMarkedBrickCannotAllocatePages() {
    return markedBrickCannotAllocatePages.get();
  }

  public void setMarkedBrickCannotAllocatePages(
      AtomicBoolean markedBrickCannotAllocatePages) {
    this.markedBrickCannotAllocatePages = markedBrickCannotAllocatePages;
  }

  public boolean setMarkedBrickCannotAllocatePages(boolean markedBrickCannotAllocatePages) {
    return this.markedBrickCannotAllocatePages
        .compareAndSet(!markedBrickCannotAllocatePages, markedBrickCannotAllocatePages);
  }

  public boolean isPageFree(int index) {
    return brickSpaceManager.isPageFree(index);
  }

  @JsonIgnore
  public int getUsedPageCount() {
    return pageCount - freePageCount;
  }

  @JsonIgnore
  public boolean isPreAllocated() {
    return isPreAllocated.get();
  }

  @JsonIgnore
  public void setIsPreAllocated(boolean isPreAllocated) {
    this.isPreAllocated.set(isPreAllocated);
  }

  @JsonIgnore
  public boolean isBelongToSegment() {
    return belongToSegment.get();
  }

  @JsonIgnore
  public void setBelongToSegment(boolean belongToSegment) {
    this.belongToSegment.set(belongToSegment);
  }

  @Override
  public String toString() {
    return "BrickMetadata{"
        + "segId=" + segId
        + ", dataOffset=" + dataOffset
        + ", metadataOffsetInArchive=" + metadataOffsetInArchive
        + ", brickStatus=" + status
        + ", pageCount=" + pageCount
        + ", freePageCount=" + freePageCount
        + ", isBitmapNeedPersisted=" + isBitmapNeedPersisted
        + ", markedBrickCannotAllocatePages=" + markedBrickCannotAllocatePages
        + ", dirty=" + dirty
        + '}';
  }
}
