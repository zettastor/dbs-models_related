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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.brick.BrickMetadata;
import py.archive.segment.SegmentUnitBitmap;

public class ArchiveOptions {
  /**
   * Segment unit metadata.
   */
  // segment unit description data offset. segment unit description data includes segment meta data,
  // bitmap for page
  // free and snapshot
  // segment meta data is 1K, bitmap for page 32K if segment unit size 16G and page size 64k and
  // snapshot metadata is
  // 16K for one segment
  public static final int SEGMENTUNIT_DESCDATA_REGION_OFFSET = ArchiveType.getArchiveHeaderLength();
  public static final int SEGMENTUNIT_METADATA_REGION_OFFSET = ArchiveType
      .getArchiveHeaderLength(); // 128KB
  // The length of the region where include segment unit meta data, bitmap for page free and
  // snapshot meta data
  // For every page that stores segment unit metadatas, there are reserved space at the end of page.
  // This constant defines its value (2K)
  public static final int SEGMENTUNIT_METADATA_LENGTH = 2048;
  public static final int SEGMENTUNIT_ACCEPTOR_LENGTH = 512;
  public static final int BRICK_METADATA_LENGTH = 1024;
  public static final int SECTOR_SIZE = 512;
  public static final int SEGMENT_UNIT_DATA_ALIGNED_SIZE = 4096;
  /**
   * PAGE_MAGIC_NUM is written to the first 8 bytes of page metadata. It is a random number, and
   * used in big endian byte order.
   */
  public static final long PAGE_MAGIC_NUM = 0x1847EBD7F527B3C1L;
  /**
   * SEGMENT_UNIT_MAGIC.
   */
  public static final long SEGMENT_UNIT_MAGIC = 0x1847EBD7F527BC2L;
  /**
   * SHADOW_UNIT_MAGIC.
   */
  public static final long SHADOW_UNIT_MAGIC = 0x1847EBD7F527BC3L;
  /**
   * BRICK_MAGIC.
   */
  public static final long BRICK_MAGIC = 0x1847EBD7F527BC4L;
  public static final int DEFAULT_MAX_FLEXIBLE_COUNT = 1500;
  private static final Logger logger = LoggerFactory.getLogger(ArchiveOptions.class);
  // brick meta data information
  public static long BRICK_METADATA_REGION_OFFSET;
  public static int BRICK_BITMAP_LENGTH;
  public static boolean PAGE_METADATA_NEED_FLUSH_DISK = true;
  public static int PAGE_METADATA_LENGTH; // used to store page's checksum, address and etc
  /**
   * some constants with data node start When data node startup, those constants is decided.
   */
  public static long PAGE_SIZE;
  public static long SEGMENT_SIZE;
  public static long PAGE_PHYSICAL_SIZE;
  public static long PAGE_SIZE_IN_PAGE_MANAGER;
  public static long SEGMENT_PHYSICAL_SIZE;
  public static int PAGE_NUMBER_PER_SEGMENT;
  // the length of bitmap of page in segment unit, it may has padding space if total byte of bitmap
  // is less than 512
  // In order to align with 512 byte, there will be added more padding
  public static int SEGMENTUNIT_BITMAP_LENGTH;
  public static long SEGMENTUNIT_DESCDATA_LENGTH;
  public static int MAX_FLEXIBLE_COUNT = DEFAULT_MAX_FLEXIBLE_COUNT;

  //private static long ALL_ARBITERS_LENGTH;

  //Length of all FLEXIBLE segment unit. (MAX_FLEXIBLE_COUNT * )
  public static long ALL_FLEXIBLE_LENGTH;

  //Length of arbiter and flexible segment unit
  // TODO : remove, because :
  // DYMANIC_SPACE_LENGTH = ALL_FLEXIBLE_LENGTH;
  // public static long DYMANIC_SPACE_LENGTH;

  public static double ARCHIVE_RESERVED_RATIO = 0;
  public static long BRICK_DESCDATA_LENGTH;
  private static ByteBuffer zeroData = null;

  /*
   * According to page size and segment size, initiate some parameters and those parameters will
   *  not change once
   * initiated
   */

  public static void initContants(long pageSize, long segmentSize, int pageMetadataSize,
      double archiveReservedRatio, int flexibleLimit, boolean pageMetadataNeedFlushToDisk) {
    Validate.isTrue((pageSize % SECTOR_SIZE) == 0);
    Validate.isTrue((segmentSize % SECTOR_SIZE) == 0L);
    Validate.isTrue(segmentSize % pageSize == 0L);
    Validate.isTrue(pageMetadataSize % SECTOR_SIZE == 0);

    PAGE_SIZE = pageSize;
    SEGMENT_SIZE = segmentSize;
    PAGE_METADATA_LENGTH = pageMetadataSize;
    if (flexibleLimit <= 0) {
      MAX_FLEXIBLE_COUNT = DEFAULT_MAX_FLEXIBLE_COUNT;
    } else {
      MAX_FLEXIBLE_COUNT = flexibleLimit;
    }
    PAGE_SIZE_IN_PAGE_MANAGER = PAGE_SIZE + PAGE_METADATA_LENGTH;

    PAGE_METADATA_NEED_FLUSH_DISK = pageMetadataNeedFlushToDisk;
    if (pageMetadataNeedFlushToDisk) {
      PAGE_PHYSICAL_SIZE = PAGE_SIZE_IN_PAGE_MANAGER;
    } else {
      PAGE_PHYSICAL_SIZE = PAGE_SIZE;
    }

    PAGE_NUMBER_PER_SEGMENT = (int) (segmentSize / pageSize);
    SEGMENT_PHYSICAL_SIZE = (long) PAGE_PHYSICAL_SIZE * PAGE_NUMBER_PER_SEGMENT;
    SEGMENTUNIT_BITMAP_LENGTH = SegmentUnitBitmap.bitMapLength(PAGE_NUMBER_PER_SEGMENT);

    // keep bitmap aligned to 512 byte
    if (SEGMENTUNIT_BITMAP_LENGTH % 512 != 0) {
      SEGMENTUNIT_BITMAP_LENGTH = (SEGMENTUNIT_BITMAP_LENGTH / 512 + 1) * 512;
    }

    SEGMENTUNIT_DESCDATA_LENGTH =
        SEGMENTUNIT_METADATA_LENGTH + SEGMENTUNIT_BITMAP_LENGTH + SEGMENTUNIT_ACCEPTOR_LENGTH;

    //calc costing space by all flexible segment units.
    ALL_FLEXIBLE_LENGTH = MAX_FLEXIBLE_COUNT * SEGMENTUNIT_DESCDATA_LENGTH;

    BRICK_BITMAP_LENGTH = BrickMetadata.bitmapLength(PAGE_NUMBER_PER_SEGMENT);
    // keep bitmap aligned to 512 byte
    if (BRICK_BITMAP_LENGTH % 512 != 0) {
      BRICK_BITMAP_LENGTH = (BRICK_BITMAP_LENGTH / 512 + 1) * 512;
    }
    BRICK_DESCDATA_LENGTH = BRICK_METADATA_LENGTH + BRICK_BITMAP_LENGTH;

    BRICK_METADATA_REGION_OFFSET = SEGMENTUNIT_METADATA_REGION_OFFSET + ALL_FLEXIBLE_LENGTH;

    logger.warn(
        "page size {}, segment size {}, page physical size {}, page number per segment unit {}, "
            + "segment physical size {}, "
            + "bitmap length {}, segment unit description data len {}, flexible space:{}, brick "
            + "desc"
            + " data length:{}",
        PAGE_SIZE, SEGMENT_SIZE, PAGE_PHYSICAL_SIZE, PAGE_NUMBER_PER_SEGMENT, SEGMENT_PHYSICAL_SIZE,
        SEGMENTUNIT_BITMAP_LENGTH, SEGMENTUNIT_DESCDATA_LENGTH, ALL_FLEXIBLE_LENGTH,
        BRICK_DESCDATA_LENGTH);
    zeroData = ByteBuffer.allocate((int) PAGE_SIZE);

    ARCHIVE_RESERVED_RATIO = archiveReservedRatio;
  }

  /**
   * only for unit test.
   */
  public static void initContants(long pageSize, long segmentSize, int flexibleLimit) {
    initContants(pageSize, segmentSize, SECTOR_SIZE, (double) 0, flexibleLimit, true);
  }

  public static void validateNotStartWithPageMetadataMagic(ByteBuffer data, int offset) {
    int bufferPos = data.position();

    if (data.remaining() <= offset) {
      logger.error("buffer not enough remaining {}, {}", data, offset);
      return;
    }

    data.position(bufferPos + offset);

    long firstLong = data.getLong();

    if (firstLong == ArchiveOptions.PAGE_MAGIC_NUM) {
      logger.error("writing metadata to data buffer {}", firstLong, new Exception());
    }

    data.position(bufferPos);
  }

  public static void validateNotStartWithPageMetadataMagic(ByteBuffer data, int offset,
      AtomicBoolean atomicBooleans) {
    int bufferPos = data.position();

    if (data.remaining() <= offset) {
      logger.error("buffer not enough remaining {}, {}", data, offset);
      return;
    }

    data.position(bufferPos + offset);

    long firstLong = data.getLong();

    if (firstLong == ArchiveOptions.PAGE_MAGIC_NUM && atomicBooleans.get()) {
      atomicBooleans.set(false);
      logger.error("writing metadata to data buffer {}", firstLong, new Exception());
    }

    data.position(bufferPos);
  }

  public static ByteBuffer getZeroData() {
    return zeroData.duplicate();
  }

}
