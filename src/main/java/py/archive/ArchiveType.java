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

import org.apache.commons.lang.NotImplementedException;
import py.exception.NotSupportedException;

/**
 * Different raw disk is used for different use purpose.
 */
public enum ArchiveType {
  RAW_DISK(0) {
    @Override
    public long getMagicNumber() {
      return RAW_DISK_MAGIC_NUMBER;
    }
  },
  UNSETTLED_DISK(6) {
    @Override
    public long getMagicNumber() {
      return UNSETTLED_DISK_MAGIC_NUMBER;
    }
  };

  /**
   * archive metadata.
   */
  private static final int DEFAULT_ARCHIVE_HEADER_OFFSET = 0;
  private static final int DEFAULT_ARCHIVE_HEADER_LENGTH = 128 * 1024; // 128KB
  private static final long RAW_DISK_MAGIC_NUMBER = 0x1847EBD7F527B3C0L;
  private static final long UNSETTLED_DISK_MAGIC_NUMBER = 0x1847EBD7F527B3C6L;
  private final int value;

  ArchiveType(int value) {
    this.value = value;
  }

  public static ArchiveType findByMagicNumber(long magicNumber) throws NotSupportedException {
    if (magicNumber == RAW_DISK_MAGIC_NUMBER) {
      return RAW_DISK;
    } else if (magicNumber == UNSETTLED_DISK_MAGIC_NUMBER) {
      return UNSETTLED_DISK;
    } else {
      throw new NotSupportedException("the magic number " + magicNumber + " not support");
    }
  }

  public static ArchiveType findByValue(int value) {
    switch (value) {
      case 0:
        return RAW_DISK;
      case 6:
        return UNSETTLED_DISK;
      default:
        throw new RuntimeException("the value " + value + " not support");
    }
  }

  public static int getArchiveHeaderOffset() {
    return DEFAULT_ARCHIVE_HEADER_OFFSET;
  }

  public static int getArchiveHeaderLength() {
    return DEFAULT_ARCHIVE_HEADER_LENGTH;
  }

  public int getValue() {
    return value;
  }

  /**
   * archive magic number to the first 8 bytes of every archive. It is a random number, and used in
   * big endian byte order.
   */
  public long getMagicNumber() {
    throw new NotImplementedException("not support the type=" + value);
  }
}
