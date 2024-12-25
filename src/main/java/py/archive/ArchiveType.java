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
