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

package py.utils;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataConsistencyUtils {
  private static final Logger logger = LoggerFactory.getLogger(DataConsistencyUtils.class);

  public static byte[] generateFixedData(int length, byte number) {
    byte[] src = new byte[length];

    for (int i = 0; i < length; i++) {
      src[i] = number;
    }

    return src;
  }

  public static boolean checkFixedData(byte[] data, byte number) {
    return checkFixedData(data, 0, data.length, number);
  }

  public static boolean checkFixedData(byte[] data, int offset, int length, byte number) {
    for (int i = offset; i < offset + length; i++) {
      if (data[i] != number) {
        logger.warn("byte: {}, expected: {}", data[i], number);
        return false;
      }
    }

    return true;
  }

  public static boolean checkFixedData(ByteBuffer data, byte number) {
    for (int i = data.position(); i < data.remaining(); i++) {
      if (data.get(i) != number) {
        logger.warn("byte: {}, expected: {}", data.get(i), number);
        return false;
      }
    }
    return true;
  }

  public static ByteBuffer generateDataByOffset(int size, long offset) {
    ByteBuffer buffer = ByteBuffer.allocate(size);
    for (int i = 0; i < size / Long.BYTES; i++) {
      buffer.putLong(offset + i);
    }

    buffer.clear();
    return buffer;
  }

  public static ByteBuffer generateDataByTime(int size) {
    long currentTime = System.currentTimeMillis();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    for (int i = 0; i < size / Long.BYTES; i++) {
      buffer.putLong(currentTime);
    }

    buffer.clear();
    return buffer;
  }

  public static boolean checkDataByTime(byte[] data, int offsetInData, int length) {
    return checkDataByTime(ByteBuffer.wrap(data, offsetInData, length));
  }

  public static boolean checkDataByTime(ByteBuffer data) {
    ByteBuffer duplicate = data.duplicate();
    long startTime = 0;
    boolean mismatch = false;
    for (int i = 0; i < duplicate.remaining() / Long.BYTES; i++) {
      long value = duplicate.getLong();
      if (i == 0) {
        startTime = value;
        logger.debug("the page is write at time: {}={}", startTime, longTimeToString(startTime));
        continue;
      }

      if (value != startTime) {
        logger.error("time: {}={}, expected time: {}={}", startTime, longTimeToString(startTime),
            value,
            longTimeToString(value));
        mismatch = true;
      }
    }

    return !mismatch;
  }

  public static boolean checkDataByOffset(byte[] data, int offsetInData, int length, long offset) {
    return checkDataByOffset(ByteBuffer.wrap(data, offsetInData, length), offset);
  }

  public static boolean checkDataByOffset(ByteBuffer data, long offset) {
    ByteBuffer duplicate = data.duplicate();
    for (int i = 0; i < duplicate.remaining() / Long.BYTES; i++) {
      long value = duplicate.getLong();
      if (value != (offset + i)) {
        logger.error("originValue: {}, expected: {}, index: {}", value, offset + i, i);
        return false;
      }
    }

    return true;
  }

  public static String longTimeToString(long time) {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
    String value = null;
    try {
      value = formatter.format(new Date(time));
    } catch (Exception e) {
      logger.warn("caught an exception", e);
    }

    return value;
  }
}
