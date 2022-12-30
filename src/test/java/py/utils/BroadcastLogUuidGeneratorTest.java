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

package py.utils;

import static org.testng.Assert.assertEquals;

import java.util.Random;
import org.junit.Assert;
import org.junit.Test;
import py.common.RequestIdBuilder;
import py.informationcenter.Utils;
import py.test.TestBase;

public class BroadcastLogUuidGeneratorTest extends TestBase {
  @Test
  public void basic() throws Exception {
    int testSize = 100;

    Random random = new Random(System.currentTimeMillis());
    int high = random.nextInt();
    int low = Integer.MAX_VALUE - random.nextInt(testSize / 2);

    BroadcastLogUuidGenerator generator = new BroadcastLogUuidGenerator(Integer.SIZE, Integer.SIZE,
        high, low) {
      @Override
      public long randomWatchDog() {
        return 0;
      }
    };

    // test over flow
    long uuid = generator.generateUuid();
    low++;
    long highPart = generator.parseHighPartial(uuid);
    long lowPart = generator.parseLowerPartial(uuid);
    for (int i = 0; i < testSize; i++) {
      long newUuid = generator.generateUuid();
      logger.info("high {}, low {}, val {}", high, low, newUuid);
      if (low + 1 < low) {
        logger.info("low part overflowed {}", low);
      }
      assertEquals(1, newUuid - uuid);
      uuid = newUuid;
      low++;

      highPart = generator.parseHighPartial(uuid);
      lowPart = generator.parseLowerPartial(uuid);
      assertEquals(high, (int) highPart);
      assertEquals(low, (int) lowPart);
    }

    // test negative to positive
    low = -random.nextInt(testSize / 2);
    generator = new BroadcastLogUuidGenerator(Integer.SIZE, Integer.SIZE, high, low) {
      @Override
      public long randomWatchDog() {
        return 0;
      }
    };
    uuid = generator.generateUuid();
    low++;
    for (int i = 0; i < testSize; i++) {
      long newUuid = Utils.generateUuid(high, low + 1);
      logger.info("high {}, low {}, val {}", high, low + 1, newUuid);
      if (low + 1 == 0) {
        logger.info("low part turning positive {}", low);
        long expectedDiff = -0xffffffffL; // - ((long) Integer.MAX_VALUE) * 2 - 1;
        assertEquals(expectedDiff, newUuid - uuid);
      } else {
        assertEquals(1, newUuid - uuid);
      }
      uuid = newUuid;
      low++;
    }
  }

  @Test
  public void testCompare() throws Exception {
    BroadcastLogUuidGenerator uuidGenerator = new BroadcastLogUuidGenerator(60, 4) {
      @Override
      public long randomWatchDog() {
        return 0;
      }
    };

    Assert.assertTrue(uuidGenerator.compare(14, 0) < 0);
    Assert.assertTrue(uuidGenerator.compare(15, 0) < 0);
    Assert.assertTrue(uuidGenerator.compare(14, 1) < 0);
    Assert.assertTrue(uuidGenerator.compare(13, 1) < 0);

    Assert.assertTrue(uuidGenerator.compare(0, 14) > 0);
    Assert.assertTrue(uuidGenerator.compare(1, 14) > 0);
    Assert.assertTrue(uuidGenerator.compare(0, 15) > 0);
    Assert.assertTrue(uuidGenerator.compare(1, 13) > 0);

    uuidGenerator = new BroadcastLogUuidGenerator(56, 8) {
      @Override
      public long randomWatchDog() {
        return 0;
      }
    };

    Assert.assertTrue(uuidGenerator.compare(214, 0) < 0);
    Assert.assertTrue(uuidGenerator.compare(215, 0) < 0);
    Assert.assertTrue(uuidGenerator.compare(214, 1) < 0);
    Assert.assertTrue(uuidGenerator.compare(213, 1) < 0);

    Assert.assertTrue(uuidGenerator.compare(0, 214) > 0);
    Assert.assertTrue(uuidGenerator.compare(1, 214) > 0);
    Assert.assertTrue(uuidGenerator.compare(0, 215) > 0);
    Assert.assertTrue(uuidGenerator.compare(1, 213) > 0);
  }

  @Test
  public void testDiffBitsGenerator() throws Exception {
    testGeneratorWithSpecialBits(10, 54);
    testGeneratorWithSpecialBits(16, 48);
    testGeneratorWithSpecialBits(32, 32);
    testGeneratorWithSpecialBits(48, 16);
  }

  public void testGeneratorWithSpecialBits(int highBits, int lowerBits) throws Exception {
    final long highVal = RequestIdBuilder.get() >> lowerBits;
    BroadcastLogUuidGenerator uuidGenerator = new BroadcastLogUuidGenerator(highBits, lowerBits) {
      @Override
      public long randomWatchDog() {
        return highVal;
      }
    };

    long lowGudge = 0xffffffffffffffffL;
    lowGudge = ~(lowGudge << lowerBits);

    long lowVal = 0;

    long size = 10000;
    long uuid = 0;
    for (long i = lowVal; i < size + lowVal; i++) {
      long newUuid = uuidGenerator.generateUuid();
      if (uuid != 0) {
        Assert.assertEquals(uuid + 1, newUuid);
      }
      uuid = newUuid;

      long parseHighVal = uuidGenerator.parseHighPartial(uuid);
      Assert.assertEquals(highVal, parseHighVal);
      long parseLowerVal = uuidGenerator.parseLowerPartial(uuid);
      Assert.assertEquals(i + 1, parseLowerVal);
    }

    lowVal = lowGudge - 1000;
    uuidGenerator = new BroadcastLogUuidGenerator(highBits, lowerBits, highVal, lowVal) {
      @Override
      public long randomWatchDog() {
        return highVal;
      }
    };

    uuid = 0;
    for (long i = lowVal; i < size + lowVal; i++) {
      long newUuid = uuidGenerator.generateUuid();
      logger.warn("uuid : {}, high : {}, lower : {}, old uuid {}", Long.toHexString(newUuid),
          Long.toHexString(i + 1),
          Long.toHexString(lowGudge), Long.toHexString(uuid + 1));
      if (uuid != 0) {
        if (i == lowGudge) {
          Assert.assertEquals(uuid - lowGudge, newUuid);
        } else {
          Assert.assertEquals(uuid + 1, newUuid);
        }
      }
      uuid = newUuid;

      long parseHighVal = uuidGenerator.parseHighPartial(uuid);
      Assert.assertEquals(highVal, parseHighVal);
      long parseLowerVal = uuidGenerator.parseLowerPartial(uuid);
      if (i >= lowGudge) {
        Assert.assertEquals(i - lowGudge, parseLowerVal);
      } else {
        Assert.assertEquals(i + 1, parseLowerVal);
      }
    }

  }

  @Test
  public void testDefaultGenerator() throws Exception {
    int count = 10000;
    long uuid = BroadcastLogUuidGenerator.getInstance().generateUuid();
    do {
      long newUuid = BroadcastLogUuidGenerator.getInstance().generateUuid();
      assertEquals(uuid + 1, newUuid);
      uuid = newUuid;

      logger.warn("uuid {}", uuid);
    } while (--count > 0);
  }

  @Test
  public void performanceTest() throws Exception {
    long testSize = 100_000_000L;

    Random random = new Random(System.currentTimeMillis());
    int high = random.nextInt();
    int low = 0;

    BroadcastLogUuidGenerator generator = new BroadcastLogUuidGenerator(Integer.SIZE, Integer.SIZE,
        high, low) {
      @Override
      public long randomWatchDog() {
        return 0;
      }
    };
    long start = System.currentTimeMillis();
    for (long i = 0; i < testSize; i++) {
      generator.generateUuid();
    }
    logger.warn("count {}, cost {}", testSize, System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    for (long i = 0; i < testSize; i++) {
      Utils.generateUuid(high, low++);
    }
    logger.warn("count {}, cost {}", testSize, System.currentTimeMillis() - start);
  }
}
