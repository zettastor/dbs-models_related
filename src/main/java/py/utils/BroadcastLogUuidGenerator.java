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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;

/**
 * a uuid generator just for broadcast log what is create by coordinator in write process. uuid
 * divided into two parts : * the high part like a watch dog, each newly coordinator has a watch dog
 * different from other coordinator * the lower part is a atomic increment number, when it reaches
 * the maximum value, it starts from 0.
 *
 * <p>i set an abstract function for it, so that the user can specify the generation method of
 * watch
 * dog. because the default method can be theoretically repeated, and it is only used because it is
 * simple
 */
public abstract class BroadcastLogUuidGenerator {
  protected static final Logger logger = LoggerFactory.getLogger(BroadcastLogUuidGenerator.class);
  private static final int DEFAULT_HIGH_BITS = Integer.SIZE;
  private static final int DEFAULT_LOWER_BITS = Integer.SIZE;
  private static final long ALL_SET_VAL = 0xFFFFFFFFFFFFFFFFL;
  private static final long ALL_CLEAR_VAL = 0x0L;
  private static final Object lock = new Object();
  private static BroadcastLogUuidGenerator defaultGenerator;
  private UuidWatchDogPartial highPartial;
  private UuidIncreasePartial lowerPartial;

  public BroadcastLogUuidGenerator() {
    this(DEFAULT_HIGH_BITS, DEFAULT_LOWER_BITS);
  }

  public BroadcastLogUuidGenerator(int highBits, int lowerBits) {
    long highVal = randomWatchDog();
    highPartial = new UuidWatchDogPartial(highBits, highVal);
    lowerPartial = new UuidIncreasePartial(lowerBits, 0L);
  }

  public BroadcastLogUuidGenerator(int highBits, int lowerBits, long highVal, long lowerVal) {
    highPartial = new UuidWatchDogPartial(highBits, highVal);
    lowerPartial = new UuidIncreasePartial(lowerBits, lowerVal);
  }

  public static BroadcastLogUuidGenerator getInstance() {
    if (defaultGenerator == null) {
      synchronized (lock) {
        if (defaultGenerator == null) {
          defaultGenerator = new BroadcastLogUuidGenerator() {
            @Override
            public long randomWatchDog() {
              return Math.abs((int) RequestIdBuilder.get());
            }
          };
        }
      }
    }

    return defaultGenerator;
  }

  public int compare(long v1, long v2) {
    long diff = Math.abs(v1 - v2);
    if (diff == 0) {
      return 0;
    } else if (diff >= lowerPartial.getJudge() / 2) {
      if (v1 < v2) {
        return 1;
      }
    } else {
      if (v1 > v2) {
        return 1;
      }
    }

    return -1;
  }

  public long generateUuid() {
    return (highPartial.getVal() << lowerPartial.getBits()) | lowerPartial.incrementAndGet();
  }

  public long parseHighPartial(long val) {
    return (val >> lowerPartial.getBits()) & highPartial.getJudge();
  }

  public long parseLowerPartial(long val) {
    return val & lowerPartial.getJudge();
  }

  public abstract long randomWatchDog();

  private class UuidPartial {
    private final int bits;
    private final long judge;
    private AtomicLong val = new AtomicLong();

    protected UuidPartial(int bits) {
      this(bits, ALL_CLEAR_VAL);
    }

    protected UuidPartial(int bits, long initVal) {
      Validate.isTrue(bits < Long.SIZE);
      this.bits = bits;

      judge = ~(ALL_SET_VAL << bits);
      val.set(judge & initVal);
    }

    protected long getJudge() {
      return judge;
    }

    public int getBits() {
      return bits;
    }

    public long getVal() {
      return val.get();
    }

  }

  private class UuidIncreasePartial extends UuidPartial {
    public UuidIncreasePartial(int bits) {
      super(bits);
    }

    public UuidIncreasePartial(int bits, long initVal) {
      super(bits, initVal);
    }

    protected long incrementAndGet() {
      long v = super.val.incrementAndGet();
      if (v > super.judge) {
        synchronized (lock) {
          v = super.val.incrementAndGet();
          if (v > super.judge) {
            logger.warn("reset uuid lower part from {} to 0", Long.toHexString(v));
            v = 0;
            super.val.set(v);
          }
        }
      }
      return v;
    }
  }

  private class UuidWatchDogPartial extends UuidPartial {
    public UuidWatchDogPartial(int bits) {
      super(bits);
    }

    public UuidWatchDogPartial(int bits, long initVal) {
      super(bits, initVal);
    }
  }
}
