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

package py.volume.special.purpose;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import org.junit.Test;
import py.test.TestBase;

public class SpecialVolumeIdGeneratorTest extends TestBase {
  @Test
  public void test() {
    int testCount = 10_000;
    Random random = new Random(System.currentTimeMillis());

    for (int i = 0; i < testCount; i++) {
      String validName = generateVolumeName(true, random);
      String invalidName = generateVolumeName(false, random);

      long validId = SpecialVolumeIdGenerator.generateVolumeId(validName);
      long invalidId = SpecialVolumeIdGenerator.generateVolumeId(invalidName);

      logger.warn("valid   {} {}", validId, validName);
      logger.warn("invalid {} {}", invalidId, invalidName);

      assertTrue(SpecialVolumeIdGenerator.matches(validId));
      assertFalse(SpecialVolumeIdGenerator.matches(invalidId));

    }

  }

  private String generateVolumeName(boolean valid, Random random) {
    byte[] pre = new byte[random.nextInt(5)];
    random.nextBytes(pre);

    byte[] post = new byte[random.nextInt(5)];
    random.nextBytes(post);

    if (valid) {
      return new String(pre) + SpecialVolumeIdGenerator.magicName + new String(post);
    } else {
      String name = new String(pre) + "non-magic" + new String(post);
      if (name.toLowerCase().contains(SpecialVolumeIdGenerator.magicName)) {
        return generateVolumeName(false, random);
      } else {
        return name;
      }
    }

  }

}