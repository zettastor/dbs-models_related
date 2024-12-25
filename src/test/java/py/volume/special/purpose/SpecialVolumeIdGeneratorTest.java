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