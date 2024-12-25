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

import py.common.RequestIdBuilder;

public class SpecialVolumeIdGenerator {
  static final String magicName = "perf";
  static final int[] caredBits = new int[]{5, 7, 9, 11};

  public static long generateVolumeId(String volumeName) {
    long volumeId = RequestIdBuilder.get();

    if (volumeName != null && volumeName.toLowerCase().contains(magicName)) {
      volumeId = convert(volumeId, false);
    } else {
      volumeId = convert(volumeId, true);
    }

    return volumeId;
  }

  public static boolean matches(long volumeId) {
    for (int caredBit : caredBits) {
      if ((volumeId & 1 << caredBit) == 0) {
        return false;
      }
    }
    return true;
  }

  static long convert(long baseId, boolean reverse) {
    for (int caredBit : caredBits) {
      if (reverse) {
        baseId &= ~(1 << caredBit);
      } else {
        baseId |= 1 << caredBit;
      }
    }
    return baseId;
  }

}
