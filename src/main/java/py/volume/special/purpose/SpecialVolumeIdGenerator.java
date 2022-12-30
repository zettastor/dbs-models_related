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
