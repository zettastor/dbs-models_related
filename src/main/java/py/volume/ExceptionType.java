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

package py.volume;

public enum ExceptionType {
  VolumeIsCopingExceptionThrift(1),
  VolumeInExtendingExceptionThrift(5),
  volumeIsCloningExceptionThrift(6),
  VolumeDeletingExceptionThrift(7),
  VolumeCyclingExceptionThrift(8),
  VolumeNotAvailableExceptionThrift(9);

  private final int value;

  ExceptionType(int value) {
    this.value = value;
  }

  public static String findByValue(int value) {
    switch (value) {
      case 1:
        return "VolumeIsCopingExceptionThrift";
      case 5:
        return "VolumeInExtendingExceptionThrift";
      case 6:
        return "volumeIsCloningExceptionThrift";
      case 7:
        return "VolumeDeletingExceptionThrift";
      case 8:
        return "VolumeCyclingExceptionThrift";
      case 9:
        return "VolumeNotAvailableExceptionThrift";

      default:
        return null;
    }
  }
}
