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

package py.archive.brick;

public enum BrickStatus {
  allocated(0),
  shadowPageAllocated(1),
  free(2);

  private int value;

  BrickStatus(int value) {
    this.value = value;
  }

  public static BrickStatus findByValue(int value) {
    switch (value) {
      case 0:
        return allocated;
      case 1:
        return shadowPageAllocated;
      case 2:
        return free;
      default:
        return null;
    }
  }

  public int getValue() {
    return value;
  }
}
