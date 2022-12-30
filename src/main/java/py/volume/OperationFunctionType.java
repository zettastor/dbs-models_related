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

public enum OperationFunctionType {
  extendVolume(1),
  deleteVolume(2),
  recycleVolume(3),
  fixVolume(4),
  launchDriver(10),
  umountDriver(11),

  //
  recycleVolumeInfo(14);

  private final int value;

  OperationFunctionType(int value) {
    this.value = value;
  }
}
