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

package py.driver;

/**
 * An enum to list all driver state event to change driver status.
 */
public enum DriverStateEvent {
  ACCEPT_MOUNT_REQUEST(1), ACCEPT_UMOUNT_REQUEST(2), DRIVER_SERVER_PROCESS_UP(3), DRIVER_PROCESS_UP(
      4), DRIVER_PROCESS_DOWN(5), TIMEOUT(6), REPORTTIMEOUT(
      7), NEWREPORT(8);

  private int value;

  private DriverStateEvent(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }
}
