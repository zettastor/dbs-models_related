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

package py.icshare;

public enum CheckStatus {
  OK(1), ERROR(2);

  private int value;

  private CheckStatus(int value) {
    this.value = value;
  }

  public static CheckStatus findByValue(int value) {
    switch (value) {
      case 1:
        return OK;
      case 2:
        return ERROR;
      default:
        return null;
    }
  }

  public static CheckStatus findByName(String name) {
    if (name == null) {
      return null;
    }

    if (name.equals("OK")) {
      return OK;
    } else if (name.equals("ERROR")) {
      return ERROR;
    } else {
      return null;
    }
  }

  public int getValue() {
    return value;
  }

}
