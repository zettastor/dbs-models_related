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

public enum PortalType {
  IPV4(1),

  IPV6(2);

  private final int value;

  private PortalType(int value) {
    this.value = value;
  }

  /**
   * Find corresponding instance of {@link PortalType} to the given value.
   *
   * @return instance of {@link PortalType} if found, otherwise null value will be returned.
   */
  public static PortalType findByValue(int value) {
    for (PortalType portType : PortalType.values()) {
      if (value == portType.value) {
        return portType;
      }
    }

    return null;
  }

  public int getValue() {
    return value;
  }
}
