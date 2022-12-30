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

public enum DriverType {
  //BOGUS type used to interrupt upgrade thread in DriverUpgradeProcessor class
  NBD(1), JSCSI(2), ISCSI(3), NFS(4), FSD(5), BOGUS(6);

  private final int value;

  private DriverType(int value) {
    this.value = value;
  }

  public static DriverType findByValue(int value) {
    switch (value) {
      case 1:
        return NBD;
      case 2:
        return JSCSI;
      case 3:
        return ISCSI;
      case 4:
        return NFS;
      case 5:
        return FSD;
      case 6:
        return BOGUS;
      default:
        return null;
    }
  }

  public static DriverType findByName(String name) {
    name = name.toUpperCase();

    if (name.equals(NBD.name())) {
      return NBD;
    }

    if (name.equals(JSCSI.name())) {
      return JSCSI;
    }

    if (name.equals(ISCSI.name())) {
      return ISCSI;
    }

    if (name.equals(NFS.name())) {
      return NFS;
    }

    if (name.equals(FSD.name())) {
      return FSD;
    }

    if (name.equals(BOGUS.name())) {
      return BOGUS;
    }
    return null;
  }

  public int getValue() {
    return value;
  }
}
