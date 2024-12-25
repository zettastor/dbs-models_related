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
