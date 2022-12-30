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

import org.apache.commons.lang3.NotImplementedException;
import py.thrift.share.CacheTypeThrift;

public enum CacheType {
  NONE(0) {
    public CacheTypeThrift getCacheTypeThrift() {
      return CacheTypeThrift.NONE;
    }
  },

  SSD(1) {
    public CacheTypeThrift getCacheTypeThrift() {
      return CacheTypeThrift.SSD;
    }
  };

  private final int value;

  private CacheType(int value) {
    this.value = value;
  }

  public static CacheType findByValue(int value) {
    switch (value) {
      case 0:
        return NONE;
      case 1:
        return SSD;
      default:
        return null;
    }
  }

  public int getValue() {
    return value;
  }

  public CacheTypeThrift getCacheTypeThrift() {
    throw new NotImplementedException("not support the value=" + value);
  }
}
