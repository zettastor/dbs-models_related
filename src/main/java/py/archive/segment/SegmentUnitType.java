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

package py.archive.segment;

import org.apache.commons.lang3.NotImplementedException;
import py.thrift.share.SegmentUnitTypeThrift;

public enum SegmentUnitType {
  Normal(1) {
    @Override
    public SegmentUnitTypeThrift getSegmentUnitTypeThrift() {
      return SegmentUnitTypeThrift.Normal;
    }
  }, Arbiter(2) {
    @Override
    public SegmentUnitTypeThrift getSegmentUnitTypeThrift() {
      return SegmentUnitTypeThrift.Arbiter;
    }
  }, Flexible(3) {
    @Override
    public SegmentUnitTypeThrift getSegmentUnitTypeThrift() {
      return SegmentUnitTypeThrift.Flexible;
    }
  };

  private int value;

  SegmentUnitType(int value) {
    this.value = value;
  }

  public static SegmentUnitType findByValue(int value) {
    switch (value) {
      case 1:
        return Normal;
      case 2:
        return Arbiter;
      default:
        return null;
    }
  }

  public int getValue() {
    return value;
  }

  public SegmentUnitTypeThrift getSegmentUnitTypeThrift() {
    throw new NotImplementedException("not support value=" + value);
  }
}
