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
