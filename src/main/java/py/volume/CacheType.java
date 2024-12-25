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
