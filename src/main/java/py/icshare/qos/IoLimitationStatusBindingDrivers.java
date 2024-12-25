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

package py.icshare.qos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum IoLimitationStatusBindingDrivers {
  /**
   * rule exists and not applied to any drivers.
   */
  FREE(1),
  /**
   * is being applied to some drivers.
   */
  APPLING(2),
  /**
   * is already applied to some drivers.
   */
  APPLIED(3),
  /**
   * is being canceled from some drivers.
   */
  CANCELING(4);

  private static final Logger logger = LoggerFactory
      .getLogger(IoLimitationStatusBindingDrivers.class);
  private int value;

  private IoLimitationStatusBindingDrivers(int value) {
    this.setValue(value);
  }

  public static IoLimitationStatusBindingDrivers findByName(String name) {
    switch (name) {
      case "FREE":
        return FREE;
      case "APPLING":
        return APPLING;
      case "APPLIED":
        return APPLIED;
      case "CANCELING":
        return CANCELING;
      default:
        logger.error("can not find value by name:{}", name);
        return null;
    }
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }
}
