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

package py.icshare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All status in relationship between access rules and volumes.
 */
public enum AccessRuleStatusBindingVolume {
  /**
   * volume access rule exists and not applied to any volume.
   */
  FREE(1),
  /**
   * volume is being applied to some volume.
   */
  APPLING(2),
  /**
   * volume is already applied to some voume.
   */
  APPLIED(3),
  /**
   * volume is being canceled from some volume.
   */
  CANCELING(4);

  private static final Logger logger = LoggerFactory.getLogger(AccessRuleStatusBindingVolume.class);
  private int value;

  private AccessRuleStatusBindingVolume(int value) {
    this.setValue(value);
  }

  public static AccessRuleStatusBindingVolume findByName(String name) {
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
