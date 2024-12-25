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

package py.icshare.iscsiaccessrule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All status in relationship between access rules and iscsi drivers.
 */
public enum AccessRulesStatusBindingIscsi {
  /**
   * iscsi access rule exists and not applied to any iscsi drivers.
   */
  FREE(1),
  /**
   * iscsi is being applied to some iscsi drivers.
   */
  APPLING(2),
  /**
   * iscsi is already applied to some iscsi drivers.
   */
  APPLIED(3),
  /**
   * iscsi is being canceled from some iscsi drivers.
   */
  CANCELING(4);

  private static final Logger logger = LoggerFactory
      .getLogger(py.icshare.iscsiaccessrule.AccessRulesStatusBindingIscsi.class);
  private int value;

  private AccessRulesStatusBindingIscsi(int value) {
    this.setValue(value);
  }

  public static AccessRulesStatusBindingIscsi findByName(String name) {
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
