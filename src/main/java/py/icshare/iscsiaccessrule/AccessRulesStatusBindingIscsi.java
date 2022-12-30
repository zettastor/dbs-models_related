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
