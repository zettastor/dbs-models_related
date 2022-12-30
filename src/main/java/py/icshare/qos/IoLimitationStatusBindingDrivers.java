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
