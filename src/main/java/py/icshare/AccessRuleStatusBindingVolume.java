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
