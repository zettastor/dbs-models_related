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

package py.membership;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.instance.InstanceId;

public class SegmentMembershipHelper {
  private static final Logger logger = LoggerFactory.getLogger(SegmentMembershipHelper.class);

  /**
   * Check if it is ok to update the current membership to the higher and new membership.
   *
   * <p>1. If the new membership contains myself, it is ok
   *
   * <p>2. If the new membership does not contain myself, do the following: 2.a. if new membership
   * does not miss any member, it is NOT ok. 2.b. if new membership misses one or more than one
   * members, it is ok
   *
   * <p>Assuming the new membership is not less than the current membership.
   *
   * @param totalMembers the number of members in the membership which doesn't miss any member
   */
  public static boolean okToUpdateToHigherMembership(SegmentMembership higherMembership,
      SegmentMembership currentMembership, InstanceId myself, int totalMembers) {
    Validate.notNull(currentMembership);
    Validate.notNull(higherMembership);
    Validate.notNull(myself);
    Validate.isTrue(totalMembers > 0);

    if (!higherMembership.contain(myself) && higherMembership.aliveSize() == totalMembers) {
      logger.error(
          "I: {} can't update my membership: {} to the new membership: {} since the new membership"
              + " does not contain me and it is full",
          myself, currentMembership, higherMembership);
      return false;
    } else {
      return true;
    }
  }
}
