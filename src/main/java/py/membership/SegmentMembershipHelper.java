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
