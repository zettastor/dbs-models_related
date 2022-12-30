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

/**
 * status of member.
 * if some status is down, means that it is all down(read and write)
 * but if some status is read down, means only read down
 */
public enum MemberIoStatus {
  Primary(1) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.Primary) {
        return true;
      }
      return false;
    }
  },

  Secondary(2) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.Secondary) {
        return true;
      }
      return false;
    }
  },

  JoiningSecondary(3) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.JoiningSecondary) {
        return true;
      }
      return false;
    }
  },

  Arbiter(4) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.Arbiter) {
        return true;
      }
      return false;
    }
  },

  PrimaryDown(5) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.Primary) {
        return true;
      }
      return false;
    }

    @Override
    public boolean isDown() {
      return true;
    }

    @Override
    public boolean isWriteDown() {
      return true;
    }

    @Override
    public boolean isReadDown() {
      return true;
    }
  },

  SecondaryDown(6) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.Secondary) {
        return true;
      }
      return false;
    }

    @Override
    public boolean isDown() {
      return true;
    }

    @Override
    public boolean isWriteDown() {
      return true;
    }

    @Override
    public boolean isReadDown() {
      return true;
    }
  },

  JoiningSecondaryDown(7) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.JoiningSecondary) {
        return true;
      }
      return false;
    }

    @Override
    public boolean isDown() {
      return true;
    }

    @Override
    public boolean isWriteDown() {
      return true;
    }

    @Override
    public boolean isReadDown() {
      return true;
    }
  },

  ArbiterDown(8) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.Arbiter) {
        return true;
      }
      return false;
    }

    @Override
    public boolean isDown() {
      return true;
    }

    @Override
    public boolean isWriteDown() {
      return true;
    }

    @Override
    public boolean isReadDown() {
      return true;
    }
  },

  InactiveSecondary(9) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.InactiveSecondary) {
        return true;
      }
      return false;
    }

    @Override
    public boolean isDown() {
      return true;
    }

    @Override
    public boolean isWriteDown() {
      return true;
    }

    @Override
    public boolean isReadDown() {
      return true;
    }
  },

  ExternalMember(10) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.ExternalMember) {
        return true;
      }
      return false;
    }

    @Override
    public boolean isDown() {
      return true;
    }

    @Override
    public boolean isWriteDown() {
      return true;
    }

    @Override
    public boolean isReadDown() {
      return true;
    }
  },

  /**
   * only secondary can turn to TempPrimary.
   */
  TempPrimary(11) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.TempPrimary
          || memberIoStatus == MemberIoStatus.Secondary) {
        return true;
      }
      return false;
    }
  },

  /**
   * only primary can turn to UnstablePrimary.
   */
  UnstablePrimary(12) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.UnstablePrimary
          || memberIoStatus == MemberIoStatus.Primary) {
        return true;
      }
      return false;
    }
  },

  /**
   * mark this secondary read down, but only read down, write still online.
   */
  SecondaryReadDown(13) {
    @Override
    public boolean match(MemberIoStatus memberIoStatus) {
      if (memberIoStatus == MemberIoStatus.SecondaryReadDown
          || memberIoStatus == MemberIoStatus.Secondary) {
        return true;
      }
      return false;
    }

    @Override
    public boolean isReadDown() {
      return true;
    }
  };

  private int value;

  MemberIoStatus(int value) {
    this.value = value;
  }

  public boolean match(MemberIoStatus memberIoStatus) {
    return false;
  }

  public boolean isPrimary() {
    return match(MemberIoStatus.Primary);
  }

  public boolean isSecondary() {
    return match(MemberIoStatus.Secondary);
  }

  public boolean isJoiningSecondary() {
    return match(MemberIoStatus.JoiningSecondary);
  }

  public boolean isArbiter() {
    return match(MemberIoStatus.Arbiter);
  }

  public boolean isTempPrimary() {
    return match(MemberIoStatus.TempPrimary);
  }

  public boolean isUnstablePrimary() {
    return match(MemberIoStatus.UnstablePrimary);
  }

  public boolean isDown() {
    return false;
  }

  public boolean isReadDown() {
    return false;
  }

  public boolean isWriteDown() {
    return false;
  }

  /**
   * follow method do not need @override.
   */
  public boolean sameRole(MemberIoStatus nextMemberIoStatus) {
    if (isPrimary()) {
      if (nextMemberIoStatus.isPrimary()) {
        return true;
      }
    } else if (isSecondary()) {
      if (nextMemberIoStatus.isSecondary()) {
        return true;
      }
    } else if (isJoiningSecondary()) {
      if (nextMemberIoStatus.isJoiningSecondary()) {
        return true;
      }
    } else if (isArbiter()) {
      if (nextMemberIoStatus.isArbiter()) {
        return true;
      }
    } else {
      /*
       * can not show up this situation
       */
      Validate.isTrue(false, "can not show up" + this);
      return false;
    }
    return false;
  }

  public boolean canMoveTo(MemberIoStatus nextMemberIoStatus) {
    if (!sameRole(nextMemberIoStatus)) {
      return false;
    }

    int myDownCount = 0;
    int nextDownCount = 0;
    if (isReadDown()) {
      myDownCount++;
    }
    if (isWriteDown()) {
      myDownCount++;
    }

    if (nextMemberIoStatus.isReadDown()) {
      nextDownCount++;
    }

    if (nextMemberIoStatus.isWriteDown()) {
      nextDownCount++;
    }

    if (nextDownCount > myDownCount) {
      return true;
    } else if (nextDownCount == myDownCount) {
      /*
       * can move when read and write are not down, or they both are down
       */
      if (nextDownCount == 0 || nextDownCount == 2) {
        return true;
      } else {
        return false;
      }
    }

    return false;
  }

  public MemberIoStatus mergeMemberIoStatus(MemberIoStatus nextMemberIoStatus) {
    if (!sameRole(nextMemberIoStatus)) {
      return this;
    }

    if (isReadDown() && isWriteDown()) {
      return this;
    }

    int myDownCount = 0;
    int nextDownCount = 0;
    if (isReadDown()) {
      myDownCount++;
    }
    if (isWriteDown()) {
      myDownCount++;
    }

    if (nextMemberIoStatus.isReadDown()) {
      nextDownCount++;
    }

    if (nextMemberIoStatus.isWriteDown()) {
      nextDownCount++;
    }

    if (nextDownCount > myDownCount) {
      return nextMemberIoStatus;
    } else if (nextDownCount == myDownCount) {
      if (myDownCount == 0) {
        return this;
      } else {
        Validate.isTrue(myDownCount == 1);
        if ((isReadDown() && nextMemberIoStatus.isReadDown()) || (isWriteDown()
            && nextMemberIoStatus
            .isWriteDown())) {
          return this;
        } else {
          if (isPrimary()) {
            return PrimaryDown;
          } else if (isSecondary()) {
            return SecondaryDown;
          } else if (isJoiningSecondary()) {
            return JoiningSecondaryDown;
          } else if (isArbiter()) {
            return ArbiterDown;
          } else {
            /*
             * can not show up this situation
             */
            Validate.isTrue(false, "can not show up" + this);
            return InactiveSecondary;
          }
        }
      }
    } else {
      return this;
    }
  }

}
