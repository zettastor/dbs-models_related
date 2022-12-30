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

package py.volume;

import org.apache.commons.lang3.NotImplementedException;
import py.thrift.share.VolumeStatusThrift;

public enum VolumeStatus {
  /**
   * ToBeCreated status transition: one segment unit report ToBeCreated---------------->Creating
   * Creating Timeout ToBeCreated------------------------>deleting User delete volume
   * ToBeCreated------------------------>deleting.
   */
  ToBeCreated(1) {
    @Override
    public VolumeStatus getNextVolumeStatus(VolumeMetadata volume) {
      if (volume.getSegmentTableSize() > 0) { //data node has report segment unit
        return Creating;
      } else if (isTimeout(
          volume)) { //Creating timeout, maybe data node not receive msg from control center
        return Deleting;
      }
      return this;  //Continue in toBeCreated state;
    }

    @Override
    public boolean isTimeout(VolumeMetadata volume) {
      return volume.isVolumeCreateTimeout();
    }

    public VolumeStatusThrift getVolumeStatusThrift() {
      return VolumeStatusThrift.ToBeCreated;
    }
  },

  /**
   * Creating status transition: All segment is OK Creating-------------------------->available User
   * delete volume Creating-------------------------->deleting  this transition is driven by user.
   */
  Creating(2) {
    @Override
    public VolumeStatus getNextVolumeStatus(VolumeMetadata volume) {
      if (volume.isAllSegmentsAvailable()) {
        return Available; //all the segment is OK, turn to Available status
      } else if (isTimeout(volume)) {
        return Deleting;
      }
      return this; //keep in creating status, no timeout
    }

    @Override
    public boolean isTimeout(VolumeMetadata volume) {
      return volume.isVolumeCreatingTimeout();
    }

    public VolumeStatusThrift getVolumeStatusThrift() {
      return VolumeStatusThrift.Creating;
    }
  },

  /**
   * Available status transition: not all segment is ok Available--------------------->Unavailable
   * User delete volume Available------------------------->deleting this transition is driven by
   * user.
   */
  Available(3) {
    @Override
    public VolumeStatus getNextVolumeStatus(VolumeMetadata volume) {
      if (!volume.isAllSegmentsAvailable()) { //not all the segment is OK, turn to unavailable
        return Unavailable;
      }

      if (volume.isStable()) {
        return Stable;
      }
      return this;
    }

    @Override
    public boolean isAvailable() {
      return true;
    }

    public VolumeStatusThrift getVolumeStatusThrift() {
      return VolumeStatusThrift.Available;
    }
  },

  /**
   * Unavailable status transition.
   *
   * <p>all segments are OK Unavailable---------------------->Available User delete volume
   * Unavailable---------------------->deleting User fix volume Unavailable----------------->Fixing
   */
  Unavailable(4) {
    @Override
    public VolumeStatus getNextVolumeStatus(VolumeMetadata volume) {
      if (volume.isAllSegmentsAvailable()) {
        return Available;
      }
      return this;
    }

    public VolumeStatusThrift getVolumeStatusThrift() {
      return VolumeStatusThrift.Unavailable;
    }
  },

  /**
   * Deleting status transition: all segments are deleting Deleting------------------------->Deleted
   * one or more segments are in deleted Deleting------------------------->Dead.
   */
  Deleting(5) {
    @Override
    public VolumeStatus getNextVolumeStatus(VolumeMetadata volume) {
      // no segment in it or someone segment is in dead, it turn to Dead
      int segmentCount = (int) (volume.getVolumeSize() / volume.getSegmentSize());
      if (volume.getSegmentTableSize() != segmentCount || volume.isSomeSegmentInDead()
          || volume.getSomeSegmentHaveUnitOrNot()) {
        return Dead;
      }

      // all segment are in deleting status, turn in Deleted status;
      if (volume.isAllSegmentInDeleting()) {
        return Deleted;
      }

      return this;
    }

    public VolumeStatusThrift getVolumeStatusThrift() {
      return VolumeStatusThrift.Deleting;
    }
  },

  /**
   * Deleted status transition one or more segments are deleted Deleted---------------->Dead.
   */
  Deleted(6) {
    @Override
    public VolumeStatus getNextVolumeStatus(VolumeMetadata volume) {
      if (volume.isSomeSegmentInDead()) {
        return Dead;
      }
      return this;
    }

    public VolumeStatusThrift getVolumeStatusThrift() {
      return VolumeStatusThrift.Deleted;
    }
  },

  /**
   * this statu mean volume is recovering from deleting or deleted status. User can get an volume in
   * deleting back.
   *
   * <p>volume become available again Undeleting-------------------------> Available recover failed
   * Undeleting---------------------------> deleting
   */
  Recycling(7) {
    @Override
    public VolumeStatus getNextVolumeStatus(VolumeMetadata volume) {
      if (volume.isAllSegmentsAvailable()) {
        return Available;
      }

      return this;
    }

    public VolumeStatusThrift getVolumeStatusThrift() {
      return VolumeStatusThrift.Recycling;
    }
  },

  /**
   * Fixing ---------------------------> Available Fixing ---------timeout-----------> Unavailable.
   */
  Fixing(8) {
    @Override
    public VolumeStatus getNextVolumeStatus(VolumeMetadata volume) {
      if (volume.isAllSegmentsAvailable()) {
        return Available;
      } else if (isTimeout(volume)) {
        return Unavailable;
      }
      return this;
    }

    @Override
    public boolean isTimeout(VolumeMetadata volume) {
      return volume.isFixVolumeTimeout();
    }

    public VolumeStatusThrift getVolumeStatusThrift() {
      return VolumeStatusThrift.Fixing;
    }
  },

  Dead(9) {
    public VolumeStatusThrift getVolumeStatusThrift() {
      return VolumeStatusThrift.Dead;
    }
  },

  /**
   * Available status transition: not all segment is ok Stable   ------------------>Unavailable.
   *
   * <p>may be some unit not good Stable   ------------------------->Available
   *
   * <p>the volume is Stable Stable Available------------------------->Stable
   */
  Stable(10) {
    @Override
    public VolumeStatus getNextVolumeStatus(VolumeMetadata volume) {
      if (!volume.isAllSegmentsAvailable()) { //not all the segment is OK, turn to unavailable
        return Unavailable;
      }

      if (volume.isStable()) { //is stable
        return this;
      } else {
        return Available;
      }
    }

    @Override
    public boolean isAvailable() {
      return true;
    }

    public VolumeStatusThrift getVolumeStatusThrift() {
      return VolumeStatusThrift.Stable;
    }
  };

  private final int value;

  VolumeStatus(int value) {
    this.value = value;
  }

  public static VolumeStatus findByValue(int value) {
    switch (value) {
      case 1:
        return ToBeCreated;
      case 2:
        return Creating;
      case 3:
        return Available;
      case 4:
        return Unavailable;
      case 5:
        return Deleting;
      case 6:
        return Deleted;
      case 7:
        return Recycling;
      case 8:
        return Fixing;
      case 9:
        return Dead;
      case 10:
        return Stable;

      default:
        return null;
    }
  }

  public static VolumeStatus findByValue(String value) {
    if (value == null) {
      return null;
    }

    if (value.equals("ToBeCreated")) {
      return ToBeCreated;
    } else if (value.equals("Creating")) {
      return Creating;
    } else if (value.equals("Available")) {
      return Available;
    } else if (value.equals("Unavailable")) {
      return Unavailable;
    } else if (value.equals("Deleting")) {
      return Deleting;
    } else if (value.equals("Deleted")) {
      return Deleted;
    } else if (value.equals("Recycling")) {
      return Dead;
    } else if (value.equals("Fixing")) {
      return Dead;
    } else if (value.equals("Dead")) {
      return Dead;
    } else if (value.equals("Stable")) {
      return Stable;
    } else {
      return null;
    }
  }

  public int getValue() {
    return value;
  }

  //get next status of volume. Almost status will override this function
  public VolumeStatus getNextVolumeStatus(VolumeMetadata volume) {
    return this;
  }

  public boolean isTimeout(VolumeMetadata volume) {
    return false;
  }

  public boolean isAvailable() {
    return false;
  }

  public VolumeStatusThrift getVolumeStatusThrift() {
    throw new NotImplementedException("not support value=" + value);
  }
}
