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

package py.volume;

public enum VolumeExtendStatus {
  /**
   * ToBeCreated status transition: one segment unit report ToBeCreated------------------->Creating
   * Creating Timeout ToBeCreated------------------------>deleting User delete volume
   * ToBeCreated------------------------>deleting.
   */
  ToBeCreated(1) {
    @Override
    public VolumeExtendStatus getNextVolumeExtendStatus(VolumeMetadata volume) {
      if (volume.getExtendSegmentTableSize() > 0) { //data node has report segment unit
        return Creating;
      } else if (isTimeout(
          volume)) { //Creating timeout, maybe data node not receive msg from control center
        return Deleting;
      }
      return this;  //Continue in toBeCreated state;
    }

    @Override
    public boolean isTimeout(VolumeMetadata volume) {
      return volume.isVolumeExtendTimeout();
    }

  },

  /**
   * Creating status transition: All segment is OK Creating-------------------------->available User
   * delete volume Creating-------------------------->deleting  this transition is driven by user.
   */
  Creating(2) {
    @Override
    public VolumeExtendStatus getNextVolumeExtendStatus(VolumeMetadata volume) {
      if (volume.isAllExtendSegmentsAvailable()) {
        return Available; //all the segment is OK, turn to Available status
      } else if (isTimeout(volume)) {
        return Deleting;
      }
      return this; //keep in creating status, no timeout
    }

    @Override
    public boolean isTimeout(VolumeMetadata volume) {
      return volume.isVolumeExtendTimeout();
    }

  },

  /**
   * Available status transition: not all segment is ok Available------------------->Unavailable
   * User delete volume Available------------------------->deleting this transition is driven by
   * user.
   */
  Available(3) {
    @Override
    public VolumeExtendStatus getNextVolumeExtendStatus(VolumeMetadata volume) {
      if (!volume.isAllExtendSegmentsAvailable()) { //not all the segment is OK, turn to unavailable
        return Unavailable;
      }
      return this;
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
    public VolumeExtendStatus getNextVolumeExtendStatus(VolumeMetadata volume) {
      if (volume.isAllExtendSegmentsAvailable()) {
        return Available;
      } else if (isTimeout(volume)) {
        return Deleting;
      }
      return this;
    }

    @Override
    public boolean isTimeout(VolumeMetadata volume) {
      return volume.isVolumeExtendTimeout();
    }

  },

  /**
   * Deleting status transition: all segments are deleting Deleting------------------------->Deleted
   * one or more segments are in deleted Deleting------------------------->Dead.
   */
  Deleting(5) {
    @Override
    public VolumeExtendStatus getNextVolumeExtendStatus(VolumeMetadata volume) {
      return this;
    }

  };

  /**
   * Deleted status transition one or more segments are deleted Deleted------------------>Dead.
   */
  private final int value;

  VolumeExtendStatus(int value) {
    this.value = value;
  }

  public static VolumeExtendStatus findByValue(int value) {
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
      default:
        return null;
    }
  }

  public static VolumeExtendStatus findByValue(String value) {
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
    } else {
      return null;
    }
  }

  public int getValue() {
    return value;
  }

  //get next status of volume. Almost status will override this function
  public VolumeExtendStatus getNextVolumeExtendStatus(VolumeMetadata volume) {
    return this;
  }

  public boolean isTimeout(VolumeMetadata volume) {
    return false;
  }
}
