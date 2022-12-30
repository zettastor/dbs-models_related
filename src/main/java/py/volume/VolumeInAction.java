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

/**
 * modified by David Wang for volume status restructure The number original volumes status is 10,
 * now reduce to 7 Volume status transition in every value override method of getNextVolumeAction.
 */
public enum VolumeInAction {
  /**
   * CREATING, EXTENDING, DELETING, RECYCLING, FIXING, CLONING, BEINGCLONED, MOVING, BEINGMOVED,
   * COPYING, BEINGCOPIED, MOVEONLINEMOVING, MOVEONLINEBEINGMOVED, NULL.
   *
   * <p>CREATING--> DELETING When the status of the volume becomes DELETING, the status changes
   * CREATING--> NULL When the status of the volume is Available and unAvailable, the status
   * changes
   *
   * <p>EXTENDING: EXTENDING-> DELETING When the status of the volume becomes DELETING, the status
   * changes EXTENDING-> NULL When the status of the volume is Available and unAvailable, the status
   * changes
   *
   * <p>DELETING: DELETING--> NULL When the status of the volume becomes Dead and Deleted
   * DELETING->
   * RECYCLING When the status of the volume changes to Recycling
   *
   * <p>RECYCLING: RECYCLING--> NULL When the status of the volume becomes Dead, Deleted, Available
   *
   * <p>FIXING: Not consider
   *
   * <p>CLONING: CLONING--> NULL   lazy: When the volume status becomes Available sync: When the
   * volume status is Available and the clone of its own data is complete
   *
   * <p>BEINGCLONED: CLONED--> NULL      lazy: When the status of the cloned volume B becomes
   * Available sync: When the cloned volume B status is Available and B data clone is completed
   *
   * <p>MOVING: MOVING--> NULL   lazy: sync:
   */
  CREATING(1) {
    @Override
    public VolumeInAction getNextVolumeAction(VolumeMetadata volume) {
      switch (volume.getVolumeStatus()) {
        case Creating:
        case ToBeCreated:
          return this;

        case Deleting:
          return DELETING;

        default:
          //other status set action null
          return NULL;
      }
    }

    @Override
    public ExceptionType checkOperation(OperationFunctionType operationFunctionType) {
      switch (operationFunctionType) {
        case extendVolume:
        case recycleVolume:
        case fixVolume:
        case launchDriver:
          return ExceptionType.VolumeNotAvailableExceptionThrift;

        default:
          return null;
      }
    }
  },

  /**
   * Creating status transition.
   *
   * <p>Deleting-------------------------->DELETING  deleting  this transition is driven by user
   * User delete volume EXTENDING-------------------------->EXTENDING
   */
  EXTENDING(2) {
    @Override
    public VolumeInAction getNextVolumeAction(VolumeMetadata volume) {
      switch (volume.getVolumeStatus()) {
        case Deleting:
          return DELETING;

        default:
          if (volume.getExtendingSize() > 0) {
            return this; //still in extending
          } else {
            return NULL;  //extend ok, set null
          }
      }
    }

    @Override
    public ExceptionType checkOperation(OperationFunctionType operationFunctionType) {
      switch (operationFunctionType) {
        case deleteVolume:
        case extendVolume:
        case recycleVolume:
        case fixVolume:
        case launchDriver:
          /* when volume in EXTENDING, can not do this */
          return ExceptionType.VolumeInExtendingExceptionThrift;

        default:
          return null;
      }
    }
  },

  /**
   * Available status transition.
   *
   * <p>Recycling------------------------->RECYCLING User delete volume
   * Deleting------------->DELETING
   */
  DELETING(3) {
    @Override
    public VolumeInAction getNextVolumeAction(VolumeMetadata volume) {
      switch (volume.getVolumeStatus()) {
        case Recycling:
          return RECYCLING;

        case Deleting:
          return this;

        default:
          //other volume status set action null
          return NULL;
      }
    }

    @Override
    public ExceptionType checkOperation(OperationFunctionType operationFunctionType) {
      switch (operationFunctionType) {
        case extendVolume:
        case deleteVolume:
        case fixVolume:
        case launchDriver:

          /* when volume in DELETING, can not do this */
          return ExceptionType.VolumeDeletingExceptionThrift;

        default:
          return null;
      }
    }
  },

  /**
   * Unavailable status transition.
   *
   * <p>Recycling------------------------>RECYCLING
   *
   * <p>other ---------------------------->NULL
   */
  RECYCLING(4) {
    @Override
    public VolumeInAction getNextVolumeAction(VolumeMetadata volume) {
      switch (volume.getVolumeStatus()) {
        case Recycling:
          return this; //
        default:
          return NULL; //OK or not
      }
    }

    @Override
    public ExceptionType checkOperation(OperationFunctionType operationFunctionType) {
      switch (operationFunctionType) {
        case extendVolume:
        case recycleVolume:
        case fixVolume:
        case launchDriver:
          return ExceptionType.VolumeCyclingExceptionThrift;

        default:
          return null;
      }
    }
  },

  /**
   * not do it this time.
   */
  FIXING(5) {
    @Override
    public VolumeInAction getNextVolumeAction(VolumeMetadata volume) {
      //TODO: not do it this time
      return NULL;
    }
  },
  
  NULL(14) {
    @Override
    public VolumeInAction getNextVolumeAction(VolumeMetadata volume) {
      switch (volume.getVolumeStatus()) {
        case Deleting:
          return DELETING;

        default:
          return this;
      }
    }
  };

  private final int value;

  VolumeInAction(int value) {
    this.value = value;
  }

  public static VolumeInAction findByValue(int value) {
    switch (value) {
      case 1:
        return CREATING;
      case 2:
        return EXTENDING;
      case 3:
        return DELETING;
      case 4:
        return RECYCLING;
      case 5:
        return FIXING;
      default:
        return NULL;

    }
  }

  public static VolumeInAction findByValue(String value) {
    if (value == null) {
      return null;
    }

    if (value.equals("CREATING")) {
      return CREATING;
    } else if (value.equals("EXTENDING")) {
      return EXTENDING;
    } else if (value.equals("DELETING")) {
      return DELETING;
    } else if (value.equals("RECYCLING")) {
      return RECYCLING;
    } else if (value.equals("FIXING")) {
      return FIXING;
    } else {
      return NULL;
    }
  }

  public int getValue() {
    return value;
  }

  //get next status of volume. Almost status will override this function
  public VolumeInAction getNextVolumeAction(VolumeMetadata volume) {
    return this;
  }

  public ExceptionType checkOperation(OperationFunctionType operationFunctionType) {
    return null;
  }
}
