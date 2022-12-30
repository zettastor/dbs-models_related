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

package py.driver;

/**
 * A enum-based state machine for driver status on different event.
 */
public enum DriverStatus {
  START(1, 5) {
    @Override
    public DriverStatus turnToNextStatusOnEvent(DriverStateEvent event) {
      switch (event) {
        case ACCEPT_MOUNT_REQUEST:
          return LAUNCHING;
        case REPORTTIMEOUT:
          return UNKNOWN;
        default:
          return this;
      }
    }
  },
  LAUNCHING(2, 2) {
    @Override
    public DriverStatus turnToNextStatusOnEvent(DriverStateEvent event) {
      switch (event) {
        case DRIVER_SERVER_PROCESS_UP:
          return SLAUNCHED;
        case DRIVER_PROCESS_UP:
          return LAUNCHED;
        case TIMEOUT:
          return UNAVAILABLE;
        case REPORTTIMEOUT:
          return UNKNOWN;
        default:
          return this;
      }
    }
  },
  /**
   * This is a immediate status for ISCSI driver which means nbd-server is launched but iscsi target
   * may not create.
   *
   * @deprecated this status is not used anymore
   */
  @Deprecated
  SLAUNCHED(3, 10) {
    @Override
    public DriverStatus turnToNextStatusOnEvent(DriverStateEvent event) {
      switch (event) {
        case DRIVER_PROCESS_UP:
          return LAUNCHED;
        case ACCEPT_MOUNT_REQUEST:
          return LAUNCHING;
        case ACCEPT_UMOUNT_REQUEST:
          return REMOVING;
        case REPORTTIMEOUT:
          return UNKNOWN;
        default:
          return this;
      }
    }
  },
  LAUNCHED(4, 1) {
    @Override
    public DriverStatus turnToNextStatusOnEvent(DriverStateEvent event) {
      switch (event) {
        case ACCEPT_MOUNT_REQUEST:
          return RECOVERING;
        case ACCEPT_UMOUNT_REQUEST:
          return REMOVING;
        case REPORTTIMEOUT:
          return UNKNOWN;
        default:
          return this;
      }
    }
  },
  REMOVING(5, 6) {
    @Override
    public DriverStatus turnToNextStatusOnEvent(DriverStateEvent event) {
      switch (event) {
        // case REPORTTIMEOUT:
        // return UNKNOWN;
        case ACCEPT_MOUNT_REQUEST:
          throw new IllegalStateException();
        default:
          return this;
      }
    }
  },
  /**
   * This is status for driver which represents the driver is not available for a while.
   *
   * @deprecated this status is not used anymore
   */
  @Deprecated
  UNAVAILABLE(6, 9) {
    @Override
    public DriverStatus turnToNextStatusOnEvent(DriverStateEvent event) {
      switch (event) {
        case ACCEPT_UMOUNT_REQUEST:
          return REMOVING;
        case REPORTTIMEOUT:
          return UNKNOWN;
        case ACCEPT_MOUNT_REQUEST:
          throw new IllegalStateException();
        default:
          return this;
      }
    }
  },
  UNKNOWN(7, 7) {
    @Override
    public DriverStatus turnToNextStatusOnEvent(DriverStateEvent event) {
      return this;
    }
  },
  ERROR(8, 8) {
    @Override
    public DriverStatus turnToNextStatusOnEvent(DriverStateEvent event) {
      return this;
    }
  },
  RECOVERING(9, 3) {
    @Override
    public DriverStatus turnToNextStatusOnEvent(DriverStateEvent event) {
      switch (event) {
        case ACCEPT_MOUNT_REQUEST:
          return RECOVERING;
        case DRIVER_PROCESS_UP:
          return LAUNCHED;
        case ACCEPT_UMOUNT_REQUEST:
          return REMOVING;
        case REPORTTIMEOUT:
          return UNKNOWN;
        default:
          return this;
      }
    }
  },
  // When volume migrate on line ,  driver which launched on the volume will be setted migrating
  // status
  MIGRATING(10, 4) {
    @Override
    public DriverStatus turnToNextStatusOnEvent(DriverStateEvent event) {
      switch (event) {
        case ACCEPT_MOUNT_REQUEST:
          return MIGRATING;
        case DRIVER_PROCESS_UP:
          return LAUNCHED;
        case ACCEPT_UMOUNT_REQUEST:
          return REMOVING;
        case REPORTTIMEOUT:
          return UNKNOWN;
        default:
          return this;
      }
    }
  };

  private final int value;

  //just for csi, choose the driver by status
  private final int priorityForCsi;

  private DriverStatus(int value, int priorityForCsi) {
    this.value = value;
    this.priorityForCsi = priorityForCsi;
  }

  public static DriverStatus findByValue(int value) {
    switch (value) {
      case 1:
        return START;
      case 2:
        return LAUNCHING;
      case 3:
        return SLAUNCHED;
      case 4:
        return LAUNCHED;
      case 5:
        return REMOVING;
      case 6:
        return UNAVAILABLE;
      case 7:
        return UNKNOWN;
      case 8:
        return ERROR;
      case 9:
        return RECOVERING;
      case 10:
        return MIGRATING;
      default:
        return null;
    }
  }

  public static DriverStatus findByName(String name) {
    name = name.toUpperCase();

    if (name.equals(START.name())) {
      return START;
    }

    if (name.equals(LAUNCHING.name())) {
      return LAUNCHING;
    }

    if (name.equals(SLAUNCHED.name())) {
      return SLAUNCHED;
    }

    if (name.equals(LAUNCHED.name())) {
      return LAUNCHED;
    }
    if (name.equals(REMOVING.name())) {
      return REMOVING;
    }
    if (name.equals(UNAVAILABLE.name())) {
      return UNAVAILABLE;
    }
    if (name.equals(UNKNOWN.name())) {
      return UNKNOWN;
    }
    if (name.equals(ERROR.name())) {
      return ERROR;
    }
    if (name.equals(RECOVERING.name())) {
      return DriverStatus.RECOVERING;
    }
    if (name.equals(MIGRATING.name())) {
      return DriverStatus.MIGRATING;
    }
    return null;
  }

  public int getValue() {
    return value;
  }

  public int getPriorityForCsi() {
    return priorityForCsi;
  }

  /**
   * Change driver status on event.
   */
  public abstract DriverStatus turnToNextStatusOnEvent(DriverStateEvent event)
      throws IllegalStateException;
}
