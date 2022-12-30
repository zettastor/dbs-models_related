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
 * This class declares all actions on driver. And following table shows conflict and shared actions
 * on one same driver.
 *
 * <p>|--------------------|--START_SERVER----|--CREATE_TARGET--|--CHECK_SERVER---|--CHECK_TARGET--
 * |--REMOVE--|--CHANGE_VOLUME-|--CHECK_MIGRATING--|
 * |--START_SERVER------|--conflict--------|--shared---------|--conflict-------|--shared--------
 * |--conflict|--conflict------|----shared---------|
 * |--CREATE_TARGET-----|--shared----------|--conflict-------|--shared---------|--conflict------
 * |--conflict|--conflict------|----shared---------|
 * |--CHECK_SERVER------|--conflict--------|--shared---------|--conflict-------|--shared--------
 * |--conflict|--shared--------|----shared---------|
 * |--CHECK_TARGET------|--shared----------|--conflict-------|--shared---------|--conflict------
 * |--conflict|--shared--------|----shared---------|
 * |--REMOVE------------|--conflict--------|--conflict-------|--conflict-------|--conflict------
 * |--conflict|--conflict------|----conflict-------|
 * |--CHANGE_VOLUME-----|--conflict--------|--conflict-------|--shared---------|--shared--------
 * |--conflict|--conflict------|----conflict-------|
 * |--CHECK_MIGRATING---|--shared----------|--shared---------|--shared---------|--shared--------
 * |--conflict|--conflict------|----conflict-------|
 *
 * <p>Action on driver is different with status of driver. The former marks whether there is a task
 * responding to the driver running. And the latter just marks static status of driver.
 *
 * <p>Since this class is designed for DriverContainer procedure, it is preferred to move this
 * class to project "pengyun-drivercontainer" later. We could not do this now because it will change
 * a lot if we do so due to many reference to {@link DriverMetadata} and there is not enough time
 * for that.
 */
public enum DriverAction {
  /**
   * Action on drivers with any type defined in {@link DriverType}.
   * <li>Start coordinator for driver with type {@link DriverType#NBD}</li>
   * <li>Start coordinator for driver with type {@link DriverType#ISCSI}</li>
   * <li>Start FS server for driver with type {@link DriverType#FSD}</li>
   */
  START_SERVER {
    @Override
    public boolean isConflictWith(DriverAction action) {
      switch (action) {
        case START_SERVER:
        case CHECK_SERVER:
        case CHANGE_VOLUME:
        case REMOVE:
          return true;
        default:
          return false;
      }
    }
  },

  /**
   * Create ISCSI target for driver with type {@link DriverType#ISCSI}.
   *
   * <p>We suppose management for PYD is part of this action.
   */
  CREATE_TARGET {
    @Override
    public boolean isConflictWith(DriverAction action) {
      switch (action) {
        case CREATE_TARGET:
        case CHECK_TARGET:
        case CHANGE_VOLUME:
        case REMOVE:
          return true;
        default:
          return false;
      }
    }
  },

  /**
   * Check status of server for drivers.
   */
  CHECK_SERVER {
    @Override
    public boolean isConflictWith(DriverAction action) {
      switch (action) {
        case START_SERVER:
        case CHECK_SERVER:
        case REMOVE:
          return true;
        default:
          return false;
      }
    }
  },

  /**
   * Check status of target for ISCSI.
   */
  CHECK_TARGET {
    @Override
    public boolean isConflictWith(DriverAction action) {
      switch (action) {
        case CREATE_TARGET:
        case CHECK_TARGET:
        case REMOVE:
          return true;
        default:
          return false;
      }
    }
  },
  /**
   * Change driver bind volume.
   */
  CHANGE_VOLUME {
    @Override
    public boolean isConflictWith(DriverAction action) {
      switch (action) {
        case CREATE_TARGET:
        case START_SERVER:
        case CHECK_MIGRATING:
        case CHANGE_VOLUME:
        case REMOVE:
          return true;
        default:
          return false;
      }
    }
  },

  /**
   * Check migrating status.
   */
  CHECK_MIGRATING {
    @Override
    public boolean isConflictWith(DriverAction action) {
      switch (action) {
        case CHECK_MIGRATING:
        case CHANGE_VOLUME:
        case REMOVE:
          return true;
        default:
          return false;
      }
    }
  },

  /**
   * Remove driver.
   */
  REMOVE {
    @Override
    public boolean isConflictWith(DriverAction action) {
      return true;
    }
  };

  /**
   * Check if the given action is conflict with current action.
   *
   * @return true if they are conflict or false.
   */
  public abstract boolean isConflictWith(DriverAction action);
}
