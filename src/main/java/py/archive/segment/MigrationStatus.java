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

package py.archive.segment;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.NotImplementedException;

/**
 * If some segment unit starts to migrate, it should be marked a status.
 */
public enum MigrationStatus {
  NONE(0) {
    @Override
    public MigrationStatus getNextStatusFromSecondary(boolean isJoining) {
      Validate.isTrue(!isMigratedStatus());
      if (isJoining) {
        return FROMJOINING;
      } else {
        return FROMVOTING;
      }
    }

    @Override
    public MigrationStatus getNextStatusFromResult(boolean success) {
      if (success) {
        return NONE;
      } else {
        return FROMFAILMIGRATION;
      }
    }

    @Override
    public boolean isMigratedStatus() {
      return false;
    }

    @Override
    public boolean isMigrating() {
      return false;
    }
  },

  FROMVOTING(1) {
    @Override
    public MigrationStatus getNextStatusFromResult(boolean success) {
      if (success) {
        return NONE;
      } else {
        return FROMFAILMIGRATION;
      }
    }

    public MigrationStatus getNextStatusFromSecondary(boolean isJoining) {
      if (isJoining) {
        return FROMJOINING;
      } else {
        return FROMVOTING;
      }
    }
  },

  FROMJOINING(2) {
    @Override
    public MigrationStatus getNextStatusFromResult(boolean success) {
      if (success) {
        return NONE;
      } else {
        return FROMFAILMIGRATION;
      }
    }

    public MigrationStatus getNextStatusFromSecondary(boolean isJoining) {
      if (isJoining) {
        return FROMJOINING;
      } else {
        return FROMVOTING;
      }
    }

  },

  FROMFAILMIGRATION(3) {
    @Override
    public MigrationStatus getNextStatusFromSecondary(boolean isSecondary) {
      if (isSecondary) {
        return FROMJOINING;
      } else {
        return FROMVOTING;
      }
    }

    @Override
    public MigrationStatus getNextStatusFromResult(boolean success) {
      if (success) {
        throw new RuntimeException("migration status from failure, success=" + success);
      } else {
        return FROMFAILMIGRATION;
      }
    }

    @Override
    public boolean isMigrating() {
      return false;
    }
  },

  DELETED(4) {
    @Override
    public MigrationStatus getNextStatusFromResult(boolean success) {
      return DELETED;
    }

    @Override
    public boolean isMigrating() {
      return false;
    }
  };

  private final int value;

  MigrationStatus(int value) {
    this.value = value;
  }

  public static MigrationStatus findByValue(int value) {
    switch (value) {
      case 0:
        return NONE;
      case 1:
        return FROMVOTING;
      case 2:
        return FROMJOINING;
      case 3:
        return FROMFAILMIGRATION;
      case 4:
        return DELETED;
      default:
        return null;
    }
  }

  public int getValue() {
    return value;
  }

  public MigrationStatus getNextStatusFromResult(boolean isSecondary) {
    throw new NotImplementedException("current value=" + value + ", secondary=" + isSecondary);
  }

  public MigrationStatus getNextStatusFromSecondary(boolean isJoining) {
    throw new NotImplementedException("current value=" + value + ", isJoining=" + isJoining);
  }

  public boolean isMigrating() {
    return true;
  }

  public boolean isMigratedStatus() {
    return true;
  }
}
