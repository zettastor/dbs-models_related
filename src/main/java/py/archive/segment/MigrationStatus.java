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
