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

package py.archive;

import py.exception.ArchiveStatusException;

public enum ArchiveStatus {
  GOOD(1) {
    @Override
    public void validate(ArchiveStatus newArchiveStatus) throws ArchiveStatusException {
      switch (newArchiveStatus) {
        case OFFLINING:
          //offline the disk
        case DEGRADED:
          //IO abnormal blow threshold
        case INPROPERLY_EJECTED:
          //plug out by force
        case CONFIG_MISMATCH:

        case BROKEN:

          //todo:
          //sometime, when the disk plugged in but the disk is be plug out be not offline before,
          // we also set it to broken in order to close the storage

          //i see only use for  unit test
        case GOOD:
          return;

        case EJECTED:
        case OFFLINED:
        default:
          throw new ArchiveStatusException(
              "current status is good ,can not change status to " + newArchiveStatus);
      }
    }

    @Override
    public boolean canDoIo() {
      return true;
    }
  },
  // IO abnormal but blow threshold
  DEGRADED(2) {
    @Override
    public void validate(ArchiveStatus newArchiveStatus) throws ArchiveStatusException {
      switch (newArchiveStatus) {
        case BROKEN:
          //IO abnormal beyond threshold
        case OFFLINING:
          //offline the disk

        case INPROPERLY_EJECTED:
          //plug out by force
        case CONFIG_MISMATCH:
        case DEGRADED:
          return;
        case GOOD:
        case EJECTED:
        case OFFLINED:
        default:
          throw new ArchiveStatusException(
              "current status is DEGRADED ,can not change status to " + newArchiveStatus);
      }
    }

    @Override
    public boolean canDoIo() {
      return true;
    }
  },
  //IO abnormal beyond threshold
  BROKEN(3) {
    @Override
    public void validate(ArchiveStatus newArchiveStatus) throws ArchiveStatusException {
      switch (newArchiveStatus) {
        case BROKEN:
        case INPROPERLY_EJECTED:
          //plug out by force
          return;
        case CONFIG_MISMATCH:
        case EJECTED:
        case GOOD:
        case OFFLINED:
        case DEGRADED:
        case OFFLINING:
        default:
          throw new ArchiveStatusException(
              "current status is BROKEN ,can not change status to " + newArchiveStatus);
      }
    }

    @Override
    public boolean canDoIo() {
      return false;
    }
  },
  //archive have metadata but the metadata belong to others,all status can be validate to this
  // status
  CONFIG_MISMATCH(4) {
    @Override
    public void validate(ArchiveStatus newArchiveStatus) throws ArchiveStatusException {
      switch (newArchiveStatus) {
        case CONFIG_MISMATCH:
        case GOOD:
          //online
        case INPROPERLY_EJECTED:
          //plug out by force
          return;
        case EJECTED:
          //plug out the disk
        case OFFLINING:
        case DEGRADED:
        case BROKEN:
        case OFFLINED:
        default:
          throw new ArchiveStatusException(
              "current status is CONFIG_MISMATCH ,can not change status to " + newArchiveStatus);
      }
    }

    @Override
    public boolean canDoIo() {
      return false;
    }
  },
  // the archive is no longer use and the data in memory has been flushed to disk.
  OFFLINED(5) {
    @Override
    public void validate(ArchiveStatus newArchiveStatus) throws ArchiveStatusException {
      switch (newArchiveStatus) {
        case OFFLINED:
        case EJECTED:
          //plug out
        case GOOD:
          //online
        case CONFIG_MISMATCH:
          return;

        case INPROPERLY_EJECTED:
        case BROKEN:
        case DEGRADED:
        case OFFLINING:
        default:
          throw new ArchiveStatusException(
              "current status is OFFLINED ,can not change status to " + newArchiveStatus);
      }
    }

    @Override
    public boolean canDoIo() {
      return false;
    }
  },

  OFFLINING(6) {
    @Override
    public void validate(ArchiveStatus newArchiveStatus) throws ArchiveStatusException {
      switch (newArchiveStatus) {
        case OFFLINED:
          //all segment unit log flush to disk
        case DEGRADED:
          //io abnormal
        case CONFIG_MISMATCH:
        case INPROPERLY_EJECTED:
        case BROKEN:
          return;
        case OFFLINING:
        case EJECTED:
        case GOOD:
        default:
          throw new ArchiveStatusException(
              "current status is OFFLINING ,can not change status to " + newArchiveStatus);
      }
    }

    @Override
    public boolean canDoIo() {
      return true;
    }
  },

  /**
   * when the archive status is {@link #OFFLINED}, the archive is plugged out, its status will
   * become {@link #EJECTED}. the status can not move to other status.
   */
  EJECTED(7) {
    @Override
    public void validate(ArchiveStatus newArchiveStatus) throws ArchiveStatusException {
      switch (newArchiveStatus) {
        case GOOD:
        case OFFLINED:
          //plug in
        case CONFIG_MISMATCH:
        case BROKEN:
          return;
        case EJECTED:
        case INPROPERLY_EJECTED:
        case DEGRADED:
        case OFFLINING:
        default:
          throw new ArchiveStatusException(
              "current status is EJECTED ,can not change status to " + newArchiveStatus);
      }
    }

    @Override
    public boolean canDoIo() {
      return false;
    }
  },

  /**
   * on matter what the archive status is except {@link #OFFLINED}, the archive is plugged out, its
   * status will become {@link #INPROPERLY_EJECTED}.
   */
  INPROPERLY_EJECTED(8) {
    @Override
    public void validate(ArchiveStatus newArchiveStatus) throws ArchiveStatusException {
      switch (newArchiveStatus) {
        case INPROPERLY_EJECTED:
          //plug in
        case CONFIG_MISMATCH:
        case OFFLINED:
        case GOOD:
        case BROKEN:
          //if plug in the archive that  the archive in diskErrorLogManager > threshold
          return;
        case OFFLINING:
        case DEGRADED:
        case EJECTED:
        default:
          throw new ArchiveStatusException(
              "current status is INPROPERLY_EJECTED ,can not change status to " + newArchiveStatus);
      }
    }

    @Override
    public boolean canDoIo() {
      return false;
    }
  },

  UNKNOWN(9) {
    @Override
    public void validate(ArchiveStatus newArchiveStatus) throws ArchiveStatusException {
      switch (newArchiveStatus) {
        case INPROPERLY_EJECTED:
          //plug in
        case CONFIG_MISMATCH:
        case OFFLINED:
        case GOOD:
        case BROKEN:
        case OFFLINING:
        case DEGRADED:
        case EJECTED:
        case UNKNOWN:
        case SEPARATED:
          //if plug in the archive that  the archive in diskErrorLogManager > threshold
          return;
        default:
          throw new ArchiveStatusException(
              "current status is UNKNOWN ,can not change status to " + newArchiveStatus);
      }
    }

    @Override
    public boolean canDoIo() {
      return false;
    }
  },

  SEPARATED(10) {
    @Override
    public void validate(ArchiveStatus newArchiveStatus) throws ArchiveStatusException {
      switch (newArchiveStatus) {
        case INPROPERLY_EJECTED:
          //plug in
        case CONFIG_MISMATCH:
        case OFFLINED:
        case GOOD:
        case BROKEN:
        case OFFLINING:
        case DEGRADED:
        case EJECTED:
        case UNKNOWN:
        case SEPARATED:
          //if plug in the archive that  the archive in diskErrorLogManager > threshold
          return;
        default:
          throw new ArchiveStatusException(
              "current status is UNKNOWN ,can not change status to " + newArchiveStatus);
      }
    }

    @Override
    public boolean canDoIo() {
      return false;
    }
  };

  private final int value;

  ArchiveStatus(int value) {
    this.value = value;
  }

  public static ArchiveStatus findByValue(int value) {
    switch (value) {
      case 1:
        return GOOD;
      case 2:
        return DEGRADED;
      case 3:
        return BROKEN;
      case 4:
        return CONFIG_MISMATCH;
      case 5:
        return OFFLINING;
      case 6:
        return OFFLINED;
      case 7:
        return EJECTED;
      case 8:
        return INPROPERLY_EJECTED;
      case 9:
        return UNKNOWN;
      case 10:
        return SEPARATED;
      default:
        return null;
    }
  }

  public static boolean isEjected(ArchiveStatus status) {
    return status == ArchiveStatus.EJECTED || status == ArchiveStatus.INPROPERLY_EJECTED;
  }

  public static boolean isCanbeUse(ArchiveStatus status) {
    return status == ArchiveStatus.GOOD || status == ArchiveStatus.DEGRADED;
  }

  public int getValue() {
    return value;
  }

  public boolean canDoIo() {
    return false;
  }

  public void validate(ArchiveStatus newArchiveStatus) throws ArchiveStatusException {
    throw new ArchiveStatusException("current status is Unknown");
  }
}
