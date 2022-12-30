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

import org.apache.commons.lang3.NotImplementedException;
import py.proto.Broadcastlog.PbSegmentUnitStatus;
import py.thrift.share.SegmentUnitStatusThrift;

/**
 * Segment unit statuses.
 */
public enum SegmentUnitStatus {
  // the segment's membership might be stale. no log/data can be written to the segment
  Start(1) {
    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.Start;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.START;
    }
  },
  // a moderator has been selected
  ModeratorSelected(2) {
    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.ModeratorSelected;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.MODERATOR_SELECTED;
    }
  },
  // Received a giveMeYourLogs request from the PrePrimary.
  SecondaryEnrolled(3) {
    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.SecondaryEnrolled;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.SECONDARY_ENROLLED;
    }
  },
  // Want to join the segment group
  SecondaryApplicant(4) {
    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.SecondaryApplicant;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.SECONDARY_APPLICANT;
    }
  },
  // Received catchUpMyLogs from the PrePrimary or primary
  PreSecondary(5) {
    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.PreSecondary;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.PRE_SECONDARY;
    }
  },
  // Similiar to PreSecondary status but it is particular for an arbiter in the membership
  PreArbiter(6) {
    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.PreArbiter;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.PRE_ARBITER;
    }
  },
  // The group has reached consensus about this primary
  PrePrimary(7) {
    @Override
    public boolean isPrimary() {
      return true;
    }

    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.PrePrimary;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.PRE_PRIMARY;
    }
  },
  // the segment is secondary and readable/writable
  Secondary(8) {
    @Override
    public boolean isStable() {
      return true;
    }

    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.Secondary;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.SECONDARY;
    }
  },
  // the segment is arbiter
  Arbiter(9) {
    @Override
    public boolean isStable() {
      return true;
    }

    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.Arbiter;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.ARBITER;
    }
  },
  // the segment is primary and readable/writable
  Primary(10) {
    @Override
    public boolean isStable() {
      return true;
    }

    @Override
    public boolean isPrimary() {
      return true;
    }

    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.Primary;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.PRIMARY;
    }
  },
  // segment unit is being offlined
  OFFLINING(12) {
    @Override
    public boolean hasGone() {
      return true;
    }

    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.OFFLINING;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.OFFLINING;
    }
  },
  // segment unit has been offlined
  OFFLINED(13) {
    @Override
    public boolean hasGone() {
      return true;
    }

    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.OFFLINED;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.OFFLINED;
    }
  },
  /**
   * The segment is being deleted Besides the case where a segment unit is deleted by a client,
   * there are two cases where a segment unit can delete itself. One is that SecondaryApplicant has
   * been denied by the primary for a couple of times. The other is when a segment unit finds its
   * membership is stale, but the higher membership that it wants to update to doesn't contain
   * itself.
   */
  Deleting(14) {
    @Override
    public boolean hasGone() {
      return true;
    }

    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.Deleting;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.DELETING;
    }
  },
  // the segment has been deleted. GC should collect this segment
  // the disk 'which contains the segment unit is broken. Right now, it has exactly the same
  // behavior as Deleting.
  Deleted(15) {
    @Override
    public boolean hasGone() {
      return true;
    }

    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.Deleted;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.DELETED;
    }
  },
  Broken(16) {
    @Override
    public boolean hasGone() {
      return true;
    }

    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.Broken;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.BROKEN;
    }
  },
  Unknown(17) {
    @Override
    public boolean hasGone() {
      return true;
    }

    public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
      return SegmentUnitStatusThrift.Unknown;
    }

    public PbSegmentUnitStatus getPbSegmentUnitStatus() {
      return PbSegmentUnitStatus.UNKNOWN;
    }
  };

  private int value;

  SegmentUnitStatus(int value) {
    this.value = value;
  }

  public static SegmentUnitStatus findByValue(int value) {
    switch (value) {
      case 1:
        return Start;
      case 2:
        return ModeratorSelected;
      case 3:
        return SecondaryEnrolled;
      case 4:
        return SecondaryApplicant;
      case 5:
        return PreSecondary;
      case 6:
        return PreArbiter;
      case 7:
        return PrePrimary;
      case 8:
        return Secondary;
      case 9:
        return Arbiter;
      case 10:
        return Primary;
      case 12:
        return OFFLINING;
      case 13:
        return OFFLINED;
      case 14:
        return Deleting;
      case 15:
        return Deleted;
      case 16:
        return Broken;
      case 17:
        return Unknown;
      default:
        return null;
    }
  }

  public boolean higher(SegmentUnitStatus s) {
    return this.value > s.value;
  }

  public int getValue() {
    return value;
  }

  public boolean isFinalStatus() {
    return (this == SegmentUnitStatus.Deleting || this == SegmentUnitStatus.Deleted
        || this == SegmentUnitStatus.Broken || this == SegmentUnitStatus.OFFLINED);
  }

  public boolean isStable() {
    return false;
  }

  public boolean isPrimary() {
    return false;
  }

  public boolean hasGone() {
    return false;
  }

  public SegmentUnitStatusThrift getSegmentUnitStatusThrift() {
    throw new NotImplementedException("not support value=" + value);
  }

  public PbSegmentUnitStatus getPbSegmentUnitStatus() {
    throw new NotImplementedException("not support value=" + value);
  }
}
