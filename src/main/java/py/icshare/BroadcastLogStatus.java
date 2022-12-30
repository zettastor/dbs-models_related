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

package py.icshare;

import org.apache.commons.lang.NotImplementedException;
import py.proto.Broadcastlog.PbBroadcastLogStatus;
import py.thrift.share.BroadcastLogStatusThrift;

public enum BroadcastLogStatus {
  Creating(1) {
    @Override
    public BroadcastLogStatusThrift getThriftLogStatus() {
      return BroadcastLogStatusThrift.Creating;
    }

    @Override
    public PbBroadcastLogStatus getPbLogStatus() {
      return PbBroadcastLogStatus.CREATING;
    }
  },
  Created(2) {
    @Override
    public BroadcastLogStatusThrift getThriftLogStatus() {
      return BroadcastLogStatusThrift.Created;
    }

    @Override
    public PbBroadcastLogStatus getPbLogStatus() {
      return PbBroadcastLogStatus.CREATED;
    }
  },
  Committed(3) {
    @Override
    public BroadcastLogStatusThrift getThriftLogStatus() {
      return BroadcastLogStatusThrift.Committed;
    }

    @Override
    public PbBroadcastLogStatus getPbLogStatus() {
      return PbBroadcastLogStatus.COMMITTED;
    }
  },
  Abort(4) {
    @Override
    public BroadcastLogStatusThrift getThriftLogStatus() {
      return BroadcastLogStatusThrift.Abort;
    }

    @Override
    public PbBroadcastLogStatus getPbLogStatus() {
      return PbBroadcastLogStatus.ABORT;
    }
  },
  AbortConfirmed(5) {
    @Override
    public BroadcastLogStatusThrift getThriftLogStatus() {
      return BroadcastLogStatusThrift.AbortConfirmed;
    }

    @Override
    public PbBroadcastLogStatus getPbLogStatus() {
      return PbBroadcastLogStatus.ABORT_CONFIRMED;
    }
  };
  private final int value;

  private BroadcastLogStatus(int value) {
    this.value = value;
  }

  public static BroadcastLogStatus findByValue(int value) {
    switch (value) {
      case 1:
        return Creating;
      case 2:
        return Created;
      case 3:
        return Committed;
      case 4:
        return Abort;
      case 5:
        return AbortConfirmed;
      default:
        return null;
    }
  }

  public int getValue() {
    return value;
  }

  public BroadcastLogStatusThrift getThriftLogStatus() {
    throw new NotImplementedException("can not convert to thrift for " + this);
  }

  public PbBroadcastLogStatus getPbLogStatus() {
    throw new NotImplementedException("can not convert to protocolbuf for " + this);
  }
}
