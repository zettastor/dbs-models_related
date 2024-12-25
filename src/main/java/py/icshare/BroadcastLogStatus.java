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
