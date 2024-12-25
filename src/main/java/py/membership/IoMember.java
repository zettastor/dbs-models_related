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

package py.membership;

import py.common.struct.EndPoint;
import py.instance.InstanceId;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.ReadCause;

public class IoMember {
  private InstanceId instanceId;
  private EndPoint endPoint;
  private MemberIoStatus memberIoStatus;
  /**
   * target this io member is true read or just check membership read.
   */
  private Broadcastlog.ReadCause readCause;

  public IoMember(InstanceId instanceId, EndPoint endPoint, MemberIoStatus memberIoStatus) {
    this(instanceId, endPoint, memberIoStatus, null);
  }

  public IoMember(InstanceId instanceId, EndPoint endPoint, MemberIoStatus memberIoStatus,
      Broadcastlog.ReadCause readCause) {
    this.instanceId = instanceId;
    this.endPoint = endPoint;
    this.memberIoStatus = memberIoStatus;
    this.readCause = readCause;
  }

  public Broadcastlog.ReadCause getReadCause() {
    return readCause;
  }

  public InstanceId getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(InstanceId instanceId) {
    this.instanceId = instanceId;
  }

  public EndPoint getEndPoint() {
    return endPoint;
  }

  public void setEndPoint(EndPoint endPoint) {
    this.endPoint = endPoint;
  }

  public MemberIoStatus getMemberIoStatus() {
    return memberIoStatus;
  }

  public void setMemberIoStatus(MemberIoStatus memberIoStatus) {
    this.memberIoStatus = memberIoStatus;
  }

  public boolean isCheckRead() {
    return this.readCause == ReadCause.CHECK;
  }

  public boolean isFetchRead() {
    return this.readCause == Broadcastlog.ReadCause.FETCH;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IoMember)) {
      return false;
    }

    IoMember ioMember = (IoMember) o;

    if (instanceId != null ? !instanceId.equals(ioMember.instanceId)
        : ioMember.instanceId != null) {
      return false;
    }
    return endPoint != null ? endPoint.equals(ioMember.endPoint) : ioMember.endPoint == null;
  }

  @Override
  public int hashCode() {
    int result = instanceId != null ? instanceId.hashCode() : 0;
    result = 31 * result + (endPoint != null ? endPoint.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "IOMember{" + "instanceId=" + instanceId + ", endPoint=" + endPoint + ", memberIoStatus="
        + memberIoStatus + ", readCause=" + readCause + '}';
  }
}
