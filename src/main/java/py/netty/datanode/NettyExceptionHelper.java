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

package py.netty.datanode;

import com.google.protobuf.InvalidProtocolBufferException;
import py.PbRequestResponseHelper;
import py.app.context.AppContext;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.membership.SegmentMembership;
import py.netty.exception.MembershipVersionHigerException;
import py.netty.exception.MembershipVersionLowerException;
import py.netty.exception.NotPrimaryException;
import py.netty.exception.NotSecondaryException;
import py.netty.exception.NotSecondaryZombieException;
import py.netty.exception.NotTempPrimaryException;
import py.netty.exception.SegmentDeleteException;
import py.netty.exception.SegmentNotFoundException;
import py.netty.exception.ServerOverLoadedException;
import py.netty.exception.ServerProcessException;
import py.netty.exception.ServerShutdownException;
import py.proto.Broadcastlog.PbMembership;

public class NettyExceptionHelper {
  public static NotPrimaryException buildNotPrimaryException(SegId segId, SegmentUnitStatus status,
      SegmentMembership membership) {
    NotPrimaryException e = new NotPrimaryException(segId.getVolumeId().getId(), segId.getIndex(),
        status.getValue());
    e.setMembership(PbRequestResponseHelper.buildPbMembershipFrom(membership).toByteArray());
    return e;
  }

  public static NotTempPrimaryException buildNotTempPrimaryException(SegId segId,
      SegmentUnitStatus status,
      SegmentMembership membership) {
    NotTempPrimaryException e = new NotTempPrimaryException(segId.getVolumeId().getId(),
        segId.getIndex(),
        status.getValue());
    e.setMembership(PbRequestResponseHelper.buildPbMembershipFrom(membership).toByteArray());
    return e;
  }

  public static NotSecondaryException buildNotSecondaryException(SegId segId,
      SegmentUnitStatus status,
      SegmentMembership membership) {
    NotSecondaryException e = new NotSecondaryException(segId.getVolumeId().getId(),
        segId.getIndex(),
        status.getValue());
    e.setMembership(PbRequestResponseHelper.buildPbMembershipFrom(membership).toByteArray());
    return e;
  }

  public static NotSecondaryZombieException buildNotSecondaryZombieException(SegId segId,
      SegmentUnitStatus status,
      SegmentMembership membership) {
    NotSecondaryZombieException e = new NotSecondaryZombieException(segId.getVolumeId().getId(),
        segId.getIndex(),
        status.getValue());
    e.setMembership(PbRequestResponseHelper.buildPbMembershipFrom(membership).toByteArray());
    return e;
  }

  public static MembershipVersionLowerException buildMembershipVersionLowerException(SegId segId,
      SegmentMembership membership) {
    MembershipVersionLowerException e = new MembershipVersionLowerException(
        segId.getVolumeId().getId(),
        segId.getIndex());
    e.setMembership(PbRequestResponseHelper.buildPbMembershipFrom(membership).toByteArray());
    return e;
  }

  public static MembershipVersionHigerException buildMembershipVersionHigerException(SegId segId,
      SegmentMembership membership) {
    MembershipVersionHigerException e = new MembershipVersionHigerException(
        segId.getVolumeId().getId(),
        segId.getIndex());
    e.setMembership(PbRequestResponseHelper.buildPbMembershipFrom(membership).toByteArray());
    return e;
  }

  public static ServerProcessException buildServerProcessException(String msg) {
    return new ServerProcessException(msg);
  }

  public static ServerProcessException buildServerProcessException(Throwable t) {
    return new ServerProcessException(t);
  }

  public static ServerShutdownException buildShutdownException(long instanceId) {
    return new ServerShutdownException("my instance: " + instanceId);
  }

  public static SegmentNotFoundException buildSegmentNotFoundException(SegId segId,
      AppContext context) {
    return new SegmentNotFoundException(segId.getIndex(), segId.getVolumeId().getId(),
        context.getInstanceId().getId());
  }

  public static SegmentDeleteException buildSegmentDeleteException(SegId segId,
      AppContext context) {
    return new SegmentDeleteException(segId.getIndex(), segId.getVolumeId().getId(),
        context.getInstanceId().getId());
  }

  public static SegmentMembership getMembershipFromBuffer(byte[] pbMembership)
      throws InvalidProtocolBufferException {
    PbMembership membership = PbMembership.parseFrom(pbMembership);
    return PbRequestResponseHelper.buildMembershipFrom(membership);
  }

  public static ServerOverLoadedException buildServerOverLoadedException(int queueLength,
      int pendingRequests) {
    return new ServerOverLoadedException(queueLength, pendingRequests);
  }

}
