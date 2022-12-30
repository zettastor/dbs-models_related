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

package py;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentVersion;
import py.icshare.BroadcastLogStatus;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbBroadcastLogStatus;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbMembership;
import py.proto.Broadcastlog.PbReadRequestUnit;
import py.proto.Broadcastlog.PbReadResponseUnit;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.proto.Broadcastlog.PbWriteResponseUnit;
import py.thrift.share.BroadcastLogStatusThrift;
import py.thrift.share.ReadUnitResultThrift;

public class PbRequestResponseHelper {
  private static final Logger logger = LoggerFactory.getLogger(PbRequestResponseHelper.class);

  public static SegmentMembership buildMembershipFrom(PbMembership pbMembership) {
    List<InstanceId> secondaries = null;
    if (pbMembership.getSecondariesCount() > 0) {
      secondaries = new ArrayList<>();
      for (Long instanceId : pbMembership.getSecondariesList()) {
        secondaries.add(new InstanceId(instanceId));
      }
    }

    List<InstanceId> joiningSecondaries = null;
    if (pbMembership.getJoiningSecondariesCount() > 0) {
      joiningSecondaries = new ArrayList<>();
      for (Long instanceId : pbMembership.getJoiningSecondariesList()) {
        joiningSecondaries.add(new InstanceId(instanceId));
      }
    }

    List<InstanceId> inactiveSecondaries = null;
    if (pbMembership.getInactiveSecondariesCount() > 0) {
      inactiveSecondaries = new ArrayList<>();
      for (Long instanceId : pbMembership.getInactiveSecondariesList()) {
        inactiveSecondaries.add(new InstanceId(instanceId));
      }
    }

    List<InstanceId> arbiterSecondaries = null;
    if (pbMembership.getArbitersCount() > 0) {
      arbiterSecondaries = new ArrayList<>();
      for (Long instanceId : pbMembership.getArbitersList()) {
        arbiterSecondaries.add(new InstanceId(instanceId));
      }
    }

    InstanceId tempPrimary = null;
    if (pbMembership.hasTempPrimary() && pbMembership.getTempPrimary() != 0) {
      tempPrimary = new InstanceId(pbMembership.getTempPrimary());
    }

    InstanceId secondaryCandidate = null;
    if (pbMembership.hasSecondaryCandidate() && pbMembership.getSecondaryCandidate() != 0) {
      secondaryCandidate = new InstanceId(pbMembership.getSecondaryCandidate());
    }

    InstanceId primaryCandidate = null;
    if (pbMembership.hasPrimaryCandidate() && pbMembership.getPrimaryCandidate() != 0) {
      primaryCandidate = new InstanceId(pbMembership.getPrimaryCandidate());
    }

    return new SegmentMembership(
        new SegmentVersion(pbMembership.getEpoch(), pbMembership.getGeneration()),
        new InstanceId(pbMembership.getPrimary()), tempPrimary, secondaries, arbiterSecondaries,
        inactiveSecondaries, joiningSecondaries, secondaryCandidate, primaryCandidate);
  }

  public static PbMembership buildPbMembershipFrom(SegmentMembership membership) {
    PbMembership.Builder builder = PbMembership.newBuilder();
    builder.setEpoch(membership.getSegmentVersion().getEpoch());
    builder.setGeneration(membership.getSegmentVersion().getGeneration());
    builder.setPrimary(membership.getPrimary().getId());
    for (InstanceId instanceId : membership.getSecondaries()) {
      builder.addSecondaries(instanceId.getId());
    }

    for (InstanceId instanceId : membership.getJoiningSecondaries()) {
      builder.addJoiningSecondaries(instanceId.getId());
    }

    for (InstanceId instanceId : membership.getArbiters()) {
      builder.addArbiters(instanceId.getId());
    }

    for (InstanceId instanceId : membership.getInactiveSecondaries()) {
      builder.addInactiveSecondaries(instanceId.getId());
    }

    if (membership.getTempPrimary() != null) {
      builder.setTempPrimary(membership.getTempPrimary().getId());
    }

    if (membership.getSecondaryCandidate() != null) {
      builder.setSecondaryCandidate(membership.getSecondaryCandidate().getId());
    }

    if (membership.getPrimaryCandidate() != null) {
      builder.setPrimaryCandidate(membership.getPrimaryCandidate().getId());
    }

    return builder.build();
  }

  public static BroadcastLogStatus convertPbStatusToStatus(PbBroadcastLogStatus pbLogStatus) {
    BroadcastLogStatus convertStatus = null;
    Validate.notNull(pbLogStatus, "can not be null status");
    if (pbLogStatus == PbBroadcastLogStatus.COMMITTED) {
      convertStatus = BroadcastLogStatus.Committed;
    } else if (pbLogStatus == PbBroadcastLogStatus.CREATED) {
      convertStatus = BroadcastLogStatus.Created;
    } else if (pbLogStatus == PbBroadcastLogStatus.CREATING) {
      convertStatus = BroadcastLogStatus.Creating;
    } else if (pbLogStatus == PbBroadcastLogStatus.ABORT) {
      convertStatus = BroadcastLogStatus.Abort;
    } else if (pbLogStatus == PbBroadcastLogStatus.ABORT_CONFIRMED) {
      convertStatus = BroadcastLogStatus.AbortConfirmed;
    } else {
      logger.warn("don't know this status: {}", pbLogStatus.name());
    }
    return convertStatus;
  }

  public static boolean isFinalStatus(BroadcastLogStatus logStatus) {
    return logStatus == BroadcastLogStatus.Committed
        || logStatus == BroadcastLogStatus.AbortConfirmed;
  }

  public static boolean isFinalStatus(PbBroadcastLogStatus pbLogStatus) {
    return pbLogStatus == PbBroadcastLogStatus.COMMITTED
        || pbLogStatus == PbBroadcastLogStatus.ABORT_CONFIRMED;
  }

  public static BroadcastLogStatusThrift convertPbStatusToThriftStatus(
      PbBroadcastLogStatus pbLogStatus) {
    BroadcastLogStatusThrift convertStatus = null;
    Validate.notNull(pbLogStatus, "can not be null status");
    if (pbLogStatus == PbBroadcastLogStatus.COMMITTED) {
      convertStatus = BroadcastLogStatusThrift.Committed;
    } else if (pbLogStatus == PbBroadcastLogStatus.CREATED) {
      convertStatus = BroadcastLogStatusThrift.Created;
    } else if (pbLogStatus == PbBroadcastLogStatus.CREATING) {
      convertStatus = BroadcastLogStatusThrift.Creating;
    } else if (pbLogStatus == PbBroadcastLogStatus.ABORT) {
      convertStatus = BroadcastLogStatusThrift.Abort;
    } else if (pbLogStatus == PbBroadcastLogStatus.ABORT_CONFIRMED) {
      convertStatus = BroadcastLogStatusThrift.AbortConfirmed;
    } else {
      logger.warn("don't know this status: {}", pbLogStatus.name());
    }
    return convertStatus;
  }

  public static PbWriteRequestUnit buildPbWriteRequestUnitFrom(long logUuid, long logId,
      long offset, int length, ByteBuf data) {
    return PbWriteRequestUnit.newBuilder().setLogUuid(logUuid).setLogId(logId).setOffset(offset)
        .setLength(length)
        .setChecksum(0L).setRandomWrite(true).build();
  }

  public static PbWriteResponseUnit buildPbWriteResponseUnitFrom(long logId,
      PbIoUnitResult result) {
    return PbWriteResponseUnit.newBuilder().setLogId(logId).setLogResult(result).build();
  }

  public static PbWriteResponseUnit buildPbWriteResponseUnitFrom(PbWriteRequestUnit writeUnit,
      long logId,
      PbIoUnitResult result) {
    return PbWriteResponseUnit.newBuilder().setLogId(logId).setLogUuid(writeUnit.getLogUuid())
        .setLogResult(result)
        .build();
  }

  public static PbWriteResponseUnit buildPbWriteResponseUnitFrom(PbWriteRequestUnit writeUnit,
      PbIoUnitResult result) {
    return PbWriteResponseUnit.newBuilder().setLogId(writeUnit.getLogId())
        .setLogUuid(writeUnit.getLogUuid())
        .setLogResult(result).build();
  }

  public static PbWriteResponseUnit buildPbWriteResponseUnitFrom(PbWriteRequestUnit writeUnit,
      PbIoUnitResult result, long logId) {
    return PbWriteResponseUnit.newBuilder().setLogId(logId).setLogUuid(writeUnit.getLogUuid())
        .setLogResult(result).build();
  }

  public static PbReadRequestUnit buildPbReadRequestUnitFrom(long offset, int length) {
    return PbReadRequestUnit.newBuilder().setOffset(offset).setLength(length).build();
  }

  public static PbReadResponseUnit buildPbReadResponseUnitFrom(PbReadRequestUnit readRequestUnit,
      PbIoUnitResult result) {
    PbReadResponseUnit.Builder builder = PbReadResponseUnit.newBuilder();
    builder.setOffset(readRequestUnit.getOffset());
    builder.setLength(readRequestUnit.getLength());
    builder.setResult(result);
    return builder.build();
  }

  public static PbReadResponseUnit buildPbReadResponseUnitFrom(PbReadRequestUnit readRequestUnit,
      byte[] data) {
    return buildPbReadResponseUnitFrom(readRequestUnit, data, 0, data.length, new ArrayList<>());
  }

  public static PbReadResponseUnit buildPbReadResponseUnitFrom(PbReadRequestUnit readRequestUnit,
      byte[] data,
      int offset, int len, List<Broadcastlog.PbBroadcastLog> logsToMerge) {
    PbReadResponseUnit.Builder builder = PbReadResponseUnit.newBuilder();
    builder.setOffset(readRequestUnit.getOffset());
    builder.setLength(readRequestUnit.getLength());
    builder.setResult(PbIoUnitResult.OK);
    builder.setChecksum(0L);
    builder.addAllLogsToMerge(logsToMerge);
    return builder.build();
  }

  public static PbReadResponseUnit buildPbReadResponseUnitFrom(PbReadRequestUnit readRequestUnit,
      ByteBuf byteBuf) {
    return buildPbReadResponseUnitFrom(readRequestUnit, byteBuf,
        new ArrayList<Broadcastlog.PbBroadcastLog>());
  }

  public static PbReadResponseUnit buildPbReadResponseUnitFrom(PbReadRequestUnit readRequestUnit,
      ByteBuf byteBuf, List<Broadcastlog.PbBroadcastLog> logsToMerge) {
    PbReadResponseUnit.Builder builder = PbReadResponseUnit.newBuilder();
    builder.setOffset(readRequestUnit.getOffset());
    builder.setLength(readRequestUnit.getLength());
    builder.setResult(PbIoUnitResult.OK);
    builder.setChecksum(0L);
    builder.addAllLogsToMerge(logsToMerge);
    return builder.build();
  }

  public static PbReadResponseUnit buildZeroPbReadResponseUnit(PbReadRequestUnit readRequestUnit) {
    return buildPbReadResponseUnitFrom(readRequestUnit, PbIoUnitResult.FREE);
  }

  public static ReadUnitResultThrift convertResultToThriftResult(PbIoUnitResult readUnitResult) {
    ReadUnitResultThrift resultThrift = null;
    if (readUnitResult == PbIoUnitResult.OK) {
      resultThrift = ReadUnitResultThrift.OK;
    } else if (readUnitResult == PbIoUnitResult.OUT_OF_RANGE) {
      resultThrift = ReadUnitResultThrift.OutOfRange;
    } else if (readUnitResult == PbIoUnitResult.CHECKSUM_MISMATCHED) {
      resultThrift = ReadUnitResultThrift.ChecksumMismatched;
    } else {
      logger.error("unknown status: {}", readUnitResult);
    }
    return resultThrift;
  }

}
