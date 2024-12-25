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

package py.utils;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.netty.datanode.PyReadResponse;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbBroadcastLogStatus;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadResponse;
import py.proto.Broadcastlog.PbReadResponseUnit;

public class DataNodeForTest implements DataNodeForTestIface {
  private static final Logger logger = LoggerFactory.getLogger(DataNodeForTest.class);

  private final ByteBufAllocator allocator;
  private final TreeMap<Long, Log> logs = new TreeMap<>();

  private long pcl = 0;

  public DataNodeForTest(ByteBufAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public synchronized void write(long id, byte[] data, long offset, int length) {
    if (logs.containsKey(id)) {
      throw new IllegalArgumentException("log exists");
    }

    Log log = new Log(id, data, offset, length);
    logs.put(log.id, log);

  }

  @Override
  public synchronized boolean containsLog(long id) {
    return logs.containsKey(id);
  }

  @Override
  public synchronized boolean isLogCommitted(long id) {
    Log log = logs.get(id);
    if (log == null) {
      return false;
    }

    return log.commit;
  }

  @Override
  public synchronized void commit(long id) {
    Log log = logs.get(id);

    if (log == null) {
      throw new IllegalArgumentException("log not found");
    }

    log.commit = true;

  }

  @Override
  public synchronized void setPcl(long pcl) {
    this.pcl = pcl;
  }

  @Override
  public synchronized PyReadResponse read(long[] offsets, int[] lengths) {
    logger.warn("reading {} {}", Arrays.toString(offsets), Arrays.toString(lengths));

    if (offsets.length != lengths.length) {
      throw new IllegalArgumentException();
    }

    ByteBuf returnBuf = null;
    CompositeByteBuf compositeByteBuf = null;
    List<PbReadResponseUnit> units = new ArrayList<>();

    for (int i = 0; i < offsets.length; i++) {
      long unitOffset = offsets[i];
      int unitLength = lengths[i];

      ByteBuf data = allocator.buffer(unitLength);
      List<PbBroadcastLog> logsToMerge = null;

      boolean free = true;
      for (Log log : logs.values()) {
        if (log.touches(unitOffset, unitLength)) {
          if (log.commit) {
            free = false;
            log.applyData(unitOffset, unitLength, data);
            if (log.id > pcl) {
              if (logsToMerge == null) {
                logsToMerge = new ArrayList<>();
              }
              logsToMerge.add(log.toPb(false));
            }
          } else {
            if (logsToMerge == null) {
              logsToMerge = new ArrayList<>();
            }
            logsToMerge.add(log.toPb(true));
          }
        }

      }

      data.writerIndex(data.capacity());
      if (free) {
        data.release();
      } else {
        if (returnBuf == null) {
          returnBuf = data;
        } else {
          if (compositeByteBuf == null) {
            compositeByteBuf = new CompositeByteBuf(allocator, true, offsets.length);
            compositeByteBuf.addComponent(true, returnBuf);
            returnBuf = compositeByteBuf;
          }
          compositeByteBuf.addComponent(true, data);
        }
      }

      units.add(buildResponseUnit(offsets[i], lengths[i],
          free ? PbIoUnitResult.FREE : PbIoUnitResult.OK, logsToMerge));

    }

    return buildResponse(pcl, units, returnBuf);
  }

  private PyReadResponse buildResponse(Long pcl, Iterable<PbReadResponseUnit> responseUnits,
      ByteBuf data) {
    logger.debug("building response from {}", data);
    PbReadResponse.Builder pbResponseBuilder = PbReadResponse.newBuilder();
    pbResponseBuilder.setRequestId(RequestIdBuilder.get());

    if (pcl != null) {
      pbResponseBuilder.setPclId(pcl);
    }

    pbResponseBuilder.addAllResponseUnits(responseUnits);

    return new PyReadResponse(pbResponseBuilder.build(), data);
  }

  private PbReadResponseUnit buildResponseUnit(long offset, int length,
      PbIoUnitResult result, Iterable<PbBroadcastLog> logsToMerge) {
    PbReadResponseUnit.Builder builder = PbReadResponseUnit.newBuilder();
    builder.setOffset(offset);
    builder.setLength(length);
    builder.setResult(result);
    if (logsToMerge != null) {
      builder.addAllLogsToMerge(logsToMerge);
    }
    return builder.build();
  }

  private class Log {
    final long id;
    final byte[] data;
    final long offset;
    final int length;
    // TODO
    final int snapshotVersion = 0;
    boolean commit = false;

    private Log(long id, byte[] data, long offset, int length) {
      this.id = id;
      this.data = data;
      this.offset = offset;
      this.length = length;
    }

    boolean touches(long offset, int length) {
      if (offset + length <= this.offset) {
        return false;
      }

      if (this.offset + this.length <= offset) {
        return false;
      }

      return true;
    }

    void applyData(long offset, int length, ByteBuf data) {
      long end = Math.min(offset + length, this.offset + this.length);
      long start = Math.max(offset, this.offset);

      int offsetInLog = (int) (start - this.offset);

      int len = (int) (end - start);
      int begin = (int) (start - offset);

      data.writerIndex(begin);
      for (int i = 0; i < len; i++) {
        data.writeByte(this.data[offsetInLog + i]);
      }
    }

    PbBroadcastLog toPb(boolean withData) {
      PbBroadcastLog.Builder builder = PbBroadcastLog.newBuilder();
      builder.setLogId(id);
      builder.setLogUuid(id);
      builder.setOffset(offset);
      builder.setLength(length);
      builder.setLogStatus(commit ? PbBroadcastLogStatus.COMMITTED : PbBroadcastLogStatus.CREATED);
      builder.setChecksum(0L);
      if (withData) {
        builder.setData(ByteString.copyFrom(data));
      }
      return builder.build();
    }

  }

}

