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

package py.netty.datanode;

import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import py.common.RequestIdBuilder;
import py.common.struct.Pair;
import py.netty.memory.SimplePooledByteBufAllocator;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbBroadcastLogStatus;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadResponse;
import py.proto.Broadcastlog.PbReadResponseUnit;
import py.test.TestBase;
import py.utils.ReadMergeHelper;

//todo: deal with old ut
public class ReadMergeHelperTest extends TestBase {
  private Random random = new Random();
  private int startLogId = 10;
  private Map<Long, Long> mapLogIdToUuid;
  private int pageSize = 8 * 1024;
  private SimplePooledByteBufAllocator allocator = new SimplePooledByteBufAllocator();
  private int largeCount = allocator.getAvailableLargePageCount();
  private int mediumCount = allocator.getAvailableMediumPageCount();
  private int littleCount = allocator.getAvailableLittlePageCount();

  public ReadMergeHelperTest() {
    mapLogIdToUuid = new HashMap<>();
    setLogLevel(Level.DEBUG);
    allocator.getAvailableLargePageCount();
  }

  @Before
  public void before() {
    mapLogIdToUuid.clear();
  }

  @After
  public void after() {
    assertEquals(largeCount, allocator.getAvailableLargePageCount());
    assertEquals(mediumCount, allocator.getAvailableMediumPageCount());
    assertEquals(littleCount, allocator.getAvailableLittlePageCount());
  }

  private void clear(int[] array) {
    for (int i = 0; i < array.length; i++) {
      array[i] = -1;
    }

  }

  @Ignore

  @Test
  public void mergeReadUnitResponseWithFreeUnit() {
    final int unitCount = 3;
    int responseCount = 3;

    List<PbReadResponse.Builder> builders = new ArrayList<>();
    for (int i = 0; i < responseCount; i++) {
      builders.add(getReadResponse());
    }

    List<Pair<Long, Integer>> unitsInfo = new ArrayList<>();
    unitsInfo.add(new Pair<Long, Integer>(0L, pageSize));
    unitsInfo.add(new Pair<Long, Integer>(pageSize + 11L, pageSize / 2));
    unitsInfo.add(new Pair<Long, Integer>(pageSize * 4 + 500L, pageSize - 100));

    int logCount = 10;
    int step = 10;
    int length = 100;
    int[] freeIndexsOfUnit = new int[responseCount];
    clear(freeIndexsOfUnit);
    int[] realIndexsOfUnit = new int[responseCount];
    clear(realIndexsOfUnit);

    List<List<PbBroadcastLog>> logsToMerge = new ArrayList<>();
    for (int i = 0; i < unitCount; i++) {
      List<PbBroadcastLog> logs = new ArrayList<>();
      long baseOffset = unitsInfo.get(i).getFirst() / pageSize * pageSize;
      for (int j = 0; j < logCount; j++) {
        PbBroadcastLog log = getBroadcastLog(startLogId + j, baseOffset + j * step, length);
        logs.add(log);
      }
      logsToMerge.add(logs);
    }

    List<int[]> logsToReadResponseIndexOfAllResponses = new ArrayList<>();
    // builder response unit for three response
    for (int i = 0; i < unitCount; i++) {
      List<PbReadResponseUnit.Builder> unitBuilders = new ArrayList<>();
      for (int j = 0; j < responseCount; j++) {
        unitBuilders
            .add(getReadResponseUnit(unitsInfo.get(i).getFirst(), unitsInfo.get(i).getSecond(),
                PbIoUnitResult.OK));
      }

      int[] logsToReadResponseIndex = new int[logCount];
      logsToReadResponseIndexOfAllResponses.add(logsToReadResponseIndex);

      // map log to unit in different responses.
      int freeIndex = random.nextInt(responseCount);
      freeIndexsOfUnit[i] = freeIndex;
      List<Integer> availableIndex = new ArrayList<>();
      for (int j = 0; j < responseCount; j++) {
        if (j != freeIndex) {
          availableIndex.add(j);
        }
      }

      int logIndex = 0;
      for (PbBroadcastLog log : logsToMerge.get(i)) {
        int index = random.nextInt(responseCount);
        if (freeIndex == index) {
          index = availableIndex.get(random.nextInt(availableIndex.size()));
          logger.warn("free response index={} for unit={}, to={}", freeIndex, i, index);
        }
        logsToReadResponseIndex[logIndex] = index;
        unitBuilders.get(index).addLogsToMerge(log);
        logger.warn("unit index={}, log Index={} to response index={}", i, logIndex, index);
        realIndexsOfUnit[i] = index;
        logIndex++;
      }

      for (int j = 0; j < responseCount; j++) {
        if (freeIndex == j) {
          unitBuilders.get(j).setResult(PbIoUnitResult.FREE);
          builders.get(j).addResponseUnits(unitBuilders.get(j).build());
          continue;
        }
        builders.get(j).addResponseUnits(unitBuilders.get(j).build());
      }
    }

    List<PyReadResponse> responses = new ArrayList<>();
    for (int i = 0; i < responseCount; i++) {
      List<Pair<Long, Integer>> pairs = getUnitsInfo(unitsInfo, freeIndexsOfUnit, i);
      ByteBuf buf = getBuffer(pairs, i);
      responses.add(new PyReadResponse(builders.get(i).build(), buf));
    }

    PyReadResponse response = ReadMergeHelper
        .merge(responses.toArray(new PyReadResponse[responses.size()]), pageSize);

    int baseIndex = getBaseResponseIndex(responses);

    // check data
    logger.warn("go to check data, base index={}, response={}", baseIndex, response.getData());
    for (int i = 0; i < unitCount; i++) {
      logger.warn("going to check index={}", i);
      ByteBuf data = response.getResponseUnitData(i);
      int start = (int) (response.getMetadata().getResponseUnits(i).getOffset() % pageSize);

      int unitLength = response.getMetadata().getResponseUnits(i).getLength();
      assertEquals(data.readableBytes(), unitLength);
      assertEquals(unitsInfo.get(i).getSecond().intValue(), unitLength);
      for (int j = 0; j < unitLength; j++) {
        if (start + j < length + step * (logCount - 1)) {
          int logIndex = (start + j) / step;
          if (logIndex >= logCount) {
            logIndex = logCount - 1;
          }
          logger.warn("current log={}", logIndex);
          assertEquals(data.readByte(), logsToReadResponseIndexOfAllResponses.get(i)[logIndex] + 1);
        } else {
          if (freeIndexsOfUnit[i] == baseIndex) {
            logger.info("new unit, baseIndex={}, new={}, {}", baseIndex, freeIndexsOfUnit[i],
                realIndexsOfUnit[i]);
            assertEquals(data.readByte(), realIndexsOfUnit[i] + 1);
          } else {
            assertEquals(data.readByte(), baseIndex + 1);
          }
        }
      }

      data.release();
    }

    response.release();
  }

  private List<Pair<Long, Integer>> getUnitsInfo(List<Pair<Long, Integer>> unitsInfo,
      int[] freeIndexsOfUnit,
      int responseIndex) {
    List<Pair<Long, Integer>> units = new ArrayList<>();
    for (int i = 0; i < unitsInfo.size(); i++) {
      for (int j = 0; j < freeIndexsOfUnit.length; j++) {
        if (freeIndexsOfUnit[j] != responseIndex) {
          units.add(unitsInfo.get(i));
        }
      }
    }

    return units;
  }

  @Ignore

  @Test
  public void mergeReadUnitResponseWithoutFreeUnit() {
    List<PbReadResponse.Builder> builders = new ArrayList<>();
    int responseCount = 2;

    for (int i = 0; i < responseCount; i++) {
      builders.add(getReadResponse());
    }

    List<Pair<Long, Integer>> unitsInfo = new ArrayList<>();
    unitsInfo.add(new Pair<Long, Integer>(0L, pageSize));
    unitsInfo.add(new Pair<Long, Integer>(pageSize + 11L, pageSize / 2));
    unitsInfo.add(new Pair<Long, Integer>(pageSize * 4 + 500L, pageSize - 100));

    int logCount = 10;
    int step = 10;
    int length = 100;

    List<List<PbBroadcastLog>> logsToMerge = new ArrayList<>();
    int unitCount = 3;
    for (int i = 0; i < unitCount; i++) {
      List<PbBroadcastLog> logs = new ArrayList<>();
      long baseOffset = unitsInfo.get(i).getFirst() / pageSize * pageSize;
      for (int j = 0; j < logCount; j++) {
        PbBroadcastLog log = getBroadcastLog(startLogId + j, baseOffset + j * step, length);
        logs.add(log);
      }
      logsToMerge.add(logs);
    }

    List<int[]> logsToReadResponseIndexOfAllResponses = new ArrayList<>();
    // builder response unit for three response
    for (int i = 0; i < unitCount; i++) {
      List<PbReadResponseUnit.Builder> unitBuilders = new ArrayList<>();
      for (int j = 0; j < responseCount; j++) {
        unitBuilders
            .add(getReadResponseUnit(unitsInfo.get(i).getFirst(), unitsInfo.get(i).getSecond(),
                PbIoUnitResult.OK));
      }

      int[] logsToReadResponseIndex = new int[logCount];
      logsToReadResponseIndexOfAllResponses.add(logsToReadResponseIndex);

      // map log to unit in different responses.
      int logIndex = 0;
      for (PbBroadcastLog log : logsToMerge.get(i)) {
        int index = random.nextInt(unitBuilders.size());
        logsToReadResponseIndex[logIndex] = index;
        unitBuilders.get(index).addLogsToMerge(log);
        logger.warn("unit index={}, log Index={} to response index={}", i, logIndex, index);
        logIndex++;
      }

      for (int j = 0; j < responseCount; j++) {
        builders.get(j).addResponseUnits(unitBuilders.get(j).build());
      }
    }

    List<PyReadResponse> responses = new ArrayList<>();
    for (int i = 0; i < responseCount; i++) {
      ByteBuf buf = getBuffer(unitsInfo, i);
      responses.add(new PyReadResponse(builders.get(i).build(), buf));
    }

    PyReadResponse response = ReadMergeHelper
        .merge(responses.toArray(new PyReadResponse[responses.size()]), pageSize);

    int baseIndex = getBaseResponseIndex(responses);

    // check data
    logger.warn("go to check data, base index={}, response={}", baseIndex, response.getData());
    for (int i = 0; i < unitCount; i++) {
      ByteBuf data = response.getResponseUnitData(i);
      int start = (int) (response.getMetadata().getResponseUnits(i).getOffset() % pageSize);

      int unitLength = response.getMetadata().getResponseUnits(i).getLength();
      assertEquals(data.readableBytes(), unitLength);
      assertEquals(unitsInfo.get(i).getSecond().intValue(), unitLength);
      for (int j = 0; j < unitLength; j++) {
        if (start + j < length + step * (logCount - 1)) {
          int logIndex = (start + j) / step;
          if (logIndex >= logCount) {
            logIndex = logCount - 1;
          }
          logger.warn("current log={}", logIndex);
          assertEquals(data.readByte(), logsToReadResponseIndexOfAllResponses.get(i)[logIndex] + 1);
        } else {
          assertEquals(data.readByte(), baseIndex + 1);
        }
      }

      data.release();
    }

    response.release();
  }

  public int getBaseResponseIndex(List<PyReadResponse> responses) {
    int index = -1;
    long pclId = -1;
    for (int i = 0; i < responses.size(); i++) {
      PyReadResponse response = responses.get(i);
      if (pclId < response.getMetadata().getPclId()) {
        pclId = response.getMetadata().getPclId();
        index = i;
      }
    }

    return index;
  }

  public ByteBuf getBuffer(List<Pair<Long, Integer>> unitsInfo, int index) {
    int length = 0;
    for (Pair<Long, Integer> pair : unitsInfo) {
      length += pair.getSecond();
    }

    ByteBuf byteBuf = allocator.ioBuffer(length);
    for (int i = 0; i < length; i++) {
      byteBuf.writeByte(1 + index);
    }
    return byteBuf;
  }

  public PbReadResponse.Builder getReadResponse() {
    PbReadResponse.Builder builder = PbReadResponse.newBuilder();
    builder.setRequestId(RequestIdBuilder.get());
    builder.setPclId(random.nextInt(startLogId));
    return builder;
  }

  public PbReadResponseUnit.Builder getReadResponseUnit(long offset, int length,
      PbIoUnitResult result) {
    PbReadResponseUnit.Builder builder = PbReadResponseUnit.newBuilder();
    builder.setResult(result);
    builder.setOffset(offset);
    builder.setLength(length);
    builder.setChecksum(0);
    return builder;
  }

  public PbBroadcastLog getBroadcastLog(long logId, long offset, int length) {
    PbBroadcastLog.Builder builder = PbBroadcastLog.newBuilder();
    builder.setChecksum(0);
    builder.setLogId(logId);
    Long uuid = mapLogIdToUuid.get(logId);
    if (uuid == null) {
      uuid = RequestIdBuilder.get();
      builder.setLogUuid(uuid);
      mapLogIdToUuid.put(logId, uuid);
    } else {
      builder.setLogUuid(uuid);
    }

    builder.setLength(length);
    builder.setLogStatus(PbBroadcastLogStatus.COMMITTED);
    builder.setOffset(offset);
    return builder.build();
  }
}
