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

import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.common.RequestIdBuilder;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog.PbBroadcastLogManager;
import py.proto.Broadcastlog.PbWriteRequest;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.test.TestBase;

public class PyWriteRequestTest extends TestBase {
  private static final Logger logger = LoggerFactory.getLogger(PyWriteRequestTest.class);
  private static final int TESTING_REQUEST_UNIT_SIZE = 10;

  public static PbWriteRequestUnit generatePbWriteRequestUnit(long logUuid, long logId,
      ByteBuf data, int length, long offset) {
    PbWriteRequestUnit.Builder builder = PbWriteRequestUnit.newBuilder();
    builder.setLogUuid(++logUuid);
    builder.setLogId(++logId);
    builder.setOffset(offset);
    builder.setLength(length);
    builder.setRandomWrite(true);
    return builder.build();
  }

  public static byte[] generateRandomData(int index) {
    byte[] data = new byte[TESTING_REQUEST_UNIT_SIZE];

    for (int i = 0; i < TESTING_REQUEST_UNIT_SIZE; i++) {
      data[i] = (byte) (index * 10 + i);
    }

    return data;
  }

  @Test
  public void testInitDataOffsets() throws Exception {
    PyWriteRequest writeRequest = createWriteRequest();

    int index = 0;
    ByteBuf data = writeRequest.getRequestUnitData(index);
    assertEquals(data.readableBytes(),
        writeRequest.getMetadata().getRequestUnits(index).getLength());
    for (int i = 0; i < TESTING_REQUEST_UNIT_SIZE; ++i) {
      assertEquals((index + 1) * 10 + i, data.getByte(i));
    }

    index = 1;
    try {
      data = writeRequest.getRequestUnitData(index);
      assertEquals(data.readableBytes(), 0);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      Assert.fail();
    }

    index = 2;
    data = writeRequest.getRequestUnitData(index);
    assertEquals(data.readableBytes(),
        writeRequest.getMetadata().getRequestUnits(index).getLength());
    for (int i = 0; i < TESTING_REQUEST_UNIT_SIZE; ++i) {
      assertEquals((index + 1) * 10 + i, data.getByte(i));
    }

  }

  public PyWriteRequest createWriteRequest() throws Exception {
    /*
     * PbWriteRequest
     */
    PbWriteRequest.Builder writeRequestBuilder = PbWriteRequest.newBuilder();
    writeRequestBuilder.setRequestId(RequestIdBuilder.get());
    writeRequestBuilder.setVolumeId(1);
    writeRequestBuilder.setSegIndex(2);
    writeRequestBuilder.setFailTimes(5);
    writeRequestBuilder.setZombieWrite(false);
    writeRequestBuilder.setRequestTime(System.currentTimeMillis());

    // generate request unit data
    List<PbWriteRequestUnit> requestUnitList = new ArrayList<PbWriteRequestUnit>();

    ByteBuf data1 = Unpooled.wrappedBuffer(generateRandomData(1));
    PbWriteRequestUnit requestUnit = generatePbWriteRequestUnit(1, 1, data1,
        TESTING_REQUEST_UNIT_SIZE,
        TESTING_REQUEST_UNIT_SIZE);

    PbWriteRequestUnit requestUnit1 = generatePbWriteRequestUnit(1, 1, null, 0,
        TESTING_REQUEST_UNIT_SIZE * 2);

    ByteBuf data2 = Unpooled.wrappedBuffer(generateRandomData(3));
    PbWriteRequestUnit requestUnit2 = generatePbWriteRequestUnit(1, 1, data2,
        TESTING_REQUEST_UNIT_SIZE,
        TESTING_REQUEST_UNIT_SIZE * 3);

    requestUnitList.add(requestUnit);
    requestUnitList.add(requestUnit1);
    requestUnitList.add(requestUnit2);
    writeRequestBuilder.addAllRequestUnits(requestUnitList);

    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(2));
    secondaries.add(new InstanceId(3));
    SegmentMembership membership = new SegmentMembership(new InstanceId(1), secondaries);
    writeRequestBuilder.setMembership(PbRequestResponseHelper.buildPbMembershipFrom(membership));
    List<PbBroadcastLogManager> logManagerList = new ArrayList<PbBroadcastLogManager>();
    writeRequestBuilder.addAllBroadcastManagers(logManagerList);
    PbWriteRequest writeRequest = writeRequestBuilder.build();

    ByteBuf finalData = Unpooled.wrappedBuffer(data1, data2);
    return new PyWriteRequest(writeRequest, finalData);
  }
}
