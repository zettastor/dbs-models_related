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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.CodedOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.HashedWheelTimer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.netty.client.AsyncResponseHandler;
import py.netty.client.MessageTimeManager;
import py.netty.core.ByteToMessageDecoder;
import py.netty.core.ProtocolFactory;
import py.netty.exception.InvalidProtocolException;
import py.netty.message.Header;
import py.netty.message.Message;
import py.netty.message.MethodType;
import py.netty.message.ProtocolBufProtocolFactory;
import py.proto.Broadcastlog;
import py.test.TestBase;

public class NettyClientPipelineTest extends TestBase {
  private static final Logger logger = LoggerFactory.getLogger(NettyClientPipelineTest.class);

  private ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
  private AsyncResponseHandler responseHandler;

  public NettyClientPipelineTest() {
    ProtocolFactory protocolFactory = null;
    try {
      protocolFactory = ProtocolBufProtocolFactory.create(AsyncDataNode.AsyncIface.class);
    } catch (InvalidProtocolException e) {
      logger.error("caught an exception", e);
      fail();
    }

    HashedWheelTimer hashedWheelTimer = new HashedWheelTimer(
        new ThreadFactoryBuilder().setNameFormat("nett4-wheel-timer-%s").build(), 100,
        TimeUnit.MILLISECONDS,
        512);
    MessageTimeManager messageTimeManager = new MessageTimeManager(hashedWheelTimer, 5000);
    responseHandler = new AsyncResponseHandlerTest(protocolFactory.getProtocol(),
        protocolFactory.getGetMethodTypeInterface(), messageTimeManager);

  }

  @Before
  public void beforeMethod() {
    String ip = "127.0.0.1";
    InetSocketAddress inetSocketAddress = new InetSocketAddress(ip, 8080);
    Channel channel = mock(Channel.class);

    when(channel.remoteAddress()).thenReturn(inetSocketAddress);

    when(ctx.channel()).thenReturn(channel);
  }

  @Test
  public void testPipeline1() {
    AtomicInteger requestCount = new AtomicInteger(0);
    ByteToMessageDecoder decoder = new ByteToMessageDecoder() {
      @Override
      public void fireChanelRead(ChannelHandlerContext ctx, Message msg) {
        try {
          requestCount.incrementAndGet();
          responseHandler.channelRead(ctx, msg);
          super.fireChanelRead(ctx, msg);
        } catch (Exception e) {
          logger.error("caught an exception", e);
          fail();
        }
      }
    };

    // two read response, first response data length is 0
    ByteBuf readResponseA = generateReadResponse(0);
    ByteBuf readResponseB = generateReadResponse(4096);

    int step1InOneLength = Header.headerLength();

    int step2InOneLength = readResponseA.readableBytes() - step1InOneLength;

    ByteBuf channelRead1 = readResponseA.retainedSlice(0, step1InOneLength);

    final ByteBuf channelRead2 = Unpooled
        .wrappedBuffer(readResponseA.retainedSlice(step1InOneLength, step2InOneLength),
            readResponseB);

    assertEquals(3, readResponseA.refCnt());
    assertEquals(1, readResponseB.refCnt());
    assertEquals(1, channelRead1.refCnt());
    assertEquals(1, channelRead2.refCnt());

    // prepare leak env
    try {
      decoder.channelRead(ctx, channelRead1);
      assertEquals(2, readResponseA.refCnt());
      assertEquals(1, readResponseB.refCnt());
      assertEquals(0, channelRead1.refCnt());
      assertEquals(1, channelRead2.refCnt());
      assertTrue(decoder.getHeader() != null);
      assertTrue(decoder.getCumulation() == null);
      assertEquals(0, requestCount.get());
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail();
    }

    // no cumulation, header exist, first request(data length = 0 leak), parse second header
    try {
      decoder.channelRead(ctx, channelRead2);
      assertEquals(1, readResponseA.refCnt());
      assertEquals(0, readResponseB.refCnt());
      assertEquals(0, channelRead1.refCnt());
      assertEquals(0, channelRead2.refCnt());
      assertTrue(decoder.getHeader() == null);
      assertTrue(decoder.getCumulation() == null);
      assertEquals(2, requestCount.get());
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail();
    }

    assertTrue(readResponseA.release());
    assertEquals(0, readResponseA.refCnt());
  }

  @Test
  public void testPipelineRandom1() {
  }

  private ByteBuf generateReadResponse(int dataLength) {
    // header + metadata + data(optional)
    final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
    // header + metadata
    Header header = new Header((byte) MethodType.READ.getValue(), 0, dataLength,
        RequestIdBuilder.get());

    Broadcastlog.PbReadResponse.Builder builder = Broadcastlog.PbReadResponse.newBuilder();
    builder.setRequestId(RequestIdBuilder.get());

    int readUnitCount = 5;
    int eachUnitDataLength = dataLength / readUnitCount;
    int offset = 0;
    for (int i = 0; i < readUnitCount; i++) {
      int unitDataLength;
      if (i + 1 == readUnitCount) {
        // the last loop
        unitDataLength = eachUnitDataLength + (dataLength % readUnitCount);
      } else {
        unitDataLength = eachUnitDataLength;
      }
      Broadcastlog.PbReadResponseUnit.Builder unitBuilder = Broadcastlog.PbReadResponseUnit
          .newBuilder();
      unitBuilder.setOffset(offset);
      unitBuilder.setLength(unitDataLength);
      offset += unitDataLength;
      unitBuilder.setResult(Broadcastlog.PbIoUnitResult.OK);
      builder.addResponseUnits(unitBuilder.build());
    }

    Broadcastlog.PbReadResponse readResponse = builder.build();
    int metadataLength = readResponse.getSerializedSize();
    header.setMetadataLength(metadataLength);

    ByteBuf byteBuf = allocator.buffer(Header.headerLength() + metadataLength + dataLength);
    // header
    header.toBuffer(byteBuf);

    // metadata
    byte[] metadata = new byte[metadataLength];
    try {
      readResponse.writeTo(CodedOutputStream.newInstance(metadata, 0, metadataLength));
      byteBuf.writeBytes(metadata, 0, metadataLength);
    } catch (IOException e) {
      logger.error("caught an exception", e);
      fail();
    }

    // data
    if (dataLength > 0) {
      byte[] data = new byte[dataLength];
      byteBuf.writeBytes(data, 0, dataLength);
    }

    return byteBuf;
  }

  private ByteBuf generateWriteResponse() {
    final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
    // header + metadata
    Header header = new Header((byte) MethodType.WRITE.getValue(), 0, 0, RequestIdBuilder.get());

    Broadcastlog.PbWriteResponse.Builder builder = Broadcastlog.PbWriteResponse.newBuilder();
    builder.setRequestId(RequestIdBuilder.get());

    int unitCount = 5;
    for (int i = 0; i < unitCount; i++) {
      Broadcastlog.PbWriteResponseUnit.Builder unitBuilder = Broadcastlog.PbWriteResponseUnit
          .newBuilder();
      unitBuilder.setLogUuid(RequestIdBuilder.get());
      unitBuilder.setLogId(RequestIdBuilder.get());
      unitBuilder.setLogResult(Broadcastlog.PbIoUnitResult.BROADCAST_FAILED);
      builder.addResponseUnits(unitBuilder.build());
    }
    Broadcastlog.PbWriteResponse pbWriteResponse = builder.build();
    int metadataLength = pbWriteResponse.getSerializedSize();

    header.setMetadataLength(metadataLength);

    ByteBuf byteBuf = allocator.buffer(Header.headerLength() + metadataLength);

    header.toBuffer(byteBuf);

    byte[] metadata = new byte[metadataLength];
    try {
      pbWriteResponse.writeTo(CodedOutputStream.newInstance(metadata, 0, metadataLength));
      byteBuf.writeBytes(metadata, 0, metadataLength);
    } catch (IOException e) {
      logger.error("caught an exception", e);
      fail();
    }
    return byteBuf;
  }
}
