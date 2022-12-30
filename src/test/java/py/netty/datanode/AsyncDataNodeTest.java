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

import com.google.protobuf.AbstractMessageLite;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import py.PbRequestResponseHelper;
import py.common.struct.EndPoint;
import py.connection.pool.udp.NioUdpEchoServer;
import py.exception.ChecksumMismatchedException;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.client.TransferenceClientOption;
import py.netty.core.AbstractMethodCallback;
import py.netty.core.MethodCallback;
import py.netty.core.Protocol;
import py.netty.core.ProtocolFactory;
import py.netty.core.TransferenceConfiguration;
import py.netty.core.TransferenceOption;
import py.netty.exception.AbstractNettyException;
import py.netty.exception.InvalidProtocolException;
import py.netty.exception.NotSupportedException;
import py.netty.exception.ServerProcessException;
import py.netty.exception.TooBigFrameException;
import py.netty.memory.SimplePooledByteBufAllocator;
import py.netty.message.Header;
import py.netty.message.Message;
import py.netty.message.ProtocolBufProtocolFactory;
import py.netty.server.GenericAsyncServer;
import py.netty.server.GenericAsyncServerBuilder;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchRequest;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchResponse;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogsResponse;
import py.proto.Broadcastlog.PbCopyPageResponse;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbMembership;
import py.proto.Broadcastlog.PbReadRequest;
import py.proto.Broadcastlog.PbReadRequestUnit;
import py.proto.Broadcastlog.PbReadResponse;
import py.proto.Broadcastlog.PbReadResponseUnit;
import py.proto.Broadcastlog.PbWriteRequest;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.proto.Broadcastlog.PbWriteResponse;
import py.proto.Broadcastlog.PbWriteResponseUnit;
import py.proto.Commitlog;
import py.test.TestBase;
import py.utils.DataConsistencyUtils;

public class AsyncDataNodeTest extends TestBase {
  private static final int DEFAULT_SERVER_PORT = 12345;
  private static ProtocolBufProtocolFactory protocolFactory = null;
  private static GenericAsyncServer server = null;
  private static NioUdpEchoServer udpServer;
  private GenericAsyncClientFactory<AsyncDataNode.AsyncIface> clientFactory;
  private SimplePooledByteBufAllocator allocator =
      new SimplePooledByteBufAllocator(1024 * 1024 * 10, 8192);

  @BeforeClass
  public static void beforeClass() throws Exception {
    protocolFactory = (ProtocolBufProtocolFactory) ProtocolBufProtocolFactory
        .create(AsyncDataNode.AsyncIface.class);
    EndPoint endPoint = new EndPoint("localhost", DEFAULT_SERVER_PORT);
    GenericAsyncServerBuilder serverFactory = new GenericAsyncServerBuilder(new ServiceImpl(),
        protocolFactory, GenericAsyncServerBuilder.defaultConfiguration());
    serverFactory.getConfiguration().option(TransferenceOption.MAX_MESSAGE_LENGTH, 1 * 1024 * 1024);
    udpServer = new NioUdpEchoServer(endPoint);
    udpServer.startEchoServer();
    server = serverFactory.build(endPoint);
  }

  @AfterClass
  public static void afterClass() {
    if (server != null) {
      server.shutdown();
    }

    if (udpServer != null) {
      udpServer.stopEchoServer();
    }
  }

  private static PbMembership generateMembership() {
    PbMembership.Builder membershipBuilder = PbMembership.newBuilder();
    membershipBuilder.setEpoch(0);
    membershipBuilder.setGeneration(0);
    membershipBuilder.setPrimary(0L);
    List<Long> secondaries = new ArrayList<Long>();
    secondaries.add(1L);
    secondaries.add(2L);
    membershipBuilder.addAllSecondaries(secondaries);
    return membershipBuilder.build();
  }

  @After
  public void after() {
    if (clientFactory != null) {
      clientFactory.close();
    }
  }

  @Test
  public void exception() throws Exception {
    GenericAsyncServer server1 = null;
    EndPoint endPoint = new EndPoint("localhost", DEFAULT_SERVER_PORT + 1);
    final AtomicBoolean invalidProtocol = new AtomicBoolean(false);
    final AtomicBoolean notSupport = new AtomicBoolean(false);
    try {
      GenericAsyncServerBuilder serverFactory = new GenericAsyncServerBuilder(
          new AsyncDataNode.AsyncIface() {
            @Override
            public void ping(MethodCallback<Object> callback) {
              callback.fail(new Exception("i am not ok"));
            }

            @Override
            public void write(PyWriteRequest request, MethodCallback<PbWriteResponse> callback) {
              logger.info("receive a write request");
              PbWriteResponse.Builder builder = PbWriteResponse.newBuilder();
              builder.setRequestId(request.getMetadata().getRequestId());
              if (request.getMetadata().getRequestUnits(0).getLength() > 0) {
                throw new RuntimeException();
              }
              callback.complete(builder.build());
            }

            @Override
            public void read(PbReadRequest request, MethodCallback<PyReadResponse> callback) {
            }

            @Override
            public void copy(PyCopyPageRequest request,
                MethodCallback<PbCopyPageResponse> callback) {
            }

            @Override
            public void check(Broadcastlog.PbCheckRequest request,
                MethodCallback<Broadcastlog.PbCheckResponse> callback) {
            }

            @Override
            public void giveYouLogId(Broadcastlog.GiveYouLogIdRequest request,
                MethodCallback<Broadcastlog.GiveYouLogIdResponse> callback) {
            }

            @Override
            public void getMembership(Broadcastlog.PbGetMembershipRequest request,
                MethodCallback<Broadcastlog.PbGetMembershipResponse> callback) {
            }

            @Override
            public void addOrCommitLogs(Commitlog.PbCommitlogRequest request,
                MethodCallback<Commitlog.PbCommitlogResponse> callback) {
            }

            @Override
            public void discard(PbWriteRequest request, MethodCallback<PbWriteResponse> callback) {
            }

            @Override
            public void startOnlineMigration(Commitlog.PbStartOnlineMigrationRequest request,
                MethodCallback<Commitlog.PbStartOnlineMigrationResponse> callback) {
            }

            @Override
            public void syncLog(PbAsyncSyncLogsBatchRequest request,
                MethodCallback<PbAsyncSyncLogsBatchResponse> callback) {
            }

            @Override
            public void backwardSyncLog(PbBackwardSyncLogsRequest request,
                MethodCallback<PbBackwardSyncLogsResponse> callback) {
            }
          }, new
          ProtocolFactory() {
            @Override
            public Protocol getProtocol() {
              return new Protocol() {
                @Override
                public Method getMethod(int methodType) throws NotSupportedException {
                  if (notSupport.get()) {
                    throw new NotSupportedException(methodType);
                  } else {
                    return protocolFactory.getProtocol().getMethod(methodType);
                  }
                }

                @Override
                public AbstractNettyException decodeException(ByteBuf buffer)
                    throws InvalidProtocolException {
                  return protocolFactory.getProtocol().decodeException(buffer);
                }

                @Override
                public ByteBuf encodeRequest(Header header, AbstractMessageLite metadata)
                    throws InvalidProtocolException {
                  return protocolFactory.getProtocol().encodeRequest(header, metadata);
                }

                @Override
                public ByteBuf encodeResponse(Header header, AbstractMessageLite metadata)
                    throws InvalidProtocolException {
                  return protocolFactory.getProtocol().encodeResponse(header, metadata);
                }

                @Override
                public Object decodeRequest(Message msg) throws InvalidProtocolException {
                  if (invalidProtocol.get()) {
                    throw new InvalidProtocolException("method type: " + msg);
                  } else {
                    return protocolFactory.getProtocol().decodeRequest(msg);
                  }
                }

                @Override
                public Object decodeResponse(Message msg) throws InvalidProtocolException {
                  return protocolFactory.getProtocol().decodeResponse(msg);
                }

                @Override
                public ByteBuf encodeException(long requestId, AbstractNettyException e) {
                  return protocolFactory.getProtocol().encodeException(requestId, e);
                }
              };
            }
          }, GenericAsyncServerBuilder.defaultConfiguration());

      server1 = serverFactory.build(endPoint);
      PbWriteRequest.Builder builder = PbWriteRequest.newBuilder();
      builder.setSegIndex(0);
      builder.setFailTimes(0);
      builder.setRequestId(0L);
      builder.setVolumeId(0L);
      builder.setZombieWrite(false);
      builder.setMembership(generateMembership());
      builder.setRequestTime(System.currentTimeMillis());

      List<PbWriteRequestUnit> requests = new ArrayList<PbWriteRequestUnit>();
      builder.addAllRequestUnits(requests);

      AsyncDataNode.AsyncIface client = generateClient(endPoint);
      final CountDownLatch latch1 = new CountDownLatch(1);
      final AtomicInteger errorCounter = new AtomicInteger(0);

      // process exception
      client.write(new PyWriteRequest(builder.build(), null),
          new AbstractMethodCallback<PbWriteResponse>() {
            @Override
            public void complete(PbWriteResponse object) {
              logger.warn("receive a response: {}", object);
            }

            @Override
            public void fail(Exception e) {
              logger.warn("receive a exception", e);
              assertTrue(e instanceof ServerProcessException);
              errorCounter.incrementAndGet();
              latch1.countDown();
            }
          });
      latch1.await();
      assertEquals(errorCounter.get(), 1);

      // call fail() method
      final CountDownLatch latch2 = new CountDownLatch(1);
      client.ping(new AbstractMethodCallback<Object>() {
        @Override
        public void complete(Object object) {
          logger.info("receive a response");
        }

        @Override
        public void fail(Exception e) {
          logger.info("receive a exception", e);
          assertTrue(e instanceof ServerProcessException);
          errorCounter.incrementAndGet();
          latch2.countDown();
        }
      });
      latch2.await();
      assertEquals(errorCounter.get(), 2);

      // parse request exception
      invalidProtocol.set(true);
      final CountDownLatch latch3 = new CountDownLatch(1);
      client.ping(new AbstractMethodCallback<Object>() {
        @Override
        public void complete(Object object) {
          logger.info("receive a response");
        }

        @Override
        public void fail(Exception e) {
          logger.info("receive a exception", e);
          assertTrue(e instanceof InvalidProtocolException);
          errorCounter.incrementAndGet();
          latch3.countDown();
        }
      });
      latch3.await();
      assertEquals(errorCounter.get(), 3);

      invalidProtocol.set(false);
      notSupport.set(true);

      final CountDownLatch latch4 = new CountDownLatch(1);
      client.ping(new AbstractMethodCallback<Object>() {
        @Override
        public void complete(Object object) {
          logger.info("receive a response");
        }

        @Override
        public void fail(Exception e) {
          logger.info("receive a exception", e);
          assertTrue(e instanceof NotSupportedException);
          errorCounter.incrementAndGet();
          latch4.countDown();
        }
      });
      latch4.await();
      assertEquals(errorCounter.get(), 4);

    } catch (Exception e) {
      fail("exception: " + e);
    } finally {
      try {
        clientFactory.close();
      } catch (Exception e) {
        logger.warn("caught an exception", e);
      }
    }
  }

  @Test
  public void pingFailure() throws Exception {
    protocolFactory = (ProtocolBufProtocolFactory) ProtocolBufProtocolFactory
        .create(AsyncDataNode.AsyncIface.class);
    EndPoint endPoint = new EndPoint("localhost", DEFAULT_SERVER_PORT + 1);
    GenericAsyncServerBuilder serverFactory = new GenericAsyncServerBuilder(new ServiceImpl(),
        protocolFactory, GenericAsyncServerBuilder.defaultConfiguration());
    serverFactory.getConfiguration().option(TransferenceOption.MAX_MESSAGE_LENGTH, 1 * 1024 * 1024);
    GenericAsyncServer server = serverFactory.build(endPoint);

    AsyncDataNode.AsyncIface client = generateClient(
        new EndPoint("localhost", DEFAULT_SERVER_PORT + 1), 1);
    final CountDownLatch latch = new CountDownLatch(1);
    client.ping(new AbstractMethodCallback<Object>() {
      @Override
      public void complete(Object object) {
        logger.warn("complete ping notifyAllListeners");
        latch.countDown();
      }

      @Override
      public void fail(Exception exception) {
        logger.error("caught an exception", exception);
        latch.countDown();
        Assert.fail("ping failure");
      }
    });
    latch.await();

    server.shutdown();
    Thread.sleep(3000);
    final CountDownLatch latch1 = new CountDownLatch(1);
    client.ping(new AbstractMethodCallback<Object>() {
      @Override
      public void complete(Object object) {
        logger.warn("complete ping notifyAllListeners");
        latch1.countDown();
        Assert.fail("ping success");
      }

      @Override
      public void fail(Exception exception) {
        latch1.countDown();
        logger.error("caught an exception", exception);
      }
    });

    latch1.await();
  }

  @Test
  public void ping() throws Exception {
    AsyncDataNode.AsyncIface client = generateClient(
        new EndPoint("localhost", DEFAULT_SERVER_PORT));
    final CountDownLatch latch = new CountDownLatch(1);
    client.ping(new AbstractMethodCallback<Object>() {
      @Override
      public void complete(Object object) {
        logger.warn("complete ping notifyAllListeners");
        latch.countDown();
      }

      @Override
      public void fail(Exception exception) {
        logger.error("caught an exception", exception);
        latch.countDown();
        Assert.fail("fail to ping");
      }
    });

    latch.await();
  }

  @Test
  public void multiClient() throws Exception {
    AsyncDataNode.AsyncIface client = generateClient(
        new EndPoint("localhost", DEFAULT_SERVER_PORT));
    final CountDownLatch latch = new CountDownLatch(1);
    client.ping(new AbstractMethodCallback<Object>() {
      @Override
      public void complete(Object object) {
        logger.warn("complete ping notifyAllListeners");
        latch.countDown();
      }

      @Override
      public void fail(Exception exception) {
        logger.error("caught an exception", exception);
      }
    });

    client = generateClient(new EndPoint("localhost", DEFAULT_SERVER_PORT));
    final CountDownLatch latch1 = new CountDownLatch(1);
    client.ping(new AbstractMethodCallback<Object>() {
      @Override
      public void complete(Object object) {
        logger.info("complete ping notifyAllListeners");
        latch1.countDown();
      }

      @Override
      public void fail(Exception exception) {
        logger.error("caught an exception", exception);
      }
    });

    latch.await();
    latch1.await();
  }

  @Test
  public void stringConvert() throws Exception {
    String src = "i love you";
    byte[] data = AbstractNettyException.stringToBuffer(src);
    String des = AbstractNettyException.bufferToString(data, 0, data.length);
    assertEquals(data.length, src.length());
    assertEquals(0, src.compareTo(des));
  }

  @Test
  public void readData() throws Exception {
    final AsyncDataNode.AsyncIface client = generateClient(
        new EndPoint("localhost", DEFAULT_SERVER_PORT));
    Random random = new Random();

    PbReadRequest.Builder builder = PbReadRequest.newBuilder();
    builder.setSegIndex(0);
    builder.setFailTimes(0);
    builder.setRequestId(0L);
    builder.setVolumeId(0L);
    builder.setReadCause(Broadcastlog.ReadCause.FETCH);
    builder.setMembership(generateMembership());
    final int count = random.nextInt(3);
    List<PbReadRequestUnit> requests = new ArrayList<PbReadRequestUnit>();
    for (int i = 0; i < count; i++) {
      requests.add(PbRequestResponseHelper.buildPbReadRequestUnitFrom(i * 512, 8192));
    }

    builder.addAllRequestUnits(requests);
    final CountDownLatch latch = new CountDownLatch(1);

    MethodCallback<PyReadResponse> callback = new AbstractMethodCallback<PyReadResponse>() {
      @Override
      public void complete(PyReadResponse object) {
        logger.warn("receive a response object: {}", object.getMetadata());
        try {
          PbReadResponse response = object.getMetadata();
          for (int i = 0; i < response.getResponseUnitsCount(); i++) {
            PbReadResponseUnit unit = response.getResponseUnits(i);
            ByteBuf data = object.getResponseUnitData(i);
            // compare data is consistent
            if (!DataConsistencyUtils.checkDataByOffset(data.nioBuffer(), unit.getOffset())) {
              assertTrue(false);
            }
          }
        } finally {
          latch.countDown();
        }
      }

      @Override
      public void fail(Exception exception) {
        logger.error("caught an exception", exception);
        latch.countDown();
        assertTrue(false);
      }
    };

    PbReadRequest readRequest = builder.build();
    logger.warn("send size: {}", readRequest.getSerializedSize());
    client.read(readRequest, callback);
    latch.await();
  }

  @Test
  public void tooBigData() throws Exception {
    final AsyncDataNode.AsyncIface client = generateClient(
        new EndPoint("localhost", DEFAULT_SERVER_PORT));

    PbWriteRequest.Builder builder = PbWriteRequest.newBuilder();
    builder.setSegIndex(0);
    builder.setFailTimes(0);
    builder.setRequestId(0L);
    builder.setVolumeId(0L);
    builder.setZombieWrite(false);
    builder.setMembership(generateMembership());
    builder.setRequestTime(System.currentTimeMillis());

    List<PbWriteRequestUnit> requests = new ArrayList<PbWriteRequestUnit>();
    int length = 1024 * 1024 + 1;
    ByteBuf dataToSend = allocator.buffer(length)
        .writeBytes(DataConsistencyUtils.generateDataByOffset(length, 0).array());
    requests.add(PbRequestResponseHelper.buildPbWriteRequestUnitFrom(0L, 0, 0, length, dataToSend));

    builder.addAllRequestUnits(requests);
    final CountDownLatch latch = new CountDownLatch(1);
    final Exception[] exceptions = new Exception[1];

    MethodCallback<PbWriteResponse> callback = new AbstractMethodCallback<PbWriteResponse>() {
      @Override
      public void complete(PbWriteResponse object) {
        logger.warn("receive an response: {}", object);
        latch.countDown();
      }

      @Override
      public void fail(Exception exception) {
        logger.error("caught an exception", exception);
        exceptions[0] = exception;
        latch.countDown();

      }
    };

    PyWriteRequest request = new PyWriteRequest(builder.build(), dataToSend);
    logger.info("send size: {}", request.getMetadata().getSerializedSize());
    client.write(request, callback);
    latch.await();
    assertTrue(exceptions[0] instanceof TooBigFrameException);
    exceptions[0] = null;

    PbReadRequest.Builder readbuilder = PbReadRequest.newBuilder();
    readbuilder.setSegIndex(0);
    readbuilder.setFailTimes(0);
    readbuilder.setRequestId(0L);
    readbuilder.setVolumeId(0L);
    readbuilder.setMembership(generateMembership());
    readbuilder.setReadCause(Broadcastlog.ReadCause.FETCH);
    List<PbReadRequestUnit> readRequests = new ArrayList<PbReadRequestUnit>();
    readRequests.add(PbRequestResponseHelper.buildPbReadRequestUnitFrom(0, 1024 * 1024 + 1));

    readbuilder.addAllRequestUnits(readRequests);
    final CountDownLatch latch1 = new CountDownLatch(1);

    MethodCallback<PyReadResponse> callback1 = new AbstractMethodCallback<PyReadResponse>() {
      @Override
      public void complete(PyReadResponse object) {
        logger.warn("receive a response object: {}", object.getMetadata());
        latch1.countDown();
      }

      @Override
      public void fail(Exception exception) {
        logger.error("caught an exception", exception);
        exceptions[0] = exception;
        latch1.countDown();
      }
    };

    PbReadRequest readRequest = readbuilder.build();
    logger.warn("send size: {}", readRequest.getSerializedSize());
    client.read(readRequest, callback1);
    latch1.await();
    assertTrue(exceptions[0] instanceof TooBigFrameException);
  }

  @Test
  public void writeData() throws Exception {
    final AsyncDataNode.AsyncIface client = generateClient(
        new EndPoint("localhost", DEFAULT_SERVER_PORT));

    Random random = new Random();

    PbWriteRequest.Builder builder = PbWriteRequest.newBuilder();
    builder.setSegIndex(0);
    builder.setFailTimes(0);
    builder.setRequestId(0L);
    builder.setVolumeId(0L);
    builder.setZombieWrite(false);
    builder.setMembership(generateMembership());
    builder.setRequestTime(System.currentTimeMillis());

    final int count = random.nextInt(3);
    ByteBuf dataToSend = null;
    List<PbWriteRequestUnit> requests = new ArrayList<PbWriteRequestUnit>();
    logger.warn("count: {}", count);
    for (int i = 0; i < count; i++) {
      long offset = i * 512;
      int length = 8193;
      ByteBuf data = allocator.buffer(length)
          .writeBytes(DataConsistencyUtils.generateDataByOffset(length, offset).array());
      requests.add(PbRequestResponseHelper.buildPbWriteRequestUnitFrom(0L, 0, i * 512, 8193, data));
      dataToSend = dataToSend == null ? data : Unpooled.wrappedBuffer(dataToSend, data);
    }

    builder.addAllRequestUnits(requests);
    final CountDownLatch latch = new CountDownLatch(1);

    MethodCallback<PbWriteResponse> callback = new AbstractMethodCallback<PbWriteResponse>() {
      @Override
      public void complete(PbWriteResponse object) {
        logger.warn("receive an response: {}", object);
        try {
          assertEquals(object.getResponseUnitsCount(), count);
        } finally {
          latch.countDown();
        }
      }

      @Override
      public void fail(Exception exception) {
        logger.error("caught an exception", exception);
        latch.countDown();
        assertTrue(false);
      }
    };

    PyWriteRequest request = new PyWriteRequest(builder.build(), dataToSend);
    logger.info("send size: {}", request.getMetadata().getSerializedSize());
    client.write(request, callback);
    latch.await();
  }

  private AsyncDataNode.AsyncIface generateClient(EndPoint endPoint) {
    return generateClient(endPoint, 10);
  }

  private AsyncDataNode.AsyncIface generateClient(EndPoint endPoint, int connectionCount) {
    TransferenceConfiguration cfg = GenericAsyncClientFactory.defaultConfiguration();
    cfg.option(TransferenceClientOption.CONNECTION_COUNT_PER_ENDPOINT, connectionCount);
    cfg.option(TransferenceClientOption.IO_TIMEOUT_MS, 100000);
    cfg.option(TransferenceClientOption.MAX_MESSAGE_LENGTH, 1 * 1024 * 1024);
    clientFactory = new GenericAsyncClientFactory<>(AsyncDataNode.AsyncIface.class, protocolFactory,
        cfg);
    clientFactory.setAllocator(allocator);
    clientFactory.init();
    return clientFactory.generate(endPoint);
  }

  private static class ServiceImpl implements AsyncDataNode.AsyncIface {
    public ServiceImpl() {
    }

    @Override
    public void ping(MethodCallback<Object> callback) {
      logger.info("ping is ok");
      callback.complete(null);
    }

    @Override
    public void write(PyWriteRequest writeRequest, MethodCallback<PbWriteResponse> callback) {
      logger.info("receive size: {}", writeRequest.getSerializedSize());
      PbWriteRequest request = writeRequest.getMetadata();

      PbWriteResponse.Builder builder = PbWriteResponse.newBuilder();
      builder.setMembership(generateMembership());
      builder.setRequestId(request.getRequestId());
      List<PbWriteResponseUnit> responseUnits = new ArrayList<>();

      try {
        for (int i = 0; i < request.getRequestUnitsCount(); i++) {
          PbWriteRequestUnit unit = request.getRequestUnits(i);
          ByteBuf buffer = writeRequest.getRequestUnitData(i);

          if (!DataConsistencyUtils.checkDataByOffset(buffer.nioBuffer(), unit.getOffset())) {
            throw new Exception("unit: " + unit + " data not right");
          }

          responseUnits
              .add(PbRequestResponseHelper.buildPbWriteResponseUnitFrom(unit, PbIoUnitResult.OK));
        }
      } catch (Exception e) {
        logger.warn("caught an exception", e);
        callback.fail(e);
      }

      callback.complete(builder.addAllResponseUnits(responseUnits).build());
    }

    @Override
    public void read(PbReadRequest request, MethodCallback<PyReadResponse> callback) {
      logger.info("receive size: {}", request.getSerializedSize());
      PbReadResponse.Builder builder = PbReadResponse.newBuilder();
      builder.setRequestId(request.getRequestId());

      List<PbReadResponseUnit> responseUnits = new ArrayList<>();
      ByteBuf dataToSend = null;
      for (PbReadRequestUnit requestUnit : request.getRequestUnitsList()) {
        byte[] data = DataConsistencyUtils
            .generateDataByOffset(requestUnit.getLength(), requestUnit.getOffset()).array();
        ByteBuf buffer = Unpooled.wrappedBuffer(data);
        responseUnits.add(PbRequestResponseHelper.buildPbReadResponseUnitFrom(requestUnit, buffer));

        dataToSend = (dataToSend == null ? buffer : Unpooled.wrappedBuffer(dataToSend, buffer));
      }

      builder.addAllResponseUnits(responseUnits);
      callback.complete(new PyReadResponse(builder.build(), dataToSend));
    }

    @Override
    public void copy(PyCopyPageRequest request, MethodCallback<PbCopyPageResponse> callback) {
    }

    @Override
    public void check(Broadcastlog.PbCheckRequest request,
        MethodCallback<Broadcastlog.PbCheckResponse> callback) {
    }

    @Override
    public void giveYouLogId(Broadcastlog.GiveYouLogIdRequest request,
        MethodCallback<Broadcastlog.GiveYouLogIdResponse> callback) {
    }

    @Override
    public void getMembership(Broadcastlog.PbGetMembershipRequest request,
        MethodCallback<Broadcastlog.PbGetMembershipResponse> callback) {
    }

    @Override
    public void addOrCommitLogs(Commitlog.PbCommitlogRequest request,
        MethodCallback<Commitlog.PbCommitlogResponse> callback) {
    }

    @Override
    public void discard(PbWriteRequest request, MethodCallback<PbWriteResponse> callback) {
    }

    @Override
    public void startOnlineMigration(Commitlog.PbStartOnlineMigrationRequest request,
        MethodCallback<Commitlog.PbStartOnlineMigrationResponse> callback) {
    }

    @Override
    public void syncLog(PbAsyncSyncLogsBatchRequest request,
        MethodCallback<PbAsyncSyncLogsBatchResponse> callback) {
    }

    @Override
    public void backwardSyncLog(PbBackwardSyncLogsRequest request,
        MethodCallback<PbBackwardSyncLogsResponse> callback) {
    }
  }
}
