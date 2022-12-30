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

package py.common.rpc;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import py.app.context.AppContextImpl;
import py.app.thrift.ThriftAppEngine;
import py.app.thrift.ThriftProcessorFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.NamedThreadFactory;
import py.common.rpc.server.NettyServerConfig;
import py.common.rpc.server.NettyServerTransport;
import py.common.rpc.server.ThriftServerDef;
import py.common.rpc.server.ThriftServerDefBuilder;
import py.common.rpc.share.TransDuplexProtocolFactory;
import py.common.struct.EndPoint;
import py.instance.PortType;
import py.test.DummyTestServiceAbstract;
import py.test.TestBase;
import py.thrift.infocenter.service.ReserveVolumeRequest;
import py.thrift.infocenter.service.ReserveVolumeResponse;
import py.thrift.share.ArchiveMetadataThrift;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.thrift.testing.service.DummyTestService;
import py.thrift.testing.service.DummyTestService.Iface;
import py.thrift.testing.service.PingRequest;
import py.thrift.testing.service.TestInternalErrorThrift;

public class SyncClientTest extends TestBase {
  private static final Logger logger = LoggerFactory.getLogger(SyncClientTest.class);
  private static final int SERVER_LISTEN_PORT = 34343;
  private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;
  private ThriftAppEngine engine = null;
  private NettyServerTransport server = null;

  @Before
  public void init() throws Exception {
    super.init();
  }

  @After
  public void close() throws Exception {
    if (engine != null) {
      engine.stop();
      engine = null;
    }

    if (server != null) {
      server.stop();
      server = null;
    }
  }

  @Test
  public void testClientTimeout() throws Exception {
    final int sleepTime = 2000;
    TProcessor processor = new DummyTestService.Processor<>(new DummyTestServiceAbstract() {
      @Override
      public String ping(PingRequest request) throws TException, TestInternalErrorThrift {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        byte[] data = request.getData();
        if (data == null) {
          return "i am coming";
        } else {
          return AsyncClientTest.bytesTolongsString(data);
        }
      }
    });

    engine = makeServerEngine(processor, SERVER_LISTEN_PORT);

    Exception exception = null;
    GenericThriftClientFactory<DummyTestService.Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class);
    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    factory.setChannelCountToEndPoint(endPoint, 1);
    int bufferSize = 1024;
    try {
      DummyTestService.Iface client = (DummyTestService.Iface) factory
          .generateSyncClient(endPoint, 1000);
      testPing(client, 0, bufferSize);
    } catch (TTransportException e) {
      Assert.assertTrue(e.getType() == TTransportException.TIMED_OUT);
      exception = e;
    } catch (TException e) {
      logger.error("timeout:", e);
    }

    Assert.assertNotNull(exception);
    exception = null;

    // then i will send again with increasing timeout
    try {
      DummyTestService.Iface client = (DummyTestService.Iface) factory
          .generateSyncClient(endPoint, 10000);
      testPing(client, 0, bufferSize);
    } catch (Exception e) {
      logger.error("timeout:", e);
      exception = e;
    }

    Assert.assertNull(exception);
    factory.close();
  }

  @Test
  public void testServerException() throws Exception {
    final AtomicInteger changeInteger = new AtomicInteger(0);

    TProcessor processor = new DummyTestService.Processor<>(new DummyTestServiceAbstract() {
      @Override
      public String ping(PingRequest request) throws TException, TestInternalErrorThrift {
        int index = changeInteger.getAndIncrement();
        System.out.println("index: " + index);
        if (index == 0) {
          throw new TException("i am coming");
        } else if (index == 1) {
          throw new RuntimeException("i am coming");
        } else if (index == 2) {
          throw new TestInternalErrorThrift();
        } else {
          return "i am coming";
        }
      }
    });

    engine = makeServerEngine(processor, SERVER_LISTEN_PORT);

    Exception exception = null;
    GenericThriftClientFactory<DummyTestService.Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class);
    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    long requestId = 0;
    factory.setChannelCountToEndPoint(endPoint, 1);
    DummyTestService.Iface client = null;
    try {
      client = (DummyTestService.Iface) factory.generateSyncClient(endPoint, 300000);
      testPing(client, requestId++, DEFAULT_BUFFER_SIZE);
    } catch (TException e) {
      logger.error("timeout:", e);
      exception = e;
    }

    Assert.assertNotNull(exception);
    Assert.assertTrue(exception instanceof TException);
    exception = null;

    // then i will send again with increasing timeout
    try {
      testPing(client, requestId++, DEFAULT_BUFFER_SIZE);
    } catch (Exception e) {
      logger.error("timeout:", e);
      exception = e;
    }

    Assert.assertNotNull(exception);
    exception = null;

    try {
      String str = testPing(client, requestId++, DEFAULT_BUFFER_SIZE);
      logger.info("response: {}", str);
    } catch (TestInternalErrorThrift e) {
      logger.error("timeout:", e);
      exception = e;
    } catch (Exception e) {
      logger.error("timeout:", e);
    }

    Assert.assertNotNull(exception);
    Assert.assertTrue(exception instanceof TestInternalErrorThrift);
    factory.close();
  }

  @Test
  public void testNetWorkBuffer() throws Exception {
    final Integer segmentCount = 5000;
    final Integer groupCount = 3;
    TProcessor process = new DummyTestService.Processor<>(new DummyTestServiceAbstract() {
      @Override
      public ReserveVolumeResponse reserveVolume(ReserveVolumeRequest request)
          throws TestInternalErrorThrift,
          TException {
        logger.info("reserveVolume successfully");

        Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
            mapIntegerToInstances = new HashMap<>();
        for (int i = 0; i < segmentCount.intValue(); i++) {
          Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>
              unitType2InstancesMap = new HashMap<>();
          List<InstanceIdAndEndPointThrift> instances = new ArrayList<>();
          unitType2InstancesMap.put(SegmentUnitTypeThrift.Normal, instances);
          for (int j = 0; j < groupCount.intValue(); j++) {
            List<ArchiveMetadataThrift> archives = new ArrayList<ArchiveMetadataThrift>();
            InstanceIdAndEndPointThrift instanceIdAndEndPoint = new InstanceIdAndEndPointThrift(i,
                new EndPoint("10.0.1.127", 8080).toString());
            instances.add(instanceIdAndEndPoint);
          }
          mapIntegerToInstances.put(i, unitType2InstancesMap);
        }
        return new ReserveVolumeResponse(0, mapIntegerToInstances);
      }
    });

    engine = makeServerEngine(process, SERVER_LISTEN_PORT);

    GenericThriftClientFactory<Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class);
    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    try {
      DummyTestService.Iface client = (DummyTestService.Iface) factory
          .generateSyncClient(endPoint, 20000);
      client.reserveVolume(new ReserveVolumeRequest());
      logger.info("ok response");
    } catch (Exception e) {
      logger.error("caught an exception", e);
      Assert.assertTrue(false);
    }
    factory.close();
  }

  @Test
  public void testSyncClient() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);
    GenericThriftClientFactory<Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class);
    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    Exception exception = null;

    long startTime = System.currentTimeMillis();
    long endTime = startTime;
    long requestId = 0;

    while (endTime - startTime < 3000) {
      try {
        DummyTestService.Iface client = (DummyTestService.Iface) factory
            .generateSyncClient(endPoint, 10000);
        testPing(client, requestId++, 1024);
        endTime = System.currentTimeMillis();
      } catch (Exception e) {
        logger.error("caught an exception", e);
        exception = e;
        break;
      }
    }

    logger.info("testSyncClient successfully send the number of package: {}", requestId);
    Assert.assertNull(exception);
    factory.close();
  }

  @Test
  public void testSendWithOneClient() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);

    final GenericThriftClientFactory<Iface> factory = GenericThriftClientFactory.create(
        DummyTestService.Iface.class, 1);
    EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    Iface client = (DummyTestService.Iface) factory.generateSyncClient(endPoint, 5000);

    int bufferSize = 1024;
    int requestId = 0;
    int count = 50;
    while (count-- > 0) {
      try {
        testPing(client, requestId++, bufferSize);
      } catch (Exception e) {
        logger.error("caught an exception", e);
        Assert.assertTrue(false);
      }
    }

    logger.info("testSendWithOneClient successfully send the number of package: {}", requestId);
    factory.close();
  }

  @Test
  public void testReconnectionWithPostData() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);

    final GenericThriftClientFactory<Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class);

    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    int bufferSize = 1024;
    int requestId = 0;

    // send data to server,
    try {
      Iface client = (DummyTestService.Iface) factory.generateSyncClient(endPoint, 5000);
      String str = testPing(client, requestId++, bufferSize);
      logger.info("1 response: {}", str);
    } catch (Exception e) {
      Assert.assertTrue(false);
    }

    engine.stop();
    Exception exception = null;
    try {
      Iface client = (DummyTestService.Iface) factory.generateSyncClient(endPoint, 5000);
      String str = testPing(client, requestId++, bufferSize);
      logger.info("2 response: {}", str);
    } catch (Exception e) {
      exception = e;
    }

    Assert.assertTrue(exception != null);
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);
    Thread.sleep(3000);
    try {
      Iface client = (DummyTestService.Iface) factory.generateSyncClient(endPoint, 5000);
      String str = testPing(client, requestId++, bufferSize);
      logger.info("3 response: {}", str);
    } catch (Exception e) {
      Assert.assertTrue(false);
    }

    factory.close();
  }

  @Test
  public void testMultiSyncClient() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);

    final GenericThriftClientFactory<Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class);

    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    factory.setChannelCountToEndPoint(endPoint, 1);

    int threadCount = 10;
    final int bufferSize = 256;
    final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

    final CountDownLatch latch = new CountDownLatch(threadCount);
    final AtomicLong requestId = new AtomicLong(0);
    final AtomicLong packages = new AtomicLong(0);

    final long totalPackageCounts = 10000;
    for (int i = 0; i < threadCount; i++) {
      Thread thread = new Thread() {
        public void run() {
          try {
            while (requestId.incrementAndGet() <= totalPackageCounts) {
              Iface client = (DummyTestService.Iface) factory.generateSyncClient(endPoint, 5000);
              testPing(client, requestId.get(), bufferSize);
              packages.incrementAndGet();
            }
          } catch (Exception e) {
            logger.error("caught an exception", e);
            exceptions.add(e);
          } finally {
            latch.countDown();
          }
        }
      };
      thread.start();
    }

    latch.await();
    logger.info("testMultiSyncClient successfully send the number of package: {}", packages.get());
    Assert.assertTrue(exceptions.isEmpty());
    Assert.assertEquals(packages.get(), totalPackageCounts);
    factory.close();
  }

  // TODO: @Test

  public void testMultiChannelsWithOneServer() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);
    final GenericThriftClientFactory<Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class);
    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    final List<Exception> exceptions = new ArrayList<Exception>();

    int channelCount = 4;
    factory.setChannelCountToEndPoint(endPoint, channelCount);

    int threadCount = 100;
    final CountDownLatch latch = new CountDownLatch(threadCount);
    final AtomicLong requestId = new AtomicLong(0);
    final AtomicLong packages = new AtomicLong(0);
    final int bufferSize = 1024;

    final long totalPackageCount = 1000;
    final AtomicInteger clientExceptionCount = new AtomicInteger(0);
    final AtomicInteger serverExceptionCount = new AtomicInteger(0);

    for (int i = 0; i < threadCount; i++) {
      Thread thread = new Thread() {
        public void run() {
          long currentIndex = 0;
          while ((currentIndex = requestId.incrementAndGet()) <= totalPackageCount) {
            try {
              Iface client = (DummyTestService.Iface) factory.generateSyncClient(endPoint, 1000);
              testPing(client, currentIndex, bufferSize);
              packages.incrementAndGet();
            } catch (Exception e) {
              if (e.getCause() instanceof RejectedExecutionException) {
                clientExceptionCount.incrementAndGet();
              } else if (e instanceof TApplicationException) {
                serverExceptionCount.incrementAndGet();
              } else {
                logger.error("catch an exception, index: {}", currentIndex, e);
                exceptions.add(e);
                break;
              }
            }
          }
          logger.info("drop thread");
          latch.countDown();
        }
      };
      thread.start();
    }

    latch.await();
    logger.info(
        "testMultiChannelsWithOneServer successfully send the number of package: {},"
            + " server exception "
            + "count: {}, client exception count: {}, other exception count: {}", packages.get(),
        serverExceptionCount.get(), clientExceptionCount.get(), exceptions.size());
    if (!exceptions.isEmpty()) {
      logger.info("exception information", exceptions.get(0));
    }
    Assert.assertTrue(exceptions.isEmpty());
    Assert.assertEquals(packages.get() + serverExceptionCount.get() + clientExceptionCount.get(),
        totalPackageCount);
    factory.close();
  }

  @Test
  public void testMultiChanelsToEndPoint() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);
    GenericThriftClientFactory<DummyTestService.Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class);

    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    int channelCount = 50;
    factory.setChannelCountToEndPoint(endPoint, channelCount);

    for (int i = 0; i < channelCount; i++) {
      factory.generateSyncClient(endPoint, 2000);
      Assert.assertEquals(factory.getCurrentChannelCount(endPoint), i + 1);
    }

    factory.close();
  }

  @Test
  public void testSendingWithServerCorrupted() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);

    final GenericThriftClientFactory<DummyTestService.Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class);
    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    factory.setChannelCountToEndPoint(endPoint, 2);

    final AtomicLong sequenceId = new AtomicLong();
    final List<Exception> exceptions = new ArrayList<Exception>();
    final CountDownLatch latch = new CountDownLatch(1);
    Thread thread = new Thread() {
      public void run() {
        try {
          while (true) {
            Iface client = (DummyTestService.Iface) factory.generateSyncClient(endPoint, 2000);
            testPing(client, sequenceId.get(), DEFAULT_BUFFER_SIZE);
            Thread.sleep(10);
          }
        } catch (Exception e) {
          logger.error("caught an exception", e);
          exceptions.add(e);
          latch.countDown();
        }
      }
    };

    thread.start();

    // send some data
    Thread.sleep(2000);
    logger.info("testSendingWithServerCorrupted successfully send the number of package: {}",
        sequenceId.get());
    Assert.assertTrue(exceptions.isEmpty());

    // stop server then ,the client will send failure
    engine.stop();

    latch.await(2000, TimeUnit.MILLISECONDS);
    Assert.assertFalse(exceptions.isEmpty());

    factory.close();
  }

  @Test
  public void testClientAndServerWithCompactProtocol() throws Exception {
    ThriftServerDefBuilder thriftServerDefBuilder = ThriftServerDef.newBuilder();
    thriftServerDefBuilder
        .protocol(TransDuplexProtocolFactory.fromSingleFactory(new TBinaryProtocol.Factory()));
    thriftServerDefBuilder.listen(SERVER_LISTEN_PORT);
    thriftServerDefBuilder.hostname("localhost");
    thriftServerDefBuilder.using(new ThreadPoolExecutor(1, 5, 60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), new NamedThreadFactory("response-worker-")));
    thriftServerDefBuilder
        .withProcessor(new DummyTestService.Processor<>(new DummyTestServiceAbstract() {
          @Override
          public String ping(PingRequest request) throws TException {
            byte[] data = request.getData();
            if (data == null) {
              return "i am coming";
            } else {
              return AsyncClientTest.bytesTolongsString(data);
            }
          }
        }));

    server = new NettyServerTransport(thriftServerDefBuilder.build(),
        NettyServerConfig.newBuilder().build(),
        new DefaultChannelGroup());

    server.start();

    GenericThriftClientFactory<Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class,
            new TBinaryProtocol.Factory(), 1, 0);
    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    Exception exception = null;

    long startTime = System.currentTimeMillis();
    long endTime = startTime;
    int bufferSize = 1024;
    AtomicLong requestId = new AtomicLong(0);

    while (endTime - startTime < 3000) {
      try {
        DummyTestService.Iface client = (DummyTestService.Iface) factory
            .generateSyncClient(endPoint, 10000);
        testPing(client, requestId.getAndIncrement(), bufferSize);
        endTime = System.currentTimeMillis();
        Thread.sleep(5);
      } catch (Exception e) {
        logger.error("caught an exception", e);
        exception = e;
        break;
      }
    }

    logger
        .info("testClientAndServerWithCompactProtocol successfully send the number of package: {}",
            requestId.get());
    Assert.assertNull(exception);
    factory.close();
  }

  private PingRequest generatePingRequest(long requestId, int bufferSize)
      throws UnsupportedEncodingException {
    PingRequest request = new PingRequest();
    request.setRequestId(requestId);
    request.setData(AsyncClientTest.getData(bufferSize));
    return request;
  }

  private String testPing(DummyTestService.Iface client, long requestId, int bufferSize)
      throws Exception {
    PingRequest pingRequest = generatePingRequest(requestId, bufferSize);
    String str = client.ping(pingRequest);
    AsyncClientTest.checkData(str, pingRequest.getData());
    return str;
  }

  private ThriftAppEngine makeServerEngine(final TProcessor process, int port) throws Exception {
    ThriftAppEngine engine = null;
    if (process != null) {
      engine = new ThriftAppEngine(new ThriftProcessorFactory() {
        @Override
        public TProcessor getProcessor() {
          return process;
        }
      });
    } else {
      engine = new ThriftAppEngine(new ThriftProcessorFactory() {
        @Override
        public TProcessor getProcessor() {
          return new DummyTestService.Processor<>(new DummyTestServiceAbstract() {
            @Override
            public String ping(PingRequest request) throws TException {
              byte[] data = request.getData();
              if (data == null) {
                return "i am coming";
              } else {
                return AsyncClientTest.bytesTolongsString(data);
              }
            }
          });
        }
      });
    }

    AppContextImpl appContext = new AppContextImpl("test-async");
    appContext.putEndPoint(PortType.CONTROL, new EndPoint("localhost", port));
    engine.setContext(appContext);

    engine.start();
    return engine;
  }
}
