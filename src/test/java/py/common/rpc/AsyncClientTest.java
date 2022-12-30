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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContextImpl;
import py.app.thrift.ThriftAppEngine;
import py.app.thrift.ThriftProcessorFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.instance.PortType;
import py.test.DummyTestServiceAbstract;
import py.test.TestBase;
import py.thrift.testing.service.DummyTestService;
import py.thrift.testing.service.DummyTestService.AsyncClient.ping_call;
import py.thrift.testing.service.DummyTestService.AsyncIface;
import py.thrift.testing.service.PingRequest;
import py.thrift.testing.service.TestInternalErrorThrift;

public class AsyncClientTest extends TestBase {
  private static final int SERVER_LISTEN_PORT = 34344;
  private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;
  private static final Random random = new Random(System.currentTimeMillis());
  private static Logger logger = LoggerFactory.getLogger(AsyncClientTest.class);
  private final List<Exception> exceptions = Collections
      .synchronizedList(new ArrayList<Exception>());
  private final AtomicLong atomicCount = new AtomicLong(0);
  private ThriftAppEngine engine;

  // align with 8 bytes

  public static byte[] getData(int size) {
    int longCount = size / 8;
    if (longCount == 0) {
      longCount = 1;
    }

    ByteBuffer buffer = ByteBuffer.allocate(longCount * 8);
    for (int i = 0; i < longCount; i++) {
      buffer.putLong(random.nextLong());
    }

    return buffer.array();
  }

  public static String bytesTolongsString(byte[] buffer) {
    // convert byte array to string
    ByteBuffer temp = ByteBuffer.wrap(buffer, 0, buffer.length);
    String str = "";
    for (int i = 0; i < buffer.length / 8; i++) {
      str += Long.toString(temp.getLong());
      str += "=";
    }

    return str;
  }

  public static boolean checkData(String str, byte[] bytes) {
    // converting string to long string array which is split by -
    String[] strLongs = str.split("=");
    long[] longs = new long[strLongs.length];
    for (int i = 0; i < strLongs.length; i++) {
      if (!strLongs[i].isEmpty()) {
        longs[i] = Long.valueOf(strLongs[i]);
      }
    }

    // converting string long array and converting byte array to long array, then comparing the two
    // long array
    Assert.assertTrue("bytes contains the count of long: " + bytes.length / 8
        + ", longs: " + longs.length, bytes.length / 8 == longs.length);
    ByteBuffer temp = ByteBuffer.wrap(bytes);
    for (int i = 0; i < longs.length; i++) {
      if (temp.getLong() != longs[i]) {
        return false;
      }
    }

    return true;
  }

  @Before
  public void init() throws Exception {
    super.init();
    atomicCount.set(0);
    exceptions.clear();
  }

  @After
  public void close() throws Exception {
    if (engine != null) {
      engine.stop();
      engine = null;
    }
    atomicCount.set(0);
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testReflectionNewInstance() throws Exception {
    Constructor constructor = null;
    try {
      constructor = DummyTestService.AsyncClient.class.getDeclaredConstructor(
          org.apache.thrift.protocol.TProtocolFactory.class,
          org.apache.thrift.async.TAsyncClientManager.class,
          org.apache.thrift.transport.TNonblockingTransport.class);
    } catch (NoSuchMethodException | SecurityException e) {
      e.printStackTrace();
    }

    Object[] intArgs = new Object[]{null, null, null};

    try {
      DummyTestService.AsyncClient client = (DummyTestService.AsyncClient) constructor
          .newInstance(intArgs);
      Assert.assertNotNull(client);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testReflectionModifyFinalVariable() throws Exception {
    TestReflection testReflection = new TestReflection();
    Field field = TestReflection.class.getDeclaredField("value");
    field.setAccessible(true);
    field.set(testReflection, new Integer(100));
    Assert.assertTrue(testReflection.getValue() == 100);
  }

  @Test
  public void testClientTimeOut() throws Exception {
    final long timeout = 2000;
    TProcessor processor = new DummyTestService.Processor<>(new DummyTestServiceAbstract() {
      @Override
      public String ping(PingRequest request) throws TException {
        try {
          Thread.sleep(timeout);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return "i am coming";
      }
    });

    engine = makeServerEngine(processor, SERVER_LISTEN_PORT);

    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    final GenericThriftClientFactory<DummyTestService.AsyncIface> factory =
        GenericThriftClientFactory.create(DummyTestService.AsyncIface.class);
    factory.setChannelCountToEndPoint(endPoint, 1);

    long requestId = 0;
    DummyTestService.AsyncIface client = (DummyTestService.AsyncIface) factory
        .generateAsyncClient(endPoint, 1000);

    PingRequest request = generatePingRequest(requestId++, DEFAULT_BUFFER_SIZE);
    CountDownLatch latch = new CountDownLatch(1);
    AsyncMethodCallback<ping_call> callback = buildCallback(request.getRequestId(), latch,
        request.getData());

    client.ping(request, callback);
    latch.await();

    Assert.assertTrue(!exceptions.isEmpty());
    try {
      throw exceptions.get(0);
    } catch (TTransportException e) {
      Assert.assertTrue(e.getType() == TTransportException.TIMED_OUT);
    }
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

    GenericThriftClientFactory<DummyTestService.AsyncIface> factory = GenericThriftClientFactory
        .create(DummyTestService.AsyncIface.class);
    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    factory.setChannelCountToEndPoint(endPoint, 1);
    DummyTestService.AsyncIface client = null;
    long requestId = 0;
    try {
      client = (DummyTestService.AsyncIface) factory.generateAsyncClient(endPoint, 10000);
      testPing(client, requestId++, DEFAULT_BUFFER_SIZE);
    } catch (TException e) {
      logger.error("timeout:", e);
      Assert.assertTrue(false);
    }

    Assert.assertTrue("can not catch a TException", !exceptions.isEmpty());
    Assert.assertTrue(exceptions.get(0) instanceof TException);
    exceptions.clear();

    // then i will send again with increasing timeout
    try {
      testPing(client, requestId++, DEFAULT_BUFFER_SIZE);
    } catch (Exception e) {
      logger.error("timeout:", e);
      Assert.assertTrue(false);
    }

    Assert.assertTrue("can not catch a TException", !exceptions.isEmpty());
    exceptions.clear();

    try {
      testPing(client, requestId++, DEFAULT_BUFFER_SIZE);
    } catch (Exception e) {
      logger.error("timeout:", e);
      Assert.assertTrue(false);
    }
    // should catch a TestInternalErrorThrift
    Assert.assertTrue("can not catch a TException", !exceptions.isEmpty());
    Assert.assertTrue(exceptions.get(0) instanceof TestInternalErrorThrift);

    exceptions.clear();
    factory.close();
  }

  @Test
  public void testWithNoServer() throws Exception {
    final GenericThriftClientFactory<AsyncIface> factory = GenericThriftClientFactory
        .create(DummyTestService.AsyncIface.class);

    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    Exception exception = null;
    try {
      factory.generateAsyncClient(endPoint, 2000);
    } catch (Exception e) {
      logger.error("caught an Exception", e);
      exception = e;
    }

    Assert.assertNotNull(exception);
    factory.close();
  }

  @Test
  public void testWithServerCorrupted() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);

    final GenericThriftClientFactory<DummyTestService.AsyncIface> factory =
        GenericThriftClientFactory.create(DummyTestService.AsyncIface.class);

    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    factory.setChannelCountToEndPoint(endPoint, 1);
    DummyTestService.AsyncIface client = (DummyTestService.AsyncIface) factory
        .generateAsyncClient(endPoint, 10000);

    testPing(client, 0L, DEFAULT_BUFFER_SIZE);

    engine.stop();
    engine = null;

    try {
      testPing(client, 1L, DEFAULT_BUFFER_SIZE);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      exceptions.add(e);
    }

    Assert.assertFalse(exceptions.isEmpty());
    factory.close();
  }

  @Test
  public void testSendWithOneClientObject() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);

    final GenericThriftClientFactory<DummyTestService.AsyncIface> factory =
        GenericThriftClientFactory.create(DummyTestService.AsyncIface.class);

    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    factory.setChannelCountToEndPoint(endPoint, 1);
    DummyTestService.AsyncIface client = (DummyTestService.AsyncIface) factory
        .generateAsyncClient(endPoint, 2000);
    int requestId = 0;
    int maxTimes = 1000;
    int times = maxTimes;
    while (times-- > 0) {
      try {
        testPing(client, requestId++, 1024);
        if (!exceptions.isEmpty()) {
          break;
        }
      } catch (Exception e) {
        logger.error("caught an exception", e);
      }
    }

    if (!exceptions.isEmpty()) {
      logger.error("caught an exception", exceptions.get(0));
      Assert.assertTrue(false);
    }

    Assert.assertEquals(atomicCount.get(), maxTimes);
    factory.close();
  }

  /**
   * add a method for manually test service return a big package.
   */
  public void testSendWithOneClientObjectForRead() throws Exception {
    final GenericThriftClientFactory<DummyTestService.AsyncIface> factory =
        GenericThriftClientFactory.create(DummyTestService.AsyncIface.class);

    final EndPoint endPoint = new EndPoint("10.0.1.204", 33333);
    factory.setChannelCountToEndPoint(endPoint, 1);
    DummyTestService.AsyncIface client = (DummyTestService.AsyncIface) factory
        .generateAsyncClient(endPoint, 3000);

    int requestId = 0;
    int maxTimes = 50000;
    int times = maxTimes;
    while (times-- > 0) {
      try {
        PingRequest pingRequest = generatePingRequest(requestId++, 8);
        AsyncMethodCallback<ping_call> callback = buildCallback(pingRequest.getRequestId(), null,
            pingRequest.getData());
        client.ping(pingRequest, callback);
        if (times % 100 == 0) {
          Thread.sleep(1);
        }
      } catch (Exception e) {
        logger.error("caught an exception", e);
      }
    }

    long startTime = System.currentTimeMillis();
    long endTime = startTime;
    while (endTime - startTime < 4000) {
      if (atomicCount.get() == maxTimes) {
        break;
      }
      Thread.sleep(200);
      endTime = System.currentTimeMillis();
    }

    Assert.assertEquals(atomicCount.get(), maxTimes);
    Assert.assertTrue(exceptions.isEmpty());
    factory.close();
  }

  @Test
  public void testClientAndServerSingleThread() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);
    GenericThriftClientFactory<DummyTestService.AsyncIface> factory = GenericThriftClientFactory
        .create(DummyTestService.AsyncIface.class);

    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    final int bufferSize = 256;
    long requestId = 0;
    final int times = 100000;
    int leftTimes = times;
    while (leftTimes-- > 0) {
      try {
        DummyTestService.AsyncIface client = (DummyTestService.AsyncIface) factory
            .generateAsyncClient(
                endPoint, 10000);
        testPing(client, requestId++, bufferSize);
        if (!exceptions.isEmpty()) {
          break;
        }
      } catch (Exception e) {
        exceptions.add(e);
        break;
      }
    }

    if (!exceptions.isEmpty()) {
      logger.error("caught an exception", exceptions.get(0));
      Assert.assertTrue(false);
    }

    Assert.assertEquals(atomicCount.get(), times);
    factory.close();
  }

  @Test
  public void testClientAndServerMultiThread() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);
    final GenericThriftClientFactory<DummyTestService.AsyncIface> factory =
        GenericThriftClientFactory.create(DummyTestService.AsyncIface.class);

    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    factory.setChannelCountToEndPoint(endPoint, 4);

    final int bufferSize = 128;
    final AtomicLong requestId = new AtomicLong(0);
    int threadCount = 8;
    final CountDownLatch mainLatch = new CountDownLatch(threadCount);
    final long maxTimes = 100000;

    for (int i = 0; i < threadCount; i++) {
      Thread thread = new Thread() {
        public void run() {
          while (requestId.getAndIncrement() < maxTimes) {
            try {
              AsyncIface client = (DummyTestService.AsyncIface) factory
                  .generateAsyncClient(endPoint,
                      10000);
              testPing(client, requestId.get(), bufferSize);
              if (!exceptions.isEmpty()) {
                break;
              }
            } catch (Exception e) {
              logger.info("caught an Exception", e);
              exceptions.add(e);
              break;
            }
          }
          mainLatch.countDown();
        }
      };
      thread.start();
    }

    mainLatch.await();
    if (!exceptions.isEmpty()) {
      logger.error("caught an exception : count" + exceptions.size(), exceptions.get(0));
      Assert.assertTrue(false);
    }

    Assert.assertEquals(atomicCount.get(), maxTimes);
    factory.close();
  }

  @Test
  public void testMultiChanelsToEndPoint() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);
    GenericThriftClientFactory<DummyTestService.AsyncIface> factory = GenericThriftClientFactory
        .create(DummyTestService.AsyncIface.class);

    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);
    int channelCount = 50;
    factory.setChannelCountToEndPoint(endPoint, channelCount);

    for (int i = 0; i < channelCount; i++) {
      factory.generateAsyncClient(endPoint, 2000);
      Assert.assertEquals(factory.getCurrentChannelCount(endPoint), i + 1);
    }

    factory.close();
  }

  //TODO: @Test

  public void testMultiChannelsWithOneServer() throws Exception {
    engine = makeServerEngine(null, SERVER_LISTEN_PORT);
    final GenericThriftClientFactory<DummyTestService.AsyncIface> factory =
        GenericThriftClientFactory.create(DummyTestService.AsyncIface.class);
    final EndPoint endPoint = new EndPoint("localhost", SERVER_LISTEN_PORT);

    final int bufferSize = 64;
    int threadCount = 100;
    int channelCount = 4;
    factory.setChannelCountToEndPoint(endPoint, channelCount);

    final CountDownLatch latchMain = new CountDownLatch(threadCount);
    final AtomicLong requestId = new AtomicLong(0);
    final long maxTimes = 100000L;
    final AtomicInteger clientExceptionCount = new AtomicInteger(0);
    final AtomicInteger serverExceptionCount = new AtomicInteger(0);

    for (int i = 0; i < threadCount; i++) {
      Thread thread = new Thread() {
        public void run() {
          while (requestId.getAndIncrement() < maxTimes) {
            try {
              DummyTestService.AsyncIface client = (AsyncIface) factory
                  .generateAsyncClient(endPoint,
                      10000);
              testPing(client, requestId.get(), bufferSize);
              if (!exceptions.isEmpty()) {
                break;
              }
            } catch (Exception e) {
              if (e.getCause() instanceof RejectedExecutionException) {
                clientExceptionCount.incrementAndGet();
              } else if (e instanceof TApplicationException) {
                serverExceptionCount.incrementAndGet();
              } else {
                logger.info("catch an exception", e);
                exceptions.add(e);
                break;
              }
            }
          }

          latchMain.countDown();
        }
      };
      thread.start();
    }

    latchMain.await();
    Assert.assertTrue("caught an exception" + exceptions.toString(), exceptions.isEmpty());
    Assert.assertEquals(atomicCount.get(), maxTimes);
    factory.close();
  }

  public AsyncMethodCallback<ping_call> buildCallback(final long sequenceId,
      final CountDownLatch latch,
      final byte[] buffer) {
    AsyncMethodCallback<ping_call> callback = new AsyncMethodCallback<ping_call>() {
      @Override
      public void onError(Exception exception) {
        exceptions.add(exception);
        atomicCount.incrementAndGet();
        logger.error("caught an exception", exception);
        if (latch != null) {
          latch.countDown();
        }
      }

      @Override
      public void onComplete(ping_call response) {
        try {
          String out = response.getResult();
          atomicCount.incrementAndGet();
          Assert.assertTrue(checkData(out, buffer));
        } catch (Exception e) {
          logger.error("caught an exception", e);
          exceptions.add(e);
        } finally {
          if (latch != null) {
            latch.countDown();
          }
        }
      }
    };

    return callback;
  }

  private PingRequest generatePingRequest(long requestId, int bufferSize)
      throws UnsupportedEncodingException {
    PingRequest request = new PingRequest();
    request.setRequestId(requestId);
    request.setData(getData(bufferSize));
    return request;
  }

  private void testPing(DummyTestService.AsyncIface client, long requestId, int bufferSize)
      throws Exception {
    PingRequest request = generatePingRequest(requestId, bufferSize);
    CountDownLatch latch = new CountDownLatch(1);
    AsyncMethodCallback<ping_call> callback = buildCallback(request.getRequestId(), latch,
        request.getData());
    client.ping(request, callback);
    latch.await();
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
                return bytesTolongsString(data);
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

  public static class TestReflection {
    private final Integer value = 1;

    public Integer getValue() {
      return value;
    }
  }
}
